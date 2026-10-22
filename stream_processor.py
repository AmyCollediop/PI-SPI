"""
stream_processor.py  v4.1.0
============================
Correction du MinIO writer :
  - Suppression de save_to_minio() (1 fichier/tx → crash + surcharge)
  - Remplacement par MinIOWriter avec buffer + flush batch (500 tx ou 30s)
  - Ajout de l'import time manquant
    - Log du total de tx écrites dans MinIO
Amélioration de la robustesse du push vers Feast :
  - Ajout d'une vérification des colonnes attendues avant le push
    - Log des erreurs de push sans faire planter le process
Règles métier :
  - Ajout de 2 règles supplémentaires : RTP_BURST et NEW_DEVICE
    - RTP_BURST : transactions RTP avec plus de 5 tx dans la dernière minute
    - NEW_DEVICE : transaction avec un nouveau device et montant > 50k
"""

from __future__ import annotations

import io
import json
import logging
import math
import signal
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Tuple

import numpy as np
import pandas as pd
from confluent_kafka import Consumer, Producer
from feast import FeatureStore
from minio import Minio
from minio.error import S3Error

# ════════════════════════════════════════════════════════════════
# CONFIG
# ════════════════════════════════════════════════════════════════

KAFKA_BOOTSTRAP      = "localhost:9092"
TOPIC_INPUT          = "pispi_transactions"
TOPIC_ALERT          = "fraud-alerts"
TOPIC_DLQ            = "txn-dead-letter"
FEAST_REPO           = "feast_repo"

MINIO_ENDPOINT       = "localhost:9000"
MINIO_ACCESS         = "minioadmin"
MINIO_SECRET         = "minioadmin"
MINIO_BUCKET         = "pispi-raw"
MINIO_BATCH_SIZE     = 500    # nb de tx par fichier JSON
MINIO_FLUSH_INTERVAL = 30     # flush forcé toutes les 30 secondes

# ════════════════════════════════════════════════════════════════
# LOGGING
# ════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
logger = logging.getLogger("stream_processor")

# ════════════════════════════════════════════════════════════════
# ÉTAT EN MÉMOIRE
# ════════════════════════════════════════════════════════════════
# Historique des transactions par alias, limité aux 500 dernières pour éviter une croissance infinie de la mémoire.
# Chaque transaction est un dict avec les champs nécessaires au calcul des features et à la détection de fraude.
state: Dict[str, Deque[Dict[str, Any]]] = defaultdict(lambda: deque(maxlen=500))

# ════════════════════════════════════════════════════════════════
# MINIO WRITER  (buffer + flush batch)
# ════════════════════════════════════════════════════════════════
# MinIOWriter gère un buffer en mémoire des transactions à écrire, 
# et effectue un flush vers MinIO soit lorsque le buffer atteint une taille définie (MINIO_BATCH_SIZE), 
# soit lorsqu'un certain temps s'est écoulé depuis le dernier flush (MINIO_FLUSH_INTERVAL).
#  Les fichiers sont écrits au format JSON avec un objet par ligne, dans un chemin structuré par date/heure 
# pour faciliter l'organisation et la récupération ultérieure.
class MinIOWriter:

    def __init__(self, client, bucket, batch_size, flush_interval):
        self._client = client
        self._bucket = bucket
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._buffer = []
        self._last_flush = time.monotonic()
        self._total_written = 0

    def add(self, tx):
        self._buffer.append(tx)

        elapsed = time.monotonic() - self._last_flush

        if (
            len(self._buffer) >= self._batch_size
            or elapsed >= self._flush_interval
        ):
            self.flush()

    def flush_if_due(self):
        elapsed = time.monotonic() - self._last_flush

        if self._buffer and elapsed >= self._flush_interval:
            self.flush()

    def flush(self):
        if not self._buffer:
            return

        batch = self._buffer[:]
        self._buffer = []
        self._last_flush = time.monotonic()

        now = datetime.now(timezone.utc)

        path = (
            f"{now.strftime('%Y/%m/%d/%H')}/"
            f"batch_{uuid.uuid4().hex[:12]}.json"
        )

        content = "\n".join(
            json.dumps(tx, ensure_ascii=False)
            for tx in batch
        )

        data = content.encode("utf-8")

        try:
            self._client.put_object(
                bucket_name=self._bucket,
                object_name=path,
                data=io.BytesIO(data),
                length=len(data),
                content_type="application/json",
            )

            self._total_written += len(batch)

            logger.info(
                "MinIO flush : %d tx → %s (total=%d)",
                len(batch),
                path,
                self._total_written,
            )

        except S3Error as e:
            logger.error("Erreur MinIO flush : %s", e)
            self._buffer = batch + self._buffer

    @property
    def total_written(self):
        return self._total_written
# ════════════════════════════════════════════════════════════════
# UTILS
# ════════════════════════════════════════════════════════════════
# Fonction utilitaire pour calculer le z-score d'une valeur par rapport à une liste de valeurs historiques,
#  en gérant les cas où il y a moins de 2 valeurs ou un écart-type nul pour éviter les divisions par zéro.
def _zscore(values: List[float], current: float) -> float:
    if len(values) < 2:
        return 0.0
    mu  = float(np.mean(values))
    std = float(np.std(values))
    return 0.0 if std == 0.0 else round((current - mu) / std, 4)

# Fonction utilitaire pour calculer l'entropie de Shannon d'une liste de valeurs catégorielles,
#  en utilisant les probabilités de chaque catégorie 
# et en ajoutant une petite constante pour éviter les log(0) lorsque les probabilités sont très faibles.
def _shannon_entropy(values: List[str]) -> float:
    if not values:
        return 0.0
    probs = pd.Series(values).value_counts(normalize=True)
    return float(round(-np.sum(probs * np.log2(probs + 1e-12)), 4))


# ════════════════════════════════════════════════════════════════
# FEATURE COMPUTATION  (21 stream features — inchangé v4)
# ════════════════════════════════════════════════════════════════
# compute_features() calcule les 21 features temps réel à partir de la transaction courante
#  et de l'historique en mémoire de l'alias,
#  en utilisant des fenêtres temporelles pour les features de fréquence, des calculs de z-score pour les features d'anomalie, 
# et des comparaisons avec la dernière transaction pour les features de changement de comportement.
def compute_features(tx: Dict[str, Any]) -> Dict[str, Any]:
    alias  = tx["payer_alias"]
    hist   = list(state[alias])
    now_ms = int(tx["timestamp"])
    now_dt = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)

    W1   = [t for t in hist if now_ms - t["timestamp"] <=      60_000]
    W5   = [t for t in hist if now_ms - t["timestamp"] <=     300_000]
    W10  = [t for t in hist if now_ms - t["timestamp"] <=     600_000]
    W24h = [t for t in hist if now_ms - t["timestamp"] <= 86_400_000]

    log_amount                 = round(math.log1p(tx["amount"]), 6)
    amount_zscore_short_window = _zscore([t["amount"] for t in W10], tx["amount"])
    tx_count_1min              = len(W1)
    tx_count_5min              = len(W5)
    tx_count_10min             = len(W10)

    if hist:
        time_since_last_tx = (now_ms - hist[-1]["timestamp"]) / 1000.0
        inter_times = [(hist[i]["timestamp"] - hist[i-1]["timestamp"]) / 1000.0
                       for i in range(1, len(hist))]
        avg_inter_tx_time = float(np.mean(inter_times)) if inter_times else 0.0
    else:
        time_since_last_tx = 0.0
        avg_inter_tx_time  = 0.0

    burst_score               = round(tx_count_1min / max(tx_count_10min, 1), 4)
    is_night_time             = int(0 <= now_dt.hour <= 5)
    channel_entropy_24h       = _shannon_entropy([t["device"] for t in W24h])
    channel_switch_flag       = int(bool(hist) and tx["device"] != hist[-1]["device"])
    transaction_type_change_flag = int(
        bool(hist) and tx["transaction_type"] != hist[-1]["transaction_type"]
    )
    country_switch_flag       = int(
        bool(hist) and tx.get("receiver_country","") != hist[-1].get("receiver_country","")
    )
    is_cross_border           = int(tx.get("is_cross_border", 0))
    institution_switch_flag   = int(
        bool(hist) and tx["issuer_institution"] != hist[-1]["issuer_institution"]
    )
    latencies          = [t["response_time_ms"] for t in hist]
    latency_zscore     = _zscore(latencies, tx["response_time_ms"])
    latency_spike_flag = int(latency_zscore > 3.0)

    last_10           = hist[-10:]
    failed_count      = sum(1 for t in last_10 if t["status"] in ("FAILED","REJECTED"))
    failure_rate_10tx = round(failed_count / max(len(last_10), 1), 4)

    last_3_statuses    = [t["status"] for t in hist[-3:]]
    retry_pattern_flag = int(last_3_statuses in [
        ["FAILED","FAILED","SUCCESS"],
        ["FAILED","FAILED","FAILED"],
    ])

    is_p2p_flag = int(tx.get("is_p2p", 0))
    is_rtp_flag = int(tx.get("is_rtp", 0))

    return {
        "payer_alias"                  : alias,
        "event_timestamp"              : now_dt,
        "log_amount"                   : log_amount,
        "amount_zscore_short_window"   : amount_zscore_short_window,
        "tx_count_1min"                : tx_count_1min,
        "tx_count_5min"                : tx_count_5min,
        "tx_count_10min"               : tx_count_10min,
        "avg_inter_tx_time"            : round(avg_inter_tx_time, 4),
        "burst_score"                  : burst_score,
        "is_night_time"                : is_night_time,
        "time_since_last_tx"           : round(time_since_last_tx, 2),
        "channel_entropy_24h"          : channel_entropy_24h,
        "channel_switch_flag"          : channel_switch_flag,
        "transaction_type_change_flag" : transaction_type_change_flag,
        "country_switch_flag"          : country_switch_flag,
        "latency_spike_flag"           : latency_spike_flag,
        "latency_zscore"               : latency_zscore,
        "failure_rate_10tx"            : failure_rate_10tx,
        "retry_pattern_flag"           : retry_pattern_flag,
        "is_cross_border"              : is_cross_border,
        "is_p2p_flag"                  : is_p2p_flag,
        "is_rtp_flag"                  : is_rtp_flag,
        "institution_switch_flag"      : institution_switch_flag,
    }


# ════════════════════════════════════════════════════════════════
# PUSH FEAST  
# ════════════════════════════════════════════════════════════════
def get_expected_stream_cols(store: FeatureStore) -> List[str]:
    """
    Charge une seule fois les colonnes attendues par Feast.
    """
    try:
        return [
            f.name
            for fv in store.list_feature_views()
            if fv.stream_source
            and fv.stream_source.name == "stream_features_source"
            for f in fv.features
        ]
    except Exception as e:
        logger.warning(
            "Impossible de charger les colonnes Feast attendues : %s",
            e,
        )
        return []
    
# _push_to_feast() gère le push des features calculées vers Feast, en convertissant les features en DataFrame,
def _push_to_feast(
    store: FeatureStore,
    features: Dict[str, Any],
    expected_cols: List[str],
) -> None:

    df = pd.DataFrame([features])

    for col in expected_cols:
        if col not in df.columns:
            df[col] = 0

    try:
        store.push("stream_features_source", df)
    except Exception as e:
        logger.error("Feast push error : %s", e)


# ════════════════════════════════════════════════════════════════
# RULE ENGINE  ( v4 + RTP_BURST et NEW_DEVICE)
# ════════════════════════════════════════════════════════════════
# Règles métier pour la détection de fraude en temps réel, basées sur les features calculées.
RULES: List[Tuple[str, Any]] = [
    ("BURST",            lambda tx, f: f["tx_count_1min"] > 10),
    ("LATENCY",          lambda tx, f: f["latency_spike_flag"] == 1),
    ("RETRY",            lambda tx, f: f["retry_pattern_flag"] == 1),
    ("CROSS_BORDER",     lambda tx, f: f["is_cross_border"] == 1 and tx["amount"] > 500_000),
    ("ACCOUNT_TAKEOVER", lambda tx, f: (f["channel_switch_flag"] == 1
                                        and f["institution_switch_flag"] == 1
                                        and tx["amount"] > 200_000)),
    ("MULE",             lambda tx, f: f["tx_count_10min"] > 8 and f["is_p2p_flag"] == 1),
    ("STRUCTURING",      lambda tx, f: (f["tx_count_5min"] > 4
                                        and tx["amount"] < 500_000
                                        and f["transaction_type_change_flag"] == 0)),
    ("COUNTRY_HOP",      lambda tx, f: (f["country_switch_flag"] == 1
                                        and f["tx_count_10min"] > 3)),
    ("AMOUNT_SPIKE",     lambda tx, f: f["amount_zscore_short_window"] > 3.5),
    ("SLEEPER",          lambda tx, f: (f["time_since_last_tx"] > 7_776_000
                                        and tx["amount"] > 50_000)),
    ("RTP_BURST",        lambda tx, f: f["is_rtp_flag"] == 1 and f["tx_count_1min"] > 5),
    ("NEW_DEVICE",       lambda tx, f: tx.get("new_device_flag", 0) == 1
                                       and tx["amount"] > 50_000),
]

# detect_fraud() applique les règles métier sur une transaction et ses features, 
# et retourne la liste des raisons de suspicion et un score de risque calculé à partir du nombre de règles déclenchées.
def detect_fraud(tx: Dict[str, Any], f: Dict[str, Any]) -> Tuple[List[str], int]:
    reasons = [name for name, rule in RULES if rule(tx, f)]
    score   = min(len(reasons) * 20, 100)
    return reasons, score

# ==================================================================
# API CALL
# ==================================================================

import requests

API_URL = "http://localhost:8000"

def _score_transaction(tx: dict) -> None:
    """Envoie la transaction à l'API pour construire l'historique."""
    try:
        payload = {
            "payer_alias"          : tx.get("payer_alias", ""),
            "receiver_alias"       : tx.get("receiver_alias", ""),
            "amount"               : float(tx.get("amount", 0)),
            "timestamp"            : int(tx.get("timestamp", 0)),
            "device"               : tx.get("device", "ANDROID"),
            "transaction_type"     : tx.get("transaction_type", "P2P"),
            "issuer_institution"   : tx.get("issuer_institution", ""),
            "receiver_institution" : tx.get("receiver_institution", ""),
            "issuer_country"       : tx.get("issuer_country", "SN"),
            "receiver_country"     : tx.get("receiver_country", "SN"),
            "status"               : tx.get("status", "SUCCESS"),
            "response_time_ms"     : float(tx.get("response_time_ms", 250)),
            "new_device_flag"      : int(tx.get("new_device_flag", 0)),
            "is_cross_border"      : int(tx.get("is_cross_border", 0)),
            "is_p2p"               : int(tx.get("is_p2p", 0)),
            "is_rtp"               : int(tx.get("is_rtp", 0)),
        }
        requests.post(f"{API_URL}/score", json=payload, timeout=2)
    except Exception:
        pass   # ne pas bloquer le stream si l'API est down
# ════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════
# run() est la fonction principale qui initialise les consommateurs Kafka, le producteur, le client MinIO et le store Feast,
#  puis entre dans une boucle de traitement des messages du topic d'entrée,
def run() -> None:

    consumer = Consumer({
        "bootstrap.servers" : KAFKA_BOOTSTRAP,
        "group.id"          : "stream-processor-v4",
        "auto.offset.reset" : "earliest",  # relit depuis le début si nouveau groupe
    })
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    store    = FeatureStore(repo_path=FEAST_REPO)

    # =====================================================
    # Charger UNE SEULE FOIS les colonnes attendues par Feast
    # =====================================================
    expected_cols = get_expected_stream_cols(store)

    logger.info(
        "Feast : %d colonnes attendues chargées.",
        len(expected_cols)
    )


    # MinIO writer avec buffer batch
    minio_writer = MinIOWriter(
        client         = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS,
                               secret_key=MINIO_SECRET, secure=False),
        bucket         = MINIO_BUCKET,
        batch_size     = MINIO_BATCH_SIZE,
        flush_interval = MINIO_FLUSH_INTERVAL,
    )

    consumer.subscribe([TOPIC_INPUT])
    logger.info("Stream Processor v4.1 démarré (topic=%s).", TOPIC_INPUT)
    logger.info("MinIO : bucket=%s batch=%d flush=%ds",
                MINIO_BUCKET, MINIO_BATCH_SIZE, MINIO_FLUSH_INTERVAL)

    running = True
    total   = 0
    alerts  = 0

    def _stop(sig: int, frame: Any) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT,  _stop)
    signal.signal(signal.SIGTERM, _stop)

    while running:
        msg = consumer.poll(1.0)

        if msg is None:
            minio_writer.flush_if_due()  # flush sur timeout (garantit les 30s)
            continue

        if msg.error():
            logger.error("Consumer error : %s", msg.error())
            continue

        try:
            tx = json.loads(msg.value().decode("utf-8"))

            required = {
                "payer_alias","timestamp","amount","issuer_country",
                "receiver_country","issuer_institution","device",
                "transaction_type","status","response_time_ms",
            }
            missing = required - tx.keys()
            if missing:
                raise ValueError(f"Champs manquants : {missing}")

            # ── 1. Features ───────────────────────────────────────
            features = compute_features(tx)

            # ── 2. Feast (Redis online store) ─────────────────────
            _push_to_feast(store, features, expected_cols)

            # ── 3. MinIO (batch ML / Airflow) ─────────────────────
            minio_writer.add(tx)
            # ── 4. Alimenter l'historique API ─────────────────────
            _score_transaction(tx)
            # ── 5. Détection temps réel ───────────────────────────
            reasons, score = detect_fraud(tx, features)
            if reasons:
                alert = {
                    "transaction_id" : tx.get("transaction_id",""),
                    "payer_alias"    : tx["payer_alias"],
                    "reason"         : ",".join(reasons),
                    "risk_score"     : score,
                    "timestamp"      : tx["timestamp"],
                    "amount"         : tx["amount"],
                    "fraud_scenario" : tx.get("fraud_scenario","NONE"),
                    "tx_count_1min"  : features["tx_count_1min"],
                    "amount_zscore"  : features["amount_zscore_short_window"],
                    "country_switch" : features["country_switch_flag"],
                }
                producer.produce(
                    TOPIC_ALERT,
                    value=json.dumps(alert).encode("utf-8"),
                )
                alerts += 1
                logger.warning(
                    "ALERTE | alias=%-20s | reasons=%-30s | score=%d | amount=%.0f",
                    tx["payer_alias"], reasons, score, tx["amount"],
                )

            # ── 5. Historique en mémoire ──────────────────────────
            state[tx["payer_alias"]].append({
                "timestamp"          : tx["timestamp"],
                "device"             : tx["device"],
                "transaction_type"   : tx["transaction_type"],
                "response_time_ms"   : tx["response_time_ms"],
                "status"             : tx["status"],
                "receiver_country"   : tx.get("receiver_country",""),
                "issuer_institution" : tx["issuer_institution"],
                "amount"             : tx["amount"],
                "is_cross_border"    : tx.get("is_cross_border", 0),
            })

            producer.poll(0)
            total += 1

            if total % 1000 == 0:
                logger.info("Traités=%d | Alertes=%d | MinIO=%d tx",
                            total, alerts, minio_writer.total_written)

        except Exception as exc:
            logger.error("Erreur : %s", exc, exc_info=True)
            try:
                producer.produce(TOPIC_DLQ, value=msg.value())
            except Exception:
                pass

    # Flush final avant arrêt propre
    minio_writer.flush()
    consumer.close()
    producer.flush()
    logger.info("Arrêt propre. Traités=%d | MinIO total=%d",
                total, minio_writer.total_written)


if __name__ == "__main__":
    run()