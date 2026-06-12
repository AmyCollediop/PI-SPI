"""
stream_processor.py  v4.1.0
============================
Correction du MinIO writer :
  - Suppression de save_to_minio() (1 fichier/tx → crash + surcharge)
  - Remplacement par MinIOWriter avec buffer + flush batch (500 tx ou 30s)
  - Ajout de l'import time manquant
  - Tout le reste inchangé (features, règles, Feast push)
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

state: Dict[str, Deque[Dict[str, Any]]] = defaultdict(lambda: deque(maxlen=500))

# ════════════════════════════════════════════════════════════════
# MINIO WRITER  (buffer + flush batch)
# ════════════════════════════════════════════════════════════════

class MinIOWriter:
    """
    Buffer en mémoire → flush toutes les MINIO_BATCH_SIZE tx
    ou toutes les MINIO_FLUSH_INTERVAL secondes.

    Chemin : pispi-raw/YYYY/MM/DD/HH/batch_<uuid>.json
    Format : JSON, 1 objet par ligne (compatible build_training_dataset2.py)
    """

    def __init__(self, client: Minio, bucket: str,
                 batch_size: int, flush_interval: float) -> None:
        self._client         = client
        self._bucket         = bucket
        self._batch_size     = batch_size
        self._flush_interval = flush_interval
        self._buffer         : List[dict] = []
        self._last_flush     : float      = time.monotonic()
        self._total_written  : int        = 0
        self._ensure_bucket()

    def _ensure_bucket(self) -> None:
        try:
            if not self._client.bucket_exists(self._bucket):
                self._client.make_bucket(self._bucket)
                logger.info("Bucket créé : %s", self._bucket)
        except S3Error as e:
            logger.error("Erreur création bucket : %s", e)

    def add(self, tx: dict) -> None:
        self._buffer.append(tx)
        elapsed = time.monotonic() - self._last_flush
        if len(self._buffer) >= self._batch_size or elapsed >= self._flush_interval:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            self._last_flush = time.monotonic()
            return

        batch        = self._buffer[:]
        self._buffer = []
        self._last_flush = time.monotonic()

        now  = datetime.now(timezone.utc)
        path = f"{now.strftime('%Y/%m/%d/%H')}/batch_{uuid.uuid4().hex[:12]}.json"

        content = "\n".join(json.dumps(tx, ensure_ascii=False) for tx in batch)
        data    = content.encode("utf-8")

        try:
            self._client.put_object(
                bucket_name  = self._bucket,
                object_name  = path,
                data         = io.BytesIO(data),
                length       = len(data),
                content_type = "application/json",
            )
            self._total_written += len(batch)
            logger.info(
                "MinIO flush : %d tx → %s (total=%d)",
                len(batch), path, self._total_written,
            )
        except S3Error as e:
            logger.error("Erreur MinIO flush : %s", e)
            self._buffer = batch + self._buffer   # retry au prochain flush

    @property
    def total_written(self) -> int:
        return self._total_written


# ════════════════════════════════════════════════════════════════
# UTILS
# ════════════════════════════════════════════════════════════════

def _zscore(values: List[float], current: float) -> float:
    if len(values) < 2:
        return 0.0
    mu  = float(np.mean(values))
    std = float(np.std(values))
    return 0.0 if std == 0.0 else round((current - mu) / std, 4)


def _shannon_entropy(values: List[str]) -> float:
    if not values:
        return 0.0
    probs = pd.Series(values).value_counts(normalize=True)
    return float(round(-np.sum(probs * np.log2(probs + 1e-12)), 4))


# ════════════════════════════════════════════════════════════════
# FEATURE COMPUTATION  (21 stream features — inchangé v4)
# ════════════════════════════════════════════════════════════════

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
# PUSH FEAST  (inchangé v4 — robuste)
# ════════════════════════════════════════════════════════════════

def _push_to_feast(store: FeatureStore, features: Dict[str, Any]) -> None:
    df = pd.DataFrame([features])
    try:
        expected_cols = [
            f.name
            for fv in store.list_feature_views()
            if fv.stream_source and fv.stream_source.name == "stream_features_source"
            for f in fv.features
        ]
        for col in expected_cols:
            if col not in df.columns:
                df[col] = 0
    except Exception:
        pass
    try:
        store.push("stream_features_source", df)
    except Exception as e:
        logger.error("Feast push error : %s", e)


# ════════════════════════════════════════════════════════════════
# RULE ENGINE  (inchangé v4 + RTP_BURST et NEW_DEVICE)
# ════════════════════════════════════════════════════════════════

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


def detect_fraud(tx: Dict[str, Any], f: Dict[str, Any]) -> Tuple[List[str], int]:
    reasons = [name for name, rule in RULES if rule(tx, f)]
    score   = min(len(reasons) * 20, 100)
    return reasons, score


# ════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════

def run() -> None:

    consumer = Consumer({
        "bootstrap.servers" : KAFKA_BOOTSTRAP,
        "group.id"          : "stream-processor-v4",
        "auto.offset.reset" : "earliest",  # relit depuis le début si nouveau groupe
    })
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    store    = FeatureStore(repo_path=FEAST_REPO)

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
            minio_writer.flush()   # flush sur timeout (garantit les 30s)
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
            _push_to_feast(store, features)

            # ── 3. MinIO (batch ML / Airflow) ─────────────────────
            minio_writer.add(tx)

            # ── 4. Détection temps réel ───────────────────────────
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