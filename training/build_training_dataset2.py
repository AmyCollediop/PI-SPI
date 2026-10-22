"""
build_training_dataset2.py  v5.0.0
====================================
Aligné avec producer_pispi.py v5 (10 scénarios) et stream_processor.py v4.

CORRECTIONS v5 :
  1. label_fraud = is_fraud  (champ réel du producer, pas fraud_scenario != NONE)
  2. Recalcul des 21 stream_features par alias (fenêtres glissantes)
  3. Features comportementales per-alias (30j)
  4. Export Feast complet (21 stream + 6 behavioral)
  5. Rapport qualité avec tous les 10 scénarios
  6. Rééchantillonnage bidirectionnel vers TARGET_FRAUD_RATE

SCÉNARIOS RECONNUS :
  NONE, ACCOUNT_TAKEOVER, BURST, SMURFING, MULE_ACCOUNT,
  COLLECTOR_ACCOUNT, CROSS_BORDER, RETRY, LATENCY, RTP_ABUSE, NEW_DEVICE
"""

import json
import logging
import math
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional

import numpy as np
import pandas as pd
from minio import Minio
from minio.error import S3Error

# ════════════════════════════════════════════════════════════════
# LOGGING
# ════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s – %(message)s",
)
logger = logging.getLogger(__name__)

# ════════════════════════════════════════════════════════════════
# CONFIG
# ════════════════════════════════════════════════════════════════

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS   = "minioadmin"
MINIO_SECRET   = "minioadmin"
BUCKET         = "pispi-raw"

OUTPUT_DIR      = Path("datasets")
FEAST_DATA_PATH = Path("feast_repo/data/pispi_features.parquet")

# Taux de fraude cible après rééchantillonnage (None = garder naturel ~5%)
TARGET_FRAUD_RATE: Optional[float] = 0.15

OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

# Tous les scénarios du producer v5
ALL_SCENARIOS = [
    "NONE", "ACCOUNT_TAKEOVER", "BURST", "SMURFING",
    "MULE_ACCOUNT", "COLLECTOR_ACCOUNT", "CROSS_BORDER",
    "RETRY", "LATENCY", "RTP_ABUSE", "NEW_DEVICE",
]

# ════════════════════════════════════════════════════════════════
# MINIO
# ════════════════════════════════════════════════════════════════

client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)

# Lecture des transactions depuis MinIO, avec support de plusieurs formats JSON (newline-delimited, array, single object).
def load_transactions() -> List[dict]:
    """
    Lit les fichiers JSON depuis MinIO.
    Supporte 3 formats émis par le stream_processor :
      - Newline-delimited JSON : {"tx":1}\n{"tx":2}\n...  (format MinIOWriter v4.1)
      - JSON array             : [{"tx":1}, {"tx":2}]
      - JSON object unique     : {"tx":1}
    """
    transactions: List[dict] = []
    file_count = errors = 0
    try:
        for obj in client.list_objects(BUCKET, recursive=True):
            if not obj.object_name.endswith(".json"):
                continue
            response = None
            try:
                response = client.get_object(BUCKET, obj.object_name)
                content  = response.read().decode("utf-8").strip()

                # ── Newline-delimited JSON (format MinIOWriter) ──────
                # Chaque ligne est un objet JSON indépendant
                parsed_ok = False
                lines = [l.strip() for l in content.splitlines() if l.strip()]

                if len(lines) > 1:
                    # Essayer le format newline-delimited
                    batch = []
                    line_errors = 0
                    for line in lines:
                        try:
                            batch.append(json.loads(line))
                        except json.JSONDecodeError:
                            line_errors += 1
                    if line_errors == 0 or len(batch) > len(lines) * 0.8:
                        transactions.extend(batch)
                        file_count += 1
                        parsed_ok = True

                if not parsed_ok:
                    # Fallback : JSON array ou objet unique
                    data = json.loads(content)
                    if isinstance(data, list):
                        transactions.extend(data)
                    else:
                        transactions.append(data)
                    file_count += 1

            except Exception as e:
                errors += 1
                logger.error("Erreur %s : %s", obj.object_name, e)
            finally:
                if response:
                    response.close()
                    response.release_conn()
    except S3Error as e:
        logger.error("MinIO error : %s", e)
        return []

    logger.info("📂 Fichiers lus      : %d", file_count)
    logger.info("📊 Transactions      : %d", len(transactions))
    logger.info("⚠  Erreurs           : %d", errors)
    return transactions


# ════════════════════════════════════════════════════════════════
# STREAM FEATURES  (réplique stream_processor.py v4)
# ════════════════════════════════════════════════════════════════
# Les 21 stream_features sont calculées de manière chronologique pour chaque alias, 
# en utilisant un historique glissant des transactions passées.
def _zscore(values: List[float], current: float) -> float:
    if len(values) < 2:
        return 0.0
    mu  = float(np.mean(values))
    std = float(np.std(values))
    return 0.0 if std == 0.0 else round((current - mu) / std, 4)

# L'entropie est calculée sur les valeurs d'une feature catégorielle (ex: device) dans la fenêtre glissante de 24h,
def _entropy(values: List[str]) -> float:
    if not values:
        return 0.0
    probs = pd.Series(values).value_counts(normalize=True)
    return float(round(-np.sum(probs * np.log2(probs + 1e-12)), 4))

# Calcule les 21 stream_features pour une ligne donnée, 
# en utilisant l'historique glissant des transactions précédentes du même alias.
def _stream_features_for_row(
    row    : pd.Series,
    history: Deque[Dict[str, Any]],
) -> Dict[str, Any]:
    """Calcule les 21 stream_features depuis l'historique glissant."""
    now_ms = int(row["timestamp"])
    hist   = list(history)

    W1   = [t for t in hist if now_ms - t["ts"] <=      60_000]
    W5   = [t for t in hist if now_ms - t["ts"] <=     300_000]
    W10  = [t for t in hist if now_ms - t["ts"] <=     600_000]
    W24h = [t for t in hist if now_ms - t["ts"] <= 86_400_000]

    log_amount                = round(math.log1p(row["amount"]), 6)
    amount_zscore_short_window= _zscore([t["amount"] for t in W10], row["amount"])
    tx_count_1min             = len(W1)
    tx_count_5min             = len(W5)
    tx_count_10min            = len(W10)

    if hist:
        time_since_last_tx = (now_ms - hist[-1]["ts"]) / 1000.0
        inter_times = [(hist[i]["ts"] - hist[i-1]["ts"]) / 1000.0 for i in range(1, len(hist))]
        avg_inter_tx_time = float(np.mean(inter_times)) if inter_times else 0.0
    else:
        time_since_last_tx = 0.0
        avg_inter_tx_time  = 0.0

    burst_score              = round(tx_count_1min / max(tx_count_10min, 1), 4)
    dt                       = pd.to_datetime(now_ms, unit="ms", utc=True)
    is_night_time            = int(0 <= dt.hour <= 5)
    channel_entropy_24h      = _entropy([t["device"] for t in W24h])
    channel_switch_flag      = int(bool(hist) and row["device"] != hist[-1]["device"])
    transaction_type_change_flag = int(bool(hist) and row["transaction_type"] != hist[-1]["tx_type"])
    country_switch_flag      = int(bool(hist) and row.get("receiver_country","") != hist[-1].get("recv_country",""))
    institution_switch_flag  = int(bool(hist) and row["issuer_institution"] != hist[-1]["institution"])
    is_cross_border          = int(row.get("is_cross_border", int(row.get("issuer_country","") != row.get("receiver_country",""))))

    latencies        = [t["latency"] for t in hist]
    latency_zscore   = _zscore(latencies, row["response_time_ms"])
    latency_spike_flag = int(latency_zscore > 3.0)

    last_10 = hist[-10:]
    failure_rate_10tx = round(sum(1 for t in last_10 if t["status"] in ("FAILED","REJECTED")) / max(len(last_10),1), 4)
    last_3  = [t["status"] for t in hist[-3:]]
    retry_pattern_flag = int(last_3 in [["FAILED","FAILED","SUCCESS"],["FAILED","FAILED","FAILED"]])

    is_p2p_flag = int(row.get("is_p2p", int(row.get("transaction_type","") == "P2P")))
    is_rtp_flag = int(row.get("is_rtp", int(row.get("transaction_type","") == "RTP")))

    return {
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

# Les 5 profils d'utilisateurs synthétiques avec leurs caractéristiques et poids de scénario.
def add_stream_features(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Calcul stream_features (fenêtres glissantes par alias)…")
    df     = df.sort_values(["payer_alias","timestamp"]).reset_index(drop=True)
    hists  : Dict[str, Deque] = defaultdict(lambda: deque(maxlen=500))
    rows   : List[Dict]       = []

    for _, row in df.iterrows():
        alias = row["payer_alias"]
        rows.append(_stream_features_for_row(row, hists[alias]))
        hists[alias].append({
            "ts"         : int(row["timestamp"]),
            "amount"     : float(row["amount"]),
            "device"     : str(row.get("device","")),
            "tx_type"    : str(row.get("transaction_type","")),
            "latency"    : int(row.get("response_time_ms",0)),
            "status"     : str(row.get("status","")),
            "recv_country": str(row.get("receiver_country","")),
            "institution": str(row.get("issuer_institution","")),
        })

    return pd.concat([df, pd.DataFrame(rows, index=df.index)], axis=1)


# ════════════════════════════════════════════════════════════════
# BEHAVIORAL FEATURES  (agrégats 30j par alias)
# ════════════════════════════════════════════════════════════════

def add_behavioral_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule les features comportementales de façon CHRONOLOGIQUE.
    Chaque ligne ne voit que les transactions qui la PRÉCÈDENT dans le temps.
    Évite le leakage : une tx de janvier ne "voit" pas celles de mars.
    """
    logger.info("Calcul behavioral_features (chronologique)…")

    # Trier chronologiquement (déjà fait dans add_stream_features mais on sécurise)
    df = df.sort_values(["payer_alias","timestamp"]).reset_index(drop=True)

    # ── Features calculées avec expanding() = uniquement le passé ────
    grp = df.groupby("payer_alias", sort=False)

    # Nombre de tx vues jusqu'à maintenant (expanding count)
    df["tx_count_30d"]    = grp["amount"].expanding().count().reset_index(level=0, drop=True)

    # Montant moyen des tx passées
    df["avg_amount_30d"]  = grp["amount"].expanding().mean().reset_index(level=0, drop=True).round(2)

    # Z-score par rapport aux tx passées du même alias
    expanding_mean = grp["amount"].expanding().mean().reset_index(level=0, drop=True)
    expanding_std  = grp["amount"].expanding().std().reset_index(level=0, drop=True).fillna(1.0).replace(0.0,1.0)
    df["amount_zscore_30d"] = ((df["amount"] - expanding_mean) / expanding_std).round(4)

    # Nombre de receivers uniques vus jusqu'à maintenant (calcul chronologique)
    # expanding().apply(nunique) ne fonctionne pas sur strings → calcul manuel
    if "receiver_alias" in df.columns:
        seen_receivers: dict = {}
        unique_recv_counts = []
        for _, row in df[["payer_alias","receiver_alias"]].iterrows():
            alias = row["payer_alias"]
            recv  = row["receiver_alias"]
            if alias not in seen_receivers:
                seen_receivers[alias] = set()
            seen_receivers[alias].add(recv)
            unique_recv_counts.append(len(seen_receivers[alias]))
        df["unique_receivers_30d"] = unique_recv_counts
    else:
        df["unique_receivers_30d"] = 0

    # Ancienneté du compte (depuis la première tx vue)
    first_ts = grp["timestamp"].expanding().min().reset_index(level=0, drop=True)
    df["alias_age_days"] = ((df["timestamp"] - first_ts) / 86_400_000).clip(lower=0).round(1)

    df["in_out_ratio"] = 1.0

    # ── Features graph chronologiques (MULE / COLLECTOR) ──────────
    if "receiver_alias" in df.columns:
        # receiver_degree : nb de payeurs distincts vers ce receiver (vu jusqu'ici)
        # unique_senders_30d : nb de payeurs distincts qui ont envoyé à ce payer
        # Les deux sont calculés chronologiquement avec une boucle
        seen_senders_per_recv: dict = {}   # receiver → set of senders vus
        seen_senders_per_payer: dict = {}  # payer (en tant que receiver) → set of senders

        recv_degree_list   = []
        unique_senders_list= []

        for _, row in df[["payer_alias","receiver_alias"]].iterrows():
            payer = row["payer_alias"]
            recv  = row["receiver_alias"]

            # receiver_degree du receiver de cette tx
            if recv not in seen_senders_per_recv:
                seen_senders_per_recv[recv] = set()
            seen_senders_per_recv[recv].add(payer)
            recv_degree_list.append(len(seen_senders_per_recv[recv]))

            # unique_senders_30d du payer (combien d'alias ont payé ce payer)
            count = len(seen_senders_per_payer.get(payer, set()))
            unique_senders_list.append(count)
            # Mise à jour : ce payer a reçu de 'payer' (le payer courant reçoit d'autres)
            if recv not in seen_senders_per_payer:
                seen_senders_per_payer[recv] = set()
            seen_senders_per_payer[recv].add(payer)

        df["receiver_degree"]    = recv_degree_list
        df["collector_flag"]     = (df["receiver_degree"] > 20).astype(int)
        df["unique_senders_30d"] = unique_senders_list
    else:
        df["receiver_degree"]    = 0
        df["collector_flag"]     = 0
        df["unique_senders_30d"] = 0

    return df


# ════════════════════════════════════════════════════════════════
# FEATURE ENGINEERING GLOBAL
# ════════════════════════════════════════════════════════════════
# Les features sont organisées en 3 FeatureViews pour Feast :
# 1. stream_features : les 21 features calculées à partir de l'historique glissant de chaque alias (comme dans stream_processor)
# 2. behavioral_features : les 6 features comportementales calculées de façon chronologique (expanding) pour chaque alias
# 3. static_features : les features statiques de la transaction (device, pays, institution, etc) transmises par le producer
def build_features(df: pd.DataFrame) -> pd.DataFrame:

    # Renommage défensif (rétro-compat ancien producer)
    df = df.rename(columns={k:v for k,v in {
        "alias_id":"payer_alias","country_sender":"issuer_country",
        "country_receiver":"receiver_country","institution_sender":"issuer_institution",
        "latency_ms":"response_time_ms","device_id":"device","tx_id":"transaction_id",
    }.items() if k in df.columns})

    # Timestamp
    df["event_timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", errors="coerce", utc=True)
    df = df.dropna(subset=["event_timestamp","payer_alias","amount"])
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)

    # fraud_scenario : normaliser les valeurs manquantes
    df["fraud_scenario"] = df.get("fraud_scenario", "NONE")
    df["fraud_scenario"] = df["fraud_scenario"].fillna("NONE").astype(str)

    # ── LABEL  ──────────────────────────────────────────────────────────
    # RÈGLE ABSOLUE : utiliser is_fraud du producer
    # Ne JAMAIS dériver le label de fraud_scenario != "NONE"
    # (les tx NONE intercalées dans les scénarios ont fraud_scenario != "NONE"
    #  mais is_fraud = 0 → fraud_scenario != "NONE" donne un label FAUX)
    if "is_fraud" in df.columns:
        df["label_fraud"] = df["is_fraud"].astype(int)
    else:
        # Fallback legacy (producer sans champ is_fraud)
        df["label_fraud"] = (df["fraud_scenario"] != "NONE").astype(int)
        logger.warning("⚠  Champ 'is_fraud' absent — fallback fraud_scenario!=NONE (imprécis)")

    # Temporel
    df["hour"]        = df["event_timestamp"].dt.hour
    df["day_of_week"] = df["event_timestamp"].dt.dayofweek

    # Statut
    df["status_failed"]  = df["status"].isin(["FAILED","REJECTED"]).astype(int)
    df["status_success"] = (df["status"] == "SUCCESS").astype(int)
    df["high_latency"]   = (df["response_time_ms"] > 1_000).astype(int)

    # Device one-hot
    for col, val in [("device_android","ANDROID"),("device_ios","IOS"),
                     ("device_web","WEB"),("device_feature_phone","FEATURE_PHONE"),
                     ("device_ussd","USSD")]:
        df[col] = (df["device"].astype(str).str.upper() == val).astype(int)

    # new_device_flag (transmis directement par le producer v5)
    if "new_device_flag" not in df.columns:
        df["new_device_flag"] = 0

    # Stream features
    df = add_stream_features(df)

    # Behavioral features
    df = add_behavioral_features(df)

    return df


# ════════════════════════════════════════════════════════════════
# RÉÉCHANTILLONNAGE
# ════════════════════════════════════════════════════════════════
# Rééchantillonnage bidirectionnel pour atteindre TARGET_FRAUD_RATE :
def resample(df: pd.DataFrame, target: float) -> pd.DataFrame:
    fraud = df[df["label_fraud"] == 1]
    legit = df[df["label_fraud"] == 0]
    n_f, n_l = len(fraud), len(legit)

    if n_f == 0 or n_l == 0:
        logger.warning("Rééchantillonnage impossible (classe vide).")
        return df

    current = n_f / (n_f + n_l)
    if abs(current - target) <= 0.02:
        logger.info("Fraud rate %.3f déjà proche de la cible %.3f.", current, target)
        return df

    if current > target:
        n_fraud_target = int(n_l * target / (1 - target))
        fraud_s = fraud.sample(n=min(n_fraud_target, n_f), random_state=42)
        legit_s = legit
    else:
        n_legit_target = int(n_f * (1 - target) / target)
        fraud_s = fraud
        legit_s = legit.sample(n=min(n_legit_target, n_l), random_state=42)

    result = pd.concat([fraud_s, legit_s]).sample(frac=1, random_state=42).reset_index(drop=True)
    logger.info("Resample : fraude=%d légit=%d → fraud_rate=%.3f", len(fraud_s), len(legit_s), result["label_fraud"].mean())
    return result


# ════════════════════════════════════════════════════════════════
# SAUVEGARDE
# ════════════════════════════════════════════════════════════════
# Avant de sauvegarder, on supprime les colonnes dupliquées (ex: event_timestamp) pour éviter les conflits à la lecture.
def save_datasets(df: pd.DataFrame) -> None:
    # Supprimer les colonnes dupliquées (garde la première occurrence)
    df = df.loc[:, ~df.columns.duplicated()]
    df.to_csv(OUTPUT_DIR / "pispi_training_dataset.csv", index=False)
    df.to_parquet(OUTPUT_DIR / "pispi_training_dataset.parquet", index=False)
    logger.info("💾 Saved CSV     : %s", OUTPUT_DIR / "pispi_training_dataset.csv")
    logger.info("💾 Saved Parquet : %s", OUTPUT_DIR / "pispi_training_dataset.parquet")


# ════════════════════════════════════════════════════════════════
# EXPORT FEAST  (21 stream + 6 behavioral)
# ════════════════════════════════════════════════════════════════
 
FEAST_STREAM = [
    "log_amount","amount_zscore_short_window",
    "tx_count_1min","tx_count_5min","tx_count_10min",
    "avg_inter_tx_time","burst_score",
    "is_night_time","time_since_last_tx",
    "channel_entropy_24h","channel_switch_flag",
    "transaction_type_change_flag","country_switch_flag",
    "latency_spike_flag","latency_zscore",
    "failure_rate_10tx","retry_pattern_flag",
    "is_cross_border","is_p2p_flag","is_rtp_flag",
    "institution_switch_flag",
]
FEAST_BEHAVIORAL = [
    "tx_count_30d","avg_amount_30d","amount_zscore_30d",
    "unique_receivers_30d","alias_age_days","in_out_ratio",
    "receiver_degree","collector_flag","unique_senders_30d",
]

# Lors de l'export vers Feast, on vérifie que les colonnes attendues sont présentes, on logge les absences, 
# et on sauvegarde uniquement les colonnes disponibles.
def update_feast(df: pd.DataFrame) -> None:
    df = df.loc[:, ~df.columns.duplicated()]
    cols = ["payer_alias","event_timestamp"] + FEAST_STREAM + FEAST_BEHAVIORAL
    available = [c for c in cols if c in df.columns]
    missing   = [c for c in cols if c not in df.columns]
    if missing:
        logger.warning("Colonnes absentes de l'export Feast : %s", missing)
    FEAST_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    df[available].to_parquet(FEAST_DATA_PATH, index=False)
    logger.info("✅ Feast mis à jour : %d features, %d lignes", len(available)-2, len(df))


# ════════════════════════════════════════════════════════════════
# RAPPORT QUALITÉ
# ════════════════════════════════════════════════════════════════
# Un rapport de qualité simple pour vérifier la distribution des labels, la présence des scénarios, 
# la cohérence label/is_fraud, et les nulls dans les features clés.
def quality_report(df: pd.DataFrame) -> None:
    logger.info("=" * 60)
    logger.info("RAPPORT QUALITÉ")
    logger.info("=" * 60)
    logger.info("📏 Shape              : %s", df.shape)

    rate = df["label_fraud"].mean()
    logger.info("🚨 Fraud rate         : %.4f  (%.1f%%)", rate, rate*100)

    # Labels
    logger.info("── label_fraud ──")
    for v, c in df["label_fraud"].value_counts().items():
        logger.info("   %s : %d  (%.1f%%)", v, c, c/len(df)*100)

    # Scénarios — tous les 10 doivent apparaître
    logger.info("── fraud_scenario ──")
    sc_counts = df["fraud_scenario"].value_counts()
    for sc in ALL_SCENARIOS:
        c = sc_counts.get(sc, 0)
        present = "✅" if c > 0 else "❌"
        if sc != "NONE":
            fraud_in_sc = df[df["fraud_scenario"]==sc]["label_fraud"].mean() if c > 0 else 0
            logger.info("   %s %-22s %6d  (fraud_rate=%.0f%%)", present, sc, c, fraud_in_sc*100)
        else:
            logger.info("   ── %-22s %6d", sc, c)

    # Cohérence label / is_fraud
    if "is_fraud" in df.columns:
        mismatch = (df["label_fraud"] != df["is_fraud"].astype(int)).sum()
        if mismatch == 0:
            logger.info("✅ label_fraud == is_fraud sur toutes les lignes")
        else:
            logger.warning("⚠  %d lignes label_fraud != is_fraud", mismatch)

    # Nulls dans les features clés
    null_sum = df[FEAST_STREAM[:8]].isnull().sum().sum()
    logger.info("✅ Nulls stream features : %d", null_sum)

    logger.info("=" * 60)


# ════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════

def main() -> None:
    transactions = load_transactions()
    if not transactions:
        logger.error("❌ Aucune donnée.")
        return

    df = pd.DataFrame(transactions)
    df = build_features(df)

    if df.empty:
        logger.error("❌ Dataset vide.")
        return

    # Rééchantillonnage
    if TARGET_FRAUD_RATE is not None:
        df = resample(df, TARGET_FRAUD_RATE)

    save_datasets(df)
    update_feast(df)
    quality_report(df)

    logger.info("✅ DONE")


if __name__ == "__main__":
    main()