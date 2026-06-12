"""
fraud_api.py  v4.1.0
=====================
Ajout des 12 règles métier du stream_processor.py v4.

NOUVEAUTÉS vs v4.0 :
  1. RULE ENGINE complet — 12 règles identiques au stream_processor.py
  2. Décision finale = ML score + règles métier (deux couches complémentaires)
  3. rule_score booste le final_score si des règles sont déclenchées
  4. Réponse enrichie : rule_triggers, rule_score, rule_reasons
  5. /alerts endpoint — résumé des alertes récentes par alias
  6. risk_level tient compte des deux couches (ML + règles)

ARCHITECTURE DE DÉCISION :
  ┌─────────────────────────────────────────────────────────┐
  │  Transaction entrante                                   │
  │       ↓                                                 │
  │  [COUCHE 1] Rule Engine  ← <1ms, déterministe           │
  │  12 règles évaluées en parallèle                        │
  │  → rule_triggers (liste), rule_score (0-100)            │
  │       ↓                                                 │
  │  [COUCHE 2] ML Scoring   ← ~5ms, probabiliste           │
  │  0.6×XGB + 0.2×ISO + 0.2×GNN                           │
  │  → ml_score (0-1)                                       │
  │       ↓                                                 │
  │  [FUSION] final_score                                   │
  │  Si règles déclenchées → boost du ML score              │
  │  final_score = max(ml_score, rule_score/100 × 0.9)      │
  │  → is_fraud, risk_level, reasons                        │
  └─────────────────────────────────────────────────────────┘
"""

from __future__ import annotations

import logging
import math
import pickle
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import torch
import torch.nn.functional as F

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prometheus.metrics import setup_monitoring, record_scoring, update_infra_gauges
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from feast import FeatureStore
from torch_geometric.data import Data
from torch_geometric.nn import SAGEConv

# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
logger = logging.getLogger("fraud_api")

# =========================================================
# CONFIG
# =========================================================

BASE_DIR   = Path(__file__).resolve().parent
MODEL_DIR  = BASE_DIR.parent / "models"
FEAST_REPO = BASE_DIR.parent / "feast_repo"

STRICT_FEATURES = [
    "burst_score", "tx_count_1min", "tx_count_5min", "tx_count_10min",
    "avg_inter_tx_time", "time_since_last_tx",
    "amount", "log_amount", "amount_zscore_short_window", "amount_zscore_30d",
    "latency_zscore", "failure_rate_10tx", "retry_pattern_flag",
    "channel_entropy_24h", "channel_switch_flag",
    "institution_switch_flag", "transaction_type_change_flag",
    "tx_count_30d", "unique_receivers_30d", "alias_age_days",
    "receiver_degree", "collector_flag", "alias_degree", "centrality_score",
    "hour", "day_of_week",
]

FEAST_BEHAVIORAL = [
    "tx_count_30d", "avg_amount_30d", "amount_zscore_30d",
    "unique_receivers_30d", "alias_age_days", "in_out_ratio",
    
]
FEAST_INTERBANK = [
    "distinct_issuer_count_24h", "distinct_receiver_count_24h",
    "institution_switch_flag",
    "cross_institution_burst", "cross_country_flow_count",
]

FEAST_GRAPH = [
    "receiver_degree",
    "collector_flag",
    "alias_degree",
    "centrality_score",
]

# =========================================================
# 12 RÈGLES MÉTIER — identiques à stream_processor.py v4
# Chaque règle : (nom, lambda(tx_dict, features_dict) -> bool)
# =========================================================

RULES: List[Tuple[str, Any]] = [
    ("BURST",
     lambda tx, f: f["tx_count_1min"] > 10),

    ("LATENCY",
     lambda tx, f: f.get("latency_spike_flag", 0) == 1),

    ("RETRY",
     lambda tx, f: f.get("retry_pattern_flag", 0) == 1),

    ("CROSS_BORDER",
     lambda tx, f: f.get("is_cross_border", 0) == 1
                   and float(tx.get("amount", 0)) > 500_000),

    ("ACCOUNT_TAKEOVER",
     lambda tx, f: f.get("channel_switch_flag", 0) == 1
                   and f.get("institution_switch_flag", 0) == 1
                   and float(tx.get("amount", 0)) > 200_000),

    ("MULE",
     lambda tx, f: f["tx_count_10min"] > 8
                   and f.get("is_p2p_flag", 0) == 1),

    ("STRUCTURING",
     lambda tx, f: f["tx_count_5min"] > 4
                   and float(tx.get("amount", 0)) < 500_000
                   and f.get("transaction_type_change_flag", 0) == 0),

    ("COUNTRY_HOP",
     lambda tx, f: f.get("country_switch_flag", 0) == 1
                   and f["tx_count_10min"] > 3),

    ("AMOUNT_SPIKE",
     lambda tx, f: f.get("amount_zscore_short_window", 0) > 3.5),

    ("SLEEPER",
     lambda tx, f: f.get("time_since_last_tx", 0) > 7_776_000
                   and float(tx.get("amount", 0)) > 50_000),

    ("RTP_BURST",
     lambda tx, f: f.get("is_rtp_flag", 0) == 1
                   and f["tx_count_1min"] > 5),

    ("NEW_DEVICE",
     lambda tx, f: int(tx.get("new_device_flag", 0)) == 1
                   and float(tx.get("amount", 0)) > 50_000),
]


def _apply_rules(tx: dict, features: dict) -> Tuple[List[str], int]:
    """
    Évalue les 12 règles métier.
    Retourne (triggers, rule_score).
    rule_score = min(nb_règles × 20, 100) — identique au stream_processor.
    """
    triggers = [name for name, rule in RULES if _safe_rule(rule, tx, features)]
    score    = min(len(triggers) * 20, 100)
    return triggers, score


def _safe_rule(rule, tx: dict, features: dict) -> bool:
    """Évalue une règle avec protection contre les exceptions."""
    try:
        return bool(rule(tx, features))
    except Exception:
        return False


# =========================================================
# GNN
# =========================================================

class GraphSAGE(torch.nn.Module):
    def __init__(self, in_dim: int, hidden: int = 64):
        super().__init__()
        self.conv1 = SAGEConv(in_dim, hidden)
        self.conv2 = SAGEConv(hidden, 2)

    def forward(self, data: Data):
        x = F.relu(self.conv1(data.x, data.edge_index))
        x = F.dropout(x, p=0.4, training=self.training)
        return self.conv2(x, data.edge_index)


# =========================================================
# CHARGEMENT DU MODÈLE
# =========================================================

def _load_latest_model():
    candidates = sorted(MODEL_DIR.glob("hybrid_*"))
    if not candidates:
        raise RuntimeError(f"Aucun modèle trouvé dans {MODEL_DIR}")

    path = candidates[-1]
    logger.info("Chargement modèle : %s", path.name)

    with open(path / "xgb.pkl", "rb") as f:
        xgb = pickle.load(f)
    with open(path / "iso.pkl", "rb") as f:
        iso = pickle.load(f)
    with open(path / "meta.pkl", "rb") as f:
        meta = pickle.load(f)

    features  = meta["features"]
    threshold = meta.get("threshold", 0.5)
    node_map  = meta.get("node_map", {})
    iso_min   = meta.get("iso_score_min", 0.0)
    iso_max   = meta.get("iso_score_max", 1.0)

    gnn = None
    gnn_path = path / "gnn.pt"
    if gnn_path.exists():
        in_dim = max(len([f for f in features[:8] if f in STRICT_FEATURES]), 1)
        gnn = GraphSAGE(in_dim=in_dim)
        gnn.load_state_dict(torch.load(gnn_path, map_location="cpu"))
        gnn.eval()
        logger.info("GNN chargé (in_dim=%d)", in_dim)
    else:
        logger.warning("gnn.pt absent — GNN désactivé, fallback 0.5")

    logger.info(
        "Modèle chargé : %d features | threshold=%.3f | GNN=%s",
        len(features), threshold, "actif" if gnn else "inactif",
    )
    return xgb, iso, gnn, node_map, features, threshold, iso_min, iso_max, path


xgb, iso, gnn_model, node_map, MODEL_FEATURES, THRESHOLD, ISO_MIN, ISO_MAX, MODEL_PATH = \
    _load_latest_model()

# =========================================================
# FEAST
# =========================================================

try:
    feast_store = FeatureStore(repo_path=str(FEAST_REPO))
    FEAST_AVAILABLE = True
    logger.info("Feast connecté")
except Exception as e:
    feast_store = None
    FEAST_AVAILABLE = False
    logger.warning("Feast indisponible (%s)", e)

# =========================================================
# SHAP
# =========================================================

try:
    import shap
    shap_explainer = shap.TreeExplainer(xgb)
    logger.info("SHAP activé")
except Exception:
    shap_explainer = None

# =========================================================
# HISTORIQUE EN MÉMOIRE (9 champs — identique stream_processor)
# =========================================================

history: Dict[str, Deque[Dict[str, Any]]] = defaultdict(lambda: deque(maxlen=500))

# Compteur d'alertes récentes par alias (pour /alerts)
alert_log: Deque[Dict[str, Any]] = deque(maxlen=1000)


def _update_history(alias: str, tx: dict, now_ms: int) -> None:
    history[alias].append({
        "ts"          : now_ms,
        "amount"      : float(tx.get("amount", 0)),
        "device"      : str(tx.get("device", "")),
        "tx_type"     : str(tx.get("transaction_type", "")),
        "latency"     : int(tx.get("response_time_ms", 0)),
        "status"      : str(tx.get("status", "SUCCESS")),
        "recv_country": str(tx.get("receiver_country", "")),
        "institution" : str(tx.get("issuer_institution", "")),
        "recv_alias"  : str(tx.get("receiver_alias", "")),
    })

# =========================================================
# UTILS
# =========================================================

def _zscore(values: List[float], current: float) -> float:
    if len(values) < 2:
        return 0.0
    mu  = float(np.mean(values))
    std = float(np.std(values))
    return 0.0 if std == 0 else round((current - mu) / std, 4)


def _shannon_entropy(values: List[str]) -> float:
    if not values:
        return 0.0
    probs = pd.Series(values).value_counts(normalize=True)
    return float(round(-np.sum(probs * np.log2(probs + 1e-12)), 4))


# =========================================================
# STREAM FEATURES (formules identiques stream_processor.py v4)
# =========================================================

def _compute_stream_features(tx: dict, now_ms: int) -> Dict[str, Any]:
    alias = tx["payer_alias"]
    hist  = list(history[alias])

    amount  = float(tx["amount"])
    latency = float(tx["response_time_ms"])

    W1   = [t for t in hist if now_ms - t["ts"] <=      60_000]
    W5   = [t for t in hist if now_ms - t["ts"] <=     300_000]
    W10  = [t for t in hist if now_ms - t["ts"] <=     600_000]
    W24h = [t for t in hist if now_ms - t["ts"] <= 86_400_000]

    log_amount                 = round(math.log1p(amount), 6)
    amount_zscore_short_window = _zscore([t["amount"] for t in W10], amount)

    tx_count_1min  = len(W1)
    tx_count_5min  = len(W5)
    tx_count_10min = len(W10)
    burst_score    = round(tx_count_1min / max(tx_count_10min, 1), 4)

    if hist:
        time_since_last_tx = (now_ms - hist[-1]["ts"]) / 1000.0
        inter_times = [(hist[i]["ts"] - hist[i-1]["ts"]) / 1000.0
                       for i in range(1, len(hist))]
        avg_inter_tx_time = float(np.mean(inter_times)) if inter_times else 0.0
    else:
        time_since_last_tx = 0.0
        avg_inter_tx_time  = 0.0

    dt            = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
    is_night_time = int(0 <= dt.hour <= 5)
    hour          = dt.hour
    day_of_week   = dt.weekday()

    channel_entropy_24h          = _shannon_entropy([t["device"] for t in W24h])
    channel_switch_flag          = int(bool(hist) and tx.get("device","") != hist[-1]["device"])
    transaction_type_change_flag = int(
        bool(hist) and tx.get("transaction_type","") != hist[-1]["tx_type"]
    )
    country_switch_flag = int(
        bool(hist) and tx.get("receiver_country","") != hist[-1]["recv_country"]
    )
    institution_switch_flag = int(
        bool(hist) and tx.get("issuer_institution","") != hist[-1]["institution"]
    )

    latency_zscore     = _zscore([t["latency"] for t in hist], latency)
    latency_spike_flag = int(latency_zscore > 3.0)

    last_10           = hist[-10:]
    failure_rate_10tx = round(
        sum(1 for t in last_10 if t["status"] in ("FAILED","REJECTED")) /
        max(len(last_10), 1), 4
    )
    last_3_statuses    = [t["status"] for t in hist[-3:]]
    retry_pattern_flag = int(last_3_statuses in [
        ["FAILED","FAILED","SUCCESS"],
        ["FAILED","FAILED","FAILED"],
    ])

    is_p2p_flag     = int(tx.get("transaction_type","") == "P2P")
    is_rtp_flag     = int(tx.get("transaction_type","") == "RTP")
    is_cross_border = int(tx.get("issuer_country","") != tx.get("receiver_country",""))

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
        # raw
        "amount"          : amount,
        "response_time_ms": latency,
        "hour"            : hour,
        "day_of_week"     : day_of_week,
        "new_device_flag" : int(tx.get("new_device_flag", 0)),
        "high_latency"    : int(latency > 1_000),
        "status_failed"   : int(tx.get("status","") in ("FAILED","REJECTED")),
    }

# =========================================================
# FEAST FEATURES
# =========================================================
def _get_feast_features(alias: str) -> Dict[str, Any]:
    defaults = {f: 0.0 for f in FEAST_BEHAVIORAL + FEAST_INTERBANK + FEAST_GRAPH}
    if not FEAST_AVAILABLE or feast_store is None:
        return defaults

    feature_refs = (
        [f"behavioral_features:{f}" for f in FEAST_BEHAVIORAL] +
        [f"interbank_features:{f}"  for f in FEAST_INTERBANK]  +
        [f"graph_features:{f}"      for f in FEAST_GRAPH]
    )
    try:
        response = feast_store.get_online_features(
            entity_rows=[{"payer_alias": alias}],
            features=feature_refs,
        ).to_dict()
        result = {}
        for f in FEAST_BEHAVIORAL + FEAST_INTERBANK + FEAST_GRAPH:
            v = (response.get(f) or [None])[0]
            result[f] = float(v) if v is not None else 0.0
        return result
    except Exception as e:
        logger.warning("Feast erreur (%s) — fallback 0", e)
        return defaults

# =========================================================
# GNN SCORE
# =========================================================

def _get_gnn_score(alias: str) -> float:
    if gnn_model is None:
        return 0.5
    if alias in node_map:
        return float(getattr(gnn_model, "_train_scores", {}).get(node_map[alias], 0.5))
    return 0.5


# =========================================================
# VECTEUR DE FEATURES
# =========================================================

def _build_feature_vector(
    stream_feats: Dict[str, Any],
    feast_feats : Dict[str, Any],
) -> pd.DataFrame:
    merged = {**stream_feats}
    merged.update({
        "tx_count_30d"           : feast_feats.get("tx_count_30d",         stream_feats.get("tx_count_10min", 0)),
        "avg_amount_30d"         : feast_feats.get("avg_amount_30d",        stream_feats.get("amount", 0)),
        "amount_zscore_30d"      : feast_feats.get("amount_zscore_30d",     stream_feats.get("amount_zscore_short_window", 0)),
        "unique_receivers_30d"   : feast_feats.get("unique_receivers_30d",  0),
        "alias_age_days"         : feast_feats.get("alias_age_days",        0),
        "in_out_ratio"           : feast_feats.get("in_out_ratio",          1.0),
        "receiver_degree"        : feast_feats.get("receiver_degree",       0),
        "collector_flag"         : feast_feats.get("collector_flag",        0),
        #"unique_senders_30d"     : feast_feats.get("unique_senders_30d",    0),
        "alias_degree"           : feast_feats.get("alias_degree",          0),
        "centrality_score"       : feast_feats.get("centrality_score",      0),
        # institution_switch : max entre stream (tx-à-tx) et Feast (24h)
        "institution_switch_flag": max(
            int(stream_feats.get("institution_switch_flag", 0)),
            int(feast_feats.get("institution_switch_flag", 0)),
        ),
    })
    return pd.DataFrame([{f: merged.get(f, 0) for f in MODEL_FEATURES}])


# =========================================================
# FASTAPI
# =========================================================
app = FastAPI(
    title="Fraud Detection API v4.1",
    description="PI-SPI — ML hybride + 12 règles métier + Feast + GNN",
    version="4.1.0",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class TransactionRequest(BaseModel):
    payer_alias         : str
    receiver_alias      : str   = ""
    amount              : float
    timestamp           : int
    device              : str   = ""
    transaction_type    : str   = "P2P"
    issuer_institution  : str   = ""
    receiver_institution: str   = ""
    issuer_country      : str   = ""
    receiver_country    : str   = ""
    status              : str   = "SUCCESS"
    response_time_ms    : float = 250.0
    new_device_flag     : int   = 0
    is_cross_border     : int   = 0
    is_p2p              : int   = 0
    is_rtp              : int   = 0
    fraud_scenario      : str   = "NONE"

setup_monitoring(app)
# =========================================================
# /score — endpoint principal
# =========================================================

@app.post("/score")
def score(tx: TransactionRequest):
    t_start = time.perf_counter()
    now_ms  = tx.timestamp
    tx_dict = tx.model_dump()

    # ── 1. Stream features ───────────────────────────────────
    stream_feats = _compute_stream_features(tx_dict, now_ms)

    # ── 2. Feast features (Airflow batch) ────────────────────
    feast_feats = _get_feast_features(tx.payer_alias)

    # ── 3. COUCHE 1 : 12 règles métier ───────────────────────
    # Évaluées sur les features stream + données brutes de la tx
    # Exactement les mêmes lambdas que dans stream_processor.py
    rule_triggers, rule_score = _apply_rules(tx_dict, stream_feats)

    # ── 4. COUCHE 2 : ML scoring ──────────────────────────────
    X         = _build_feature_vector(stream_feats, feast_feats)
    xgb_score = float(xgb.predict_proba(X)[0, 1])
    iso_raw   = float(-iso.score_samples(X)[0])
    iso_score = float(np.clip(
        (iso_raw - ISO_MIN) / (ISO_MAX - ISO_MIN + 1e-9), 0.0, 1.0
    ))
    gnn_score = _get_gnn_score(tx.payer_alias)
    ml_score  = 0.6 * xgb_score + 0.2 * iso_score + 0.2 * gnn_score

    # ── 5. FUSION règles + ML ─────────────────────────────────
    #
    # Logique de fusion :
    #   - Si aucune règle déclenchée → final_score = ml_score pur
    #   - Si règles déclenchées → final_score = max(ml_score, rule_floor)
    #     rule_floor = rule_score / 100 * 0.9  (règle à 100 → floor 0.90)
    #
    # Exemple :
    #   ml_score=0.30, rule_score=60 (3 règles)
    #   rule_floor = 0.54 → final_score = max(0.30, 0.54) = 0.54
    #
    # Les règles NE remplacent PAS le ML — elles constituent un plancher.
    # Un score ML élevé sans règle est toujours retenu.
    #
    rule_floor  = (rule_score / 100.0) * 0.9 if rule_triggers else 0.0
    final_score = max(ml_score, rule_floor)

    # ── 6. Risk level (tient compte des deux couches) ─────────
    #
    # Une transaction avec des règles déclenchées ET un score ML modéré
    # peut quand même être HIGH/CRITICAL via rule_floor.
    #
    risk_level = (
        "CRITICAL" if final_score >= 0.90           else
        "HIGH"     if final_score >= THRESHOLD       else
        "MEDIUM"   if final_score >= THRESHOLD * 0.7 else
        "LOW"
    )

    # ── 7. SHAP ────────────────────────────────────────────────
    top_features = []
    explanations = []
    if shap_explainer is not None:
        try:
            shap_vals = shap_explainer.shap_values(X)
            vals = shap_vals[1] if isinstance(shap_vals, list) else shap_vals
            importances = pd.Series(vals[0], index=MODEL_FEATURES).abs().nlargest(5)
            top_features = [
                {"feature": feat, "importance": round(float(imp), 4)}
                for feat, imp in importances.items()
            ]
            explanations = [
                f"{feat}={round(float(X[feat].iloc[0]),4)} (shap={round(float(imp),4)})"
                for feat, imp in importances.items()
            ]
        except Exception as e:
            logger.debug("SHAP erreur : %s", e)

    # ── 8. Mise à jour historique ──────────────────────────────
    _update_history(tx.payer_alias, tx_dict, now_ms)

    # ── 9. Journaliser les alertes (règles OU ML élevé) ────────
    is_fraud = bool(final_score >= THRESHOLD or len(rule_triggers) > 0)
    if is_fraud:
        alert_log.append({
            "ts"           : now_ms,
            "payer_alias"  : tx.payer_alias,
            "final_score"  : round(final_score, 4),
            "ml_score"     : round(ml_score, 4),
            "rule_score"   : rule_score,
            "rule_triggers": rule_triggers,
            "risk_level"   : risk_level,
            "amount"       : tx.amount,
        })

    latency_ms = round((time.perf_counter() - t_start) * 1000, 2)

# ── Métriques Prometheus ────────────────────────────────
    record_scoring(
        tx_dict,
        {
            "fraud_score"   : round(final_score, 4),
            "ml_score"      : round(ml_score, 4),
            "xgb_score"     : round(xgb_score, 4),
            "iso_score"     : round(iso_score, 4),
            "gnn_score"     : round(gnn_score, 4),
            "risk_level"    : risk_level,
            "is_fraud"      : is_fraud,
            "rule_triggers" : rule_triggers,
            "rule_score"    : rule_score,
            "features"      : {"stream": stream_feats},
        },
        latency_ms / 1000.0,
    )
    update_infra_gauges(
        feast_ok    = FEAST_AVAILABLE,
        gnn_ok      = gnn_model is not None,
        n_aliases   = len(history),
        n_tx_memory = sum(len(v) for v in history.values()),
    )

    # ── 10. Réponse complète ──────────────────────────────────
    return {
        "payer_alias"      : tx.payer_alias,
        "transaction_id"   : str(uuid.uuid4()),
        "fraud_score"      : round(final_score, 4),
        "final_score"      : round(final_score, 4),
        "fraud_probability": round(final_score, 4),
        "is_fraud"         : is_fraud,
        "risk_level"       : risk_level,
        "threshold"        : round(THRESHOLD, 4),
        "ml_score"         : round(ml_score, 4),
        "xgb_score"        : round(xgb_score, 4),
        "iso_score"        : round(iso_score, 4),
        "gnn_score"        : round(gnn_score, 4),
        "gnn_active"       : gnn_model is not None,
        "rule_score"       : rule_score,
        "rule_triggers"    : rule_triggers,
        "rule_reasons"     : [f"Règle {name} déclenchée (+{20} pts)" for name in rule_triggers],
        "rules_fired"      : len(rule_triggers),
        "features": {
            "stream"          : {k: v for k, v in stream_feats.items() if k in STRICT_FEATURES},
            "feast"           : feast_feats,
            "feast_available" : FEAST_AVAILABLE,
        },
        "top_features" : top_features,
        "explanations" : explanations,
        "latency_ms"   : latency_ms,
    }
# =========================================================
# /health
# =========================================================

@app.get("/health")
def health():
    return {
        "status"           : "ok",
        "version"          : "4.1.0",
        "model"            : MODEL_PATH.name,
        "features_count"   : len(MODEL_FEATURES),
        "threshold"        : round(THRESHOLD, 4),
        "gnn_active"       : gnn_model is not None,
        "feast_available"  : FEAST_AVAILABLE,
        "rules_count"      : len(RULES),
        "aliases_in_memory": len(history),
        "alerts_in_log"    : len(alert_log),
        "shap_active"      : shap_explainer is not None,
    }


# =========================================================
# /metrics
# =========================================================

@app.get("/api/metrics")
def metrics():
    alias_sizes = [len(v) for v in history.values()]
    recent_alerts = [a for a in alert_log
                     if time.time() * 1000 - a["ts"] <= 3_600_000]  # dernière heure
    rule_counts: Dict[str, int] = defaultdict(int)
    for a in recent_alerts:
        for r in a["rule_triggers"]:
            rule_counts[r] += 1

    return {
        "total_aliases"         : len(history),
        "total_tx_in_memory"    : sum(alias_sizes),
        "avg_history_per_alias" : round(float(np.mean(alias_sizes)) if alias_sizes else 0.0, 1),
        "feast_available"       : FEAST_AVAILABLE,
        "gnn_active"            : gnn_model is not None,
        "alerts_last_hour"      : len(recent_alerts),
        "rule_triggers_last_hour": dict(sorted(
            rule_counts.items(), key=lambda x: -x[1]
        )),
    }


# =========================================================
# /alerts — 50 dernières alertes
# =========================================================

@app.get("/alerts")
def alerts(limit: int = 50):
    """Retourne les dernières alertes (règles déclenchées OU score ML élevé)."""
    recent = list(alert_log)[-limit:]
    recent.reverse()   # plus récent en premier
    return {
        "count" : len(recent),
        "alerts": recent,
    }


# =========================================================
# /features/{alias} — debug
# =========================================================

@app.get("/features/{alias}")
def get_features(alias: str):
    feast_feats = _get_feast_features(alias)
    hist = list(history.get(alias, deque()))
    return {
        "alias"        : alias,
        "feast"        : feast_feats,
        "history_size" : len(hist),
        "last_tx_ms"   : hist[-1]["ts"] if hist else None,
    }


# =========================================================
# /rules — liste des 12 règles et leur logique
# =========================================================

@app.get("/rules")
def list_rules():
    """Retourne la liste des 12 règles métier actives."""
    return {
        "count": len(RULES),
        "rules": [
            {"name": name, "score_contribution": 20}
            for name, _ in RULES
        ],
        "score_formula": "rule_score = min(nb_règles_déclenchées × 20, 100)",
        "fusion_formula": "final_score = max(ml_score, rule_score/100 × 0.9)",
    }


# =========================================================
# RUN
# =========================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("fraud_api:app", host="0.0.0.0", port=8000, reload=False)
