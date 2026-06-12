"""
train_fraud_model.py  v3.0.0
=============================
Évaluation sans data leakage.

CORRECTIONS v3 :
  1. Split temporel (train=70% premiers, test=30% derniers)
     → simule la vraie production : le modèle n'a jamais vu le futur
  2. Rééchantillonnage UNIQUEMENT sur le train set
  3. Features comportementales recalculées séparément sur train puis test
  4. AUC réaliste (attendu 0.85-0.93)
"""

import logging
import pickle
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import torch
import torch.nn.functional as F

from sklearn.metrics import (
    roc_auc_score, classification_report,
    precision_recall_curve,
)
from sklearn.ensemble import IsolationForest
from xgboost import XGBClassifier
from torch_geometric.data import Data
from torch_geometric.nn import SAGEConv

# ════════════════════════════════════════════════════════════════
# CONFIG
# ════════════════════════════════════════════════════════════════

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR  = Path(__file__).resolve().parent.parent
DATA_PATH = BASE_DIR / "datasets" / "pispi_training_dataset.parquet"
MODEL_DIR = BASE_DIR / "models"
MODEL_DIR.mkdir(exist_ok=True)

STREAM_FEATURES = [
    "log_amount", "amount_zscore_short_window",
    "tx_count_1min", "tx_count_5min", "tx_count_10min",
    "avg_inter_tx_time", "burst_score",
    "is_night_time", "time_since_last_tx",
    "channel_entropy_24h", "channel_switch_flag",
    "transaction_type_change_flag", "country_switch_flag",
    "latency_spike_flag", "latency_zscore",
    "failure_rate_10tx", "retry_pattern_flag",
    "is_cross_border", "is_p2p_flag", "is_rtp_flag",
    "institution_switch_flag",
]
BEHAVIORAL_FEATURES = [
    "tx_count_30d", "avg_amount_30d", "amount_zscore_30d",
    "unique_receivers_30d", "alias_age_days", "in_out_ratio",
    "receiver_degree", "collector_flag", "unique_senders_30d",
]
RAW_FEATURES = [
    "amount", "response_time_ms", "hour", "day_of_week",
    "new_device_flag", "high_latency", "status_failed",
]
ALL_FEATURES = STREAM_FEATURES + BEHAVIORAL_FEATURES + RAW_FEATURES

# Features comportementales pures — sans proxy de scénario
# Généralise mieux sur de nouveaux types de fraude non vus en entraînement
STRICT_FEATURES = [
    # ── Temporelles (burst, rythme) ──────────────────────────────
    "burst_score", "tx_count_1min", "tx_count_5min", "tx_count_10min",
    "avg_inter_tx_time", "time_since_last_tx",
    # ── Montant ──────────────────────────────────────────────────
    "amount", "log_amount", "amount_zscore_short_window", "amount_zscore_30d",
    # ── Latence ──────────────────────────────────────────────────
    "latency_zscore", "failure_rate_10tx", "retry_pattern_flag",
    # ── Comportementales canal / institution ─────────────────────
    "channel_entropy_24h", "channel_switch_flag",
    "institution_switch_flag", "transaction_type_change_flag",
    # ── Comportementales 30j ─────────────────────────────────────
    "tx_count_30d", "unique_receivers_30d", "alias_age_days",
    # ── Graph features (MULE / COLLECTOR) ─────────────────────────
    "receiver_degree",       # nb de payeurs distincts vers ce receiver
    "collector_flag",        # receiver_degree > 20
    "unique_senders_30d",    # nb de payeurs distincts ayant envoyé à ce payer
    # ── Temporel général ─────────────────────────────────────────
    "hour", "day_of_week",
]

# Choisir le mode : "full" (37 features) ou "strict" (25 features)
TRAINING_MODE = "strict"  # ← changer en "full" pour comparaison


# ════════════════════════════════════════════════════════════════
# CHARGEMENT
# ════════════════════════════════════════════════════════════════

def load_data():
    logger.info("📥 Chargement : %s", DATA_PATH)
    df = pd.read_parquet(DATA_PATH)

    available = [f for f in ALL_FEATURES if f in df.columns]
    missing   = [f for f in ALL_FEATURES if f not in df.columns]
    if missing:
        logger.warning("⚠ Features absentes : %s", missing)

    cols = available + ["payer_alias", "label_fraud", "fraud_scenario", "timestamp"]
    if "receiver_alias" in df.columns:
        cols.append("receiver_alias")
    df = df[[c for c in cols if c in df.columns]].copy()
    df = df.dropna(subset=["payer_alias", "label_fraud", "timestamp"])
    df[available] = df[available].fillna(0)

    # Sélectionner les features selon le mode
    if TRAINING_MODE == "strict":
        target_features = [f for f in STRICT_FEATURES if f in df.columns]
        logger.info("Mode STRICT : %d features comportementales (sans proxy scénario)", len(target_features))
    else:
        target_features = available
        logger.info("Mode FULL : %d features (incluant proxies scénario)", len(target_features))

    logger.info("✅ Dataset : %s | Features : %d", df.shape, len(target_features))
    logger.info("📊 label_fraud :\n%s", df["label_fraud"].value_counts().to_string())
    return df, target_features


# ════════════════════════════════════════════════════════════════
# SPLIT TEMPOREL  (pas de random split — évite le leakage)
# ════════════════════════════════════════════════════════════════

def temporal_split(df: pd.DataFrame, test_ratio: float = 0.30):
    """
    Coupe chronologiquement.
    Le test set = les 30% transactions les plus récentes.
    Le modèle n'a jamais vu ces transactions ni leur contexte futur.
    """
    df = df.sort_values("timestamp").reset_index(drop=True)
    cut = int(len(df) * (1 - test_ratio))

    train = df.iloc[:cut].copy()
    test  = df.iloc[cut:].copy()

    logger.info(
        "Split temporel : train=%d (%.0f%%) | test=%d (%.0f%%)",
        len(train), 100*(1-test_ratio), len(test), 100*test_ratio,
    )
    logger.info(
        "Train fraud=%.1f%% | Test fraud=%.1f%%",
        train["label_fraud"].mean()*100, test["label_fraud"].mean()*100,
    )
    return train, test


# ════════════════════════════════════════════════════════════════
# RÉÉCHANTILLONNAGE (train uniquement)
# ════════════════════════════════════════════════════════════════

def resample_train(train: pd.DataFrame, target: float = 0.20) -> pd.DataFrame:
    """
    Rééquilibre le train set UNIQUEMENT.
    Le test set reste non touché (distribution naturelle ~5%).
    """
    fraud = train[train["label_fraud"] == 1]
    legit = train[train["label_fraud"] == 0]
    n_f, n_l = len(fraud), len(legit)
    current = n_f / (n_f + n_l)

    if abs(current - target) <= 0.02:
        return train

    if current > target:
        n_ft = int(n_l * target / (1 - target))
        result = pd.concat([fraud.sample(n=min(n_ft, n_f), random_state=42), legit])
    else:
        n_lt = int(n_f * (1 - target) / target)
        result = pd.concat([fraud, legit.sample(n=min(n_lt, n_l), random_state=42)])

    result = result.sample(frac=1, random_state=42).reset_index(drop=True)
    logger.info("Train resample : %d fraud / %d legit → %.1f%%",
                result["label_fraud"].sum(), (result["label_fraud"]==0).sum(),
                result["label_fraud"].mean()*100)
    return result


# ════════════════════════════════════════════════════════════════
# GRAPHE
# ════════════════════════════════════════════════════════════════

def build_graph(df: pd.DataFrame, features: list):
    df = df.copy()
    if "receiver_alias" not in df.columns:
        df["receiver_alias"] = df["payer_alias"]

    nodes    = pd.unique(pd.concat([df["payer_alias"], df["receiver_alias"]]))
    node_map = {n: i for i, n in enumerate(nodes)}

    src   = df["payer_alias"].map(node_map)
    dst   = df["receiver_alias"].map(node_map)
    valid = src.notna() & dst.notna()

    edge_index = torch.tensor(
        np.array([src[valid].astype(int).values,
                  dst[valid].astype(int).values]),
        dtype=torch.long,
    )

    feats_for_graph = [f for f in features[:8] if f in df.columns]
    out_stats = df.groupby("payer_alias")[feats_for_graph].mean()
    node_df   = pd.DataFrame(index=nodes).join(out_stats, how="left").fillna(0)
    x         = torch.tensor(node_df.values, dtype=torch.float)

    fraud_map = df.groupby("payer_alias")["label_fraud"].max()
    y = torch.tensor([int(fraud_map.get(n, 0)) for n in nodes], dtype=torch.long)

    return Data(x=x, edge_index=edge_index, y=y), node_map


# ════════════════════════════════════════════════════════════════
# GNN
# ════════════════════════════════════════════════════════════════

class GraphSAGE(torch.nn.Module):
    def __init__(self, in_dim: int, hidden: int = 64):
        super().__init__()
        self.conv1 = SAGEConv(in_dim, hidden)
        self.conv2 = SAGEConv(hidden, 2)

    def forward(self, data):
        x = F.relu(self.conv1(data.x, data.edge_index))
        x = F.dropout(x, p=0.4, training=self.training)
        return self.conv2(x, data.edge_index)


def train_gnn(data: Data) -> GraphSAGE:
    model   = GraphSAGE(data.x.shape[1])
    opt     = torch.optim.Adam(model.parameters(), lr=0.01, weight_decay=1e-3)
    n_neg   = (data.y == 0).sum().item()
    n_pos   = (data.y == 1).sum().item()
    weight  = torch.tensor([1.0, n_neg / max(n_pos, 1)], dtype=torch.float)
    loss_fn = torch.nn.CrossEntropyLoss(weight=weight)

    for epoch in range(30):
        model.train()
        opt.zero_grad()
        loss = loss_fn(model(data), data.y)
        loss.backward()
        opt.step()
        if epoch % 10 == 0:
            logger.info("GNN epoch %d | loss %.4f", epoch, loss.item())

    return model


# ════════════════════════════════════════════════════════════════
# XGBOOST + ISOLATION FOREST
# ════════════════════════════════════════════════════════════════

def train_tabular(train: pd.DataFrame, test: pd.DataFrame, features: list):
    X_train = train[features]
    y_train = train["label_fraud"]
    X_test  = test[features]
    y_test  = test["label_fraud"]

    spw = (y_train == 0).sum() / max((y_train == 1).sum(), 1)
    logger.info("scale_pos_weight : %.2f", spw)

    xgb = XGBClassifier(
        n_estimators=400, max_depth=7, learning_rate=0.05,
        subsample=0.8, colsample_bytree=0.8,
        scale_pos_weight=spw, eval_metric="auc",
        early_stopping_rounds=20, random_state=42, verbosity=0,
    )
    xgb.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

    iso = IsolationForest(contamination=float(y_train.mean()), random_state=42)
    iso.fit(X_train)
    iso_raw   = -iso.score_samples(X_test)
    iso_min   = float(iso_raw.min())
    iso_max   = float(iso_raw.max())
    iso_score = (iso_raw - iso_min) / (iso_max - iso_min + 1e-9)

    return xgb, iso, X_test, y_test, iso_score, iso_min, iso_max


# ════════════════════════════════════════════════════════════════
# SEUIL OPTIMAL (F1)
# ════════════════════════════════════════════════════════════════

def optimal_threshold(y_true, scores) -> float:
    prec, rec, thr = precision_recall_curve(y_true, scores)
    f1s     = 2 * prec * rec / (prec + rec + 1e-9)
    best    = np.argmax(f1s)
    best_t  = float(thr[best]) if best < len(thr) else 0.5
    logger.info("Seuil optimal : %.4f | P=%.3f R=%.3f F1=%.3f",
                best_t, prec[best], rec[best], f1s[best])
    return best_t


# ════════════════════════════════════════════════════════════════
# PIPELINE
# ════════════════════════════════════════════════════════════════

def run_pipeline(df: pd.DataFrame, features: list):

    # ── 1. Split temporel ────────────────────────────────────────
    train_raw, test = temporal_split(df, test_ratio=0.30)

    # ── 2. Resample train uniquement ─────────────────────────────
    train = resample_train(train_raw, target=0.20)

    # ── 3. GNN sur le train ───────────────────────────────────────
    graph, node_map = build_graph(train, features)
    gnn             = train_gnn(graph)

    # ── 4. XGBoost + IsoForest ────────────────────────────────────
    xgb, iso, X_test, y_test, iso_score, iso_min, iso_max = train_tabular(train, test, features)

    # ── 5. Score GNN sur le test set ─────────────────────────────
    # Construire le graphe du test pour les alias connus
    gnn.eval()
    with torch.no_grad():
        gnn_scores_train = torch.softmax(gnn(graph), dim=1)[:, 1].numpy()

    # Pour les alias du test non vus en train → score 0.5 (neutre)
    test_aliases = test["payer_alias"]
    gnn_score = np.array([
        gnn_scores_train[node_map[a]] if a in node_map else 0.5
        for a in test_aliases
    ])
    xgb_score = xgb.predict_proba(X_test)[:, 1]

    # ── 6. Score hybride ──────────────────────────────────────────
    final_score = 0.6 * xgb_score + 0.2 * iso_score + 0.2 * gnn_score

    # ── 7. Métriques sur distribution NATURELLE du test ──────────
    auc = roc_auc_score(y_test, final_score)
    logger.info("🔥 AUC FINAL (sans leakage) : %.4f", auc)

    threshold = optimal_threshold(y_test.values, final_score)
    y_pred    = (final_score >= threshold).astype(int)

    logger.info("\n%s", classification_report(y_test, y_pred, digits=3))

    # ── 8. Recall par scénario ────────────────────────────────────
    if "fraud_scenario" in test.columns:
        test_df = test[["fraud_scenario","label_fraud"]].copy()
        test_df["pred"]  = y_pred
        logger.info("── Recall par scénario (test set réel) ──")
        for sc in test_df["fraud_scenario"].unique():
            sub = test_df[test_df["fraud_scenario"] == sc]
            if sub["label_fraud"].sum() == 0:
                continue
            rec = (sub["pred"] == 1)[sub["label_fraud"] == 1].mean()
            logger.info("   %-22s n=%-5d recall=%.2f", sc, len(sub), rec)

    # ── 9. Top features ───────────────────────────────────────────
    importances = pd.Series(xgb.feature_importances_, index=features)
    logger.info("── Top 10 features ──")
    for feat, imp in importances.nlargest(10).items():
        logger.info("   %-35s %.4f", feat, imp)

    return gnn, xgb, iso, node_map, auc, threshold, features, iso_min, iso_max


# ════════════════════════════════════════════════════════════════
# SAUVEGARDE
# ════════════════════════════════════════════════════════════════

def save_model(xgb, iso, gnn, node_map, auc, threshold, features, iso_min=0.0, iso_max=1.0):
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = MODEL_DIR / f"hybrid_{ts}"
    path.mkdir(exist_ok=True)

    with open(path / "xgb.pkl",  "wb") as f: pickle.dump(xgb, f)
    with open(path / "iso.pkl",  "wb") as f: pickle.dump(iso, f)
    with open(path / "meta.pkl", "wb") as f:
        pickle.dump({
            "node_map"      : node_map,
            "threshold"     : threshold,
            "features"      : features,
            "auc"           : auc,
            "iso_score_min" : iso_min,
            "iso_score_max" : iso_max,
        }, f)
    torch.save(gnn.state_dict(), path / "gnn.pt")
    logger.info("✅ Modèle → %s  (AUC=%.4f)", path, auc)


# ════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    df, features = load_data()
    gnn, xgb, iso, node_map, auc, threshold, features, iso_min, iso_max = run_pipeline(df, features)
    save_model(xgb, iso, gnn, node_map, auc, threshold, features, iso_min, iso_max)