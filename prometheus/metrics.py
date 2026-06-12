"""
metrics.py  ·  PI-SPI Prometheus Metrics
=========================================
Métriques Prometheus exposées sur GET /metrics pour Grafana.

UTILISATION DANS fraud_api.py :
    from prometheus.metrics import setup_monitoring, record_scoring, update_infra_gauges

    setup_monitoring(app)        # après app = FastAPI(...)

    # Dans /score :
    record_scoring(tx_dict, result, latency_s)

    # Dans /health :
    update_infra_gauges(FEAST_AVAILABLE, gnn_model is not None, len(history), n_tx)
"""

from __future__ import annotations

import threading
from collections import deque

from prometheus_client import Counter, Gauge, Histogram, make_asgi_app

# ════════════════════════════════════════════════════════════════
# A. SCORING
# ════════════════════════════════════════════════════════════════

transactions_total = Counter(
    "pispi_transactions_total",
    "Nombre total de transactions scorées",
    labelnames=["transaction_type", "issuer_country"],
)
fraud_detected_total = Counter(
    "pispi_fraud_detected_total",
    "Fraudes détectées (final_score >= threshold)",
    labelnames=["risk_level"],
)
score_histogram = Histogram(
    "pispi_fraud_score_distribution",
    "Distribution des scores finaux",
    buckets=[0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
)
score_avg_gauge  = Gauge("pispi_fraud_score_avg",  "Score moyen glissant (100 tx)")
fraud_rate_gauge = Gauge("pispi_fraud_rate",        "Taux de fraude actuel")

# ════════════════════════════════════════════════════════════════
# B. DÉCISIONS
# ════════════════════════════════════════════════════════════════

risk_level_total = Counter(
    "pispi_risk_level_total",
    "Décisions par niveau de risque",
    labelnames=["level"],
)
legitimate_total = Counter(
    "pispi_legitimate_total",
    "Transactions légitimes (is_fraud=False, 0 règle)",
)

# ════════════════════════════════════════════════════════════════
# C. RÈGLES MÉTIER
# ════════════════════════════════════════════════════════════════

rule_triggered_total = Counter(
    "pispi_rule_triggered_total",
    "Déclenchements par règle métier",
    labelnames=["rule_name"],
)
rules_fired_histogram = Histogram(
    "pispi_rules_fired_per_transaction",
    "Nombre de règles déclenchées par transaction",
    buckets=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12],
)
rule_score_histogram = Histogram(
    "pispi_rule_score_distribution",
    "Distribution des rule_scores (0-100)",
    buckets=[0, 20, 40, 60, 80, 100],
)
rule_alert_total = Counter(
    "pispi_rule_alert_total",
    "Transactions déclenchant au moins 1 règle métier",
)

# ════════════════════════════════════════════════════════════════
# D. MODÈLES
# ════════════════════════════════════════════════════════════════

xgb_score_avg = Gauge("pispi_xgb_score_avg",  "Score moyen XGBoost")
iso_score_avg = Gauge("pispi_iso_score_avg",  "Score moyen IsolationForest")
gnn_score_avg = Gauge("pispi_gnn_score_avg",  "Score moyen GNN")

xgb_score_histogram = Histogram(
    "pispi_xgb_score_distribution",
    "Distribution des scores XGBoost",
    buckets=[0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
)
scoring_latency = Histogram(
    "pispi_scoring_latency_seconds",
    "Latence scoring end-to-end (s)",
    buckets=[0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5],
)
feast_latency = Histogram(
    "pispi_feast_latency_seconds",
    "Latence get_online_features() Feast/Redis",
    buckets=[0.001, 0.005, 0.010, 0.025, 0.050, 0.100],
)

# ════════════════════════════════════════════════════════════════
# E. FEATURES STREAM
# ════════════════════════════════════════════════════════════════

feature_tx_count_1min_avg  = Gauge("pispi_feature_tx_count_1min_avg",  "Moyenne tx_count_1min")
feature_burst_score_avg    = Gauge("pispi_feature_burst_score_avg",    "Moyenne burst_score")
feature_amount_zscore_avg  = Gauge("pispi_feature_amount_zscore_avg",  "Moyenne amount_zscore_short_window")
feature_latency_zscore_avg = Gauge("pispi_feature_latency_zscore_avg", "Moyenne latency_zscore")
feature_failure_rate_avg   = Gauge("pispi_feature_failure_rate_avg",   "Moyenne failure_rate_10tx")
feature_cross_border_ratio = Gauge("pispi_feature_cross_border_ratio", "Proportion cross-border")

# ════════════════════════════════════════════════════════════════
# F. INFRASTRUCTURE
# ════════════════════════════════════════════════════════════════

feast_available_gauge = Gauge("pispi_feast_available",    "1 si Feast/Redis disponible")
gnn_active_gauge      = Gauge("pispi_gnn_active",         "1 si GNN chargé et actif")
aliases_in_memory     = Gauge("pispi_aliases_in_memory",  "Alias avec historique en RAM")
tx_in_memory          = Gauge("pispi_tx_in_memory_total", "Transactions totales en RAM")
feast_errors_total    = Counter("pispi_feast_errors_total",   "Erreurs appels Feast")
scoring_errors_total  = Counter("pispi_scoring_errors_total", "Erreurs scoring")

# ════════════════════════════════════════════════════════════════
# G. MINIO
# ════════════════════════════════════════════════════════════════

minio_alerts_saved_total = Counter("pispi_minio_alerts_saved_total", "Alertes sauvegardées MinIO")
minio_errors_total       = Counter("pispi_minio_errors_total",       "Erreurs MinIO")

# ════════════════════════════════════════════════════════════════
# FENÊTRE GLISSANTE INTERNE
# ════════════════════════════════════════════════════════════════

_WINDOW = 100
_lock                = threading.Lock()
_scores_window       : deque = deque(maxlen=_WINDOW)
_xgb_window          : deque = deque(maxlen=_WINDOW)
_iso_window          : deque = deque(maxlen=_WINDOW)
_gnn_window          : deque = deque(maxlen=_WINDOW)
_tx1min_window       : deque = deque(maxlen=_WINDOW)
_burst_window        : deque = deque(maxlen=_WINDOW)
_zscore_window       : deque = deque(maxlen=_WINDOW)
_lat_zscore_window   : deque = deque(maxlen=_WINDOW)
_failure_window      : deque = deque(maxlen=_WINDOW)
_cross_border_window : deque = deque(maxlen=_WINDOW)
_total_scored : int = 0
_total_fraud  : int = 0


def _avg(d: deque) -> float:
    return sum(d) / len(d) if d else 0.0


# ════════════════════════════════════════════════════════════════
# FONCTIONS PUBLIQUES
# ════════════════════════════════════════════════════════════════

def record_scoring(tx: dict, result: dict, latency: float) -> None:
    """Met à jour toutes les métriques après un scoring."""
    global _total_scored, _total_fraud

    final_score    = float(result.get("fraud_score", 0))
    xgb_s          = float(result.get("xgb_score", 0))
    iso_s          = float(result.get("iso_score", 0))
    gnn_s          = float(result.get("gnn_score", 0.5))
    risk_level     = str(result.get("risk_level", "LOW"))
    is_fraud       = bool(result.get("is_fraud", False))
    rule_triggers  = result.get("rule_triggers", [])
    rule_score_v   = int(result.get("rule_score", 0))
    rules_fired    = len(rule_triggers)
    tx_type        = str(tx.get("transaction_type", "UNKNOWN"))
    issuer_country = str(tx.get("issuer_country", "UNKNOWN"))
    is_cross       = int(
        tx.get("is_cross_border", 0) or
        tx.get("issuer_country", "") != tx.get("receiver_country", "")
    )
    stream_feats = result.get("features", {}).get("stream", result.get("features", {}))

    # A
    transactions_total.labels(transaction_type=tx_type, issuer_country=issuer_country).inc()
    score_histogram.observe(final_score)
    # B
    risk_level_total.labels(level=risk_level).inc()
    if is_fraud:
        fraud_detected_total.labels(risk_level=risk_level).inc()
    elif rules_fired == 0:
        legitimate_total.inc()
    # C
    for rule_name in rule_triggers:
        rule_triggered_total.labels(rule_name=rule_name).inc()
    rules_fired_histogram.observe(rules_fired)
    rule_score_histogram.observe(rule_score_v)
    if rules_fired > 0:
        rule_alert_total.inc()
    # D
    xgb_score_histogram.observe(xgb_s)
    scoring_latency.observe(latency)

    with _lock:
        _total_scored += 1
        if is_fraud:
            _total_fraud += 1
        _scores_window.append(final_score)
        _xgb_window.append(xgb_s)
        _iso_window.append(iso_s)
        _gnn_window.append(gnn_s)
        _cross_border_window.append(is_cross)
        _tx1min_window.append(float(stream_feats.get("tx_count_1min", 0)))
        _burst_window.append(float(stream_feats.get("burst_score", 0)))
        _zscore_window.append(float(stream_feats.get("amount_zscore_short_window", 0)))
        _lat_zscore_window.append(float(stream_feats.get("latency_zscore", 0)))
        _failure_window.append(float(stream_feats.get("failure_rate_10tx", 0)))

        score_avg_gauge.set(_avg(_scores_window))
        fraud_rate_gauge.set(_total_fraud / _total_scored)
        xgb_score_avg.set(_avg(_xgb_window))
        iso_score_avg.set(_avg(_iso_window))
        gnn_score_avg.set(_avg(_gnn_window))
        feature_tx_count_1min_avg.set(_avg(_tx1min_window))
        feature_burst_score_avg.set(_avg(_burst_window))
        feature_amount_zscore_avg.set(_avg(_zscore_window))
        feature_latency_zscore_avg.set(_avg(_lat_zscore_window))
        feature_failure_rate_avg.set(_avg(_failure_window))
        feature_cross_border_ratio.set(_avg(_cross_border_window))


def record_feast_call(latency: float, success: bool) -> None:
    """Enregistre un appel Feast. Appeler autour de _get_feast_features()."""
    feast_latency.observe(latency)
    if not success:
        feast_errors_total.inc()


def record_minio_alert(success: bool) -> None:
    """Enregistre une sauvegarde MinIO. Appeler dans _save_alert_minio()."""
    if success:
        minio_alerts_saved_total.inc()
    else:
        minio_errors_total.inc()


def update_infra_gauges(
    feast_ok: bool,
    gnn_ok: bool,
    n_aliases: int,
    n_tx_memory: int,
) -> None:
    """Met à jour les gauges infra. Appeler dans GET /health."""
    feast_available_gauge.set(1 if feast_ok else 0)
    gnn_active_gauge.set(1 if gnn_ok else 0)
    aliases_in_memory.set(n_aliases)
    tx_in_memory.set(n_tx_memory)


def setup_monitoring(app) -> None:
    """Monte /metrics sur l'app FastAPI. Appeler après app = FastAPI(...)."""
    app.mount("/metrics", make_asgi_app())