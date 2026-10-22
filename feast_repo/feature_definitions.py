# ============================================================
# FEATURE DEFINITIONS - PI-SPI FRAUD PLATFORM
# ============================================================

from datetime import timedelta

from feast import (
    Entity,
    FeatureView,
    Field,
    PushSource,
    FileSource
)

from feast.types import (
    Float32,
    Int32,
    String
)

# ============================================================
# ENTITY
# ============================================================
# L'entité centrale de notre plateforme de détection de fraude est l'alias du payeur (payer_alias),
payer_alias = Entity(
    name="payer_alias",
    join_keys=["payer_alias"],
)

# ============================================================
# DATA SOURCES
# ============================================================
# Ces sources de données sont des fichiers Parquet pré-construits,
#  qui contiennent les features extraites à partir des données brutes.
stream_file_source = FileSource(
    name="stream_file_source",
    path="data/stream_features.parquet",
    timestamp_field="event_timestamp",
)

behavioral_source = FileSource(
    name="behavioral_source",
    path="data/behavioral_features.parquet",
    timestamp_field="event_timestamp",
)

interbank_source = FileSource(
    name="interbank_source",
    path="data/interbank_features.parquet",
    timestamp_field="event_timestamp",
)

graph_source = FileSource(
    name="graph_source",
    path="data/graph_features.parquet",
    timestamp_field="event_timestamp",
)

training_source = FileSource(
    name="training_source",
    path="data/pispi_training_dataset.parquet",
    timestamp_field="event_timestamp",
)

# ============================================================
# STREAM PUSH SOURCE
# ============================================================
# Ce PushSource permet d'alimenter en temps réel les features extraites du flux de transactions,
stream_push_source = PushSource(
    name="stream_features_source",
    batch_source=stream_file_source,
)

# ============================================================
# STREAM FEATURES (REAL-TIME FRAUD SIGNALS)
# ============================================================
# Ces features sont extraites en temps réel à partir du flux de transactions, 
# et capturent les signaux de fraude immédiats liés à la transaction en cours.
stream_features = FeatureView(
    name="stream_features",
    entities=[payer_alias],
    ttl=timedelta(hours=2),
    schema=[

        # AMOUNT & VELOCITY
        Field(name="log_amount", dtype=Float32),
        Field(name="amount_zscore_short_window", dtype=Float32),

        Field(name="tx_count_1min", dtype=Int32),
        Field(name="tx_count_5min", dtype=Int32),
        Field(name="tx_count_10min", dtype=Int32),
        Field(name="avg_inter_tx_time", dtype=Float32),
        Field(name="burst_score", dtype=Float32),

        # TEMPORAL
        Field(name="time_since_last_tx", dtype=Float32),
        Field(name="is_night_time", dtype=Int32),

        # BEHAVIORAL SHIFT
        Field(name="channel_entropy_24h", dtype=Float32),
        Field(name="channel_switch_flag", dtype=Int32),
        Field(name="transaction_type_change_flag", dtype=Int32),

        # GEO
        Field(name="country_switch_flag", dtype=Int32),
        Field(name="is_cross_border", dtype=Int32),

        # INTERBANK
        Field(name="institution_switch_flag", dtype=Int32),

        # LATENCY
        Field(name="latency_zscore", dtype=Float32),
        Field(name="latency_spike_flag", dtype=Int32),

        # RETRY / FAILURE
        Field(name="failure_rate_10tx", dtype=Float32),
        Field(name="retry_pattern_flag", dtype=Int32),

        # TYPE
        Field(name="is_p2p_flag", dtype=Int32),
        Field(name="is_rtp_flag", dtype=Int32),
    ],
    source=stream_push_source
)

# ============================================================
# BEHAVIORAL FEATURES (USER HISTORY)
# ============================================================
# Ces features sont extraites de l'historique de transactions de chaque alias, et capturent les patterns comportementaux, 
# les changements de comportement, et les anomalies par rapport à l'historique de l'alias.
behavioral_features = FeatureView(
    name="behavioral_features",
    entities=[payer_alias],
    ttl=timedelta(days=30),
    schema=[

        Field(name="alias_age_days", dtype=Int32),

        Field(name="tx_count_7d", dtype=Int32),
        Field(name="tx_count_30d", dtype=Int32),

        Field(name="avg_amount_30d", dtype=Float32),
        Field(name="median_amount_30d", dtype=Float32),
        Field(name="std_amount_30d", dtype=Float32),

        Field(name="amount_zscore_30d", dtype=Float32),
        Field(name="amount_quantile", dtype=Float32),
        Field(name="amount_deviation_score", dtype=Float32),

        Field(name="unique_receivers_30d", dtype=Int32),
        Field(name="new_receiver_flag", dtype=Int32),
        Field(name="receiver_novelty_score", dtype=Float32),

        Field(name="in_out_ratio", dtype=Float32),

        Field(name="alias_reactivation_flag", dtype=Int32),

        Field(name="weekend_activity_ratio", dtype=Float32),
        Field(name="night_activity_ratio", dtype=Float32),
    ],
    source=behavioral_source
)

# ============================================================
# INTERBANK FEATURES
# ============================================================
# Ces features capturent les interactions entre institutions financières, les flux interbancaires,
# et les changements de comportement liés aux institutions.
interbank_features = FeatureView(
    name="interbank_features",
    entities=[payer_alias],
    ttl=timedelta(hours=24),
    schema=[

        Field(name="distinct_issuer_count_24h", dtype=Int32),
        Field(name="distinct_receiver_count_24h", dtype=Int32),

        Field(name="institution_switch_flag", dtype=Int32),
        Field(name="cross_institution_burst", dtype=Float32),

        Field(name="cross_country_flow_count", dtype=Int32),
        Field(name="cross_border_ratio", dtype=Float32),

        Field(name="new_institution_flag", dtype=Int32),
    ],
    source=interbank_source
)

# ============================================================
# GRAPH FEATURES (AML / NETWORK)
# ============================================================
# Ces features sont extraites d'un graphe de transactions construit à partir des données historiques,
# et alimenté par un script de feature engineering dédié (graph_features.py).
# Elles capturent des patterns de réseau, des anomalies structurelles, et des rôles potentiels dans le graphe de transactions.
graph_features = FeatureView(
    name="graph_features",
    entities=[payer_alias],
    ttl=timedelta(days=7),
    schema=[

        Field(name="alias_degree", dtype=Int32),
        Field(name="in_degree", dtype=Int32),
        Field(name="out_degree", dtype=Int32),
        Field(name="receiver_degree",      dtype=Int32),   

        Field(name="centrality_score", dtype=Float32),
        Field(name="pagerank_score", dtype=Float32),

        Field(name="community_size", dtype=Int32),

        Field(name="cycle_presence_flag", dtype=Int32),

        Field(name="collector_flag", dtype=Int32),
        Field(name="money_mule_score", dtype=Float32),

        Field(name="hub_score", dtype=Float32),
    ],
    source=graph_source
)

# ============================================================
# TRAINING FEATURES (ML)
# ============================================================
# Ce FeatureView regroupe toutes les features utilisées pour entraîner le modèle ML,
# ainsi que le label de fraude. Il est alimenté par un fichier de dataset d'entraînement pré-construit, 
# qui peut être régénéré à volonté à partir des données brutes et des scripts de feature engineering.
training_features = FeatureView(
    name="training_features",
    entities=[payer_alias],
    ttl=timedelta(days=365),
    schema=[

        Field(name="log_amount", dtype=Float32),
        Field(name="tx_count_1min", dtype=Int32),
        Field(name="burst_score", dtype=Float32),

        Field(name="failure_rate_10tx", dtype=Float32),
        Field(name="retry_pattern_flag", dtype=Int32),

        Field(name="is_cross_border", dtype=Int32),

        Field(name="amount_zscore_30d", dtype=Float32),
        Field(name="unique_receivers_30d", dtype=Int32),

        Field(name="alias_degree", dtype=Int32),
        Field(name="centrality_score", dtype=Float32),

        Field(name="institution_switch_flag", dtype=Int32),
        Field(name="latency_zscore", dtype=Float32),

        Field(name="label_fraud", dtype=Int32),
    ],
    source=training_source
)

# ============================================================
# ML FEATURE CONTRACT
# ============================================================
# Cette liste définit l'ordre des features utilisées par le modèle ML (XGBoost) pour faire ses prédictions.
ML_FEATURE_ORDER = [
    "log_amount",
    "tx_count_1min",
    "burst_score",
    "time_since_last_tx",
    "failure_rate_10tx",
    "retry_pattern_flag",
    "is_cross_border",
    "amount_zscore_30d",
    "unique_receivers_30d",
    "alias_degree",
    "centrality_score",
    "institution_switch_flag",
    "latency_zscore",
]