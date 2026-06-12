import pandas as pd
import os

# =========================================================
# CREATE DATA FOLDER
# =========================================================

os.makedirs("data", exist_ok=True)

payer = ["test"]
ts = [pd.Timestamp("2024-01-01")]

# =========================================================
# STREAM FEATURES (COMPLETE)
# =========================================================

df_stream = pd.DataFrame({
    "payer_alias": payer,
    "event_timestamp": ts,

    "log_amount": [0.0],
    "amount_zscore_short_window": [0.0],

    "tx_count_1min": [1],
    "tx_count_5min": [1],
    "tx_count_10min": [1],
    "avg_inter_tx_time": [60.0],
    "burst_score": [0.0],

    "time_since_last_tx": [60.0],
    "is_night_time": [0],

    "channel_entropy_24h": [0.1],
    "channel_switch_flag": [0],
    "transaction_type_change_flag": [0],

    "country_switch_flag": [0],
    "is_cross_border": [0],

    "institution_switch_flag": [0],

    "latency_zscore": [0.0],
    "latency_spike_flag": [0],

    "failure_rate_10tx": [0.0],
    "retry_pattern_flag": [0],

    "is_p2p_flag": [1],
    "is_rtp_flag": [0],
})

# =========================================================
# BEHAVIORAL FEATURES
# =========================================================

df_behavior = pd.DataFrame({
    "payer_alias": payer,
    "event_timestamp": ts,

    "alias_age_days": [10],

    "tx_count_7d": [5],
    "tx_count_30d": [20],

    "avg_amount_30d": [1000.0],
    "median_amount_30d": [900.0],
    "std_amount_30d": [200.0],

    "amount_zscore_30d": [0.0],
    "amount_quantile": [0.5],
    "amount_deviation_score": [0.0],

    "unique_receivers_30d": [3],
    "new_receiver_flag": [0],
    "receiver_novelty_score": [0.1],

    "in_out_ratio": [1.0],

    "alias_reactivation_flag": [0],

    "weekend_activity_ratio": [0.2],
    "night_activity_ratio": [0.1],
})

# =========================================================
# INTERBANK FEATURES
# =========================================================

df_interbank = pd.DataFrame({
    "payer_alias": payer,
    "event_timestamp": ts,

    "distinct_issuer_count_24h": [1],
    "distinct_receiver_count_24h": [1],

    "institution_switch_flag": [0],
    "cross_institution_burst": [0.0],

    "cross_country_flow_count": [0],
    "cross_border_ratio": [0.0],

    "new_institution_flag": [0],
})

# =========================================================
# GRAPH FEATURES
# =========================================================

df_graph = pd.DataFrame({
    "payer_alias": payer,
    "event_timestamp": ts,

    "alias_degree": [1],
    "in_degree": [0],
    "out_degree": [1],

    "centrality_score": [0.1],
    "pagerank_score": [0.1],

    "community_size": [1],

    "cycle_presence_flag": [0],

    "collector_flag": [0],
    "money_mule_score": [0.0],

    "hub_score": [0.1],
})

# =========================================================
# ✅ MERGE ALL (GLOBAL DATASET)
# =========================================================

df = df_stream.merge(df_behavior, on=["payer_alias", "event_timestamp"])
df = df.merge(df_interbank, on=["payer_alias", "event_timestamp"])
df = df.merge(df_graph, on=["payer_alias", "event_timestamp"])

# =========================================================
# ✅ TRAINING LABEL
# =========================================================

df["label_fraud"] = [0]

# =========================================================
# SAVE FILES FOR FEAST
# =========================================================

df_stream.to_parquet("data/stream_features.parquet", index=False)
df_behavior.to_parquet("data/behavioral_features.parquet", index=False)
df_interbank.to_parquet("data/interbank_features.parquet", index=False)
df_graph.to_parquet("data/graph_features.parquet", index=False)

# ✅ training dataset
df.to_parquet("data/pispi_training_dataset.parquet", index=False)

print("✅ ALL FEATURE FILES GENERATED SUCCESSFULLY")