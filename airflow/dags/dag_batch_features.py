"""
dag_batch_features.py — Airflow DAG PI-SPI  v3
================================================
Fix XCom overflow : les DataFrames passent par MinIO
au lieu de XCom PostgreSQL (limite 1GB dépassée avec 66k tx).

Chaque task écrit son résultat dans MinIO et retourne
le chemin (string courte) via XCom.
"""

from datetime import datetime, timedelta

import networkx as nx
import pandas as pd
from airflow.decorators import dag, task
from feast import FeatureStore

DEFAULT_ARGS = {
    "owner":            "pispi",
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
}

MINIO_ENDPOINT  = "minio:9000"
MINIO_KEY       = "minioadmin"
MINIO_SECRET    = "minioadmin"
RAW_BUCKET      = "pispi-raw"
FEAT_BUCKET     = "pispi-features"
FEAST_REPO_PATH = "/opt/airflow/feast_repo"
WINDOW_30D      = timedelta(days=30)
WINDOW_24H      = timedelta(hours=24)


# ── Helpers ───────────────────────────────────────────────────

def _minio_client():
    from minio import Minio
    return Minio(MINIO_ENDPOINT, access_key=MINIO_KEY,
                 secret_key=MINIO_SECRET, secure=False)


def _ensure_bucket(client, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def _write_df(df: pd.DataFrame, path: str) -> str:
    """Écrit un DataFrame Parquet dans MinIO et retourne le chemin."""
    import io
    client = _minio_client()
    _ensure_bucket(client, FEAT_BUCKET)
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    client.put_object(FEAT_BUCKET, path, buf, buf.getbuffer().nbytes)
    return path


def _read_df(path: str) -> pd.DataFrame:
    """Lit un DataFrame Parquet depuis MinIO."""
    import io
    client = _minio_client()
    resp = client.get_object(FEAT_BUCKET, path)
    buf  = io.BytesIO(resp.read())
    resp.close(); resp.release_conn()
    return pd.read_parquet(buf)


def _load_raw_df() -> pd.DataFrame:
    import json
    client = _minio_client()
    rows = []
    for obj in client.list_objects(RAW_BUCKET, recursive=True):
        if not obj.object_name.endswith(".json"):
            continue
        resp    = client.get_object(RAW_BUCKET, obj.object_name)
        content = resp.read().decode("utf-8").strip()
        resp.close(); resp.release_conn()
        if not content:
            continue
        lines = [l.strip() for l in content.splitlines() if l.strip()]
        if len(lines) > 1:
            for line in lines:
                try:
                    rows.append(json.loads(line))
                except Exception:
                    pass
        else:
            try:
                data = json.loads(content)
                if isinstance(data, list):
                    rows.extend(data)
                else:
                    rows.append(data)
            except Exception:
                pass

    df = pd.DataFrame(rows)
    df["ts"] = pd.to_datetime(df["timestamp"], unit="ms")
    df.sort_values(["payer_alias", "ts"], inplace=True)
    return df


# ════════════════════════════════════════════════════════════════
# DAG DAILY
# ════════════════════════════════════════════════════════════════

@dag(
    dag_id="pispi_batch_features_daily",
    schedule="0 2 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["pispi", "features", "batch"],
)
def batch_features_daily():

    @task(multiple_outputs=False)
    def load_raw() -> str:
        """Charge les tx depuis MinIO → écrit Parquet → retourne le chemin."""
        df   = _load_raw_df()
        path = "tmp/raw_df.parquet"
        _write_df(df, path)
        print(f"✅ load_raw : {len(df)} transactions")
        return path

    @task(multiple_outputs=False)
    def compute_behavioral(raw_path: str) -> str:
        df         = _read_df(raw_path)
        df["ts"]   = pd.to_datetime(df["ts"])
        now        = df["ts"].max()
        cutoff_30d = now - WINDOW_30D
        df_30d     = df[df["ts"] >= cutoff_30d].copy()

        alias_first = df.groupby("payer_alias")["ts"].min().rename("first_tx_ts")
        grp         = df_30d.groupby("payer_alias")

        behavioral = pd.DataFrame({
            "tx_count_30d":         grp["transaction_id"].count(),
            "avg_amount_30d":       grp["amount"].mean(),
            "std_amount_30d":       grp["amount"].std().fillna(0),
            "unique_receivers_30d": grp["receiver_alias"].nunique(),
            "total_out_30d":        grp["amount"].sum(),
        })

        grp_in = df_30d.groupby("receiver_alias")["amount"].sum().rename("total_in_30d")
        behavioral = behavioral.join(grp_in, how="left").fillna({"total_in_30d": 0})
        behavioral["in_out_ratio"] = (
            behavioral["total_in_30d"] / (behavioral["total_out_30d"] + 1e-6)
        ).clip(0, 100)

        behavioral = behavioral.join(alias_first)
        behavioral["alias_age_days"] = (now - behavioral["first_tx_ts"]).dt.days

        known_receivers = df_30d.groupby("payer_alias")["receiver_alias"].apply(set)
        behavioral["unique_receivers_30d"] = known_receivers.apply(len)

        last_tx   = df.groupby("payer_alias")["ts"].max()
        prev_last = df[df["ts"] < cutoff_30d].groupby("payer_alias")["ts"].max()
        gap       = (last_tx - prev_last).dt.days
        behavioral["alias_reactivation_flag"] = (gap > 30).astype(int)

        behavioral["amount_zscore_30d"] = (
            df_30d.groupby("payer_alias")["amount"]
            .transform(lambda x: (x - x.mean()) / (x.std() + 1e-6))
            .groupby(df_30d["payer_alias"]).last()
        )
        behavioral["amount_quantile"] = (
            df_30d.groupby("payer_alias")["amount"]
            .transform(lambda x: x.rank(pct=True))
            .groupby(df_30d["payer_alias"]).last()
        )
        last_amount = df_30d.groupby("payer_alias")["amount"].last()
        behavioral["amount_deviation_score"] = (
            (last_amount - behavioral["avg_amount_30d"]) /
            (behavioral["std_amount_30d"] + 1e-6)
        ).abs()

        last_receiver = df_30d.groupby("payer_alias")["receiver_alias"].last()
        behavioral["new_receiver_flag"] = behavioral.index.map(
            lambda alias: int(
                last_receiver.get(alias) not in
                (known_receivers.get(alias, set()) - {last_receiver.get(alias)})
            )
        )

        behavioral["event_timestamp"] = now
        behavioral = behavioral.reset_index()

        keep_cols = [
            "payer_alias", "event_timestamp",
            "alias_age_days", "tx_count_30d", "avg_amount_30d",
            "amount_zscore_30d", "amount_quantile", "amount_deviation_score",
            "unique_receivers_30d", "new_receiver_flag",
            "in_out_ratio", "alias_reactivation_flag",
        ]
        path = "tmp/behavioral.parquet"
        _write_df(behavioral[keep_cols], path)
        print(f"✅ compute_behavioral : {len(behavioral)} alias")
        return path

    @task(multiple_outputs=False)
    def compute_graph(raw_path: str) -> str:
        df  = _read_df(raw_path)
        now = pd.to_datetime(df["ts"]).max()
        df  = df[pd.to_datetime(df["ts"]) >= now - WINDOW_30D]

        G = nx.DiGraph()
        for _, row in df.iterrows():
            src, dst, amt = row["payer_alias"], row["receiver_alias"], row["amount"]
            if G.has_edge(src, dst):
                G[src][dst]["weight"] += amt
                G[src][dst]["count"]  += 1
            else:
                G.add_edge(src, dst, weight=amt, count=1)

        in_degree  = dict(G.in_degree())
        out_degree = dict(G.out_degree())
        try:
            centrality = nx.pagerank(G, weight="weight", max_iter=100)
        except Exception:
            centrality = {n: 1 / max(len(G), 1) for n in G.nodes()}

        nodes_in_cycles = set()
        try:
            for cycle in nx.simple_cycles(G):
                nodes_in_cycles.update(cycle)
        except Exception:
            pass

        node_community_size = {}
        for community in nx.connected_components(G.to_undirected()):
            for node in community:
                node_community_size[node] = len(community)

        rows_graph = []
        for alias in G.nodes():
            in_d  = in_degree.get(alias, 0)
            out_d = out_degree.get(alias, 0)
            rows_graph.append({
                "payer_alias"        : alias,
                "alias_degree"       : in_d + out_d,
                "centrality_score"   : round(centrality.get(alias, 0.0), 6),
                "cycle_presence_flag": int(alias in nodes_in_cycles),
                "community_size"     : node_community_size.get(alias, 1),
                "collector_flag"     : int(in_d > 5 and out_d <= 1),
                "receiver_degree"    : in_d,
                "event_timestamp"    : now,
            })

        path = "tmp/graph.parquet"
        _write_df(pd.DataFrame(rows_graph), path)
        print(f"✅ compute_graph : {len(rows_graph)} nœuds")
        return path

    @task()
    def write_offline_store(behavioral_path: str, graph_path: str) -> None:
        import io
        client = _minio_client()
        _ensure_bucket(client, FEAT_BUCKET)

        for src, dst in [(behavioral_path, "behavioral/data.parquet"),
                         (graph_path,      "graph/data.parquet")]:
            resp = client.get_object(FEAT_BUCKET, src)
            buf  = io.BytesIO(resp.read())
            resp.close(); resp.release_conn()
            buf.seek(0)
            client.put_object(FEAT_BUCKET, dst, buf, buf.getbuffer().nbytes)

        print("✅ Offline store écrit : behavioral + graph")

@task()
def materialize_online() -> None:
    import io
    client = _minio_client()

    # Copier les Parquet dans feast_repo/data/ pour Feast offline store (type: file)
    for src, dst in [
        ("behavioral/data.parquet", "/opt/airflow/feast_repo/data/behavioral_features.parquet"),
        ("graph/data.parquet",      "/opt/airflow/feast_repo/data/graph_features.parquet"),
    ]:
        resp = client.get_object(FEAT_BUCKET, src)
        data = resp.read()
        resp.close(); resp.release_conn()
        with open(dst, "wb") as f:
            f.write(data)
        print(f"✅ Copié {src} → {dst}")

    store      = FeatureStore(repo_path=FEAST_REPO_PATH)
    end_date   = datetime.utcnow()
    start_date = end_date - timedelta(days=2)
    store.materialize(
        start_date=start_date,
        end_date=end_date,
        feature_views=["behavioral_features", "graph_features"],
    )
    print(f"✅ Feast matérialisé ({start_date} → {end_date})")


# ════════════════════════════════════════════════════════════════
# DAG HOURLY
# ════════════════════════════════════════════════════════════════

@dag(
    dag_id="pispi_batch_features_hourly",
    schedule="0 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["pispi", "features", "batch", "hourly"],
)
def batch_features_hourly():

    @task(multiple_outputs=False)
    def load_raw() -> str:
        df   = _load_raw_df()
        path = "tmp/raw_hourly.parquet"
        _write_df(df, path)
        print(f"✅ load_raw hourly : {len(df)} transactions")
        return path

    @task(multiple_outputs=False)
    def compute_interbank(raw_path: str) -> str:
        df     = _read_df(raw_path)
        df["ts"] = pd.to_datetime(df["ts"])
        now    = df["ts"].max()
        cutoff = now - WINDOW_24H
        df_24h = df[df["ts"] >= cutoff].copy()
        grp    = df_24h.groupby("payer_alias")

        interbank = pd.DataFrame({
            "distinct_issuer_count_24h":   grp["issuer_institution"].nunique(),
            "distinct_receiver_count_24h": grp["receiver_alias"].nunique(),
            "cross_country_flow_count":    grp.apply(
                lambda x: (x["issuer_country"] != x["receiver_country"]).sum()
            ),
        })
        interbank["institution_switch_flag"] = (interbank["distinct_issuer_count_24h"] > 1).astype(int)
        interbank["cross_institution_burst"] = (interbank["distinct_issuer_count_24h"] > 3).astype(int)
        interbank["event_timestamp"] = now
        interbank = interbank.reset_index()

        keep_cols = [
            "payer_alias", "event_timestamp",
            "distinct_issuer_count_24h", "distinct_receiver_count_24h",
            "institution_switch_flag", "cross_institution_burst",
            "cross_country_flow_count",
        ]
        path = "tmp/interbank.parquet"
        _write_df(interbank[keep_cols], path)
        print(f"✅ compute_interbank : {len(interbank)} alias")
        return path

@task()
def write_and_materialize(interbank_path: str) -> None:
    import io
    client = _minio_client()
    _ensure_bucket(client, FEAT_BUCKET)

    # Lire depuis MinIO
    resp = client.get_object(FEAT_BUCKET, interbank_path)
    data = resp.read()
    resp.close(); resp.release_conn()

    # Copier vers feast_repo/data/ (offline store type:file)
    with open("/opt/airflow/feast_repo/data/interbank_features.parquet", "wb") as f:
        f.write(data)

    # Copier aussi vers interbank/data.parquet
    buf = io.BytesIO(data)
    client.put_object(FEAT_BUCKET, "interbank/data.parquet", buf, len(data))

    store      = FeatureStore(repo_path=FEAST_REPO_PATH)
    end_date   = datetime.utcnow()
    start_date = end_date - timedelta(hours=2)
    store.materialize(
        start_date=start_date,
        end_date=end_date,
        feature_views=["interbank_features"],
    )
    print("✅ Interbank matérialisé")


# Enregistrement
daily_dag  = batch_features_daily()
hourly_dag = batch_features_hourly()