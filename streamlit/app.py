import io
import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st
from minio import Minio
from minio.error import S3Error

# ════════════════════════════════════════════════════════════════
# CONFIG
# ════════════════════════════════════════════════════════════════

# Les valeurs par défaut correspondent aux noms des services Docker Compose.
# Les variables d'environnement permettent aussi d'exécuter l'application hors Docker.
API_URL = os.getenv("API_URL", "http://fraud-api:8000").rstrip("/")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS = os.getenv("MINIO_ACCESS", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET", "minioadmin")
ALERTS_BUCKET = os.getenv("ALERTS_BUCKET", "pispi-alerts")
RAW_BUCKET = os.getenv("RAW_BUCKET", "pispi-raw")



st.set_page_config(
    page_title="PI-SPI Fraud Detection",
    page_icon="⬡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS dark mode SOC ─────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Syne:wght@400;600;700&display=swap');
html, body, [class*="css"] { font-family: 'Syne', sans-serif; background-color: #0a0c0f; color: #e8edf5; }
.stApp { background-color: #0a0c0f; }
section[data-testid="stSidebar"] { background: #111318; border-right: 1px solid #1e2530; }
div[data-testid="stMetric"] { background: #111318; border: 1px solid #1e2530; border-radius: 8px; padding: 16px; }
div[data-testid="stMetricValue"] { color: #e8edf5 !important; font-family: 'DM Mono'; }
div[data-testid="stMetricLabel"] { color: #5a6478 !important; font-size: 11px !important; text-transform: uppercase; letter-spacing: .1em; }
.stButton > button { background: #181c23; color: #7a8498; border: 1px solid #1e2530; border-radius: 4px; font-family: 'DM Mono', monospace; font-size: 12px; transition: .15s; width: 100%; }
.stButton > button:hover { background: rgba(0,212,255,.08); color: #00d4ff; border-color: #00d4ff; }
.stTextInput > div > div > input, .stNumberInput > div > div > input, .stSelectbox > div > div { background: #181c23 !important; color: #e8edf5 !important; border: 1px solid #252d3a !important; font-family: 'DM Mono', monospace !important; font-size: 13px !important; }
.stTabs [data-baseweb="tab-list"] { background: #111318; border-bottom: 1px solid #1e2530; }
.stTabs [data-baseweb="tab"] { color: #5a6478; font-family: 'DM Mono'; font-size: 12px; }
.stTabs [aria-selected="true"] { color: #00d4ff !important; border-bottom-color: #00d4ff !important; }
details { background: #111318; border: 1px solid #1e2530; border-radius: 6px; padding: 8px; }
summary { color: #7a8498; font-size: 13px; }
.badge { display: inline-block; padding: 3px 10px; border-radius: 3px; font-family: 'DM Mono', monospace; font-size: 11px; font-weight: 500; }
.badge-low      { background: rgba(46,213,115,.12); color: #2ed573; border: 1px solid rgba(46,213,115,.3); }
.badge-medium   { background: rgba(255,165,2,.10);  color: #ffa502; border: 1px solid rgba(255,165,2,.25); }
.badge-high     { background: rgba(255,71,87,.12);  color: #ff4757; border: 1px solid rgba(255,71,87,.3); }
.badge-critical { background: rgba(255,71,87,.20);  color: #ff4757; border: 1px solid rgba(255,71,87,.5); }
.rule-tag { display:inline-block; padding:2px 8px; border-radius:3px; margin:2px;
            background:rgba(255,71,87,.15); color:#ff4757; border:1px solid rgba(255,71,87,.3);
            font-family:'DM Mono'; font-size:11px; font-weight:500; }
.rule-ok  { background:rgba(46,213,115,.10); color:#2ed573; border:1px solid rgba(46,213,115,.2); }
</style>
""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════════
# MINIO CLIENT
# ════════════════════════════════════════════════════════════════

@st.cache_resource
def _minio_client():
    """Connexion MinIO unique — mise en cache pour toute la session."""
    try:
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS,
            secret_key=MINIO_SECRET,
            secure=False,
        )
        # Créer le bucket alertes s'il n'existe pas
        if not client.bucket_exists(ALERTS_BUCKET):
            client.make_bucket(ALERTS_BUCKET)
        return client
    except Exception:
        return None

minio = _minio_client()
MINIO_OK = minio is not None


def _save_alert_minio(alert: dict) -> None:
    """
    Persiste une alerte dans MinIO.
    Chemin : pispi-alerts/YYYY/MM/DD/alert_<uuid>.json
    Non bloquant — en cas d'échec MinIO, on continue.
    """
    if not MINIO_OK:
        return
    try:
        now  = datetime.utcnow()
        path = f"{now.strftime('%Y/%m/%d')}/alert_{uuid.uuid4().hex[:12]}.json"
        data = json.dumps(alert, ensure_ascii=False).encode("utf-8")
        minio.put_object(
            ALERTS_BUCKET, path,
            io.BytesIO(data), len(data),
            content_type="application/json",
        )
    except Exception:
        pass  # silencieux — ne pas casser l'UI si MinIO est down


@st.cache_data(ttl=300)  # recharge depuis MinIO toutes les 5 minutes
def _load_alerts_minio(days: int = 1) -> List[dict]:
    """
    Charge les alertes des derniers N jours depuis MinIO.
    Mis en cache 5 minutes pour ne pas saturer MinIO à chaque interaction.
    """
    if not MINIO_OK:
        return []
    alerts = []
    try:
        for obj in minio.list_objects(ALERTS_BUCKET, recursive=True):
            if not obj.object_name.endswith(".json"):
                continue
            resp = minio.get_object(ALERTS_BUCKET, obj.object_name)
            alerts.append(json.loads(resp.read().decode("utf-8")))
            resp.close(); resp.release_conn()
    except Exception:
        pass
    # Trier par timestamp décroissant
    return sorted(alerts, key=lambda x: x.get("ts", 0), reverse=True)


@st.cache_data(ttl=60)
def _load_raw_stats() -> dict:
    """
    Lit les stats globales depuis MinIO pispi-raw.
    Mis en cache 1 minute.
    """
    if not MINIO_OK:
        return {}
    try:
        total_files = sum(1 for _ in minio.list_objects(RAW_BUCKET, recursive=True))
        return {"total_files_raw": total_files}
    except Exception:
        return {}


# ════════════════════════════════════════════════════════════════
# SCÉNARIOS
# ════════════════════════════════════════════════════════════════

SCENARIOS: Dict[str, Dict[str, Any]] ={
    "🟢 Transaction normale": {
        "payer_alias"   : "als_c5671f46e2",   
        "receiver_alias": "als_b0db9e7d51",
        "amount"        : 15000.0,
        "issuer_country": "SN",
        "receiver_country": "SN",
        "device"        : "ANDROID",
        "transaction_type": "P2P",
    },
    "🔴 Burst Fraud": {
        "payer_alias"   : "als_d96eb7dae1",   
        "receiver_alias": "als_4dabfc9140",
        "amount"        : 22000.0,
        "issuer_country": "SN",
        "receiver_country": "SN",
        "device"        : "ANDROID",
        "transaction_type": "P2P",
    },
    "🔴 Mule Account": {
        "payer_alias"   : "als_5da479bc7c",   
        "receiver_alias": "als_50514661be",
        "amount"        : 45000.0,
        "issuer_country": "SN",
        "receiver_country": "SN",
        "device"        : "ANDROID",
        "transaction_type": "P2P",
    },
    "🔴 Account Takeover": {
        "payer_alias"   : "als_6cd6002b68",   # HIGH_RISK_USER — 418 tx
        "receiver_alias": "als_ae44b2cd57",
        "amount"        : 620000.0,
        "issuer_country": "SN",
        "receiver_country": "GH",
        "device"        : "UNKNOWN_ANDROID",
        "transaction_type": "TRANSFER",
        "response_time_ms": 1000.0,
        "new_device_flag": 1,
    },
    "🔴 Cross Border Fraud": {
        "payer_alias"   : "als_adef31858e",   
        "receiver_alias": "als_6f58b06c8c",
        "amount"        : 750000.0,
        "issuer_country": "SN",
        "receiver_country": "NG",
        "device"        : "IOS",
        "transaction_type": "TRANSFER",
        "is_cross_border": 1,
    },
    "🔴 Structuring": {
        "payer_alias"   : "als_4dabfc9140",  
        "receiver_alias": "als_d96eb7dae1",
        "amount"        : 48000.0,
        "issuer_country": "SN",
        "receiver_country": "SN",
        "device"        : "ANDROID",
        "transaction_type": "P2P",
    },
    "🔴 High Net Worth": {
        "payer_alias"   : "als_4dae8edd4b",  
        "receiver_alias": "als_5f4eb58139",
        "amount"        : 850000.0,
        "issuer_country": "CI",
        "receiver_country": "CI",
        "device"        : "IOS",
        "transaction_type": "TRANSFER",
    },
    "🟡 Sleeper Account": {
        "payer_alias"   : "als_aea4fa2f74",   
        "receiver_alias": "als_e2dba06490",
        "amount"        : 120000.0,
        "issuer_country": "SN",
        "receiver_country": "SN",
        "device"        : "FEATURE_PHONE",
        "transaction_type": "P2P",
    },
}

# ════════════════════════════════════════════════════════════════
# SESSION STATE
# ════════════════════════════════════════════════════════════════
# Initialisation du session state de Streamlit pour stocker le résultat du dernier scoring, 
# les alertes de la session, le total de transactions scorées, le total de fraudes détectées, 
# le formulaire en cours, et le filtre d'alias pour l'historique.
for k, v in {
    "last_result"   : None,
    "session_alerts": [],      # alertes de la session (RAM)
    "total_scored"  : 0,
    "total_fraud"   : 0,
    "form"          : SCENARIOS["🟢 Transaction normale"],
    "alias_filter"  : "",
}.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ════════════════════════════════════════════════════════════════
# HELPERS API
# ════════════════════════════════════════════════════════════════
# Ces fonctions facilitent les appels à l'API de scoring, la gestion des alertes, et la lecture des features.
@st.cache_data(ttl=10)
def api_health() -> Optional[dict]:
    try:
        r = requests.get(f"{API_URL}/health", timeout=2)
        return r.json() if r.ok else None
    except Exception:
        return None


def call_predict(payload: dict) -> Optional[dict]:
    try:
        r = requests.post(f"{API_URL}/score", json=payload, timeout=10)
        return r.json() if r.ok else {"error": r.text}
    except Exception as e:
        return {"error": str(e)}


def risk_color(level: str) -> str:
    return {"LOW":"#2ed573","MEDIUM":"#ffa502","HIGH":"#ff4757","CRITICAL":"#ff4757"}.get(level,"#7a8498")

# ════════════════════════════════════════════════════════════════
# CHARTS
# ════════════════════════════════════════════════════════════════
# Ces fonctions génèrent des graphiques Plotly pour visualiser le score de fraude, les importances SHAP, les tendances, etc.
def gauge_chart(score: float, threshold: float) -> go.Figure:
    color = "#ff4757" if score >= threshold else "#ffa502" if score >= threshold * 0.7 else "#2ed573"
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=round(score * 100, 1),
        number={"suffix":"%","font":{"size":36,"color":"#e8edf5","family":"DM Mono"}},
        gauge={
            "axis":{"range":[0,100],"tickcolor":"#3a4455","tickfont":{"color":"#5a6478","size":10}},
            "bar":{"color":color,"thickness":.22},
            "bgcolor":"#181c23","bordercolor":"#1e2530",
            "steps":[
                {"range":[0,threshold*70],"color":"rgba(46,213,115,.06)"},
                {"range":[threshold*70,threshold*100],"color":"rgba(255,165,2,.06)"},
                {"range":[threshold*100,100],"color":"rgba(255,71,87,.06)"},
            ],
            "threshold":{"line":{"color":"#7a8498","width":2},"thickness":.8,"value":threshold*100},
        },
    ))
    fig.update_layout(height=220,margin=dict(t=20,b=0,l=30,r=30),
                      paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)")
    return fig


def bar_chart(xgb: float, iso: float, gnn: float, final: float) -> go.Figure:
    labels = ["XGBoost","IsoForest","GNN","Final"]
    values = [xgb,iso,gnn,final]
    colors = ["#378ADD","#1D9E75","#7F77DD","#00d4ff"]
    fig = go.Figure(go.Bar(
        x=labels,y=values,marker_color=colors,
        text=[f"{v:.3f}" for v in values],textposition="outside",
        textfont={"family":"DM Mono","size":12,"color":"#e8edf5"},
    ))
    fig.update_layout(
        height=200,margin=dict(t=20,b=0,l=0,r=0),
        paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
        yaxis={"range":[0,1.15],"gridcolor":"#1e2530","tickfont":{"color":"#5a6478"}},
        xaxis={"tickfont":{"color":"#7a8498","size":11}},showlegend=False,
    )
    return fig

# Graphe SHAP — affiche les 8 features les plus importantes 
# avec leur valeur SHAP (importance) et une couleur rouge/vert 
# selon l'impact sur le score de fraude.
def shap_chart(top_features: List[dict]) -> go.Figure:
    """Graphe SHAP — lit 'importance' (champ API v4.1)."""
    if not top_features:
        return go.Figure()
    # Correction : l'API retourne "importance", pas "shap"
    names  = [f["feature"]    for f in top_features[:8]]
    vals   = [f.get("importance", f.get("shap", 0)) for f in top_features[:8]]
    colors = ["#ff4757" if v > 0 else "#2ed573" for v in vals]
    fig = go.Figure(go.Bar(
        y=names[::-1],x=vals[::-1],orientation="h",
        marker_color=colors[::-1],
        text=[f"{v:+.4f}" for v in vals[::-1]],
        textposition="outside",
        textfont={"family":"DM Mono","size":11,"color":"#e8edf5"},
    ))
    fig.add_vline(x=0,line_color="#3a4455",line_width=1)
    fig.update_layout(
        height=280,margin=dict(t=10,b=0,l=0,r=60),
        paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
        xaxis={"gridcolor":"#1e2530","tickfont":{"color":"#5a6478"}},
        yaxis={"tickfont":{"color":"#7a8498","size":11}},showlegend=False,
    )
    return fig

# Graphe de tendance des scores de fraude pour les dernières transactions du même alias,
# afin de détecter des patterns de montée en puissance (ex: burst fraud).
def trend_chart(alerts: list) -> go.Figure:
    if len(alerts) < 2:
        return go.Figure()
    scores = [a["score"] for a in alerts]
    fig = go.Figure(go.Scatter(
        x=list(range(1, len(scores)+1)), y=scores,
        mode="lines+markers",
        line={"color":"#00d4ff","width":2},
        marker={"color":"#00d4ff","size":6},
        fill="tozeroy",fillcolor="rgba(0,212,255,.05)",
    ))
    if st.session_state.last_result:
        thr = st.session_state.last_result.get("threshold", 0.7)
        fig.add_hline(y=thr,line_color="#ff4757",line_dash="dash",line_width=1,
                      annotation_text="seuil",annotation_font_color="#ff4757")
    fig.update_layout(
        height=200,margin=dict(t=10,b=0,l=0,r=0),
        paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
        xaxis={"gridcolor":"#1e2530","tickfont":{"color":"#5a6478"}},
        yaxis={"range":[0,1.05],"gridcolor":"#1e2530","tickfont":{"color":"#5a6478"}},
        showlegend=False,
    )
    return fig


# ════════════════════════════════════════════════════════════════
# SIDEBAR
# ════════════════════════════════════════════════════════════════
# Le sidebar contient :
# - un header avec le logo et le nom de la plateforme 
# - un indicateur de santé de l'API et de MinIO
# - une section de boutons pour pré-remplir le formulaire avec des scénarios types (fraude normale, burst fraud, mule account, etc.)
# - un formulaire pour saisir les détails d'une transaction à scorer (alias, montant, pays, etc.)
with st.sidebar:
    st.markdown("""
    <div style="display:flex;align-items:center;gap:12px;padding:8px 0 20px">
      <div style="width:28px;height:28px;background:#00d4ff;clip-path:polygon(50% 0%,100% 25%,100% 75%,50% 100%,0% 75%,0% 25%);flex-shrink:0"></div>
      <div>
        <div style="font-size:14px;font-weight:700;letter-spacing:.05em;color:#e8edf5">PI-SPI</div>
        <div style="font-size:10px;color:#5a6478;font-family:'DM Mono'">FRAUD DETECTION v2</div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    health = api_health()
    minio_dot = "#2ed573" if MINIO_OK else "#ff4757"
    api_dot   = "#2ed573" if health    else "#ff4757"
    st.markdown(f"""
    <div style="font-family:'DM Mono';font-size:11px;color:#5a6478;margin-bottom:16px;line-height:2">
        <div style="display:flex;align-items:center;gap:8px">
            <div style="width:7px;height:7px;border-radius:50%;background:{api_dot};box-shadow:0 0 6px {api_dot}"></div>
            API {'OK · ' + str(health.get('features_count',0)) + ' features · seuil ' + str(round(health.get('threshold',0),4)) if health else 'hors ligne'}
        </div>
        <div style="display:flex;align-items:center;gap:8px">
            <div style="width:7px;height:7px;border-radius:50%;background:{minio_dot}"></div>
            MinIO {'connecté · ' + ALERTS_BUCKET if MINIO_OK else 'hors ligne'}
        </div>
        {'<div style="display:flex;align-items:center;gap:8px"><div style="width:7px;height:7px;border-radius:50%;background:#7F77DD"></div>GNN ' + ("actif" if health and health.get("gnn_active") else "inactif (fallback 0.5)") + "</div>" if health else ""}
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<div style='font-family:\"DM Mono\";font-size:10px;letter-spacing:.1em;color:#3a4455;text-transform:uppercase;margin-bottom:8px'>Scénarios</div>", unsafe_allow_html=True)
    for sc_name in SCENARIOS:
        if st.button(sc_name, key=f"sc_{sc_name}"):
            st.session_state.form = SCENARIOS[sc_name].copy()
            st.rerun()

    st.divider()
    st.markdown("<div style='font-family:\"DM Mono\";font-size:10px;letter-spacing:.1em;color:#3a4455;text-transform:uppercase;margin-bottom:8px'>Transaction</div>", unsafe_allow_html=True)

    f = st.session_state.form
    payer_alias          = st.text_input("payer_alias",       value=f.get("payer_alias",""))
    receiver_alias       = st.text_input("receiver_alias",    value=f.get("receiver_alias",""))
    amount               = st.number_input("amount (XOF)",    value=float(f.get("amount",15000)), min_value=0.0, step=1000.0)

    c1,c2 = st.columns(2)
    with c1:
        uemoa = ["SN","CI","ML","BF","BJ","TG","GW","NE"]
        issuer_country = st.selectbox("issuer_country", uemoa,
            index=uemoa.index(f.get("issuer_country","SN")) if f.get("issuer_country","SN") in uemoa else 0)
    with c2:
        all_countries = ["SN","CI","ML","BF","BJ","TG","GW","NE","NG","GH","CM","FR","US", "CN", "AE"]
        rc = f.get("receiver_country","SN")
        receiver_country = st.selectbox("receiver_country", all_countries,
            index=all_countries.index(rc) if rc in all_countries else 0)

    c3,c4 = st.columns(2)
    institutions = ["CBAO-SN", "SGBS-SN", "BIS-SN", "WAVE-SN", "ORANGE-MONEY-SN", "FREE-MONEY-SN",
                    "MTN-MOMO-CI", "ORANGE-MONEY-CI", "WAVE-CI", "SOGEMAT-ML", "BMS-ML", "BDM-ML",
                     "CBI-BF", "MOOV-BF","BDU-GW", "ORANGE-GW","ORANGE-BF","BOA-BJ", 
                     "MTN-BJ", "MOOV-BJ","ORANGE-BJ","ORANGE-TG","MOOV-TG","ORANGE-GW","ORANGE-NE",
                     "BIA-NE", "AIRTEL-NE","MOBILE-MONEY-CM","ORANGE-CM","WAVE-CM","ORANGE-FR","WAVE-FR",
                     "ORANGE-US","WAVE-US","ORANGE-CN","WAVE-CN","ZENITH-NG", "GTB-NG", "UBA-NG", "ACCESS-NG"]
    with c3:
        ii = f.get("issuer_institution","WAVE-SN")
        issuer_institution = st.selectbox("issuer_inst.", institutions,
            index=institutions.index(ii) if ii in institutions else 0)
    with c4:
        ri = f.get("receiver_institution","WAVE-SN")
        receiver_institution = st.selectbox("receiver_inst.", institutions,
            index=institutions.index(ri) if ri in institutions else 0)

    c5,c6 = st.columns(2)
    devices   = ["ANDROID","IOS","WEB","USSD","UNKNOWN_ANDROID"]
    tx_types  = ["P2P","TRANSFER","RTP","P2M","P2G"]
    with c5:
        d = f.get("device","ANDROID")
        device = st.selectbox("device", devices, index=devices.index(d) if d in devices else 0)
    with c6:
        tt = f.get("transaction_type","P2P")
        transaction_type = st.selectbox("tx_type", tx_types, index=tx_types.index(tt) if tt in tx_types else 0)

    response_time_ms = st.number_input("latency (ms)", value=float(f.get("response_time_ms",220)), min_value=0.0)
    new_device_flag  = int(device.startswith("UNKNOWN"))

    score_clicked = st.button("⬡  Scorer", key="scorer", type="primary")

# ════════════════════════════════════════════════════════════════
# HEADER
# ════════════════════════════════════════════════════════════════

st.markdown("""
<div style="border-bottom:1px solid #1e2530;padding-bottom:20px;margin-bottom:24px">
  <h1 style="font-size:22px;font-weight:700;color:#e8edf5;margin:0;letter-spacing:.03em">
    PI-SPI Fraud Detection Platform
  </h1>
  <p style="color:#5a6478;font-size:13px;margin:4px 0 0;font-family:'DM Mono'">
    Real-Time Fraud Scoring · UEMOA / BCEAO · ML + Rules + MinIO History
  </p>
</div>
""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════════
# SCORING
# ════════════════════════════════════════════════════════════════
# Lorsqu'on clique sur "Scorer", on construit le payload à partir du formulaire,
# on appelle l'API /score, puis on affiche le résultat et on stocke l'alerte 
# dans la session et dans MinIO si c'est une fraude ou une règle déclenchée.
if score_clicked:
    payload = {
        "payer_alias"         : payer_alias,
        "receiver_alias"      : receiver_alias,
        "amount"              : amount,
        "timestamp"           : int(datetime.now(timezone.utc).timestamp() * 1000),
        "issuer_country"      : issuer_country,
        "receiver_country"    : receiver_country,
        "issuer_institution"  : issuer_institution,
        "receiver_institution": receiver_institution,
        "device"              : device,
        "transaction_type"    : transaction_type,
        "response_time_ms"    : response_time_ms,
        "new_device_flag"     : new_device_flag,
        "is_cross_border"     : int(issuer_country != receiver_country),
        "is_p2p"              : int(transaction_type == "P2P"),
        "is_rtp"              : int(transaction_type == "RTP"),
    }
    with st.spinner("Scoring…"):
        result = call_predict(payload)

    if "error" in result:
        st.error(f"❌ {result['error']}")
    else:
        st.session_state.last_result   = result
        st.session_state.total_scored += 1
        if result.get("is_fraud"):
            st.session_state.total_fraud += 1

        alert_record = {
            "ts"           : payload["timestamp"],
            "time"         : datetime.now().strftime("%H:%M:%S"),
            "alias"        : payer_alias,
            "amount"       : amount,
            "score"        : result["fraud_score"],
            "ml_score"     : result.get("ml_score", result["fraud_score"]),
            "rule_score"   : result.get("rule_score", 0),
            "rule_triggers": result.get("rule_triggers", []),
            "risk"         : result["risk_level"],
            "is_fraud"     : result.get("is_fraud", False),
            "explanations" : result.get("explanations", [])[:3],
        }
        st.session_state.session_alerts.append(alert_record)

        # Persister dans MinIO si c'est une alerte (fraude OU règle déclenchée)
        if result.get("is_fraud") or result.get("rules_fired", 0) > 0:
            _save_alert_minio(alert_record)

# ════════════════════════════════════════════════════════════════
# TABS
# ════════════════════════════════════════════════════════════════

tab_score, tab_monitor, tab_history = st.tabs([
    "⬡  Scoring",
    "📊  Monitoring",
    "🗄️  Historique MinIO",
])

# ══════════════════════ TAB SCORING ════════════════════════════
# Affiche le résultat du scoring : niveau de risque, score final, scores ML et règles,
# règles métier déclenchées, explications SHAP, et tendance des scores précédents 
# pour le même alias.
with tab_score:
    res = st.session_state.last_result

    if res is None:
        st.markdown("""
        <div style="display:flex;flex-direction:column;align-items:center;justify-content:center;
            height:300px;gap:12px;color:#3a4455;font-family:'DM Mono';font-size:12px;text-align:center">
            <div style="font-size:40px;opacity:.3">⬡</div>
            <div>Sélectionnez un scénario et cliquez sur Scorer</div>
        </div>""", unsafe_allow_html=True)
    else:
        level     = res["risk_level"]
        score     = res["final_score"]
        threshold = res["threshold"]
        color     = risk_color(level)
        ml_score  = res.get("ml_score", score)
        rule_score= res.get("rule_score", 0)
        triggers  = res.get("rule_triggers", [])
        gnn_active= res.get("gnn_active", False)

        # ── Gauge + Verdict + Métriques ──────────────────────
        col_g, col_v, col_m = st.columns([1, 1, 1])

        with col_g:
            st.markdown("<div style='font-family:\"DM Mono\";font-size:10px;letter-spacing:.1em;color:#5a6478;text-transform:uppercase;margin-bottom:4px'>Risque global</div>", unsafe_allow_html=True)
            st.plotly_chart(gauge_chart(score, threshold), width='stretch', config={"displayModeBar":False})

        with col_v:
            badge_cls = f"badge-{level.lower()}"
            decision  = "🚨 FRAUDE DÉTECTÉE" if res["is_fraud"] else "✓ LÉGITIME"
            gnn_badge = f'<span style="background:rgba(127,119,221,.15);color:#7F77DD;border:1px solid rgba(127,119,221,.3);padding:2px 7px;border-radius:3px;font-family:\'DM Mono\';font-size:10px">GNN {"actif" if gnn_active else "fallback"}</span>'
            st.markdown(f"""
            <div style="padding:16px 0">
                <div style="font-family:'DM Mono';font-size:10px;letter-spacing:.1em;color:#5a6478;text-transform:uppercase;margin-bottom:8px">Décision</div>
                <span class="badge {badge_cls}" style="font-size:13px;padding:8px 18px">{level}</span>
                &nbsp;{gnn_badge}
                <div style="font-size:15px;font-weight:600;color:{color};margin-top:14px">{decision}</div>
                <div style="font-family:'DM Mono';font-size:11px;color:#5a6478;margin-top:6px">
                    final = <span style="color:#e8edf5">{score:.4f}</span>
                    · ml = <span style="color:#378ADD">{ml_score:.4f}</span>
                    · rules = <span style="color:#ff4757">{rule_score}</span>/100
                </div>
                <div style="font-family:'DM Mono';font-size:10px;color:#3a4455;margin-top:6px">
                    {res.get('latency_ms',0):.1f}ms
                </div>
            </div>
            """, unsafe_allow_html=True)

        with col_m:
            st.metric("Final Score",  f"{res['final_score']:.4f}")
            st.metric("ML Score",     f"{ml_score:.4f}")
            st.metric("Rule Score",   f"{rule_score}/100")

        # ── Règles métier déclenchées ────────────────────────
        st.divider()
        st.markdown("<div style='font-family:\"DM Mono\";font-size:10px;letter-spacing:.1em;color:#5a6478;text-transform:uppercase;margin-bottom:8px'>Règles métier (12 évaluées)</div>", unsafe_allow_html=True)

        all_rule_names = [
            "BURST","LATENCY","RETRY","CROSS_BORDER","ACCOUNT_TAKEOVER",
            "MULE","STRUCTURING","COUNTRY_HOP","AMOUNT_SPIKE",
            "SLEEPER","RTP_BURST","NEW_DEVICE",
        ]
        rule_html = ""
        for r in all_rule_names:
            if r in triggers:
                rule_html += f'<span class="rule-tag">{r} ✗</span>'
            else:
                rule_html += f'<span class="rule-tag rule-ok">{r} ✓</span>'
        st.markdown(f"<div style='line-height:2.2'>{rule_html}</div>", unsafe_allow_html=True)

        if triggers:
            for reason in res.get("rule_reasons", []):
                st.markdown(f"<div style='padding:6px 12px;margin-top:4px;background:#111318;border-left:3px solid #ff4757;border-radius:0 4px 4px 0;font-size:12px;color:#ff4757;font-family:\"DM Mono\"'>{reason}</div>", unsafe_allow_html=True)

        # ── Scores modèles + SHAP ────────────────────────────
        st.divider()
        col_bar, col_shap = st.columns([1, 1])

        with col_bar:
            st.markdown("<div style='font-family:\"DM Mono\";font-size:10px;letter-spacing:.1em;color:#5a6478;text-transform:uppercase;margin-bottom:4px'>Scores par modèle</div>", unsafe_allow_html=True)
            st.plotly_chart(bar_chart(res["xgb_score"],res["iso_score"],res["gnn_score"],res["final_score"]),
                            width='stretch',config={"displayModeBar":False})
            c1,c2,c3 = st.columns(3)
            c1.metric("XGBoost",  f"{res['xgb_score']:.4f}")
            c2.metric("IsoForest",f"{res['iso_score']:.4f}")
            c3.metric("GNN",      f"{res['gnn_score']:.4f}")

        with col_shap:
            st.markdown("<div style='font-family:\"DM Mono\";font-size:10px;letter-spacing:.1em;color:#5a6478;text-transform:uppercase;margin-bottom:4px'>SHAP — top features XGBoost</div>", unsafe_allow_html=True)
            top_feats = res.get("top_features",[])
            if top_feats:
                st.plotly_chart(shap_chart(top_feats), width='stretch', config={"displayModeBar":False})
            else:
                st.info("SHAP non disponible (numpy incompatible ou non configuré)")

        # ── Features stream + feast ──────────────────────────
        st.divider()
        col_e, col_f = st.columns([1, 1])

        with col_e:
            st.markdown("<div style='font-family:\"DM Mono\";font-size:10px;letter-spacing:.1em;color:#5a6478;text-transform:uppercase;margin-bottom:8px'>Explications</div>", unsafe_allow_html=True)
            for expl in res.get("explanations",[]):
                st.markdown(f"<div style='padding:8px 12px;margin-bottom:6px;background:#111318;border-left:3px solid #00d4ff;border-radius:0 4px 4px 0;font-size:13px;color:#c8d0e0'>{expl}</div>", unsafe_allow_html=True)

        with col_f:
            st.markdown("<div style='font-family:\"DM Mono\";font-size:10px;letter-spacing:.1em;color:#5a6478;text-transform:uppercase;margin-bottom:8px'>Feature Store</div>", unsafe_allow_html=True)
            # Correction : lire dans stream ET feast imbriqués
            feats_raw  = res.get("features", {})
            feats_stream = feats_raw.get("stream", feats_raw)  # fallback si non imbriqué
            feats_feast  = feats_raw.get("feast", {})
            feast_ok     = feats_raw.get("feast_available", False)

            key_feats = [
                ("tx_count_1min",             "Stream"),
                ("burst_score",               "Stream"),
                ("latency_zscore",            "Stream"),
                ("amount_zscore_short_window","Stream"),
                ("failure_rate_10tx",         "Stream"),
                ("retry_pattern_flag",        "Stream"),
                ("tx_count_30d",              "Feast 30j" if feast_ok else "Feast N/A"),
                ("unique_receivers_30d",      "Feast 30j" if feast_ok else "Feast N/A"),
                ("alias_age_days",            "Feast 30j" if feast_ok else "Feast N/A"),
                ("receiver_degree",           "Feast graph" if feast_ok else "Feast N/A"),
                ("collector_flag",            "Feast graph" if feast_ok else "Feast N/A"),
            ]
            rows = []
            for fname, group in key_feats:
                val = feats_stream.get(fname) or feats_feast.get(fname, 0)
                rows.append({"Feature":fname,"Source":group,"Value":round(float(val),4)})
            st.dataframe(pd.DataFrame(rows), width='stretch', hide_index=True, height=300)


# ══════════════════════ TAB MONITORING ═════════════════════════
# Affiche les métriques globales de la session en cours (transactions scorées, fraudes détectées, taux de fraude),
# les métriques de l'API (transactions en mémoire, alertes dernière heure, statut GNN), 
# les règles métier les plus déclenchées, et la liste des alertes de la session avec leurs détails.
with tab_monitor:
    total  = st.session_state.total_scored
    fraud  = st.session_state.total_fraud
    rate   = fraud / max(total,1) * 100
    n_sess = len(st.session_state.session_alerts)

    k1,k2,k3,k4 = st.columns(4)
    k1.metric("Scorées (session)",   total)
    k2.metric("Fraudes (session)",   fraud)
    k3.metric("Taux fraude",         f"{rate:.1f}%")
    k4.metric("Alertes session",     n_sess)

    # ── API Métriques (alignées avec v4.1) ──────────────────
    st.divider()
    col_api, col_refresh = st.columns([5,1])
    with col_refresh:
        if st.button("🔄 Refresh"):
            st.rerun()

    try:
        r = requests.get(f"{API_URL}/api/metrics", timeout=2)
        if r.ok:
            m = r.json()
            mc1,mc2,mc3,mc4 = st.columns(4)
            mc1.metric("Alias en mémoire",    m.get("total_aliases",0))
            mc2.metric("Tx en mémoire",       m.get("total_tx_in_memory",0))
            mc3.metric("Alertes /1h",         m.get("alerts_last_hour",0))
            mc4.metric("GNN actif",           "Oui" if m.get("gnn_active") else "Non")

            # Top règles déclenchées (dernière heure)
            rule_counts = m.get("rule_triggers_last_hour",{})
            if rule_counts:
                st.markdown("<div style='font-family:\"DM Mono\";font-size:10px;letter-spacing:.1em;color:#5a6478;text-transform:uppercase;margin:12px 0 8px'>Top règles déclenchées (1h)</div>", unsafe_allow_html=True)
                df_rules = pd.DataFrame(
                    sorted(rule_counts.items(), key=lambda x:-x[1]),
                    columns=["Règle","Déclenchements"]
                )
                st.dataframe(df_rules, width='stretch', hide_index=True, height=180)
    except Exception:
        st.warning("API non joignable")

    # ── Alertes session + courbe ────────────────────────────
    st.divider()
    st.markdown("<div style='font-family:\"DM Mono\";font-size:10px;letter-spacing:.1em;color:#5a6478;text-transform:uppercase;margin-bottom:12px'>Alertes de la session</div>", unsafe_allow_html=True)

    if not st.session_state.session_alerts:
        st.markdown("<div style='color:#3a4455;font-family:\"DM Mono\";font-size:12px;padding:20px'>Aucune alerte.</div>", unsafe_allow_html=True)
    else:
        for alert in reversed(st.session_state.session_alerts[-20:]):
            color = "#ff4757" if alert["risk"] in ("HIGH","CRITICAL") else "#ffa502"
            rules_str = " ".join(f'<span class="rule-tag" style="font-size:10px;padding:1px 6px">{r}</span>' for r in alert.get("rule_triggers",[]))
            st.markdown(f"""
            <div style="background:#111318;border:1px solid #1e2530;border-left:3px solid {color};
                border-radius:0 6px 6px 0;padding:12px 16px;margin-bottom:8px;font-family:'DM Mono';font-size:12px">
                <div style="display:flex;justify-content:space-between;margin-bottom:4px">
                    <span style="color:#e8edf5;font-weight:500">{alert['alias']}</span>
                    <span style="color:{color}">{alert['risk']} · final={alert['score']:.4f} · ml={alert.get('ml_score',alert['score']):.4f} · rules={alert.get('rule_score',0)}/100</span>
                </div>
                <div style="color:#5a6478">{alert['time']} · {int(alert['amount']):,} XOF {rules_str}</div>
            </div>""", unsafe_allow_html=True)

        if len(st.session_state.session_alerts) > 1:
            st.markdown("<div style='font-family:\"DM Mono\";font-size:10px;letter-spacing:.1em;color:#5a6478;text-transform:uppercase;margin:12px 0 6px'>Évolution des scores</div>", unsafe_allow_html=True)
            st.plotly_chart(trend_chart(st.session_state.session_alerts),
                            width='stretch',config={"displayModeBar":False})


# ══════════════════════ TAB HISTORIQUE MINIO ═══════════════════
# Affiche les alertes persistantes stockées dans MinIO (fraudes et règles déclenchées),
# avec des filtres par alias et niveau de risque, et la possibilité d'exporter les données au format CSV.
with tab_history:
    st.markdown("""
    <div style="font-family:'DM Mono';font-size:10px;letter-spacing:.1em;color:#5a6478;
        text-transform:uppercase;margin-bottom:12px">
        Historique persistant — MinIO pispi-alerts
    </div>""", unsafe_allow_html=True)

    if not MINIO_OK:
        st.warning("MinIO non connecté — historique non disponible.")
    else:
        col_ctrl1, col_ctrl2, col_ctrl3 = st.columns([2,2,2])
        with col_ctrl1:
            alias_filter = st.text_input("Filtrer par alias", value=st.session_state.alias_filter,
                                          placeholder="als_burst_001")
            st.session_state.alias_filter = alias_filter
        with col_ctrl2:
            risk_filter = st.selectbox("Niveau de risque", ["Tous","CRITICAL","HIGH","MEDIUM","LOW"])
        with col_ctrl3:
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("🔄 Recharger depuis MinIO"):
                st.cache_data.clear()
                st.rerun()

        # Chargement depuis MinIO (mis en cache 5min)
        all_alerts = _load_alerts_minio(days=7)

        # Filtrage
        if alias_filter:
            all_alerts = [a for a in all_alerts if alias_filter.lower() in a.get("alias","").lower()]
        if risk_filter != "Tous":
            all_alerts = [a for a in all_alerts if a.get("risk") == risk_filter]

        # KPIs MinIO
        m1,m2,m3,m4 = st.columns(4)
        m1.metric("Total alertes MinIO", len(all_alerts))
        m2.metric("Fraudes",    sum(1 for a in all_alerts if a.get("is_fraud")))
        m3.metric("Avec règles",sum(1 for a in all_alerts if a.get("rule_triggers")))
        m4.metric("Bucket",     ALERTS_BUCKET)

        # Tableau des alertes
        if all_alerts:
            st.markdown("<div style='font-family:\"DM Mono\";font-size:10px;letter-spacing:.1em;color:#5a6478;text-transform:uppercase;margin:12px 0 8px'>Alertes (50 les plus récentes)</div>", unsafe_allow_html=True)
            rows = []
            for a in all_alerts[:50]:
                rows.append({
                    "Heure"   : a.get("time",""),
                    "Alias"   : a.get("alias",""),
                    "Montant" : f"{int(a.get('amount',0)):,} XOF",
                    "Score"   : round(a.get("score",0),4),
                    "ML"      : round(a.get("ml_score", a.get("score",0)),4),
                    "Rules"   : f"{a.get('rule_score',0)}/100",
                    "Règles"  : ", ".join(a.get("rule_triggers",[])) or "—",
                    "Risque"  : a.get("risk",""),
                })
            st.dataframe(pd.DataFrame(rows), width='stretch', hide_index=True, height=400)

            # Option export
            df_export = pd.DataFrame(rows)
            csv = df_export.to_csv(index=False).encode("utf-8")
            st.download_button(
                "⬇ Exporter CSV",
                data=csv,
                file_name=f"pispi_alerts_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                mime="text/csv",
            )
        else:
            st.markdown("<div style='color:#3a4455;font-family:\"DM Mono\";font-size:12px;padding:20px'>Aucune alerte dans MinIO pour ce filtre.</div>", unsafe_allow_html=True)
