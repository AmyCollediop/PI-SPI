from fastapi import FastAPI
from pydantic import BaseModel
import numpy as np
import pickle
from pathlib import Path

app = FastAPI()

# ✅ Use absolute paths
BASE_DIR = Path(__file__).parent.parent
MODEL_PATH = BASE_DIR / "models" / "hybrid_model.pkl"

# ✅ Charger modèle hybride
with open(MODEL_PATH, "rb") as f:
    model_bundle = pickle.load(f)
xgb_model = model_bundle["xgb"]
iso_model = model_bundle["iso"]
FEATURE_ORDER = model_bundle["features"]  # ⚠️ CRITIQUE

print(f"✅ Model loaded from: {MODEL_PATH}")
print(f"✅ Expected features: {FEATURE_ORDER}")

# =============================
# INPUT
# =============================
class Transaction(BaseModel):
    sender: str
    receiver: str
    amount: float
    timestamp: str
    txn_velocity_1min: float
    txn_count_5min: int


# =============================
# SCORE
# =============================
@app.post("/score")
def score(txn: Transaction):

    # =============================
    # 1. BUILD FEATURE VECTOR
    # =============================
    # Note: Feast features would be fetched here, but feature views aren't registered yet
    # Using only stream features for now
    feature_dict = {
        # batch (would be from Feast)
        "amount_zscore_30d": 0.0,  # placeholder
        "alias_age_days": 0.0,     # placeholder
        "in_out_ratio": 0.0,       # placeholder
        "distinct_issuer_count_24h": 0.0,  # placeholder
        "cross_institution_burst": 0.0,    # placeholder

        # stream (real features from transaction)
        "txn_velocity_1min": txn.txn_velocity_1min,
        "txn_count_5min": txn.txn_count_5min,
    }

    # =============================
    # 2. PREPARE FEATURE VECTOR (in correct order)
    # =============================
    # ✅ ordre EXACT du training
    X = np.array([feature_dict[f] for f in FEATURE_ORDER]).reshape(1, -1)

    # =============================
    # 3. HYBRID MODEL
    # =============================
    xgb_score = xgb_model.predict_proba(X)[0][1]
    iso_score = -iso_model.score_samples(X)[0]
    iso_score = iso_score / (iso_score + 1e-6)  # normalisation safe

    final_score = 0.8 * xgb_score + 0.2 * iso_score

    # =============================
    # 4. DECISION
    # =============================
    if final_score > 0.9:
        decision = "BLOCK"
    elif final_score > 0.7:
        decision = "REVIEW"
    else:
        decision = "APPROVE"

    # =============================
    # 5. RESPONSE
    # =============================
    return {
        "score": float(final_score),
        "decision": decision
    }