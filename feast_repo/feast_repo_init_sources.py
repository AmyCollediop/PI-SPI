from pathlib import Path

from feast import FeatureStore

from feature_definitions import (
    payer_alias,
    stream_features,
    behavioral_features,
    interbank_features,
    graph_features,
    training_features,
)

# ============================================================
# PATH
# ============================================================

BASE_DIR = Path(__file__).resolve().parent

print(f"📁 Feast repo : {BASE_DIR}")
print(f"📄 feature_store.yaml exists : {(BASE_DIR / 'feature_store.yaml').exists()}")

# ============================================================
# INIT STORE
# ============================================================

fs = FeatureStore(repo_path=str(BASE_DIR))

# ============================================================
# APPLY OBJECTS
# ============================================================

fs.apply([
    payer_alias,
    stream_features,
    behavioral_features,
    interbank_features,
    graph_features,
    training_features,   # ✅ important
])

print("✅ Feast repository configuré avec succès")