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
# Ce script doit être exécuté depuis la racine du projet, 
# et il initialise le repository Feast en appliquant les définitions d'entités, de sources de données, et de feature views.
BASE_DIR = Path(__file__).resolve().parent

print(f"📁 Feast repo : {BASE_DIR}")
print(f"📄 feature_store.yaml exists : {(BASE_DIR / 'feature_store.yaml').exists()}")

# ============================================================
# INIT STORE
# ============================================================
# Cette étape applique les définitions d'entités, de sources de données, et de feature views au repository Feast.
fs = FeatureStore(repo_path=str(BASE_DIR))

# ============================================================
# APPLY OBJECTS
# ============================================================
# Appliquer les entités, sources de données, et feature views au repository Feast.
fs.apply([
    payer_alias,
    stream_features,
    behavioral_features,
    interbank_features,
    graph_features,
    training_features,   #<- à appliquer en dernier, car il dépend des autres
])

print("✅ Feast repository configuré avec succès")