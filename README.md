# PI-SPI — Paiement Instantané · Système de Protection Intégrée

> Plateforme de détection de fraude en temps réel pour la zone UEMOA/BCEAO  
> Real-time fraud detection platform for the WAEMU/BCEAO payment ecosystem

---

## 🎯 Présentation

PI-SPI est un système de détection de fraude de niveau production conçu pour le réseau de paiement instantané de la zone UEMOA (Union Économique et Monétaire Ouest-Africaine). Il combine apprentissage automatique hybride, règles métier et monitoring temps réel pour protéger les transactions des 8 pays membres.

---

## 🏗️ Architecture

Kafka (transactions) 
    → Stream Processor (features temps réel)
    → MinIO (stockage brut NDJSON)
    → Airflow DAGs (features batch 30j)
    → Feast / Redis (feature store)
    → API FastAPI (scoring hybride)
    → Streamlit Dashboard (SOC)
    → Prometheus / Grafana (monitoring)


### Modèle hybride

| Modèle | Poids | Rôle |
|--------|-------|------|
| XGBoost | 60% | Scoring supervisé sur features comportementales |
| IsolationForest | 20% | Détection d'anomalies non supervisée |
| GraphSAGE GNN | 20% | Analyse du réseau de transactions |

**AUC = 0.9591** · Threshold = 0.697 · 66 319 transactions · Taux fraude 4.7%

---

## ⚡ Fonctionnalités

### 12 Règles Métier UEMOA/BCEAO

| Règle | Description |
|-------|-------------|
| `BURST` | Rafale de transactions (>10 en 1 minute) |
| `LATENCY` | Latence réseau anormale (z-score > 3) |
| `RETRY` | Pattern de retry suspect (FAILED→FAILED→SUCCESS) |
| `CROSS_BORDER` | Transaction transfrontalière > 500 000 XOF |
| `ACCOUNT_TAKEOVER` | Changement device + institution + montant élevé |
| `MULE` | Compte mule (>8 P2P en 10 minutes) |
| `STRUCTURING` | Fractionnement sous seuil (smurfing) |
| `COUNTRY_HOP` | Changement de pays répété |
| `AMOUNT_SPIKE` | Pic de montant anormal (z-score > 3.5) |
| `SLEEPER` | Compte dormant réactivé (>90j) |
| `RTP_BURST` | Rafale de paiements RTP |
| `NEW_DEVICE` | Nouveau device + montant > 50 000 XOF |

### Pipeline ML
- **25 features** en mode STRICT (sans proxies de scénario)
- **Split temporel** train/test pour éviter le leakage
- **SHAP** pour l'explicabilité des décisions
- **Score de fusion** : `final = max(ml_score, rule_floor)`

### Feature Engineering
- **21 stream features** calculées en temps réel (fenêtres 1min/5min/10min)
- **9 behavioral features** calculées par Airflow sur 30 jours
- **7 graph features** via NetworkX (PageRank, centralité, collector_flag)

---

## 🛠️ Stack Technique

| Composant | Technologie |
|-----------|-------------|
| Message broker | Apache Kafka 7.5 |
| Stockage objet | MinIO |
| Feature store | Feast + Redis |
| Orchestration | Apache Airflow 2.8 |
| ML | XGBoost + Scikit-learn + PyTorch Geometric |
| API | FastAPI 0.100+ |
| Dashboard | Streamlit |
| Monitoring | Prometheus + Grafana |
| Base de données | PostgreSQL 15 |
| Containerisation | Docker Compose |

---

## 🚀 Démarrage rapide

### Prérequis
- Docker Desktop
- Python 3.10+
- 8 GB RAM minimum

### Installation

```bash
# 1. Cloner le dépôt
git clone https://github.com/AmyCollediop/PI-SPI.git
cd PI-SPI

# 2. Créer l'environnement virtuel
python -m venv pispienv
pispienv\Scripts\activate  # Windows
source pispienv/bin/activate  # Linux/Mac

# 3. Installer les dépendances
pip install -r requirements.txt

# 4. Démarrer l'infrastructure Docker
docker compose up -d

# 5. Entraîner le modèle
python training/train_fraud_model.py

# 6. Démarrer l'API
python streamlit/fraud_api.py

# 7. Démarrer le dashboard
streamlit run streamlit/app.py
```

### Interfaces

| Service | URL |
|---------|-----|
| Dashboard Streamlit | http://localhost:8501 |
| API FastAPI | http://localhost:8000 |
| Swagger / Docs | http://localhost:8000/docs |
| Grafana | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Airflow | http://localhost:8080 |
| MinIO Console | http://localhost:9001 |

---

## 📊 Métriques de Performance

```
AUC-ROC          : 0.9591
Threshold optimal : 0.697 (maximise F1)
Taux de fraude   : 4.7% (3 089 / 66 319 transactions)
Latence scoring  : ~70ms P50 / ~150ms P99
```

### Distribution des scénarios de fraude

| Scénario | Transactions |
|----------|-------------|
| BURST | 576 |
| MULE_ACCOUNT | 555 |
| COLLECTOR_ACCOUNT | 553 |
| SMURFING | 365 |
| RETRY | 298 |
| RTP_ABUSE | 245 |
| CROSS_BORDER | 205 |
| NEW_DEVICE | 143 |
| LATENCY | 137 |
| ACCOUNT_TAKEOVER | ~63 |

---

## 📁 Structure du Projet

```
PI-SPI/
├── streamlit/
│   ├── fraud_api.py          # API FastAPI v4.1
│   ├── app.py                # Dashboard Streamlit
│   └── populate_history.py   # Script de peuplement
├── airflow/
│   └── dags/
│       └── dag_batch_features.py  # DAGs Airflow
├── training/
│   ├── train_fraud_model.py  # Entraînement ML
│   └── build_training_dataset2.py
├── feast_repo/
│   ├── feature_definitions.py
│   └── feature_store.yaml
├── prometheus/
│   ├── metrics.py            # Métriques Prometheus
│   └── prometheus.yml
├── producteur_pispi.py       # Producteur Kafka
├── stream_processor.py       # Processeur de stream
├── docker-compose.yml        # Infrastructure complète
└── requirements.txt
```

---

## 🌍 Contexte UEMOA

PI-SPI est conçu pour le système de paiement instantané de la BCEAO (Banque Centrale des États de l'Afrique de l'Ouest), couvrant 8 pays :

**SN** Sénégal · **CI** Côte d'Ivoire · **ML** Mali · **BF** Burkina Faso  
**BJ** Bénin · **TG** Togo · **GW** Guinée-Bissau · **NE** Niger

Institutions supportées : WAVE, Orange Money, MTN MoMo, Free Money, et les banques commerciales de la zone.

---

## 👤 Auteur

**AMY COLLE DIOP**  
AIMS-SENEGAL  
ML Engineering · Financial Technology · UEMOA Payment Systems

---

## 📄 Licence

Ce projet est développé dans un cadre académique et de recherche appliquée.