# PI-SPI — Paiement Instantané · Système de Protection Intégrée

> Plateforme de détection de fraude en temps réel pour la zone UEMOA/BCEAO  
> Real-time fraud detection platform for the WAEMU/BCEAO payment ecosystem

---

## 🎯 Présentation

1. Présentation générale
   
PI-SPI est un système de détection de fraude de niveau production conçu pour le réseau de paiement instantané de la zone UEMOA (Union Économique et Monétaire Ouest-Africaine). Il combine apprentissage automatique hybride, règles métier et monitoring temps réel pour protéger les transactions des 8 pays membres.

Imaginons notre système de détection de la fraude comme un agent de sécurité très expérimenté, capable d’analyser 66 000 transactions afin d’identifier les opérations frauduleuses.

C’est comme un expert qui a examiné des milliers de dossiers au cours de sa carrière et qui a développé une grande capacité à détecter les dossiers non conformes.

Notre système joue ainsi le rôle d’un gardien invisible, placé entre les clients, la banque et la plateforme de paiement PISP.

2. Objectif du système

Sur 100 transactions, seulement 5 sont frauduleuses. Le système doit donc être suffisamment intelligent pour détecter ces 5 transactions suspectes sans bloquer les 95 transactions légitimes.

En effet, un système trop strict peut générer de la frustration chez les clients en bloquant des opérations normales. À l’inverse, un système trop permissif risque de laisser passer les transactions frauduleuses.

L’objectif est donc de trouver le bon équilibre entre la sécurité et la fluidité des paiements.

3. Exemple de détection d’une fraude

Lorsqu’une transaction frauduleuse est détectée, le système analyse son niveau de risque et peut immédiatement bloquer l’opération si les règles de sécurité le justifient.

Exemple :

- Score de risque : 71 %.
- Motifs : nouveau téléphone inconnu et montant de la transaction 30 fois supérieur au montant habituel.
- Décision : transaction suspecte, nécessitant une vérification ou un blocage selon le seuil de risque défini.

Le système ne se contente donc pas d’identifier une anomalie : il cherche également à expliquer pourquoi une transaction est considérée comme suspecte.

4. Un agent intelligent qui apprend les habitudes des clients

Il ne faut pas oublier que notre système est un agent intelligent conçu pour connaître les habitudes de chaque client grâce aux données historiques des transactions.

Il est entraîné à reconnaître plusieurs règles métier, par exemple :

- Fréquence anormale : effectuer 10 transactions en une minute peut indiquer un comportement automatisé ou suspect.
- Nouvel appareil : une transaction effectuée depuis un appareil inconnu, associée à un montant élevé, constitue un signal de risque.
- Pays inhabituel : une opération réalisée depuis un pays rarement utilisé par le client peut nécessiter une vérification supplémentaire.
- Montant inhabituel : une transaction dont le montant est très supérieur aux habitudes du client peut être considérée comme suspecte.

Ces éléments ne prouvent pas nécessairement une fraude. Ils constituent des indicateurs que le système combine pour évaluer le risque global d’une transaction.

5. Intégration de l’intelligence artificielle

Pour améliorer les performances du système, nous pouvons intégrer une approche d’apprentissage automatique (Machine Learning, ou ML).

Plusieurs modèles peuvent être utilisés :

- XGBoost : pour analyser les données structurées et détecter les comportements frauduleux.
- Isolation Forest (iForest) : pour repérer les transactions atypiques et les anomalies.
- RNN (Recurrent Neural Network) : pour analyser les séquences de transactions et identifier les comportements inhabituels dans le temps.

En combinant les règles métier et les modèles de Machine Learning, le système peut améliorer sa capacité à détecter les fraudes tout en limitant les faux positifs, c’est-à-dire les transactions légitimes signalées à tort comme frauduleuses.

Conclusion

Notre système de détection de la fraude agit comme un expert de la sécurité financière : il observe, apprend, analyse et prend des décisions en fonction du niveau de risque.

Son objectif principal est de détecter un maximum de transactions frauduleuses tout en préservant les transactions légitimes, afin de garantir des paiements plus sûrs et une meilleure expérience client.

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
| Orchestration | Apache Airflow 2.10 |
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
- Docker Compose v2
- 8 GB RAM minimum

### Lancer le projet avec Docker

```bash
# Cloner le dépôt
git clone https://github.com/AmyCollediop/PI-SPI.git
cd PI-SPI

# Construire les images et démarrer les services
docker compose up -d --build
```

Pour arrêter les services : `docker compose down`.

### Interfaces

| Service | Accès local |
|---------|-------------|
| Dashboard Streamlit | [Ouvrir le dashboard](http://localhost:8501) |
| API FastAPI | [http://localhost:8000](http://localhost:8000) |
| Swagger / Docs | [http://localhost:8000/docs](http://localhost:8000/docs) |
| Grafana | [http://localhost:3000](http://localhost:3000) |
| Prometheus | [http://localhost:9090](http://localhost:9090) |
| Airflow | [http://localhost:8080](http://localhost:8080) |
| Console MinIO | [http://localhost:9001](http://localhost:9001) |

Ces adresses sont accessibles que sur ma machine . 

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
