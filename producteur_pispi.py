"""
producer_pispi.py  ·  PI-SPI Fraud Detection Producer
======================================================
Version : 5.0.0
Tourne indéfiniment — arrêt UNIQUEMENT par Ctrl+C.

SCÉNARIOS (10)
──────────────
  NONE              régime normal
  ACCOUNT_TAKEOVER  nouvel appareil + pays + montant × 8-20
  BURST             15 tx du même alias en < 20 secondes
  SMURFING          50 micro-tx sous seuil BCEAO vers N bénéficiaires
  MULE_ACCOUNT      dormance 30j+ puis 100 tx en quelques heures
  COLLECTOR_ACCOUNT 1 receiver ← N payeurs différents
  CROSS_BORDER      pays à risque + institution inconnue + montant normal
  RETRY             séquence FAILED×N puis SUCCESS sur même bénéficiaire
  LATENCY           pics de latence réalistes sur baseline bruitée
  RTP_ABUSE         rafale de 100 RTP en < 60 secondes
  NEW_DEVICE        connexion depuis appareil jamais vu

DISTRIBUTION
────────────
  95% NONE   +   5% répartis sur les 10 scénarios fraud

SCHÉMA KAFKA
────────────
  transaction_id, message_id, timestamp, amount, device,
  transaction_type, issuer_institution, receiver_institution,
  issuer_country, receiver_country, payer_alias, receiver_alias,
  alias_type, status, response_time_ms,
  new_device_flag, is_cross_border, is_p2p, is_rtp,
  fraud_scenario, is_fraud, user_profile
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
import random
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Deque, Dict, List, Optional, Set, Tuple

from confluent_kafka import Producer

# ════════════════════════════════════════════════════════════════
# LOGGING
# ════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s – %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("pispi.producer")

# ════════════════════════════════════════════════════════════════
# CONFIG
# ════════════════════════════════════════════════════════════════

KAFKA_BOOTSTRAP : str   = "127.0.0.1:9092"
TOPIC_RAW       : str   = "pispi_transactions"
TX_PER_SEC      : float = 20.0
N_USERS         : int   = 300

# ════════════════════════════════════════════════════════════════
# RÉFÉRENTIELS
# ════════════════════════════════════════════════════════════════

PAYS_UEMOA      : List[str] = ["SN", "CI", "ML", "BF", "BJ", "TG", "GW", "NE"]
PAYS_RISQUE     : List[str] = ["NG", "GH", "CM", "MA", "FR", "US", "CN", "AE"]

INSTITUTIONS: Dict[str, List[str]] = {
    "SN": ["CBAO-SN", "SGBS-SN", "BIS-SN", "WAVE-SN", "ORANGE-MONEY-SN", "FREE-MONEY-SN"],
    "CI": ["BICICI-CI", "SIB-CI", "MTN-MOMO-CI", "ORANGE-CI", "ECOBANK-CI"],
    "ML": ["BDM-ML", "BNDA-ML", "ORANGE-ML"],
    "BF": ["CBI-BF", "MOOV-BF", "ORANGE-BF"],
    "BJ": ["BOA-BJ", "MTN-BJ", "MOOV-BJ"],
    "TG": ["ORABANK-TG", "TMONEY-TG", "FLOOZ-TG"],
    "GW": ["BDU-GW", "ORANGE-GW"],
    "NE": ["BIA-NE", "AIRTEL-NE", "ORANGE-NE"],
}
ALL_INSTITUTIONS : List[str] = [i for lst in INSTITUTIONS.values() for i in lst]

# Institutions "inconnues" simulant des émetteurs étrangers non-BCEAO
UNKNOWN_INSTITUTIONS : List[str] = [
    "ZENITH-NG", "GTB-NG", "ACCESS-GH", "ECO-CM",
    "ATTIJARIWAFA-MA", "BNP-FR", "CITIBANK-US", "HSBC-AE",
]

DEVICES         : List[str] = ["ANDROID", "IOS", "WEB", "FEATURE_PHONE", "USSD"]
UNKNOWN_DEVICES : List[str] = ["UNKNOWN_ANDROID", "UNKNOWN_IOS", "UNKNOWN_WEB"]
TX_TYPES        : List[str] = ["P2P", "P2M", "P2G", "RTP", "TRANSFER"]
ALIAS_TYPES     : List[str] = ["MSISDN", "QR", "CODE_MARCHAND", "SHID"]
STATUSES        : List[str] = ["SUCCESS", "FAILED", "REJECTED"]
SEUILS_BCEAO    : List[float] = [300_000, 500_000, 1_000_000, 2_000_000]


# ════════════════════════════════════════════════════════════════
# ENUMS
# ════════════════════════════════════════════════════════════════
# Les profils d'utilisateurs simulés, avec leurs caractéristiques comportementales 
class ProfileType(str, Enum):
    NORMAL_USER       = "NORMAL_USER"
    MERCHANT          = "MERCHANT"
    HIGH_NET_WORTH    = "HIGH_NET_WORTH"
    FREQUENT_TRAVELER = "FREQUENT_TRAVELER"
    NEW_ACCOUNT       = "NEW_ACCOUNT"
    HIGH_RISK_USER    = "HIGH_RISK_USER"
    MULE_ACCOUNT      = "MULE_ACCOUNT"

# Les scénarios de fraude simulés, avec leur logique spécifique et leur impact sur les features et les scores
class Scenario(str, Enum):
    NONE              = "NONE"
    ACCOUNT_TAKEOVER  = "ACCOUNT_TAKEOVER"
    BURST             = "BURST"
    SMURFING          = "SMURFING"
    MULE_ACCOUNT      = "MULE_ACCOUNT"
    COLLECTOR_ACCOUNT = "COLLECTOR_ACCOUNT"
    CROSS_BORDER      = "CROSS_BORDER"
    RETRY             = "RETRY"
    LATENCY           = "LATENCY"
    RTP_ABUSE         = "RTP_ABUSE"
    NEW_DEVICE        = "NEW_DEVICE"


# ════════════════════════════════════════════════════════════════
# ÉTAT UTILISATEUR
# ════════════════════════════════════════════════════════════════
# Représente l'état complet d'un utilisateur (alias) dans le système, avec son profil, son historique de transactions,
#  ses appareils connus, ses bénéficiaires habituels, et son état de scénario actif.
@dataclass
class TxRecord:
    ts_ms       : int
    amount      : float
    status      : str
    device      : str
    institution : str
    country     : str
    tx_type     : str
    receiver    : str
    latency_ms  : int


@dataclass
class UserState:
    alias_id         : str
    profile_type     : ProfileType
    home_country     : str
    home_institution : str
    usual_devices    : List[str]
    known_devices    : Set[str]
    beneficiaries    : List[str]
    avg_amount       : float
    stddev_amount    : float
    freq_per_day     : float
    created_at       : datetime

    history          : Deque[TxRecord] = field(default_factory=lambda: deque(maxlen=1000))
    known_senders    : Set[str]        = field(default_factory=set)
    known_receivers  : Set[str]        = field(default_factory=set)

    last_ts_ms       : Optional[int]   = None
    last_device      : Optional[str]   = None
    last_institution : Optional[str]   = None
    last_country     : Optional[str]   = None

    # Dormance (MULE_ACCOUNT)
    dormant_since    : Optional[datetime] = None

    # État scénario multi-étapes
    active_scenario  : Scenario        = Scenario.NONE
    scenario_step    : int             = 0
    scenario_data    : Dict            = field(default_factory=dict)

    # ── Helpers ──────────────────────────────────────────────────
    def record(self, r: TxRecord) -> None:
        self.history.append(r)
        self.last_ts_ms      = r.ts_ms
        self.last_device     = r.device
        self.last_institution= r.institution
        self.last_country    = r.country
        self.known_receivers.add(r.receiver)
        self.known_devices.add(r.device)

    def recent(self, window_ms: int, now_ms: int) -> List[TxRecord]:
        return [t for t in self.history if now_ms - t.ts_ms <= window_ms]

    def is_known_device(self, device: str) -> bool:
        return device in self.known_devices

    def baseline_latency(self) -> Tuple[float, float]:
        """Retourne (mu, sigma) de la latence historique."""
        lats = [t.latency_ms for t in self.history]
        if len(lats) < 3:
            return 250.0, 60.0
        mu  = sum(lats) / len(lats)
        std = math.sqrt(sum((x - mu)**2 for x in lats) / len(lats))
        return mu, max(std, 30.0)


# ════════════════════════════════════════════════════════════════
# PROFILS
# ════════════════════════════════════════════════════════════════
# Définition des profils d'utilisateurs simulés, avec leurs caractéristiques comportementales 
@dataclass
class UserProfile:
    profile_type    : ProfileType
    avg_amount      : float
    stddev_amount   : float
    min_amount      : float
    max_amount      : float
    freq_per_day    : float
    usual_devices   : List[str]
    age_days_min    : int
    age_days_max    : int
    n_beneficiaries : int
    scenario_weights: Dict[str, float]

# ════════════════════════════════════════════════════════════════
# FEAST FEATURE VIEWS (STREAMING + HISTORIQUES)       
# ════════════════════════════════════════════════════════════════
PROFILES: Dict[ProfileType, UserProfile] = {

    ProfileType.NORMAL_USER: UserProfile(
        profile_type    = ProfileType.NORMAL_USER,
        avg_amount=15_000, stddev_amount=8_000,
        min_amount=500,    max_amount=150_000,
        freq_per_day=1.5,
        usual_devices=["ANDROID", "USSD", "FEATURE_PHONE"],
        age_days_min=180, age_days_max=1800,
        n_beneficiaries=6,
        scenario_weights={
            "NONE":0.950,"ACCOUNT_TAKEOVER":0.010,"BURST":0.005,
            "SMURFING":0.000,"MULE_ACCOUNT":0.000,"COLLECTOR_ACCOUNT":0.000,
            "CROSS_BORDER":0.005,"RETRY":0.015,"LATENCY":0.005,
            "RTP_ABUSE":0.000,"NEW_DEVICE":0.010,
        },
    ),
    # Les autres profils (MERCHANT, HIGH_NET_WORTH, FREQUENT_TRAVELER, NEW_ACCOUNT, HIGH_RISK_USER, MULE_ACCOUNT) 
    # sont définis de manière similaire avec leurs propres caractéristiques et poids de scénario.
    ProfileType.MERCHANT: UserProfile(
        profile_type    = ProfileType.MERCHANT,
        avg_amount=85_000, stddev_amount=40_000,
        min_amount=1_000,  max_amount=2_000_000,
        freq_per_day=22.0,
        usual_devices=["ANDROID", "IOS", "WEB"],
        age_days_min=365, age_days_max=3650,
        n_beneficiaries=15,
        scenario_weights={
            "NONE":0.930,"ACCOUNT_TAKEOVER":0.010,"BURST":0.015,
            "SMURFING":0.010,"MULE_ACCOUNT":0.005,"COLLECTOR_ACCOUNT":0.010,
            "CROSS_BORDER":0.005,"RETRY":0.005,"LATENCY":0.005,
            "RTP_ABUSE":0.005,"NEW_DEVICE":0.000,
        },
    ),

    ProfileType.HIGH_NET_WORTH: UserProfile(
        profile_type    = ProfileType.HIGH_NET_WORTH,
        avg_amount=750_000, stddev_amount=300_000,
        min_amount=50_000,  max_amount=10_000_000,
        freq_per_day=2.5,
        usual_devices=["IOS", "WEB"],
        age_days_min=365, age_days_max=5000,
        n_beneficiaries=12,
        scenario_weights={
            "NONE":0.920,"ACCOUNT_TAKEOVER":0.020,"BURST":0.000,
            "SMURFING":0.015,"MULE_ACCOUNT":0.000,"COLLECTOR_ACCOUNT":0.000,
            "CROSS_BORDER":0.020,"RETRY":0.010,"LATENCY":0.005,
            "RTP_ABUSE":0.000,"NEW_DEVICE":0.010,
        },
    ),
    # Les autres profils (FREQUENT_TRAVELER, NEW_ACCOUNT, HIGH_RISK_USER, MULE_ACCOUNT) s
    # ont définis de manière similaire avec leurs propres caractéristiques et poids de scénario.
    ProfileType.FREQUENT_TRAVELER: UserProfile(
        profile_type    = ProfileType.FREQUENT_TRAVELER,
        avg_amount=120_000, stddev_amount=60_000,
        min_amount=5_000,   max_amount=3_000_000,
        freq_per_day=4.5,
        usual_devices=["IOS", "ANDROID", "WEB"],
        age_days_min=365, age_days_max=4000,
        n_beneficiaries=18,
        scenario_weights={
            "NONE":0.900,"ACCOUNT_TAKEOVER":0.010,"BURST":0.005,
            "SMURFING":0.005,"MULE_ACCOUNT":0.000,"COLLECTOR_ACCOUNT":0.000,
            "CROSS_BORDER":0.040,"RETRY":0.010,"LATENCY":0.010,
            "RTP_ABUSE":0.005,"NEW_DEVICE":0.015,
        },
    ),

    ProfileType.NEW_ACCOUNT: UserProfile(
        profile_type    = ProfileType.NEW_ACCOUNT,
        avg_amount=8_000, stddev_amount=5_000,
        min_amount=200,   max_amount=50_000,
        freq_per_day=0.4,
        usual_devices=["ANDROID", "FEATURE_PHONE"],
        age_days_min=1, age_days_max=30,
        n_beneficiaries=2,
        scenario_weights={
            "NONE":0.880,"ACCOUNT_TAKEOVER":0.020,"BURST":0.020,
            "SMURFING":0.000,"MULE_ACCOUNT":0.020,"COLLECTOR_ACCOUNT":0.000,
            "CROSS_BORDER":0.010,"RETRY":0.020,"LATENCY":0.010,
            "RTP_ABUSE":0.000,"NEW_DEVICE":0.020,
        },
    ),

    ProfileType.HIGH_RISK_USER: UserProfile(
        profile_type    = ProfileType.HIGH_RISK_USER,
        avg_amount=55_000, stddev_amount=45_000,
        min_amount=1_000,  max_amount=1_500_000,
        freq_per_day=8.0,
        usual_devices=["ANDROID", "USSD", "WEB", "IOS"],
        age_days_min=60, age_days_max=730,
        n_beneficiaries=20,
        scenario_weights={
            "NONE":0.840,"ACCOUNT_TAKEOVER":0.020,"BURST":0.020,
            "SMURFING":0.010,"MULE_ACCOUNT":0.010,"COLLECTOR_ACCOUNT":0.010,
            "CROSS_BORDER":0.020,"RETRY":0.020,"LATENCY":0.020,
            "RTP_ABUSE":0.015,"NEW_DEVICE":0.015,
        },
    ),

    ProfileType.MULE_ACCOUNT: UserProfile(
        profile_type    = ProfileType.MULE_ACCOUNT,
        avg_amount=45_000, stddev_amount=20_000,
        min_amount=2_000,  max_amount=800_000,
        freq_per_day=14.0,
        usual_devices=["ANDROID", "USSD", "FEATURE_PHONE", "IOS"],
        age_days_min=30, age_days_max=180,
        n_beneficiaries=25,
        scenario_weights={
            "NONE":0.700,"ACCOUNT_TAKEOVER":0.000,"BURST":0.020,
            "SMURFING":0.050,"MULE_ACCOUNT":0.100,"COLLECTOR_ACCOUNT":0.080,
            "CROSS_BORDER":0.010,"RETRY":0.010,"LATENCY":0.010,
            "RTP_ABUSE":0.010,"NEW_DEVICE":0.010,
        },
    ),
}
# Les autres profils (FREQUENT_TRAVELER, NEW_ACCOUNT, HIGH_RISK_USER, MULE_ACCOUNT) sont définis de manière similaire 
# avec leurs propres caractéristiques et poids de scénario.
PROFILE_DISTRIBUTION: List[Tuple[ProfileType, float]] = [
    (ProfileType.NORMAL_USER,       0.42),
    (ProfileType.MERCHANT,          0.15),
    (ProfileType.HIGH_NET_WORTH,    0.05),
    (ProfileType.FREQUENT_TRAVELER, 0.05),
    (ProfileType.NEW_ACCOUNT,       0.10),
    (ProfileType.HIGH_RISK_USER,    0.11),
    (ProfileType.MULE_ACCOUNT,      0.12),
]


# ════════════════════════════════════════════════════════════════
# USER POOL
# ════════════════════════════════════════════════════════════════
# Pool d'utilisateurs simulés, avec leur état complet (profil, historique, appareils connus, bénéficiaires habituels, etc.)
#  et une méthode de tirage aléatoire pondérée par la fréquence d'activité.
class UserPool:
    # Initialise le pool avec n utilisateurs synthétiques, en se basant sur les profils définis 
    # et en attribuant des caractéristiques réalistes (âge du compte, pays, institution, appareils, bénéficiaires, etc.).
    def __init__(self, n: int, seed_time: datetime) -> None:
        self._pool: Dict[str, UserState] = {}
        self._build(n, seed_time)
    # Peuplement du pool d'utilisateurs synthétiques, avec des caractéristiques réalistes basées sur les profils définis,
    #  et une distribution de scénarios frauduleux selon les poids spécifiés pour chaque profil.
    def _build(self, n: int, now: datetime) -> None:
        p_types  = [p for p, _ in PROFILE_DISTRIBUTION]
        p_weights= [w for _, w in PROFILE_DISTRIBUTION]
        # Pour chaque utilisateur à créer, on génère un alias unique, on choisit un profil selon la distribution définie, 
        # on attribue un pays et une institution de base, on détermine l'âge du compte, 
        # les montants moyens et l'écart-type des transactions, les appareils habituels, les bénéficiaires habituels,
        #  et on décide aléatoirement si le compte est dormant (pour les MULE_ACCOUNT).
        #  Le résultat est stocké dans le pool d'utilisateurs.
        for i in range(n):
            alias   = "als_" + hashlib.md5(f"u{i}".encode()).hexdigest()[:10]
            p_type  = random.choices(p_types, weights=p_weights, k=1)[0]
            profile = PROFILES[p_type]
            country = random.choice(PAYS_UEMOA)
            inst    = random.choice(INSTITUTIONS[country])
            age     = random.randint(profile.age_days_min, profile.age_days_max)
            avg     = profile.avg_amount   * random.uniform(0.75, 1.35)
            std     = profile.stddev_amount* random.uniform(0.60, 1.50)
            devs    = random.sample(
                profile.usual_devices,
                k=min(len(profile.usual_devices), random.randint(1, 3)),
            )
            bens    = [
                "als_" + hashlib.md5(f"ben_{alias}_{j}".encode()).hexdigest()[:10]
                for j in range(profile.n_beneficiaries)
            ]

            dormant = None
            if p_type in (ProfileType.MULE_ACCOUNT, ProfileType.NORMAL_USER):
                if random.random() < 0.06:
                    dormant = now - timedelta(days=random.randint(30, 120))

            self._pool[alias] = UserState(
                alias_id        = alias,
                profile_type    = p_type,
                home_country    = country,
                home_institution= inst,
                usual_devices   = devs,
                known_devices   = set(devs),
                beneficiaries   = bens,
                avg_amount      = avg,
                stddev_amount   = std,
                freq_per_day    = profile.freq_per_day * random.uniform(0.7, 1.3),
                created_at      = now - timedelta(days=age),
                dormant_since   = dormant,
            )

        log.info("UserPool initialisé : %d utilisateurs.", n)
    # Méthode de tirage aléatoire d'un utilisateur dans le pool,
    #  avec une probabilité pondérée par la fréquence d'activité quotidienne de chaque utilisateur, 
    # afin de simuler une activité réaliste dans le temps.
    def pick(self) -> UserState:
        users = list(self._pool.values())
        freqs = [u.freq_per_day for u in users]
        return random.choices(users, weights=freqs, k=1)[0]
    # Permet de connaître la taille du pool d'utilisateurs, ce qui peut être utile pour le monitoring ou le debug.
    def __len__(self) -> int:
        return len(self._pool)


# ════════════════════════════════════════════════════════════════
# HELPERS COMMUNS
# ════════════════════════════════════════════════════════════════
# Fonctions utilitaires communes à tous les scénarios, telles que la génération d'alias de bénéficiaires,
#  le clamp des montants selon les profils, la génération de latences réalistes, la réinitialisation de l'état de scénario,
#  et l'enregistrement des transactions dans l'historique de l'utilisateur.
def _alias() -> str:
    return "als_" + uuid.uuid4().hex[:10]

# Génère un alias de bénéficiaire, en choisissant parmi les bénéficiaires habituels de l'utilisateur 
# avec une forte probabilité, ou en générant un nouvel alias aléatoire dans le cas contraire.
def _beneficiary(user: UserState) -> str:
    if user.beneficiaries and random.random() < 0.72:
        return random.choice(user.beneficiaries)
    return _alias()

# Clamp le montant généré pour qu'il soit cohérent avec le profil de l'utilisateur, en respectant les limites minimales et maximales définies pour ce profil.
def _clamp(amount: float, profile: UserProfile) -> float:
    return max(profile.min_amount, min(profile.max_amount, amount))

# Génère une latence réaliste pour une transaction normale, en se basant sur une latence nominale autour de 220ms
#  avec un bruit gaussien pour simuler la variabilité naturelle du système.
def _normal_latency() -> int:
    """Latence nominale : baseline autour de 220ms avec bruit gaussien."""
    return max(50, int(random.gauss(220, 55)))

# Génère un pic de latence réaliste pour simuler des problèmes de performance ou des attaques ciblant la disponibilité,
#  en partant de la baseline historique de l'utilisateur et en ajoutant un multiplicateur aléatoire élevé 
#  créer un spike significatif, tout en conservant une certaine variabilité grâce à un bruit gaussien.
def _realistic_latency_spike(user: UserState) -> int:
    """
    Génère un vrai pic de latence réaliste.
    On part de la baseline historique + bruit, puis on ajoute un spike.
    Résultat : séquence 220, 240, 260, 270, 3200, 3500 ...
    """
    mu, sigma = user.baseline_latency()
    # Spike : baseline × multiplicateur aléatoire élevé
    spike_multiplier = random.uniform(8, 25)
    noise = random.gauss(0, sigma * 0.3)
    return max(1_500, int(mu * spike_multiplier + noise))

# Réinitialise l'état de scénario de l'utilisateur, en remettant à zéro le scénario actif,
#  l'étape du scénario, et les données associées, afin de revenir à un état normal après la fin d'un scénario frauduleux 
# ou en cas d'erreur.
def _reset(user: UserState) -> None:
    user.active_scenario = Scenario.NONE
    user.scenario_step   = 0
    user.scenario_data   = {}

# Enregistre une transaction dans l'historique de l'utilisateur, en créant un objet TxRecord 
# avec les informations pertinentes de la transaction (montant, statut, appareil, institution, pays, type de transaction,
#  bénéficiaire, latence) et en l'ajoutant à la deque d'historique de l'utilisateur.
def _record(user: UserState, tx: dict, now_ms: int) -> None:
    user.record(TxRecord(
        ts_ms       = now_ms,
        amount      = tx["amount"],
        status      = tx["status"],
        device      = tx["device"],
        institution = tx["issuer_institution"],
        country     = tx["issuer_country"],
        tx_type     = tx["transaction_type"],
        receiver    = tx["receiver_alias"],
        latency_ms  = tx["response_time_ms"],
    ))

# Construit une transaction complète et cohérente avec les caractéristiques de l'utilisateur,
#  le scénario actif, et les paramètres spécifiés (montant, appareil, type de transaction, statut, latence,
#  bénéficiaire, pays, institution), en calculant les features dérivées (flags de nouveau device, cross-border, P2P, RTP)
#  et en enregistrant la transaction dans l'historique de l'utilisateur.
def _build_tx(
    user        : UserState,
    now_ms      : int,
    amount      : float,
    device      : str,
    tx_type     : str,
    status      : str,
    latency     : int,
    receiver    : str,
    recv_country: str,
    recv_inst   : str,
    scenario    : Scenario,
    is_fraud    : int,
) -> dict:
    is_cross    = int(recv_country != user.home_country)
    new_dev_flag= int(not user.is_known_device(device))

    tx = {
        "transaction_id"      : f"PISPI{random.randint(10**9,10**10-1)}",
        "message_id"          : f"MSG{random.randint(10**9,10**10-1)}",
        "timestamp"           : now_ms,
        "amount"              : round(float(amount), 2),
        "device"              : device,
        "transaction_type"    : tx_type,
        "issuer_institution"  : user.home_institution,
        "receiver_institution": recv_inst,
        "issuer_country"      : user.home_country,
        "receiver_country"    : recv_country,
        "payer_alias"         : user.alias_id,
        "receiver_alias"      : receiver,
        "alias_type"          : random.choice(ALIAS_TYPES),
        "status"              : status,
        "response_time_ms"    : latency,
        "new_device_flag"     : new_dev_flag,
        "is_cross_border"     : is_cross,
        "is_p2p"              : int(tx_type == "P2P"),
        "is_rtp"              : int(tx_type == "RTP"),
        "fraud_scenario"      : scenario.value,
        "is_fraud"            : is_fraud,
        "user_profile"        : user.profile_type.value,
    }
    _record(user, tx, now_ms)
    return tx


# ════════════════════════════════════════════════════════════════
# SCENARIO ENGINE
# ════════════════════════════════════════════════════════════════

class ScenarioEngine:
    """
    Génère des transactions cohérentes pour chaque scénario.
    Chaque scénario multi-étapes persiste son état dans user.scenario_data.
    """

    # ── Sélection ────────────────────────────────────────────────
    # En fonction de l'état actuel de l'utilisateur (scénario actif, dormance, profil, etc.) 
    # et du timestamp actuel, sélectionne le scénario à appliquer pour la prochaine transaction.
    def select(self, user: UserState, now: datetime) -> Scenario:
        # Scénario en cours → continuer
        if user.active_scenario != Scenario.NONE and user.scenario_step > 0:
            return user.active_scenario

        # Compte dormant → MULE_ACCOUNT prioritaire
        if user.dormant_since is not None:
            idle = (now - user.dormant_since).days
            if idle >= 30 and random.random() < 0.85:
                return Scenario.MULE_ACCOUNT

        sw = PROFILES[user.profile_type].scenario_weights
        return Scenario(random.choices(list(sw.keys()), weights=list(sw.values()), k=1)[0])

    # ── Dispatch ─────────────────────────────────────────────────
    # En fonction du scénario sélectionné, appelle la méthode correspondante pour générer une transaction cohérente 
    # avec ce scénario, en passant l'état de l'utilisateur et le timestamp actuel. 
    # En cas d'erreur (scénario non implémenté ou problème dans la génération), 
    # réinitialise l'état de scénario de l'utilisateur et génère une transaction normale.
    def execute(self, user: UserState, scenario: Scenario, now_ms: int) -> dict:
        user.active_scenario = scenario
        try:
            return {
                Scenario.NONE             : self._normal,
                Scenario.ACCOUNT_TAKEOVER : self._account_takeover,
                Scenario.BURST            : self._burst,
                Scenario.SMURFING         : self._smurfing,
                Scenario.MULE_ACCOUNT     : self._mule,
                Scenario.COLLECTOR_ACCOUNT: self._collector,
                Scenario.CROSS_BORDER     : self._cross_border,
                Scenario.RETRY            : self._retry,
                Scenario.LATENCY          : self._latency,
                Scenario.RTP_ABUSE        : self._rtp_abuse,
                Scenario.NEW_DEVICE       : self._new_device,
            }[scenario](user, now_ms)
        except (KeyError, IndexError):
            _reset(user)
            return self._normal(user, now_ms)

    # ── NONE ─────────────────────────────────────────────────────
    # Transaction normale, cohérente avec le profil de l'utilisateur
    #  (montant, appareil, bénéficiaire, pays, institution, latence réaliste, etc.).
    def _normal(self, user: UserState, now_ms: int) -> dict:
        profile = PROFILES[user.profile_type]
        amount  = _clamp(random.gauss(user.avg_amount, user.stddev_amount), profile)
        tx_type = random.choices(
            ["P2P","P2M","P2G","RTP","TRANSFER"],
            weights=[0.45, 0.25, 0.12, 0.08, 0.10],
        )[0]
        return _build_tx(
            user=user, now_ms=now_ms, amount=amount,
            device=random.choice(user.usual_devices),
            tx_type=tx_type,
            status=random.choices(STATUSES, [0.94, 0.04, 0.02])[0],
            latency=_normal_latency(),
            receiver=_beneficiary(user),
            recv_country=user.home_country,
            recv_inst=random.choice(INSTITUTIONS.get(user.home_country, ALL_INSTITUTIONS)),
            scenario=Scenario.NONE, is_fraud=0,
        )

    # ── ACCOUNT_TAKEOVER ─────────────────────────────────────────
    # Scénario de prise de contrôle de compte : une fois le scénario déclenché, on simule d'abord 1-2 transactions normales 
    # pour crédibiliser la baseline, puis on génère une transaction frauduleuse avec les caractéristiques suivantes :
    # Nouvel appareil inconnu + pays à risque + montant × 8-20 + heure nocturne
    def _account_takeover(self, user: UserState, now_ms: int) -> dict:
        sd = user.scenario_data
        if "ato_device" not in sd:
            sd["ato_device"]  = random.choice(UNKNOWN_DEVICES)
            sd["ato_country"] = random.choice(PAYS_RISQUE)
            sd["ato_inst"]    = random.choice(UNKNOWN_INSTITUTIONS)
            sd["ato_amount"]  = user.avg_amount * random.uniform(8, 20)
            sd["warmup"]      = random.randint(1, 2)
            user.scenario_step= 1

        # 1-2 tx normales d'abord pour crédibiliser la baseline
        if user.scenario_step <= sd["warmup"]:
            user.scenario_step += 1
            return self._normal(user, now_ms)

        # Transaction ATO proprement dite
        # Heure nocturne : on force l'heure dans le timestamp
        dt = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
        # Forcer entre 1h et 4h du matin (heure locale UTC)
        night_offset = random.randint(1, 4) * 3600 - dt.hour * 3600
        ato_ms = now_ms + night_offset * 1000

        tx = _build_tx(
            user=user, now_ms=ato_ms,
            amount=sd["ato_amount"],
            device=sd["ato_device"],
            tx_type="TRANSFER",
            status="SUCCESS",
            latency=max(200, int(random.gauss(600, 150))),
            receiver=_alias(),
            recv_country=sd["ato_country"],
            recv_inst=sd["ato_inst"],
            scenario=Scenario.ACCOUNT_TAKEOVER, is_fraud=1,
        )
        _reset(user)
        return tx

    # ── BURST ────────────────────────────────────────────────────
    # 15 transactions du même alias en < 20 secondes
    # → tx_count_1min élevé, burst_score >> 1, avg_inter_tx_time faible
    def _burst(self, user: UserState, now_ms: int) -> dict:
        sd = user.scenario_data
        if "total" not in sd:
            sd["total"]    = random.randint(12, 20)
            sd["receiver"] = _alias()               # même bénéficiaire
            sd["amount"]   = random.uniform(1_000, 15_000)
            # Les 15 tx tiennent dans 20 secondes → inter-tx ~1.3s
            sd["interval_ms"] = random.randint(800, 1_800)
            sd["base_ms"]     = now_ms
            user.scenario_step= 1

        # Timestamp resserré dans la fenêtre de 20s
        step_ms = sd["base_ms"] + (user.scenario_step - 1) * sd["interval_ms"]
        amount  = sd["amount"] * random.uniform(0.85, 1.15)
        # Même appareil pour toutes les tx du burst (mais pas forcément connu, pour crédibiliser la fraude)
        tx = _build_tx(
            user=user, now_ms=step_ms,
            amount=amount,
            device=random.choice(user.usual_devices),
            tx_type="P2P",
            status="SUCCESS",
            latency=max(30, int(random.gauss(90, 15))),
            receiver=sd["receiver"],
            recv_country=user.home_country,
            recv_inst=random.choice(INSTITUTIONS.get(user.home_country, ALL_INSTITUTIONS)),
            scenario=Scenario.BURST, is_fraud=1,
        )

        user.scenario_step += 1
        if user.scenario_step > sd["total"]:
            _reset(user)

        return tx

    # ── SMURFING ─────────────────────────────────────────────────
    # Fractionnement d'un gros montant en ~50 micro-tx sous seuil BCEAO
    # vers de nombreux bénéficiaires différents
    def _smurfing(self, user: UserState, now_ms: int) -> dict:
        sd = user.scenario_data
        if "total" not in sd:
            threshold      = random.choice(SEUILS_BCEAO)
            n_frags        = 1
            sd["total"]    = n_frags
            # Chaque fragment = 90 000 – 120 000 XOF (bien sous 300k)
            sd["amount"]   = random.uniform(90_000, 120_000)
            sd["receivers"]= [_alias() for _ in range(n_frags)]  # N bénéficiaires
            user.scenario_step = 1

        idx      = user.scenario_step - 1
        receiver = sd["receivers"][min(idx, len(sd["receivers"]) - 1)]
        amount   = sd["amount"] * random.uniform(0.92, 1.08)

        tx = _build_tx(
            user=user, now_ms=now_ms,
            amount=amount,
            device=random.choice(user.usual_devices),
            tx_type=random.choice(["P2P", "TRANSFER"]),
            status="SUCCESS",
            latency=max(100, int(random.gauss(350, 80))),
            receiver=receiver,
            recv_country=user.home_country,
            recv_inst=random.choice(INSTITUTIONS.get(user.home_country, ALL_INSTITUTIONS)),
            scenario=Scenario.SMURFING, is_fraud=1,
        )

        user.scenario_step += 1
        if user.scenario_step > sd["total"]:
            _reset(user)

        return tx

    # ── MULE_ACCOUNT ─────────────────────────────────────────────
    # Compte dormant 30j+ → activité soudaine massive (100 tx en quelques heures)
    def _mule(self, user: UserState, now_ms: int) -> dict:
        sd = user.scenario_data
        if "total" not in sd:
            # Simuler les inbound (nombreux expéditeurs → enrichit known_senders)
            for _ in range(random.randint(10, 30)):
                user.known_senders.add(_alias())
            sd["total"]       = 1
            sd["amount_base"] = _clamp(
                random.gauss(user.avg_amount * 0.75, user.stddev_amount * 0.2),
                PROFILES[user.profile_type],
            )
            user.dormant_since  = None   # réactivation
            user.scenario_step  = 1

        receiver = _alias()   # nouveau destinataire à chaque fois
        amount   = sd["amount_base"] * random.uniform(0.85, 1.15)
        dst      = random.choice(PAYS_UEMOA) if random.random() < 0.35 \
                   else user.home_country

        tx = _build_tx(
            user=user, now_ms=now_ms,
            amount=amount,
            device=random.choice(user.usual_devices),
            tx_type="P2P",
            status="SUCCESS",
            latency=max(100, int(random.gauss(280, 70))),
            receiver=receiver,
            recv_country=dst,
            recv_inst=random.choice(INSTITUTIONS.get(dst, ALL_INSTITUTIONS)),
            scenario=Scenario.MULE_ACCOUNT, is_fraud=1,
        )

        user.scenario_step += 1
        if user.scenario_step > sd["total"]:
            _reset(user)

        return tx

    # ── COLLECTOR_ACCOUNT ────────────────────────────────────────
    # 1 receiver ← N payeurs : détecte les hubs de fraude
    # On génère depuis la perspective du payer habituel → le receiver_alias
    # est toujours le même "collecteur"
    def _collector(self, user: UserState, now_ms: int) -> dict:
        sd = user.scenario_data
        if "collector" not in sd:
            sd["collector"] = _alias()   # le compte collecteur cible
            sd["total"]     = 1
            sd["amount"]    = random.uniform(5_000, 30_000)
            user.scenario_step = 1

        amount = sd["amount"] * random.uniform(0.90, 1.10)

        tx = _build_tx(
            user=user, now_ms=now_ms,
            amount=amount,
            device=random.choice(user.usual_devices),
            tx_type="P2P",
            status="SUCCESS",
            latency=max(80, int(random.gauss(200, 50))),
            receiver=sd["collector"],    # toujours le même collecteur
            recv_country=user.home_country,
            recv_inst=random.choice(INSTITUTIONS.get(user.home_country, ALL_INSTITUTIONS)),
            scenario=Scenario.COLLECTOR_ACCOUNT, is_fraud=1,
        )

        user.scenario_step += 1
        if user.scenario_step > sd["total"]:
            _reset(user)

        return tx

    # ── CROSS_BORDER ─────────────────────────────────────────────
    # Pays à risque + institution inconnue + montant NORMAL (pas d'anomalie montant)
    # → le modèle apprend la combinaison pays/institution, pas juste le montant
    def _cross_border(self, user: UserState, now_ms: int) -> dict:
        # Montant normal (pas de multiplication)
        profile = PROFILES[user.profile_type]
        amount  = _clamp(
            random.gauss(user.avg_amount * 1.1, user.stddev_amount),
            profile,
        )
        recv_country = random.choice(PAYS_RISQUE)
        recv_inst    = random.choice(UNKNOWN_INSTITUTIONS)

        tx = _build_tx(
            user=user, now_ms=now_ms,
            amount=amount,
            device=random.choice(user.usual_devices),
            tx_type="TRANSFER",
            status="SUCCESS",
            latency=max(400, int(random.gauss(1_100, 250))),
            receiver=_alias(),
            recv_country=recv_country,
            recv_inst=recv_inst,
            scenario=Scenario.CROSS_BORDER, is_fraud=1,
        )
        _reset(user)
        return tx

    # ── RETRY ────────────────────────────────────────────────────
    # Séquence réaliste : FAILED × N puis SUCCESS sur même bénéficiaire
    # → failure_rate_10tx élevé + retry_pattern_flag = 1
    def _retry(self, user: UserState, now_ms: int) -> dict:
        sd = user.scenario_data
        if "sequence" not in sd:
            n_fails     = random.randint(1, 2)
            sd["sequence"]  = ["FAILED"] * n_fails + ["SUCCESS"]
            sd["receiver"]  = user.history[-1].receiver if user.history else _beneficiary(user)
            sd["amount"]    = _clamp(
                random.gauss(user.avg_amount, user.stddev_amount * 0.4),
                PROFILES[user.profile_type],
            )
            user.scenario_step = 0

        step   = user.scenario_step
        seq    = sd["sequence"]
        status = seq[min(step, len(seq) - 1)]

        # Latence élevée sur les FAILED (timeout → retry)
        latency = max(3_000, int(random.gauss(5_000, 800))) \
                  if status == "FAILED" \
                  else max(100, int(random.gauss(300, 80)))

        tx = _build_tx(
            user=user, now_ms=now_ms,
            amount=sd["amount"],
            device=random.choice(user.usual_devices),
            tx_type="TRANSFER",
            status=status,
            latency=latency,
            receiver=sd["receiver"],
            recv_country=user.home_country,
            recv_inst=random.choice(INSTITUTIONS.get(user.home_country, ALL_INSTITUTIONS)),
            scenario=Scenario.RETRY,
            is_fraud=int(status == "FAILED"),
        )

        user.scenario_step += 1
        if user.scenario_step >= len(seq):
            _reset(user)

        return tx

    # ── LATENCY ──────────────────────────────────────────────────
    # Pics de latence réalistes depuis une baseline bruitée.
    # Séquence : 220, 240, 260, 270, 290, 3200, 3500, 3800
    # → pas un 4000ms fixe, mais un vrai spike statistique
    def _latency(self, user: UserState, now_ms: int) -> dict:
        sd = user.scenario_data
        if "total" not in sd:
            sd["total"]     = 1  # 1 tx spike direct
            sd["spike_at"]  = 1
            user.scenario_step = 1

        step    = user.scenario_step
        is_spike= (step == sd["spike_at"])

        latency = _realistic_latency_spike(user) if is_spike \
                  else max(50, int(random.gauss(240, 40)))  # légère montée avant spike

        profile = PROFILES[user.profile_type]
        amount  = _clamp(random.gauss(user.avg_amount, user.stddev_amount), profile)

        tx = _build_tx(
            user=user, now_ms=now_ms,
            amount=amount,
            device=random.choice(user.usual_devices),
            tx_type=random.choice(["P2P","P2M","TRANSFER"]),
            status="SUCCESS",
            latency=latency,
            receiver=_beneficiary(user),
            recv_country=user.home_country,
            recv_inst=random.choice(INSTITUTIONS.get(user.home_country, ALL_INSTITUTIONS)),
            scenario=Scenario.LATENCY,
            is_fraud=int(is_spike),
        )

        user.scenario_step += 1
        if user.scenario_step > sd["total"]:
            _reset(user)

        return tx

    # ── RTP_ABUSE ─────────────────────────────────────────────────
    # 100 RTP en < 60 secondes
    # → is_rtp_flag + tx_count_1min très élevé + burst_score élevé
    def _rtp_abuse(self, user: UserState, now_ms: int) -> dict:
        sd = user.scenario_data
        if "total" not in sd:
            sd["total"]       = random.randint(3, 5)
            sd["receiver"]    = _alias()
            sd["amount"]      = random.uniform(500, 5_000)
            sd["interval_ms"] = random.randint(400, 700)   # < 1s entre chaque RTP
            sd["base_ms"]     = now_ms
            user.scenario_step= 1

        step_ms = sd["base_ms"] + (user.scenario_step - 1) * sd["interval_ms"]
        amount  = sd["amount"] * random.uniform(0.90, 1.10)

        tx = _build_tx(
            user=user, now_ms=step_ms,
            amount=amount,
            device=random.choice(user.usual_devices),
            tx_type="RTP",          # toujours RTP
            status="SUCCESS",
            latency=max(20, int(random.gauss(60, 15))),
            receiver=sd["receiver"],
            recv_country=user.home_country,
            recv_inst=random.choice(INSTITUTIONS.get(user.home_country, ALL_INSTITUTIONS)),
            scenario=Scenario.RTP_ABUSE, is_fraud=1,
        )

        user.scenario_step += 1
        if user.scenario_step > sd["total"]:
            _reset(user)

        return tx

    # ── NEW_DEVICE ────────────────────────────────────────────────
    # Connexion depuis un appareil jamais utilisé par cet alias
    # → new_device_flag = 1 (calculé dans _build_tx)
    def _new_device(self, user: UserState, now_ms: int) -> dict:
        # Choisir un device hors des habitudes (mais pas forcément "UNKNOWN")
        all_devs   = DEVICES + UNKNOWN_DEVICES
        new_device = random.choice(
            [d for d in all_devs if d not in user.known_devices]
            or UNKNOWN_DEVICES
        )
        profile = PROFILES[user.profile_type]
        amount  = _clamp(
            random.gauss(user.avg_amount * random.uniform(1.5, 4.0), user.stddev_amount),
            profile,
        )

        tx = _build_tx(
            user=user, now_ms=now_ms,
            amount=amount,
            device=new_device,        # appareil inconnu
            tx_type=random.choice(["P2P","TRANSFER"]),
            status="SUCCESS",
            latency=max(100, int(random.gauss(400, 120))),
            receiver=_alias(),        # nouveau bénéficiaire
            recv_country=user.home_country,
            recv_inst=random.choice(INSTITUTIONS.get(user.home_country, ALL_INSTITUTIONS)),
            scenario=Scenario.NEW_DEVICE, is_fraud=1,
        )
        _reset(user)
        return tx


# ════════════════════════════════════════════════════════════════
# KAFKA CALLBACK
# ════════════════════════════════════════════════════════════════
# Callback de livraison pour le producteur Kafka, qui logue les succès et les échecs de livraison des messages,
# en affichant les détails du message en cas de succès ou l'erreur en cas d'échec.
def on_delivery(err, msg) -> None:
    if err:
        log.error("❌ Livraison échouée : %s", err)
    else:
        log.debug("✅ topic=%s partition=%d offset=%d",
                  msg.topic(), msg.partition(), msg.offset())


# ════════════════════════════════════════════════════════════════
# BOUCLE PRINCIPALE  ─  arrêt UNIQUEMENT sur Ctrl+C
# ════════════════════════════════════════════════════════════════

def run() -> None:
    log.info("Connexion Kafka → %s", KAFKA_BOOTSTRAP)

    producer = Producer({
        "bootstrap.servers"  : KAFKA_BOOTSTRAP,
        "client.id"          : "pispi-producer-v5",
        "acks"               : "all",
        "retries"            : 5,
        "retry.backoff.ms"   : 1_000,
        "linger.ms"          : 5,
        "compression.type"   : "gzip",
        "request.timeout.ms" : 30_000,
        "message.timeout.ms" : 60_000,
    })

    seed_time = datetime.now(timezone.utc) - timedelta(days=30)
    pool      = UserPool(N_USERS, seed_time)
    engine    = ScenarioEngine()
    interval  = 1.0 / TX_PER_SEC

    sent_total    : int            = 0
    fraud_total   : int            = 0
    scenario_counts: Dict[str,int] = defaultdict(int)

    log.info("🚀 Producteur v5 démarré — %.0f tx/sec — Ctrl+C pour arrêter", TX_PER_SEC)

    try:
        while True:

            loop_start = time.perf_counter()

            user     = pool.pick()
            now_ms   = int(datetime.now(timezone.utc).timestamp() * 1000)
            scenario = engine.select(user, datetime.fromtimestamp(now_ms/1000, tz=timezone.utc))
            tx       = engine.execute(user, scenario, now_ms)

            try:
                producer.produce(
                    topic=TOPIC_RAW,
                    key=tx["payer_alias"].encode(),
                    value=json.dumps(tx).encode(),
                    on_delivery=on_delivery,
                )
            except BufferError:
                producer.poll(1)
                continue

            producer.poll(0)
            if sent_total % 50 == 0:
                producer.flush()

            # Padding NONE par ÉVÉNEMENT fraud (pas par tx individuelle).
            # On pad uniquement au step=1 (début de scénario) OU scénarios 1 tx.
            # Ratio : 1 event × 19 NONE = ~5% fraud rate.
            is_fraud_event_start = (
                tx["is_fraud"] == 1 and user.scenario_step <= 1
            )
            if is_fraud_event_start:
                for _ in range(19):
                    pu  = pool.pick()
                    pms = int(datetime.now(timezone.utc).timestamp() * 1000)
                    ptx = engine.execute(pu, Scenario.NONE, pms)
                    try:
                        producer.produce(
                            topic=TOPIC_RAW,
                            key=ptx["payer_alias"].encode(),
                            value=json.dumps(ptx).encode(),
                            on_delivery=on_delivery,
                        )
                        producer.poll(0)
                    except BufferError:
                        producer.flush()   # vider le buffer avant de continuer
                        producer.produce(
                            topic=TOPIC_RAW,
                            key=ptx["payer_alias"].encode(),
                            value=json.dumps(ptx).encode(),
                            on_delivery=on_delivery,
                        )
                    sent_total += 1
                    scenario_counts["NONE"] += 1
                producer.flush()   # flush après chaque bloc padding complet

            sent_total += 1
            scenario_counts[tx["fraud_scenario"]] += 1
            if tx["is_fraud"]:
                fraud_total += 1

            if sent_total % 500 == 0:
                rate = fraud_total / sent_total * 100
                log.info(
                    "Envoyées=%-7d | Fraudes=%d (%.1f%%) | %s",
                    sent_total, fraud_total, rate,
                    "  ".join(f"{k}={v}" for k, v in sorted(scenario_counts.items()) if v > 0),
                )

            elapsed = time.perf_counter() - loop_start
            time.sleep(max(0.0, interval - elapsed))

    except KeyboardInterrupt:
        log.info("🛑 Arrêt manuel (Ctrl+C)")

    finally:
        producer.flush()
        rate = fraud_total / max(sent_total, 1) * 100
        log.info("✅ Total=%d | Fraudes=%d (%.1f%%)", sent_total, fraud_total, rate)
        log.info("Distribution : %s", dict(scenario_counts))


if __name__ == "__main__":
    run()