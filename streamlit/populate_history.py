"""
populate_history.py  v2  ·  PI-SPI
=====================================
Script de peuplement de l'historique mémoire de l'API PI-SPI avec des transactions synthétiques.
Chaque alias reçoit un nombre défini de transactions, dont une petite fraction sont frauduleuses.
Corrections majeures par rapport à la v1 :
  1. /api/metrics au lieu de /metrics (Prometheus text → JSON)
  2. Progress bar sans aliases.index() (O(n²) → O(1))
  3. Timestamps corrigés (now_ms recalculé à chaque alias)
  4. Résumé final plus lisible
"""

import argparse
import random
import time
from datetime import datetime, timezone

import requests
from torch import Dict, List


# ── URL de l'API ─────────────────────────────────────────────
API_URL = "http://localhost:8000"

# ── Référentiels ────────────────────────────────────────────
PAYS_UEMOA      : List[str] = ["SN", "CI", "ML", "BF", "BJ", "TG", "GW", "NE"]
PAYS_RISQUE     : List[str] = ["NG", "GH", "CM", "MA", "FR", "US", "CN", "AE"]

# ── Institutions ────────────────────────────────────────────
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
# Institutions "risque" pour les transactions frauduleuses (souvent hors UEMOA)
INSTITUTIONS_RISQUE: List[str] = ["ZENITH-NG", "GTB-NG", "HSBC-AE"]
# Types de transactions, statuts et appareils
DEVICES  = ["ANDROID", "IOS", "WEB", "USSD", "FEATURE_PHONE"]
TX_TYPES = ["P2P", "P2M", "TRANSFER", "RTP"]
STATUSES = ["SUCCESS"] * 14 + ["FAILED"] * 4 + ["REJECTED"] * 2

# ── Profils ──────────────────────────────────────────────────
ALIAS_PROFILES = {
    "normal"  : {"avg": 15_000,  "std": 8_000,   "country": "SN"},
    "merchant": {"avg": 85_000,  "std": 40_000,  "country": "SN"},
    "hnw"     : {"avg": 750_000, "std": 300_000, "country": "CI"},
    "mule"    : {"avg": 45_000,  "std": 20_000,  "country": "SN"},
    "traveler": {"avg": 120_000, "std": 60_000,  "country": "SN"},
}

# ── Fonction de génération de transaction synthétique ───────────────────────────────
def make_transaction(alias: str, profile: dict, ts_ms: int, is_fraud: bool = False) -> dict:
    country = profile["country"]
    amount  = max(500, int(random.gauss(profile["avg"], profile["std"])))

    if is_fraud:
        amount       = amount * random.randint(8, 15)
        recv_country = random.choice(PAYS_RISQUE)
        recv_inst    = random.choice(INSTITUTIONS_RISQUE)
        device       = "UNKNOWN_ANDROID"
    else:
        recv_country = country if random.random() < 0.85 else random.choice(PAYS_UEMOA)
        recv_inst    = random.choice(INSTITUTIONS.get(recv_country, ["WAVE-SN"]))
        device       = random.choice(DEVICES)

    return {
        "payer_alias"          : alias,
        "receiver_alias"       : f"als_recv_{random.randint(1000, 9999)}",
        "amount"               : float(amount),
        "timestamp"            : ts_ms,
        "device"               : device,
        "transaction_type"     : random.choice(TX_TYPES),
        "issuer_institution"   : random.choice(INSTITUTIONS.get(country, ["WAVE-SN"])),
        "receiver_institution" : recv_inst,
        "issuer_country"       : country,
        "receiver_country"     : recv_country,
        "status"               : random.choice(STATUSES),
        "response_time_ms"     : max(50, int(random.gauss(220, 60))),
        "new_device_flag"      : int(device.startswith("UNKNOWN")),
        "is_cross_border"      : int(country != recv_country),
        "is_p2p"               : int(random.choice(TX_TYPES) == "P2P"),
        "is_rtp"               : 0,
    }

# ════════════════════════════════════════════════════════════════
# FEAST FEATURE VIEWS
# ════════════════════════════════════════════════════════════════
def populate(n_alias: int = 300, tx_per_alias: int = 20, fast: bool = False):

    # ── Vérifier l'API ────────────────────────────────────────
    try:
        r = requests.get(f"{API_URL}/health", timeout=3)
        h = r.json()
        print(f"✅ API connectée — modèle={h['model']} | threshold={h['threshold']}")
        print(f"   GNN={h.get('gnn_active','?')} | Feast={h.get('feast_available','?')}")
    except Exception as e:
        print(f"❌ API non joignable : {e}")
        print("   Lance d'abord : python streamlit/fraud_api.py")
        return

    total   = n_alias * tx_per_alias
    sent    = 0
    errors  = 0
    frauds  = 0
    t_start = time.time()

    print(f"\n🚀 Peuplement : {n_alias} alias × {tx_per_alias} tx = {total} transactions")
    print(f"   Délai entre requêtes : {'aucun (--fast)' if fast else '10ms'}\n")

    # ── Générer les alias ─────────────────────────────────────
    profile_names = list(ALIAS_PROFILES.keys())
    aliases = [
        (f"als_{profile_names[i % len(profile_names)]}_{i:04d}",
         ALIAS_PROFILES[profile_names[i % len(profile_names)]])
        for i in range(n_alias)
    ]

    # ── Envoyer les transactions ──────────────────────────────
    for idx, (alias, profile) in enumerate(aliases):

        now_ms   = int(datetime.now(timezone.utc).timestamp() * 1000)
        start_ms = now_ms - 24 * 3_600_000   # il y a 24h

        for j in range(tx_per_alias):
            ts_ms    = start_ms + int(j * 24 * 3_600_000 / tx_per_alias)
            is_fraud = (j == tx_per_alias - 1) and random.random() < 0.05

            payload = make_transaction(alias, profile, ts_ms, is_fraud)

            try:
                r = requests.post(f"{API_URL}/score", json=payload, timeout=5)
                if r.ok:
                    sent += 1
                    if is_fraud:
                        frauds += 1
                else:
                    errors += 1
            except Exception:
                errors += 1

            if not fast:
                time.sleep(0.01)

        # ── Progress bar ──────────────────────────────────────
        progress = int((idx + 1) / n_alias * 40)
        bar      = "█" * progress + "░" * (40 - progress)
        pct      = (idx + 1) / n_alias * 100
        elapsed  = time.time() - t_start
        rate     = sent / elapsed if elapsed > 0 else 0
        eta      = (total - sent) / rate if rate > 0 else 0
        print(
            f"\r  [{bar}] {pct:5.1f}%  "
            f"alias={idx+1}/{n_alias}  "
            f"tx={sent}  "
            f"erreurs={errors}  "
            f"{rate:.0f}tx/s  "
            f"ETA={eta:.0f}s   ",
            end=""
        )

    # ── Résumé final ──────────────────────────────────────────
    elapsed = time.time() - t_start
    print(f"\n\n{'='*55}")
    print(f"  ✅ Peuplement terminé en {elapsed:.1f}s")
    print(f"  📤 Envoyées  : {sent:>6d}")
    print(f"  🚨 Fraudes   : {frauds:>6d} ({frauds/max(sent,1)*100:.1f}%)")
    print(f"  ❌ Erreurs   : {errors:>6d}")
    print(f"  ⚡ Débit     : {sent/elapsed:.0f} tx/s")
    print(f"{'='*55}")

    # ── État mémoire via /api/metrics ─────────────────────────
    # CORRECTION : /api/metrics retourne JSON, /metrics retourne texte Prometheus
    try:
        r = requests.get(f"{API_URL}/api/metrics", timeout=3)
        if r.ok:
            m = r.json()
            print(f"\n📊 État mémoire API :")
            print(f"   Alias en mémoire     : {m.get('total_aliases', '?')}")
            print(f"   Tx en mémoire        : {m.get('total_tx_in_memory', '?')}")
            print(f"   Alertes /1h          : {m.get('alerts_last_hour', '?')}")
            top_rules = m.get("rule_triggers_last_hour", {})
            if top_rules:
                print(f"   Top règles déclenchées :")
                for rule, count in sorted(top_rules.items(), key=lambda x: -x[1])[:5]:
                    print(f"     {rule:<25} {count}")
    except Exception:
        pass

    print(f"\n💡 Résultat attendu dans le dashboard :")
    print(f"   - SHAP : features basées sur {tx_per_alias} tx d'historique réel")
    print(f"   - Grafana : aliases_in_memory ≈ {n_alias}")
    print(f"   - Règles  : BURST / STRUCTURING / MULE détectables")


# ════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Peuple l'historique mémoire de l'API PI-SPI"
    )
    parser.add_argument("--alias", type=int, default=300,
                        help="Nombre d'alias (défaut: 300)")
    parser.add_argument("--tx",    type=int, default=20,
                        help="Transactions par alias (défaut: 20)")
    parser.add_argument("--fast",  action="store_true",
                        help="Sans délai entre requêtes (plus rapide)")
    args = parser.parse_args()

    populate(
        n_alias      = args.alias,
        tx_per_alias = args.tx,
        fast         = args.fast,
    )