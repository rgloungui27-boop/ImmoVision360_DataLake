"""
Script 04 - Extract : Filtrage & Sélection Métier
ImmoVision 360 - ETL Phase 2

Objectif : Extraire uniquement les colonnes utiles de listings_full.csv
           pour répondre aux 3 hypothèses de la Maire de Paris,
           filtrer sur le quartier Élysée et sauvegarder dans
           data/processed/filtered_elysee.csv

Auteur   : [Votre Nom]
Date     : Avril 2026
"""

import logging
import pandas as pd
from pathlib import Path

# ─────────────────────────────────────────────
# 0. Configuration
# ─────────────────────────────────────────────

BASE_DIR     = Path(__file__).resolve().parent.parent
LISTINGS_CSV = BASE_DIR / "data" / "raw" / "tabular" / "listings_full.csv"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
OUTPUT_CSV   = PROCESSED_DIR / "filtered_elysee.csv"
LOG_FILE     = BASE_DIR / "scripts" / "04_extract.log"

QUARTIER     = "Élysée"

# Colonnes à conserver — justifiées par les 3 hypothèses métier
COLS_TO_KEEP = [
    # ── Identifiant (clé de jointure avec images et textes)
    "id",

    # ── Hypothèse A : concentration économique
    "price",                          # prix par nuit
    "property_type",                  # type de bien
    "room_type",                      # logement entier / chambre
    "availability_365",               # disponibilité annuelle
    "calculated_host_listings_count", # nombre d'annonces par hôte (multipropriétés)

    # ── Hypothèse B : déshumanisation de l'accueil
    "host_id",                        # identifiant hôte
    "host_since",                     # ancienneté sur Airbnb
    "host_response_time",             # délai de réponse
    "host_response_rate",             # taux de réponse
    "host_is_superhost",              # statut superhost

    # ── Métriques de popularité / évaluation
    "number_of_reviews",              # volume d'avis
    "review_scores_rating",           # note globale
    "reviews_per_month",              # fréquence des avis

    # ── Géolocalisation
    "latitude",
    "longitude",
    "neighbourhood_cleansed",        # quartier nettoyé

    # ── Capacité
    "accommodates",
    "bedrooms",
    "beds",
]

# ─────────────────────────────────────────────
# 1. Logging
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# 2. Pipeline
# ─────────────────────────────────────────────

def run():
    log.info("=" * 60)
    log.info("ImmoVision 360 — Script 04 : Extract")
    log.info("=" * 60)

    # Chargement
    log.info(f"Lecture de {LISTINGS_CSV} ...")
    df = pd.read_csv(LISTINGS_CSV, dtype={"id": str}, low_memory=False)
    log.info(f"  → {len(df):,} annonces totales | {len(df.columns)} colonnes")

    # Filtrage géographique Élysée
    quartier_col = None
    for col in ["neighbourhood_cleansed", "neighbourhood"]:
        if col in df.columns:
            quartier_col = col
            break
    if quartier_col is None:
        raise ValueError("Colonne quartier introuvable.")

    mask = df[quartier_col].str.contains(QUARTIER, case=False, na=False)
    df_elysee = df[mask].copy()
    log.info(f"  → {len(df_elysee):,} annonces dans le périmètre '{QUARTIER}'")

    # Sélection des colonnes utiles (on ne garde que celles qui existent)
    cols_available = [c for c in COLS_TO_KEEP if c in df_elysee.columns]
    cols_missing   = [c for c in COLS_TO_KEEP if c not in df_elysee.columns]
    if cols_missing:
        log.warning(f"  Colonnes absentes du CSV (ignorées) : {cols_missing}")

    df_filtered = df_elysee[cols_available].copy()
    log.info(f"  → {len(cols_available)} colonnes conservées sur {len(COLS_TO_KEEP)} demandées")

    # Sauvegarde
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df_filtered.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    log.info(f"  → Sauvegardé : {OUTPUT_CSV}")
    log.info(f"  → Shape final : {df_filtered.shape}")

    # Aperçu rapide
    log.info("\n--- Aperçu (5 premières lignes) ---")
    log.info(f"\n{df_filtered.head().to_string()}")

    log.info("=" * 60)
    log.info("Script 04 terminé avec succès.")
    log.info("=" * 60)

if __name__ == "__main__":
    run()
