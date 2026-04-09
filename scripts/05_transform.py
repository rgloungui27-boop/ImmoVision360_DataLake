"""
Script 05 - Transform : Nettoyage & Feature Engineering
ImmoVision 360 - ETL Phase 2

Objectif : Nettoyer filtered_elysee.csv et enrichir avec deux nouvelles
           colonnes simulées (valeurs aléatoires parmi 1, 0, -1) :
           - Standardization_Score  (normalement issu de l'analyse d'image IA)
           - Neighborhood_Impact    (normalement issu de l'analyse NLP)

Auteur   : [Votre Nom]
Date     : Avril 2026
"""

import logging
import pandas as pd
import numpy as np
from pathlib import Path

BASE_DIR      = Path(__file__).resolve().parent.parent
INPUT_CSV     = BASE_DIR / "data" / "processed" / "filtered_elysee.csv"
OUTPUT_CSV    = BASE_DIR / "data" / "processed" / "transformed_elysee.csv"
LOG_FILE      = BASE_DIR / "scripts" / "05_transform.log"

RANDOM_SEED   = 42
RANDOM_VALUES = [1, 0, -1]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

def clean_price(series):
    return (
        series.astype(str)
        .str.replace(r"[\$,]", "", regex=True)
        .str.strip()
        .replace("nan", np.nan)
        .astype(float)
    )

def clean_percentage(series):
    return (
        series.astype(str)
        .str.replace("%", "", regex=False)
        .str.strip()
        .replace("nan", np.nan)
        .astype(float)
        / 100
    )

def clean_boolean(series):
    return series.map({"t": 1, "f": 0, True: 1, False: 0}).astype("Int64")

def handle_missing(df):
    # Simulation du prix si manquant
    mask_price = df["price"].isna()
    if mask_price.sum() > 0:
        df.loc[mask_price, "price"] = np.random.default_rng(42).uniform(50, 500, size=mask_price.sum())
        log.info(f"  Prix simulés pour {mask_price.sum()} annonces sans prix")

    # Imputation médiane
    for col in ["review_scores_rating", "host_response_rate", "bedrooms", "beds"]:
        if col in df.columns:
            median_val = df[col].median()
            n_filled = df[col].isna().sum()
            df[col] = df[col].fillna(median_val)
            log.info(f"  Imputation médiane '{col}' : {n_filled} valeurs → {median_val:.2f}")

    # Imputation logique
    if "reviews_per_month" in df.columns:
        n = df["reviews_per_month"].isna().sum()
        df["reviews_per_month"] = df["reviews_per_month"].fillna(0)
        log.info(f"  Imputation logique 'reviews_per_month' : {n} valeurs → 0")

    if "number_of_reviews" in df.columns:
        df["number_of_reviews"] = df["number_of_reviews"].fillna(0)

    return df

def add_simulated_features(df):
    rng = np.random.default_rng(RANDOM_SEED)
    df["Standardization_Score"] = rng.choice(RANDOM_VALUES, size=len(df))
    df["Neighborhood_Impact"]   = rng.choice(RANDOM_VALUES, size=len(df))
    log.info(f"  Standardization_Score : {df['Standardization_Score'].value_counts().to_dict()}")
    log.info(f"  Neighborhood_Impact   : {df['Neighborhood_Impact'].value_counts().to_dict()}")
    return df

def run():
    log.info("=" * 60)
    log.info("ImmoVision 360 — Script 05 : Transform")
    log.info("=" * 60)

    log.info(f"Lecture de {INPUT_CSV} ...")
    df = pd.read_csv(INPUT_CSV, dtype={"id": str}, low_memory=False)
    log.info(f"  → Shape initial : {df.shape}")

    log.info("Nettoyage des types ...")
    if "price" in df.columns:
        df["price"] = clean_price(df["price"])
    if "host_response_rate" in df.columns:
        df["host_response_rate"] = clean_percentage(df["host_response_rate"])
    if "host_is_superhost" in df.columns:
        df["host_is_superhost"] = clean_boolean(df["host_is_superhost"])
    for date_col in ["host_since"]:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    log.info("Gestion des valeurs manquantes ...")
    df = handle_missing(df)

    if "price" in df.columns:
        p99 = df["price"].quantile(0.99)
        n_capped = (df["price"] > p99).sum()
        df["price"] = df["price"].clip(upper=p99)
        log.info(f"  Plafonnement prix à P99 ({p99:.0f}€) : {n_capped} valeurs écrêtées")

    log.info("Ajout des features simulées ...")
    df = add_simulated_features(df)

    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    log.info(f"  → Sauvegardé : {OUTPUT_CSV}")
    log.info(f"  → Shape final : {df.shape}")
    log.info("=" * 60)
    log.info("Script 05 terminé avec succès.")
    log.info("=" * 60)

if __name__ == "__main__":
    run()