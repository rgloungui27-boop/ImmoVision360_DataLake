"""
Script 06 - Load : Injection dans PostgreSQL (Data Warehouse)
ImmoVision 360 - ETL Phase 2

Objectif : Charger transformed_elysee.csv dans la base PostgreSQL
           table : elysee_listings_silver

Auteur   : [Votre Nom]
Date     : Avril 2026
"""

import logging
import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ─────────────────────────────────────────────
# 0. Configuration
# ─────────────────────────────────────────────

BASE_DIR   = Path(__file__).resolve().parent.parent
INPUT_CSV  = BASE_DIR / "data" / "processed" / "transformed_elysee.csv"
LOG_FILE   = BASE_DIR / "scripts" / "06_load.log"
ENV_FILE   = BASE_DIR / ".env"

TABLE_NAME = "elysee_listings_silver"

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
    log.info("ImmoVision 360 — Script 06 : Load → PostgreSQL")
    log.info("=" * 60)

    # Chargement des variables d'environnement
    load_dotenv(ENV_FILE)
    user     = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "")
    host     = os.getenv("DB_HOST", "localhost")
    port     = os.getenv("DB_PORT", "5432")
    dbname   = os.getenv("DB_NAME", "immovision_db")

    log.info(f"Connexion à PostgreSQL : {host}:{port}/{dbname} (user={user})")

    # Lecture du CSV Silver
    log.info(f"Lecture de {INPUT_CSV} ...")
    df = pd.read_csv(INPUT_CSV, dtype={"id": str}, low_memory=False)
    log.info(f"  → {len(df):,} lignes | {len(df.columns)} colonnes")

    # Connexion PostgreSQL
    engine = create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    )

    # Test de connexion
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        log.info("  Connexion PostgreSQL établie avec succès.")
    except Exception as e:
        log.error(f"Impossible de se connecter à PostgreSQL : {e}")
        log.error("Vérifiez votre fichier .env et que PostgreSQL est démarré.")
        raise

    # Injection (idempotente : replace)
    log.info(f"Injection dans la table '{TABLE_NAME}' (mode: replace) ...")
    df.to_sql(
        TABLE_NAME,
        engine,
        if_exists="replace",
        index=False,
        chunksize=500,
    )
    log.info(f"  → {len(df):,} lignes chargées avec succès.")

    # Vérification
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE_NAME}"))
        count = result.fetchone()[0]
    log.info(f"  Vérification PostgreSQL : {count:,} lignes dans '{TABLE_NAME}'")

    log.info("=" * 60)
    log.info("Script 06 terminé — Data Warehouse prêt !")
    log.info("=" * 60)

if __name__ == "__main__":
    run()
