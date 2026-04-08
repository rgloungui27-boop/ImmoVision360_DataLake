"""
Script 02 - Ingestion des Textes (Reviews NLP)
ImmoVision 360 - Data Lake Phase 1

Objectif : Filtrer les commentaires Airbnb sur le périmètre Élysée,
           regrouper par annonce et écrire un fichier <ID>.txt par annonce
           dans /data/raw/texts/

Auteur   : [Votre Nom]
Date     : Avril 2026
"""

import os
import re
import html
import logging
import argparse
import pandas as pd
from pathlib import Path

# ─────────────────────────────────────────────
# 0. Configuration
# ─────────────────────────────────────────────

BASE_DIR     = Path(__file__).resolve().parent.parent
LISTINGS_CSV = BASE_DIR / "data" / "raw" / "tabular" / "listings.csv"
REVIEWS_CSV  = BASE_DIR / "data" / "raw" / "tabular" / "reviews.csv"
TEXTS_DIR    = BASE_DIR / "data" / "raw" / "texts"
LOG_FILE     = BASE_DIR / "scripts" / "02_ingestion_textes.log"

QUARTIER     = "Élysée"

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
# 2. Nettoyage léger du texte
# ─────────────────────────────────────────────

def clean_text(text: str) -> str:
    """
    Nettoyage minimal : supprime les balises HTML, décode les entités,
    normalise les espaces. Préserve l'UTF-8 (fr, en, ru, de, etc.).
    """
    if not isinstance(text, str):
        return ""
    # Décode les entités HTML (&amp; → &, &lt; → <, etc.)
    text = html.unescape(text)
    # Supprime les balises HTML (<br/>, <p>, etc.)
    text = re.sub(r"<[^>]+>", " ", text)
    # Normalise les espaces multiples / tabulations
    text = re.sub(r"[ \t]+", " ", text).strip()
    return text

# ─────────────────────────────────────────────
# 3. Chargement et filtrage
# ─────────────────────────────────────────────

def load_elysee_ids(listings_path: Path) -> set:
    """
    Charge listings.csv et retourne l'ensemble des IDs
    appartenant au quartier Élysée.
    """
    log.info(f"Lecture de {listings_path} ...")
    df = pd.read_csv(listings_path, dtype={"id": str}, low_memory=False)
    log.info(f"  → {len(df):,} annonces totales.")

    # Détection de la colonne quartier
    quartier_col = None
    for col in ["neighbourhood_cleansed", "neighbourhood", "neighborhood"]:
        if col in df.columns:
            quartier_col = col
            break
    if quartier_col is None:
        raise ValueError("Colonne quartier introuvable dans listings.csv.")

    mask = df[quartier_col].str.contains(QUARTIER, case=False, na=False)
    ids  = set(df.loc[mask, "id"].dropna().astype(str))
    log.info(f"  → {len(ids):,} annonces dans le périmètre '{QUARTIER}'.")
    return ids


def load_reviews(reviews_path: Path, elysee_ids: set) -> pd.DataFrame:
    """
    Charge reviews.csv et filtre sur les IDs Élysée.
    Colonnes requises : listing_id + comments (ou comment).
    """
    log.info(f"Lecture de {reviews_path} ...")
    df = pd.read_csv(reviews_path, dtype={"listing_id": str}, low_memory=False)
    log.info(f"  → {len(df):,} commentaires totaux.")

    # Détection de la colonne texte
    text_col = None
    for col in ["comments", "comment", "review"]:
        if col in df.columns:
            text_col = col
            break
    if text_col is None:
        raise ValueError("Colonne texte introuvable dans reviews.csv.")

    df = df[df["listing_id"].isin(elysee_ids)][["listing_id", text_col]].copy()
    df.rename(columns={text_col: "text"}, inplace=True)
    df.dropna(subset=["text"], inplace=True)
    log.info(f"  → {len(df):,} commentaires sur le périmètre '{QUARTIER}'.")
    return df

# ─────────────────────────────────────────────
# 4. Regroupement et écriture
# ─────────────────────────────────────────────

def write_text_file(listing_id: str, reviews: list[str], dest_dir: Path,
                    overwrite: bool = False) -> bool:
    """
    Écrit (ou ignore) le fichier <ID>.txt.
    Retourne True si écriture effectuée, False si skippé.
    """
    dest_path = dest_dir / f"{listing_id}.txt"

    if dest_path.exists() and not overwrite:
        log.debug(f"[SKIP] {listing_id}.txt déjà présent.")
        return False

    try:
        with open(dest_path, "w", encoding="utf-8") as f:
            f.write(f"Commentaires pour l'annonce {listing_id}:\n\n")
            for i, review in enumerate(reviews, start=1):
                cleaned = clean_text(review)
                if cleaned:
                    f.write(f"• {cleaned}\n")
        return True

    except Exception as e:
        log.warning(f"[ERREUR] Écriture impossible pour l'ID {listing_id} — {e}")
        return False

# ─────────────────────────────────────────────
# 5. Pipeline principal
# ─────────────────────────────────────────────

def run(overwrite: bool = False):
    log.info("=" * 60)
    log.info("ImmoVision 360 — Script 02 : Ingestion des Textes")
    log.info("=" * 60)

    # Chargement
    elysee_ids = load_elysee_ids(LISTINGS_CSV)
    df_reviews = load_reviews(REVIEWS_CSV, elysee_ids)

    # Regroupement par annonce
    grouped = df_reviews.groupby("listing_id")["text"].apply(list)
    log.info(f"Annonces avec au moins un commentaire : {len(grouped):,}")

    # Écriture
    TEXTS_DIR.mkdir(parents=True, exist_ok=True)

    written, skipped, errors = 0, 0, 0

    for listing_id, reviews in grouped.items():
        try:
            result = write_text_file(str(listing_id), reviews, TEXTS_DIR, overwrite)
            if result:
                written += 1
            else:
                skipped += 1
        except Exception as e:
            log.warning(f"[EXCEPTION] ID {listing_id} — {type(e).__name__}: {e}")
            errors += 1

    # Rapport final
    log.info("=" * 60)
    log.info(f"TERMINÉ — Annonces traitées : {len(grouped)}")
    log.info(f"  ✔ Fichiers écrits : {written}")
    log.info(f"  ↷ Skips           : {skipped}")
    log.info(f"  ✗ Erreurs         : {errors}")
    taux = round(written / max(len(grouped) - skipped, 1) * 100, 1)
    log.info(f"  Taux de réussite (hors skips) : {taux}%")
    log.info("=" * 60)

# ─────────────────────────────────────────────
# 6. Point d'entrée
# ─────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingestion textes Élysée — ImmoVision 360")
    parser.add_argument("--overwrite", action="store_true",
                        help="Régénère les .txt même s'ils existent déjà.")
    args = parser.parse_args()
    run(overwrite=args.overwrite)
