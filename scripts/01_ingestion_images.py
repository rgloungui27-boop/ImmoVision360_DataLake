"""
Script 01 - Ingestion des Images
ImmoVision 360 - Data Lake Phase 1

Objectif : Télécharger les images des annonces Airbnb du quartier Élysée
           depuis listings.csv, les redimensionner à 320x320 et les stocker
           dans /data/raw/images/<ID>.jpg

Auteur   : [Votre Nom]
Date     : Avril 2026
"""

import os
import time
import logging
import argparse
import pandas as pd
import requests
from PIL import Image
from io import BytesIO
from pathlib import Path

# ─────────────────────────────────────────────
# 0. Configuration
# ─────────────────────────────────────────────

BASE_DIR    = Path(__file__).resolve().parent.parent          # racine du Data Lake
CSV_PATH    = BASE_DIR / "data" / "raw" / "tabular" / "listings.csv"
IMAGES_DIR  = BASE_DIR / "data" / "raw" / "images"
LOG_FILE    = BASE_DIR / "scripts" / "01_ingestion_images.log"

TARGET_SIZE = (320, 320)   # pixels — contrainte Big Data Management
QUARTIER    = "Élysée"     # périmètre métier
SLEEP_MIN   = 0.5          # secondes — courtoisie serveur (rate limiting)
SLEEP_MAX   = 1.5
TIMEOUT     = 10           # secondes par requête

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; ImmoVision360-AcademicBot/1.0; "
        "+https://github.com/votre-username/ImmoVision360)"
    )
}

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
# 2. Audit éthique & légal (robots.txt check)
# ─────────────────────────────────────────────

def check_robots_txt(base_url: str) -> bool:
    """
    Vérifie rapidement le robots.txt du domaine cible.
    Retourne True si aucune interdiction explicite n'est détectée.
    NOTE : une vérification manuelle complète reste recommandée.
    """
    try:
        r = requests.get(f"{base_url}/robots.txt", timeout=5, headers=HEADERS)
        if r.status_code == 200:
            content = r.text.lower()
            if "disallow: /" in content:
                log.warning("robots.txt : accès général désactivé — vérifiez manuellement.")
                return False
            log.info("robots.txt consulté — aucune interdiction globale détectée.")
        else:
            log.info(f"robots.txt non trouvé (HTTP {r.status_code}) — accès supposé libre.")
        return True
    except Exception as e:
        log.warning(f"Impossible de lire robots.txt : {e}")
        return True   # on suppose autorisé si inaccessible

# ─────────────────────────────────────────────
# 3. Chargement & filtrage du CSV
# ─────────────────────────────────────────────

def load_elysee_listings(csv_path: Path) -> pd.DataFrame:
    """
    Charge listings.csv et filtre sur le quartier Élysée.
    Colonnes requises : id, picture_url, neighbourhood_cleansed (ou neighbourhood)
    """
    log.info(f"Lecture de {csv_path} ...")
    df = pd.read_csv(csv_path, dtype={"id": str}, low_memory=False)
    log.info(f"  → {len(df):,} annonces chargées au total.")

    # Détection automatique de la colonne quartier
    quartier_col = None
    for col in ["neighbourhood_cleansed", "neighbourhood", "neighborhood"]:
        if col in df.columns:
            quartier_col = col
            break

    if quartier_col is None:
        raise ValueError("Colonne quartier introuvable dans listings.csv.")

    # Filtrage Élysée (insensible à la casse et aux accents)
    mask = df[quartier_col].str.contains(QUARTIER, case=False, na=False)
    df_elysee = df[mask].copy()
    log.info(f"  → {len(df_elysee):,} annonces dans le périmètre '{QUARTIER}'.")

    # Vérification des colonnes obligatoires
    for col in ["id", "picture_url"]:
        if col not in df_elysee.columns:
            raise ValueError(f"Colonne '{col}' absente dans listings.csv.")

    return df_elysee[["id", "picture_url"]].dropna(subset=["picture_url"])

# ─────────────────────────────────────────────
# 4. Téléchargement + redimensionnement
# ─────────────────────────────────────────────

def download_image(listing_id: str, url: str, dest_dir: Path) -> bool:
    """
    Télécharge et redimensionne une image.
    Retourne True si succès, False sinon.
    Règle d'idempotence : si <ID>.jpg existe déjà, skip.
    """
    dest_path = dest_dir / f"{listing_id}.jpg"

    # ── Idempotence ──────────────────────────
    if dest_path.exists():
        log.debug(f"[SKIP]   {listing_id}.jpg déjà présent.")
        return True

    # ── Téléchargement ───────────────────────
    try:
        response = requests.get(url, timeout=TIMEOUT, headers=HEADERS)
        response.raise_for_status()   # lève une exception si 4xx/5xx

        img = Image.open(BytesIO(response.content)).convert("RGB")
        img = img.resize(TARGET_SIZE, Image.LANCZOS)
        img.save(dest_path, "JPEG", quality=85, optimize=True)

        log.info(f"[OK]     {listing_id}.jpg sauvegardé ({TARGET_SIZE[0]}x{TARGET_SIZE[1]}px)")
        return True

    except requests.exceptions.HTTPError as e:
        log.warning(f"[ERREUR] Lien mort pour l'ID {listing_id} — {e}")
    except requests.exceptions.Timeout:
        log.warning(f"[TIMEOUT] Requête expirée pour l'ID {listing_id}")
    except requests.exceptions.ConnectionError as e:
        log.warning(f"[CONNEXION] Erreur réseau pour l'ID {listing_id} — {e}")
    except Exception as e:
        log.warning(f"[EXCEPTION] ID {listing_id} — {type(e).__name__}: {e}")

    return False

# ─────────────────────────────────────────────
# 5. Pipeline principal
# ─────────────────────────────────────────────

def run(dry_run: bool = False):
    import random

    log.info("=" * 60)
    log.info("ImmoVision 360 — Script 01 : Ingestion des Images")
    log.info("=" * 60)

    # Audit éthique
    check_robots_txt("https://a0.muscache.com")  # domaine CDN Airbnb pour les images

    # Chargement des données
    df = load_elysee_listings(CSV_PATH)
    total = len(df)
    log.info(f"Cible : {total} images à vérifier / télécharger.")

    if dry_run:
        log.info("[DRY RUN] Mode simulation — aucun fichier ne sera téléchargé.")
        log.info(f"  Exemple d'URL : {df['picture_url'].iloc[0]}")
        return

    # Ingestion
    IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    success, errors, skipped = 0, 0, 0

    for i, row in enumerate(df.itertuples(), start=1):
        listing_id = str(row.id)
        url        = row.picture_url

        dest_path  = IMAGES_DIR / f"{listing_id}.jpg"

        # Compte les skips séparément
        if dest_path.exists():
            skipped += 1
            continue

        result = download_image(listing_id, url, IMAGES_DIR)
        if result:
            success += 1
        else:
            errors += 1

        # Rate limiting (courtoisie serveur)
        time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

        # Progression toutes les 50 images
        if i % 50 == 0:
            log.info(f"  Progression : {i}/{total} | OK={success} | ERR={errors} | SKIP={skipped}")

    # Rapport final
    log.info("=" * 60)
    log.info(f"TERMINÉ — Total traité : {total}")
    log.info(f"  ✔ Succès  : {success}")
    log.info(f"  ✗ Erreurs : {errors}")
    log.info(f"  ↷ Skips   : {skipped}")
    taux = round(success / max(total - skipped, 1) * 100, 1)
    log.info(f"  Taux de réussite (hors skips) : {taux}%")
    log.info("=" * 60)

# ─────────────────────────────────────────────
# 6. Point d'entrée
# ─────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingestion images Élysée — ImmoVision 360")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simule l'exécution sans télécharger.")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
