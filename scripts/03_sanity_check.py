"""
Script 03 - Sanity Check du Data Lake
ImmoVision 360 - Data Lake Phase 1

Objectif : Auditer la complétude et la cohérence des deux bras multimodaux
           (images .jpg et textes .txt) sur le périmètre Élysée.

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
REVIEWS_CSV  = BASE_DIR / "data" / "raw" / "tabular" / "reviews.csv"
IMAGES_DIR   = BASE_DIR / "data" / "raw" / "images"
TEXTS_DIR    = BASE_DIR / "data" / "raw" / "texts"
LOG_FILE     = BASE_DIR / "scripts" / "03_sanity_check.log"

QUARTIER     = "Elysée"
N_ORPHANS    = 5   # nombre d'exemples d'orphelins à afficher

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
# 2. Construction du périmètre de référence
# ─────────────────────────────────────────────

def get_elysee_ids(listings_path: Path) -> tuple[set, set]:
    """
    Retourne deux ensembles d'IDs Élysée :
      - ids_image  : tous les IDs avec une picture_url valide
      - ids_text   : IDs ayant au moins un commentaire dans reviews.csv
    """
    df_list = pd.read_csv(listings_path, dtype={"id": str}, low_memory=False)

    # Détection colonne quartier
    quartier_col = None
    for col in ["neighbourhood_cleansed", "neighbourhood", "neighborhood"]:
        if col in df_list.columns:
            quartier_col = col
            break
    if quartier_col is None:
        raise ValueError("Colonne quartier introuvable.")

    mask = df_list[quartier_col].str.contains(QUARTIER, case=False, na=False)
    df_elysee = df_list[mask].copy()

    ids_image = set(
        df_elysee.loc[df_elysee["picture_url"].notna(), "id"].astype(str)
    )

    # IDs avec au moins un review
    df_rev = pd.read_csv(REVIEWS_CSV, dtype={"listing_id": str}, low_memory=False)
    ids_with_reviews = set(df_rev["listing_id"].dropna().astype(str))
    ids_text = set(df_elysee["id"].astype(str)) & ids_with_reviews

    return ids_image, ids_text

# ─────────────────────────────────────────────
# 3. Audit Images
# ─────────────────────────────────────────────

def audit_images(expected_ids: set) -> dict:
    present = {p.stem for p in IMAGES_DIR.glob("*.jpg")}
    missing = expected_ids - present
    extra   = present - expected_ids
    taux    = round(len(present & expected_ids) / max(len(expected_ids), 1) * 100, 1)

    return {
        "expected" : len(expected_ids),
        "present"  : len(present),
        "matched"  : len(present & expected_ids),
        "missing"  : len(missing),
        "extra"    : len(extra),
        "taux"     : taux,
        "samples_missing": list(missing)[:N_ORPHANS],
    }

# ─────────────────────────────────────────────
# 4. Audit Textes
# ─────────────────────────────────────────────

def audit_texts(expected_ids: set) -> dict:
    present = {p.stem for p in TEXTS_DIR.glob("*.txt")}

    # Vérifier les fichiers vides (anormalement vides < 30 octets)
    empty = {
        p.stem for p in TEXTS_DIR.glob("*.txt")
        if p.stat().st_size < 30
    }

    missing = expected_ids - present
    taux    = round(len(present & expected_ids) / max(len(expected_ids), 1) * 100, 1)

    return {
        "expected" : len(expected_ids),
        "present"  : len(present),
        "matched"  : len(present & expected_ids),
        "missing"  : len(missing),
        "empty"    : len(empty),
        "taux"     : taux,
        "samples_missing": list(missing)[:N_ORPHANS],
        "samples_empty"  : list(empty)[:N_ORPHANS],
    }

# ─────────────────────────────────────────────
# 5. Cohérence croisée image / texte
# ─────────────────────────────────────────────

def cross_check(ids_image: set, ids_text: set) -> dict:
    imgs_present  = {p.stem for p in IMAGES_DIR.glob("*.jpg")}
    texts_present = {p.stem for p in TEXTS_DIR.glob("*.txt")}

    img_sans_txt  = imgs_present - texts_present
    txt_sans_img  = texts_present - imgs_present
    les_deux      = imgs_present & texts_present

    return {
        "img_sans_txt" : len(img_sans_txt),
        "txt_sans_img" : len(txt_sans_img),
        "les_deux"     : len(les_deux),
        "samples_img_sans_txt": list(img_sans_txt)[:N_ORPHANS],
        "samples_txt_sans_img": list(txt_sans_img)[:N_ORPHANS],
    }

# ─────────────────────────────────────────────
# 6. Affichage du rapport
# ─────────────────────────────────────────────

def print_report(ids_image, ids_text, img_stats, txt_stats, cross):
    sep = "═" * 60

    print(f"\n{sep}")
    print("  RAPPORT SANITY CHECK — ImmoVision 360 Data Lake")
    print(sep)
    print(f"  Périmètre : {QUARTIER}")
    print(f"  Annonces de référence (images) : {len(ids_image)}")
    print(f"  Annonces de référence (textes) : {len(ids_text)}")
    print(sep)

    # ── Images
    print("\n  📷  BRAS IMAGES (/data/raw/images/)")
    print(f"      Attendues   : {img_stats['expected']}")
    print(f"      Présentes   : {img_stats['present']}")
    print(f"      Matchées    : {img_stats['matched']}")
    print(f"      Manquantes  : {img_stats['missing']}")
    print(f"      Taux        : {img_stats['taux']} %")
    if img_stats["samples_missing"]:
        print(f"      Exemples IDs sans .jpg : {img_stats['samples_missing']}")

    # ── Textes
    print("\n  📄  BRAS TEXTES (/data/raw/texts/)")
    print(f"      Attendus    : {txt_stats['expected']}")
    print(f"      Présents    : {txt_stats['present']}")
    print(f"      Matchés     : {txt_stats['matched']}")
    print(f"      Manquants   : {txt_stats['missing']}")
    print(f"      Fichiers vides anormaux : {txt_stats['empty']}")
    print(f"      Taux        : {txt_stats['taux']} %")
    if txt_stats["samples_missing"]:
        print(f"      Exemples IDs sans .txt : {txt_stats['samples_missing']}")
    if txt_stats["samples_empty"]:
        print(f"      Exemples fichiers vides : {txt_stats['samples_empty']}")

    # ── Cohérence croisée
    print("\n  🔗  COHÉRENCE CROISÉE images ↔ textes")
    print(f"      IDs avec image ET texte      : {cross['les_deux']}")
    print(f"      IDs avec image SANS texte    : {cross['img_sans_txt']}")
    if cross["samples_img_sans_txt"]:
        print(f"        Exemples : {cross['samples_img_sans_txt']}")
    print(f"      IDs avec texte SANS image    : {cross['txt_sans_img']}")
    if cross["samples_txt_sans_img"]:
        print(f"        Exemples : {cross['samples_txt_sans_img']}")

    print(f"\n{sep}")
    print("  Rapport complet enregistré dans : scripts/03_sanity_check.log")
    print(f"{sep}\n")

# ─────────────────────────────────────────────
# 7. Point d'entrée
# ─────────────────────────────────────────────

def run():
    log.info("=" * 60)
    log.info("ImmoVision 360 — Script 03 : Sanity Check")
    log.info("=" * 60)

    ids_image, ids_text = get_elysee_ids(LISTINGS_CSV)

    img_stats = audit_images(ids_image)
    txt_stats = audit_texts(ids_text)
    cross     = cross_check(ids_image, ids_text)

    print_report(ids_image, ids_text, img_stats, txt_stats, cross)

    # Sauvegarde aussi dans le log
    log.info(f"Images  — attendues:{img_stats['expected']} | présentes:{img_stats['present']} | taux:{img_stats['taux']}%")
    log.info(f"Textes  — attendus:{txt_stats['expected']}  | présents:{txt_stats['present']}  | taux:{txt_stats['taux']}%")
    log.info(f"Croisé  — les_deux:{cross['les_deux']} | img_sans_txt:{cross['img_sans_txt']} | txt_sans_img:{cross['txt_sans_img']}")


if __name__ == "__main__":
    run()
