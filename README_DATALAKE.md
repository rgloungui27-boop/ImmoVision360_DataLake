# README_DATALAKE — Architecture du Data Lake ImmoVision 360

## Contexte

Ce Data Lake constitue l'infrastructure de stockage du projet ImmoVision 360, dédié à l'analyse de la gentrification dans le quartier Élysée à Paris. Les données sources proviennent d'Inside Airbnb (licence Creative Commons, usage académique).

## Architecture en Zones

### Zone Bronze (Raw) — `/data/raw/`
Données brutes, non modifiées, telles qu'ingérées depuis les sources.

| Dossier | Contenu | Format |
|---|---|---|
| `tabular/` | listings.csv, reviews.csv | CSV |
| `images/` | Photos des appartements Élysée | JPEG 320×320px |
| `texts/` | Commentaires agrégés par annonce | TXT (UTF-8) |

**Convention de nommage :** chaque fichier image et texte porte le nom `[ID].jpg` / `[ID].txt` où `ID` correspond à la colonne `id` de listings.csv. Cette convention garantit la jointure entre les trois modalités.

### Zone Silver (Processed) — `/data/processed/`
Données filtrées, nettoyées et enrichies.

| Fichier | Produit par | Contenu |
|---|---|---|
| `filtered_elysee.csv` | `04_extract.py` | 20 colonnes sélectionnées, périmètre Élysée |
| `transformed_elysee.csv` | `05_transform.py` | Données nettoyées + features IA simulées |

## Scripts de la Chaîne

| Script | Rôle | Input → Output |
|---|---|---|
| `01_ingestion_images.py` | Scraping images | listings.csv → images/*.jpg |
| `02_ingestion_textes.py` | Ingestion reviews | reviews.csv → texts/*.txt |
| `03_sanity_check.py` | Audit qualité | images/ + texts/ → rapport |
| `04_extract.py` | Extraction colonnes | listings.csv → filtered_elysee.csv |
| `05_transform.py` | Nettoyage + features | filtered_elysee.csv → transformed_elysee.csv |
| `06_load.py` | Chargement PostgreSQL | transformed_elysee.csv → table SQL |
