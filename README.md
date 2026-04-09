# ImmoVision 360 — Data Lake (Phase 1 : Bronze Layer)

> **Contexte :** Ce dépôt constitue le livrable de la Phase 1 du projet fil rouge *ImmoVision 360*,
> réalisé dans le cadre du cours *"Collecte et exploration des données"*.  
> L'objectif est de construire la couche **Bronze (Raw Data)** d'un Data Lake dédié à l'analyse
> de la gentrification dans le quartier de l'Élysée à Paris, à partir des données open source
> d'Inside Airbnb.

---

## 📁 Structure du Répertoire

```
ImmoVision360_DataLake/
├── data/
│   └── raw/
│       ├── tabular/
│       │   ├── listings.csv          ← Annonces Airbnb Paris (sept. 2025)
│       │   └── reviews.csv           ← Historique des commentaires voyageurs
│       ├── images/                   ← Photos des appartements (320×320 px, ID.jpg)
│       │   └── .gitkeep              ← Dossier versionné mais contenu ignoré par Git
│       └── texts/                    ← Corpus NLP par annonce (ID.txt)
│           └── .gitkeep
├── scripts/
│   ├── 00_data.ipynb                 ← Notebook d'exploration initiale (fourni)
│   ├── 01_ingestion_images.py        ← Scraping & stockage des images Élysée
│   ├── 02_ingestion_textes.py        ← Ingestion & structuration des reviews NLP
│   ├── 03_sanity_check.py            ← Audit qualité du Data Lake
│   ├── 01_ingestion_images.log       ← Log généré à l'exécution
│   ├── 02_ingestion_textes.log       ← Log généré à l'exécution
│   └── 03_sanity_check.log           ← Log généré à l'exécution
├── .gitignore
└── README.md
```

---

## ⚙️ Notice d'Exécution

### Prérequis

Python ≥ 3.10 recommandé.

```bash
pip install pandas requests Pillow
```

### Étape 0 — Télécharger les données sources

Télécharger depuis [Inside Airbnb – Paris](https://insideairbnb.com/fr/paris/) :

| Fichier | URL |
|---|---|
| `listings.csv` | https://data.insideairbnb.com/france/ile-de-france/paris/2025-09-12/visualisations/listings.csv |
| `reviews.csv` | https://data.insideairbnb.com/france/ile-de-france/paris/2025-09-12/visualisations/reviews.csv |

Placer les deux fichiers dans `data/raw/tabular/`.

### Étape 1 — Ingestion des Images

```bash
python scripts/01_ingestion_images.py
```

Options :
```bash
python scripts/01_ingestion_images.py --dry-run   # simulation sans téléchargement
```

**Ce que fait ce script :**
- Filtre les annonces du quartier **Élysée** dans `listings.csv`
- Télécharge chaque image via son `picture_url`
- Redimensionne à **320×320 px** (optimisation stockage)
- Nomme chaque fichier `<ID>.jpg` (clé de jointure future)
- Vérifie le `robots.txt` avant de commencer (éthique)
- Applique un délai entre requêtes (rate limiting, courtoisie serveur)
- Est **idempotent** : les images déjà présentes sont ignorées

### Étape 2 — Ingestion des Textes

```bash
python scripts/02_ingestion_textes.py
```

Options :
```bash
python scripts/02_ingestion_textes.py --overwrite   # régénère même si .txt existe
```

**Ce que fait ce script :**
- Charge les IDs Élysée depuis `listings.csv`
- Filtre `reviews.csv` sur ces IDs
- Nettoie chaque commentaire (balises HTML, entités, espaces)
- Regroupe tous les avis d'une même annonce dans `<ID>.txt`
- Est **idempotent** par défaut (skip si fichier existant)

### Étape 3 — Sanity Check

```bash
python scripts/03_sanity_check.py
```

**Ce que fait ce script :**
- Recharge le périmètre Élysée (même logique que scripts 1 & 2)
- Compare les fichiers **attendus** vs **présents** pour images et textes
- Détecte les fichiers manquants, vides ou incohérents entre les deux bras
- Affiche un rapport structuré dans le terminal
- Sauvegarde les résultats dans `scripts/03_sanity_check.log`

---

## 📊 Audit des Données — Résultats du Sanity Check

> **Note :** Compléter cette section après l'exécution du script 03, en copiant
> les résultats affichés dans le terminal.

```
Périmètre : Élysée — [N] annonces de référence

📷 BRAS IMAGES :
   Attendues  : [X]
   Présentes  : [X]
   Taux       : [X]%
   5 premiers IDs sans .jpg : [...]

📄 BRAS TEXTES :
   Attendus   : [X]
   Présents   : [X]
   Taux       : [X]%
   5 premiers IDs sans .txt : [...]

🔗 COHÉRENCE CROISÉE :
   IDs avec image ET texte   : [X]
   IDs image sans texte      : [X]
   IDs texte sans image      : [X]
```

---

## 🔍 Analyse des Pertes (à compléter après exécution)

Même avec une ingestion soignée, un taux de 100 % est rarement atteignable.
Les causes de déperdition observées sont généralement :

1. **Liens expirés (HTTP 404)** : Airbnb supprime régulièrement les photos des annonces
   inactives ou supprimées. Un lien présent dans le CSV de septembre 2025 peut ne plus
   exister au moment de l'ingestion.

2. **Blocage anti-bot (HTTP 403 / 429)** : Malgré le rate limiting et le User-Agent configurés,
   certains CDN (Content Delivery Networks) appliquent des règles anti-scraping dynamiques
   qui peuvent rejeter des requêtes légitimes.

3. **Timeout réseau** : Des serveurs surchargés ou une connexion lente peuvent provoquer
   des expiration de requêtes. Le script les logge et continue.

4. **Annonces sans commentaires** : Certains logements listés dans `listings.csv` n'ont
   reçu aucun avis, ce qui explique que le nombre de fichiers `.txt` attendus est
   inférieur au nombre d'annonces.

---

## ⚖️ Audit Éthique & Légal

| Critère | Statut |
|---|---|
| **Licence Inside Airbnb** | Données sous licence Creative Commons — usage académique autorisé (non commercial) |
| **robots.txt** | Vérifié automatiquement au lancement du script 01 |
| **Rate Limiting** | Pause aléatoire 0.5–1.5s entre chaque requête |
| **User-Agent** | Identifié comme bot académique transparent |
| **RGPD** | Les données sont open source et anonymisées par Inside Airbnb |

Références consultées :
- https://insideairbnb.com/about/
- https://insideairbnb.com/fr/data-policies/
- https://insideairbnb.com/fr/get-the-data/

---

## 👤 Auteur

[Votre Nom] — Avril 2026  
Cours : *Collecte et exploration des données*
