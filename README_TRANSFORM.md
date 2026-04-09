# README_TRANSFORM — Justification des Transformations

## Vue d'ensemble

Le script `05_transform.py` opère en deux temps :
1. **Nettoyage** du fichier filtré (`filtered_elysee.csv`)
2. **Enrichissement** par deux nouvelles features multimodales simulées

## A. Nettoyage & Normalisation

### Conversions de types

| Colonne | Transformation | Raison |
|---|---|---|
| `price` | `"$120.00"` → `120.0` (float) | Le CSV stocke le prix comme chaîne avec symbole monétaire |
| `host_response_rate` | `"95%"` → `0.95` (float) | Nécessaire pour calculs statistiques |
| `host_is_superhost` | `"t"/"f"` → `1/0` (int) | Compatibilité SQL et modèles ML |
| `host_since` | string → datetime | Pour calculs d'ancienneté |

### Gestion des valeurs manquantes

| Colonne | Stratégie | Justification |
|---|---|---|
| `price` | **Suppression** (drop) | Donnée critique : une annonce sans prix est inexploitable |
| `review_scores_rating` | **Imputation médiane** | Évite de biaiser la distribution |
| `host_response_rate` | **Imputation médiane** | Valeur cohérente avec la population |
| `bedrooms`, `beds` | **Imputation médiane** | Rare, la médiane est une bonne approximation |
| `reviews_per_month` | **Imputation à 0** | Logement neuf sans avis : 0 est logiquement correct |

### Gestion des outliers

- **Prix** : plafonnement au percentile 99 (P99) pour éviter que des annonces à 10 000€/nuit faussent les statistiques agrégées.

## B. Feature Engineering Multimodal (simulé)

### Standardization_Score — Vision IA
- **Source originale prévue :** analyse de chaque `[ID].jpg` par Gemini 2.5 Flash
- **Prompt IA :** classification "Appartement industrialisé" vs "Appartement personnel" vs "Autre"
- **Valeurs :** `1` (industrialisé) | `0` (personnel) | `-1` (non classifiable)
- **Version actuelle :** valeurs aléatoires parmi {1, 0, -1} — seed fixe pour reproductibilité

### Neighborhood_Impact — NLP
- **Source originale prévue :** analyse de chaque `[ID].txt` par Gemini 2.5 Flash
- **Prompt IA :** classification "Hôtelisé" vs "Voisinage naturel"
- **Valeurs :** `1` (hôtelisé) | `0` (voisinage naturel) | `-1` (données insuffisantes)
- **Version actuelle :** valeurs aléatoires parmi {1, 0, -1} — seed fixe pour reproductibilité

> **Note :** Les valeurs actuelles sont simulées (random seed=42) pour respecter les contraintes de quota API. Le professeur fournira les valeurs réelles pour la phase EDA.

## Fichier Produit

`data/processed/transformed_elysee.csv`
