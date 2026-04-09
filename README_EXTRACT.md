# README_EXTRACT — Justification de l'Extraction

## Contexte

Le fichier `listings.csv` contient plus de 70 colonnes. Le script `04_extract.py` opère une sélection stratégique : ne conserver que les features directement utiles pour répondre aux 3 hypothèses de la Maire de Paris sur la gentrification du quartier Élysée.

## Mapping Hypothèses → Features

### Hypothèse A — Concentration économique
> *"Est-ce une économie de partage ou une industrie hôtelière masquée ?"*

| Feature | Justification |
|---|---|
| `calculated_host_listings_count` | Détecte les multipropriétaires (si >1 annonce = gestion professionnelle) |
| `price` | Indicateur de rentabilité économique |
| `property_type` | Différencie appartements résidentiels vs hébergements professionnels |
| `room_type` | Logement entier = retrait du marché locatif résidentiel |
| `availability_365` | Forte disponibilité = bien géré comme actif financier |

### Hypothèse B — Déshumanisation de l'accueil
> *"Le lien social se brise-t-il au profit de processus automatisés ?"*

| Feature | Justification |
|---|---|
| `host_response_time` | Délai très court = gestion professionnelle automatisée |
| `host_response_rate` | Taux élevé + délai court = agence, pas un particulier |
| `host_is_superhost` | Indicateur de professionnalisme |
| `host_since` | Ancienneté : corrélation avec le type d'hôte |

### Hypothèse C — Standardisation visuelle
> *"Les logements sont-ils devenus des produits financiers stériles ?"*

La feature `Standardization_Score` sera générée en Phase Transform (analyse d'image IA).  
Aucune feature CSV n'est suffisante seule pour cette hypothèse.

## Colonnes Conservées (liste complète)

```
id, price, property_type, room_type, availability_365,
calculated_host_listings_count, host_id, host_since,
host_response_time, host_response_rate, host_is_superhost,
number_of_reviews, review_scores_rating, reviews_per_month,
latitude, longitude, neighbourhood_cleansed,
accommodates, bedrooms, beds
```

**Total : 20 colonnes** sur 70+ disponibles — réduction de ~70% du volume.

## Fichier Produit

`data/processed/filtered_elysee.csv`
