# README_DATAPROFILING — Profiling de filtered_elysee.csv

> **Note :** Ce document sera complété après l'exécution de `04_extract.py` avec les résultats réels.

## Données manquantes (NaN)

> À compléter après exécution — exemple de structure attendue :

| Colonne | NaN count | NaN % | Décision |
|---|---|---|---|
| price | 0 | 0% | — |
| bedrooms | ~5% | ~5% | Imputation médiane |
| review_scores_rating | ~10% | ~10% | Imputation médiane |
| reviews_per_month | ~15% | ~15% | Imputation à 0 (logement neuf) |
| host_response_rate | ~20% | ~20% | Imputation médiane |

## Statistiques descriptives

> À compléter après exécution :

| Colonne | Min | Médiane | Max | Observations |
|---|---|---|---|---|
| price | 0 | ~100€ | ~5000€ | Outliers à écrêter (P99) |
| availability_365 | 0 | ~180 | 365 | Distribution bimodale |
| calculated_host_listings_count | 1 | ~2 | ~500 | Multipropriétaires détectés |
| review_scores_rating | 1 | ~4.8 | 5 | Distribution très asymétrique |
| accommodates | 1 | 2 | 16 | Quelques grandes capacités |

## Valeurs aberrantes détectées

- **Prix à 0€** : annonces désactivées ou erreurs de saisie → suppression
- **Prix > P99** (~500€/nuit) : annonces de luxe atypiques → plafonnement
- **calculated_host_listings_count > 100** : gestionnaires professionnels identifiés
- **availability_365 = 0** : logements indisponibles toute l'année → à surveiller

## Formats & Types à convertir

| Colonne | Type brut CSV | Type cible | Méthode |
|---|---|---|---|
| price | string ("$120.00") | float | strip $, remplacer virgule |
| host_response_rate | string ("95%") | float (0.95) | strip %, diviser par 100 |
| host_is_superhost | string ("t"/"f") | int (1/0) | mapping |
| host_since | string | datetime | pd.to_datetime |

## Décisions de nettoyage

1. **Suppression** des lignes sans prix (donnée critique pour l'hypothèse économique)
2. **Imputation médiane** pour les scores de reviews (distribution symétrique)
3. **Imputation à 0** pour reviews_per_month (logement neuf = 0 avis est logique)
4. **Plafonnement P99** pour les prix (éviter que les outliers faussent les moyennes)
