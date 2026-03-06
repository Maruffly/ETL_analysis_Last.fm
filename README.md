# Music Trends ETL & Analysis (Last.fm 2020-2024)

Ce projet implémente un pipeline ETL pour analyser les dynamiques de popularité dans l'industrie musicale sur les 5 dernières années. En utilisant l'API Last.fm, le script traite 100 titres pour identifier les corrélations entre la notoriété des artistes et le succès des morceaux.

## Architecture
- **Extraction** : Collecte des Top 200 morceaux annuels via l'API Last.fm.
- **Transformation** : 
    - Enrichissement de données via multithreading (ThreadPoolExecutor).
    - Nettoyage des valeurs manquantes (NaN) et typage des données.
    - Calcul de métriques (Ratio de dominance, True Rank).

## Problématiques
### 1. Corrélation : Notoriété de l'Artiste vs Succès du Titre
La popularité d’un artiste est-elle corrélée à son nombre de followers ? Ou à la popularité de ses tracks ?

- **Analyse des données** : La matrice de corrélation révèle un coefficient de **0.38** entre le nombre total d'auditeurs d'un artiste (listeners) et le succès d'un titre spécifique (track_listeners).
- **Conclusion** : La corrélation est faible. Cela démontre qu'une large base de "followers" (notoriété) ne garantit pas le succès d'un morceau. Le public de Last.fm consomme les morceaux de manière indépendante de la stature de l'artiste.
- On observe beaucoup "one time hit" avec un track_to_artist_ratio supérieur à 80%.

### 2. Évolution des genres (2020 - 2024)
Y a-t-il une évolution des genres les plus écoutés sur la période ?

- **Prédominance de la K-Pop** : Des groupes comme BTS, TWICE et LE SSERAFIM sont en tête de classement sur les titres placés dans le Top 200 (19 pour TWICE).
- **Globalisation** : On observe une transition d'un top dominé par la Pop/Hip-Hop US vers une diversification internationale massive où les genres coréens et alternatifs agressent les charts annuels.
- **Phénomène 2024** : L'année 2024 affiche les scores d'auditeurs les plus élevés du dataset (Billie Eilish, Sabrina Carpenter), suggérant une concentration de l'attention sur quelques "Megamusical" viraux.

---

## Dictionnaire des données

### Statistiques Artistes
| Colonne | Type | Description |
| :--- | :--- | :--- |
| `listeners` | Int | Auditeurs total uniques sur Last.fm (Notoriété globale). |
| `playcount` | Int | Nombre total d'écoutes cumulées (Fidélité/Volume). |
| `genres` | List | Tags associés (ex: [pop, k-pop, rock]). |

### Statistiques Morceaux
| Colonne | Type | Description |
| :--- | :--- | :--- |
| `track_listeners` | Int | Nombre de personnes ayant écouté ce titre. |
| `duration_ms` | Int | Durée du morceau en millisecondes. |
| `track_to_artist_ratio`| Float | % de l'audience de l'artiste représenté par le titre. |
| `true_rank` | Int | Rang réel basé sur les écoutes par année. |

---

## Installation & Usage

1. **Clonage** :
   ```bash
   git clone *<repos URL>*
   cd ETL_analysis_LastFM
   python main.py
   ```
   
2. **Dépendances** :
```pip install pandas tqdm```
