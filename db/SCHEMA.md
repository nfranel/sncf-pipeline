# Schéma de données — sncf-pipeline

## Vue d'ensemble

Ce schéma modélise les données collectées via l'API SNCF (Navitia) : gares, passages de trains (départs/arrivées unifiés), et perturbations.

Ce schéma inclut une **couche brute (raw)** qui conserve le JSON complet renvoyé par l'API, indépendamment des tables structurées ci-dessous. C'est elle qui garantit qu'aucune information n'est perdue si un besoin futur nécessite un champ non extrait aujourd'hui — voir `raw_departures` et `raw_disruptions`.

## Tables — couche brute

### `raw_departures`
Une ligne par appel API `departures`/`arrivals`, JSON conservé tel quel.

| Colonne | Type | Description |
|---|---|---|
| `id` | SERIAL (PK) | Identifiant technique auto-incrémenté |
| `collected_at` | TIMESTAMPTZ | Horodatage de la collecte |
| `station_id` | VARCHAR | Gare interrogée |
| `raw_json` | JSONB | Réponse API complète pour ce passage |

### `raw_disruptions`
Une ligne par appel API `disruptions`, JSON conservé tel quel.

| Colonne | Type | Description |
|---|---|---|
| `id` | SERIAL (PK) | Identifiant technique auto-incrémenté |
| `collected_at` | TIMESTAMPTZ | Horodatage de la collecte |
| `raw_json` | JSONB | Réponse API complète pour cette perturbation |

**Rôle dans le pipeline** : au Sprint 1, le script d'ingestion insère systématiquement dans ces tables brutes. L'insert manuel dans les tables structurées ci-dessous sert à *valider le schéma* dès maintenant ; à partir du Sprint 3, ce sont les modèles dbt de staging qui liront `raw_departures`/`raw_disruptions` pour peupler automatiquement les tables propres, remplaçant le remplissage manuel.

## Tables — couche structurée

### `stations`
Référentiel des gares suivies par le pipeline.

| Colonne | Type | Description |
|---|---|---|
| `id_station` | TEXT (PK) | Identifiant Navitia (`stop_area:SNCF:...`) |
| `id_area` | TEXT | Code de la zone administrative associée |
| `name` | TEXT | Nom de la gare |
| `lon` | FLOAT | Longitude |
| `lat` | FLOAT | Latitude |

### `lines`
Référentiel des lignes.

| Colonne | Type | Description |
|---|---|---|
| `id_line` | TEXT (PK) | Identifiant Navitia de la ligne |
| `name_line` | TEXT | Nom/code de la ligne |

### `networks`
Référentiel des réseaux (TGV INOUI, OUIGO, TER, etc.).

| Colonne | Type | Description |
|---|---|---|
| `id_network` | TEXT (PK) | Identifiant Navitia du réseau |
| `name_network` | TEXT | Nom du réseau |

### `flux`
Un passage de train à une gare donnée (unifie `departures` et `arrivals` de l'API).

| Colonne | Type | Description |
|---|---|---|
| `vehicle_journey_id` | TEXT (PK composite) | Identifiant du train (`vehicle_journey:...`) |
| `id_station` | TEXT (PK composite, FK → `stations`) | Gare où l'événement a lieu |
| `id_origin` | TEXT (FK → `stations`) | Gare d'origine du trajet global |
| `id_terminus` | TEXT (FK → `stations`) | Gare terminus du trajet global |
| `base_time_in` | TIMESTAMP (nullable) | Heure d'arrivée théorique |
| `time_in` | TIMESTAMP (nullable) | Heure d'arrivée réelle |
| `base_time_out` | TIMESTAMP (nullable) | Heure de départ théorique |
| `time_out` | TIMESTAMP (nullable) | Heure de départ réelle |
| `id_line` | TEXT (FK → `lines`) | Ligne empruntée |
| `id_network` | TEXT (FK → `networks`) | Réseau exploitant |
| `physical_mode` | TEXT | Mode physique (train, car de substitution, etc.) |

**Clé primaire composite** `(vehicle_journey_id, id_station)` : un même train génère une ligne par gare traversée. Cette combinaison garantit aussi la déduplication entre les appels `departures` et `arrivals` de l'API, qui peuvent tous deux renvoyer le même passage physique.

**Champs nullable** : `base_time_in`/`time_in` sont NULL si la gare est l'origine du trajet (pas d'arrivée avant) ; `base_time_out`/`time_out` sont NULL si la gare est le terminus (pas de départ après).

### `perturbations`
Perturbations remontées par l'API.

| Colonne | Type | Description |
|---|---|---|
| `id` | TEXT (PK) | Identifiant de la perturbation |
| `begin_time` | TIMESTAMP | Début de la perturbation |
| `end_time` | TIMESTAMP | Fin de la perturbation |
| `status` | TEXT | Statut (active, past, future...) |
| `severity` | TEXT | Niveau de sévérité |
| `message` | TEXT | Message descriptif |

### `perturbated_stations`
Table de liaison : gares affectées par chaque perturbation (relation many-to-many).

| Colonne | Type | Description |
|---|---|---|
| `id_perturbation` | (PK composite, FK → `perturbations`) | |
| `id_station` | (PK composite, FK → `stations`) | |

### `perturbated_objects`
Table de liaison : objets Navitia (ligne, réseau, trip...) affectés par chaque perturbation.

| Colonne | Type | Description |
|---|---|---|
| `id_perturbation` | (PK composite, FK → `perturbations`) | |
| `id_object` | TEXT (PK composite) | Identifiant de l'objet impacté |
| `object_type` | TEXT | Type de l'objet (`network`, `line`, `stop_area`, `route`, `commercial_mode`, `trip`) |

**Note sur `id_object`** : association polymorphe — selon `object_type`, `id_object` référence une ligne différente à chaque fois (`lines`, `stations`, `networks`...). Le modèle relationnel classique ne permet pas de `FOREIGN KEY` pointant vers une table variable ; cette contrainte n'est donc **pas appliquée nativement en base**. L'intégrité de cette colonne est gérée côté application (vérification en Python avant insertion), pas par Postgres. Choix assumé pour ce sprint plutôt qu'une table de liaison dédiée par type d'objet.

## Relations

```
stations 1──N flux              (via id_station : où l'événement a lieu)
stations 1──N flux              (via id_origin : gare d'origine du trajet)
stations 1──N flux              (via id_terminus : gare terminus du trajet)
lines    1──N flux              (via id_line)
networks 1──N flux              (via id_network)

perturbations 1──N perturbated_stations N──1 stations
perturbations 1──N perturbated_objects
```

## Décisions de modélisation notables

- **Départs/arrivées unifiés dans `flux`** plutôt que deux tables séparées : un même train fait un aller-retour arrivée→départ à chaque gare intermédiaire ; séparer aurait dupliqué l'information et complexifié la déduplication.
- **Champs théorique/réel dupliqués** (`base_*` vs `*`) pour l'arrivée ET le départ : nécessaire pour calculer des retards des deux côtés, y compris sur les gares de passage.
- **Perturbations en 3 tables** plutôt qu'une seule avec colonnes multivaluées : respecte la 1ère forme normale (pas de liste dans une colonne), permet des jointures propres plutôt que du pattern-matching sur du texte.
