# dbt transformation layer

This dbt project turns the structured Navitia (SNCF) tables stored in PostgreSQL into analytics-ready marts: passages, delays and disruptions per day, line, station, area, network and physical mode.

dbt Core runs in its own Docker Compose service (`dbt`) and connects to the `sncf-db` PostgreSQL container. Everything it builds lives in the `analytics` schema; the ingestion tables in `public` are never modified.

## Lineage

![dbt lineage graph](dbt-dag.png)

`sources (public)` → `staging` (views) → `intermediate` (views) → `marts` (tables)

## Models

| Layer | Model | Grain | Role |
|---|---|---|---|
| staging | `stg_*` (9 models, one per source table) | one row per source row | rename, cast, trivial derived fields (delays, service date and hour, `trip_id`) |
| intermediate | `int_flux_deduplicated` | trip × station | keeps one version of each passage, joins station / area / line / network names, defines `delay_min` |
| intermediate | `int_flux_disruptions` | passage × disruption × embedded type | which disruption touches which passage, and whether it is effective |
| intermediate | `int_flux_flagged` | trip × station | one row per passage with its disruption flags |
| marts | `mart_flux_daily` | day × area × station × physical mode × network × line | additive measures: passages, disruptions by kind, delays |
| marts | `mart_disruption_daily` | day × severity × embedded type × status | distinct disruptions per day and their durations |
| marts | `mart_ingestion_coverage` | day | what the collection covers: versions per passage, realtime share, distinct stations / lines |

The marts only hold additive measures (counts and sums). Rates and averages are computed when reading (`sum / count`), so the same table answers per day, month, line, station... without a new model. Medians and percentiles are not additive and are left to the dashboard.

## Running it

Create a `.env` file at the repository root with `POSTGRES_USER`, `POSTGRES_PASSWORD` and `POSTGRES_DB`. The credentials reach dbt through environment variables (`profiles.yml` contains no secret).

```bash
docker compose up -d sncf-db
docker compose run --rm dbt deps                  # installs dbt_utils
docker compose run --rm dbt build                 # models + tests, in DAG order
docker compose run --rm dbt build --select +mart_flux_daily   # one mart and its ancestors
docker compose run --rm dbt show --select mart_flux_daily --limit 20   # preview a model
docker compose run --rm dbt docs generate         # documentation site (target/)
docker compose run --rm --service-ports dbt docs serve --host 0.0.0.0 --no-browser     # To view the generated documentation on the http://localhost:8080/
```

## Key definitions

- **Passage**: a vehicle journey at a station. A passage can be stored in several versions (the base schedule and successive realtime updates, each with its own `vehicle_journey_id`). `trip_id` is the `vehicle_journey_id` without its `vehicle_journey:` prefix and its `:RealTime:<uuid>` suffix; it is the common key with the disruptions.
- **Deduplication**: one row per `trip_id` × station, preferring a realtime version, then the most recently collected.
- **`delay_min`**: departure delay, or arrival delay if missing; null when the passage has no scheduled time. Arrival and departure delays are identical for almost all passages.
- **Late-minutes measures** sum positive delays only: an early passage counts as 0.
- **Effective disruption**: not an additional service, and either a cancellation, or the scheduled time of the passage falls within a period of the disruption (and, for trip disruptions, the station is declared). A passage is **disrupted** when at least one effective disruption affects it.
- Cancelled passages are excluded from every delay measure. Additional services are counted separately and are not disruptions.

## Tests

- Generic: `not_null`, `accepted_values`, `unique_combination_of_columns` (grain of every intermediate model and mart), `accepted_range`, `equal_rowcount`, `expression_is_true` (consistency between flags and between counts, e.g. `canceled <= disrupted <= passages`).
- Singular (`tests/`): the deduplicated model has exactly one row per `trip_id` × station of `stg_flux`; the passages of `mart_flux_daily` add up to the rows of `int_flux_flagged`.
- `severity: warn` is used for data-quality alerts rather than hard failures: unexpected delay ranges, unknown severities, embedded types not yet handled, a non-zero delay on a passage without realtime update.

## ELT vs ETL

In **ETL**, data is transformed on an intermediate server before the final result is loaded: the raw data is usually not kept, and changing a rule means reprocessing from the source. In **ELT**, data is loaded first and transformed afterwards inside the warehouse, in SQL.

Here the ingestion stores the API responses (raw JSONB tables) and a first set of normalised tables; every analytical transformation (deduplication, disruption matching, KPIs) is a versioned, tested and documented SQL model run by dbt inside PostgreSQL. This matters for this source: the API exposes the current state, so the collected data is the only history of past realtime updates, and keeping it lets the rules be changed and replayed without collecting again.

Two nuances: the parsing of the JSON into structured tables happens upstream, so dbt reads the structured tables and not the raw JSONB; and ETL remains the better choice when data must be filtered or anonymised before reaching the warehouse, or when the transformation cannot be expressed in SQL.

## Known limitations

Figures come from the validation run of the project (42,689 passages after deduplication, over 7 distinct days between 2 and 20 October 2026) and must be refreshed when more data is collected.

- **Small, short sample.** Daily trends are illustrative and monthly aggregates are not meaningful yet.
- **Collection coverage.** The number of passages depends on the stations and times the ingestion polls, not on the real timetable. Rates are relative to the collected sample. `mart_ingestion_coverage` measures this coverage.
- **Delay data is sparse.** Only 1,128 passages (about 2.6%) carry a realtime update. For all the others the actual time equals the scheduled time, so a delay of 0 means "no information", not "on time". The average delay over all passages is therefore diluted and must not be presented as the real mean delay; the `*_realtime` measures give the delay where a forecast exists. These are the last forecasts collected, not observed delays.
- **The two sources disagree.** Of the 1,833 passages flagged by a "trip delayed" disruption, 876 (48%) have no realtime update, so their delay is not measured. Conversely, 66 passages show a positive delay without such a disruption.
- **Deduplication.** 1,288 of 43,977 rows (2.9%) are older versions of an existing passage and are removed.
- **Disruption matching.** Only `trip` objects exist in the data. The code for `line`, `network`, `stop_point` and `stop_area` is written but has never been validated on real data (a warning test fires when such a type appears). `route` and `commercial_mode` cannot be matched because `flux` has no key for them.
- **Period and station checks.** They do not exclude anything on the current data: trip identifiers embed the date, so matching on `trip_id` already implies the right period. They will matter when other object types appear.
- **Cancellations** are always disrupted: only 12 of the 124 matched cancellations have a declared station.
- **`mart_disruption_daily`** counts only disruptions that touch at least one collected passage. Do not sum it across embedded types (a disruption can reach a passage through two types), and a disruption lasting several days repeats its duration each day: use `sum_duration_min / nb_with_duration` for an average duration.

## Layout

```
dbt/
├── Dockerfile
├── requirements.txt     # pinned dbt-core and dbt-postgres
├── dbt_project.yml
├── profiles.yml         # environment variables only
├── packages.yml         # dbt_utils
├── models/
│   ├── staging/
│   ├── intermediate/
│   └── marts/
└── tests/               # singular tests
```

## Next steps

- Schedule `dbt build` after each ingestion run.
- Build the dashboard on the marts.
- Add an hourly mart (`service_hour` and `service_week_day` are already available) once more days are collected.
- Move the marts to incremental models if the volume grows.
- Add source freshness checks.
