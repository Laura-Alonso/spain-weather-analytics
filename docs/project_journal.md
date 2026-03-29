# **Project Log**

---

## Day 1 (2026-03-14) — Project Initialization

* Project definition.
* Select data source -- Open-Meteo API
* Define Geographic Scope -- Spain
* Choose database --  **ClickHouse**, accessed via **CH-UI**.
* Initial design of the schema:
    * Reference tables created: `dim_country` and `dim_city`.
    * Raw ingestion table: `raw_weather_hourly`
* Populate reference tables. See 001 and 002 seeds.

---

### Day 2 (2026-03-15) — Historical Weather Backfill

* Implemented ingestion script `backfill_weather_2years.py`.
* Script dynamically retrieves city coordinates from `dim_city`.
* Historical data downloaded from Open-Meteo Archive API.
* Data inserted into `raw_weather_hourly` table.

**Data coverage validation**

Executed quality checks to verify:

* temporal completeness of the dataset
* absence of duplicate hourly observations
* plausible value ranges for meteorological variables


Validation queries stored in: `sql/quality_checks/weather_backfill_validation.sql`

---

## Day 3 (2026-03-21) — Real-Time Ingestion & Production Hardening

* Deployed hourly ingestion pipeline on a Raspberry Pi (Ubuntu) using SSH.
* Configured Python runtime and environment variables (`.env`) for ClickHouse connectivity.
* Scheduled pipeline execution using cron (hourly frequency).

**Pipeline implementation**

* Implemented `update_weather_hourly.py`:
  * retrieves last 24 hours of data from Open-Meteo API (`past_days=1`)
  * dynamically iterates over all cities from `dim_city`
  * filters out future timestamps
  * inserts data into `raw_weather_hourly`

**Production issue encountered**

* Pipeline stopped after several successful runs.
* Root cause identified via logs:
  * intermittent API/network failure:
    * `SSLError: UNEXPECTED_EOF_WHILE_READING`
* Impact:
  * full pipeline failure due to lack of error handling
  * ingestion stopped entirely

**Fix implemented**

* Introduced per-city error handling (`try/except`):
  * isolates failures at city level
  * prevents full pipeline crash
* Ensured pipeline continuity despite external API instability

**Architectural decision**

* Switched to append-only ingestion model:
  * no overwrites or deduplication at ingestion layer
  * all records (including duplicates) are stored
* Introduced `ingestion_ts` column:
  * enables deterministic downstream deduplication

**Data model evolution**

* Created separate raw tables:
  * `raw_weather_2years` → historical backfill
  * `raw_weather_hourly` → incremental ingestion
* Standardized schema across raw tables:
  * `timestamp` (event time)
  * `ingestion_ts` (ingestion time)


---

## Day 4 (2026-03-22) — dbt Transformation Layer

Initialized dbt project weather_project using the ClickHouse adapter (dbt-clickhouse=1.7.2).
Configured profiles.yml with secure ClickHouse connection (port 443, TLS).
Updated dbt_project.yml with model materialization strategy:

* stg → view
* intermediate → view
* marts → table

**Project structure**

```
models/
├── src/
│   └── schema.yml          ← source declarations
└── stg/
    ├── schema.yml          ← model tests and descriptions
    ├── stg_dim_city.sql
    ├── stg_dim_country.sql
    ├── stg_raw_weather_2years.sql
    └── stg_raw_weather_hourly.sql
```

**Source layer** (src/schema.yml)

Declared all 4 ClickHouse tables as dbt sources:

* `dim_city`
* `dim_country`
* `raw_weather_2years`
* `raw_weather_hourly`


Added column descriptions with units (Celsius, km/h, mm, hPa)
Added data quality tests:

* unique + not_null on all primary keys.
* not_null on all foreign keys.
* relationships tests: raw_weather_*.city_id → dim_city, dim_city.country_id → dim_country


**Staging layer** (stg/)

Created 4 passthrough staging models reading from sources via `{{ source() }}`.
Added model-level tests mirroring source tests, using `{{ ref() }}` for cross-model relationships.
Confirmed `dbt run --select stg` executes successfully against ClickHouse.

**Key decisions**

* Kept staging as pure passthrough (no transformations) — all columns selected as-is
* Split schema.yml into two files to avoid duplicate source declaration error
* Used `{{ source() }}` in staging SQL and `{{ ref() }}` in model tests — consistent with dbt conventions.

---

## Day 5 (2026-03-27) — dbt Intermediate Layer

* Built intermediate model `int_weather_observations` to deduplicate and unify both raw tables.
* Updated `dbt_project.yml` — changed `int` materialization from `view` to `table`.

**Model: `int_weather_observations`**

* Unions `raw_weather_2years` and `raw_weather_hourly` via staging refs
* Deduplicates by `city_id + timestamp` keeping the row with the latest `ingestion_ts`
* Materialized as **incremental** with `delete+insert` strategy — ClickHouse equivalent of upsert

**Incremental strategy**

* Default run → processes only the last 2 days (via `is_incremental()` filter)
* Full refresh available via `--full-refresh` flag when a full rebuild is needed
* 2-day window chosen as a safety buffer — Open-Meteo data is finalized within the same hour it is collected

**Timestamp columns**

* `ingestion_ts` → when the raw data arrived in ClickHouse (from the Python pipeline)
* `update_ts` → when dbt last processed that row (`now()` at run time)


---

Here's the Day 6 entry:

---

## Day 6 (2026-03-28) — dbt Marts Layer

* Built first mart model `mart_weather_daily` with daily aggregations per city.
* Created `apply_column_comments()` macro to persist dbt column descriptions as ClickHouse column comments via `post_hook`.

**Model: `mart_weather_daily`**

* Reads from `int_weather_observations` joined with `stg_dim_city` and `stg_dim_country`.
* Aggregates to daily granularity per city.
* Incremental with `delete+insert` strategy and 1-day lookback window.
* Materialized as **incremental table** with explicit ClickHouse engine config.

**Aggregations included**

* **Temperature**: `avg`, `min`, `max`, `thermal_range` (max - min)
* **Precipitation**: `total_precipitation` (sum), `rainy_hours` (hours with precipitation > 0)
* **Wind**: `avg_wind_speed`, `max_wind_speed`, `max_wind_gusts`
* **Humidity**: `avg_humidity`
* **Pressure**: `avg_pressure`

**Audit columns**

* `ingestion_ts` → timestamp when the row first appeared in the mart, preserved across incremental runs via `coalesce(existing.ingestion_ts, now())`
* `update_ts` → timestamp when dbt last recalculated the row

**Issues fixed during development**

* `partition_by` dictionary syntax not supported by dbt-clickhouse 1.7.2 — switched to raw SQL string with explicit `engine='MergeTree()'`
* `group by` positional references replaced with explicit column names
* `city` added to GROUP BY to ensure correct aggregation key

**Storage strategy revised**

* `stg_dim_city` and `stg_dim_country` → remain as **views** (tiny reference tables, no storage benefit)
* `stg_raw_weather_hourly` and `stg_raw_weather_2years` → changed to **tables** with explicit `MergeTree` engine, partitioned by `toYYYYMM(timestamp)`, ordered by `(city_id, timestamp)`
* All models now include `post_hook="{{ apply_column_comments() }}"` to persist descriptions in ClickHouse

**Project structure update**

```
models/
├── src/
│   └── schema.yml
├── stg/
│   ├── schema.yml
│   ├── stg_dim_city.sql        ← view + column comments
│   ├── stg_dim_country.sql     ← view + column comments
│   ├── stg_raw_weather_2years.sql   ← table, partitioned
│   └── stg_raw_weather_hourly.sql   ← table, partitioned
├── int/
│   ├── schema.yml
│   └── int_weather_observations.sql  ← incremental, partitioned
└── marts/
    ├── schema.yml
    └── mart_weather_daily.sql        ← incremental, partitioned
```

---

## Day 7 (2026-03-29) — Hourly Mart & Tech Key

**`mart_weather_hourly`**
- Enriched hourly observations — no aggregation, context added from dim tables
- Includes `latitude` and `longitude` from `stg_dim_city` for geospatial use cases
- Incremental with `delete+insert` strategy and 2-day lookback window
- Partitioned by `toYYYYMM(observation_timestamp)`, ordered by `(city, observation_timestamp)`
- Includes full audit trail: `ingestion_ts` preserved via `coalesce`, `update_ts` updated on every run

**Tech key**

Introduced `tech_key` across the transformation layer as a deterministic surrogate key for traceability and join simplification:

| Model | Formula |
|---|---|
| `stg_raw_weather_2years` | `cityHash64(toString(city_id), toString(timestamp))` |
| `stg_raw_weather_hourly` | `cityHash64(toString(city_id), toString(timestamp))` |
| `int_weather_observations` | Inherited from staging via `select *` |
| `mart_weather_hourly` | `cityHash64(ci.city_name, toString(o.timestamp))` |
| `mart_weather_daily` | `cityHash64(ci.city_name, toString(toDate(o.timestamp)))` |

**Key decisions:**
- Not added to raw tables — append-only model contains duplicates, making a unique hash meaningless
- Not added to dim tables — single column natural keys already exist (`city_id`, `country_id`)
- Computed at staging level so it flows cleanly through the `UNION ALL` in `int_weather_observations`
- Tested with `unique` and `not_null` in all models as validation of deduplication logic

**Storage strategy revised**
- `stg_dim_city` and `stg_dim_country` remain as views — tiny reference tables, no storage benefit
- `stg_raw_weather_hourly` and `stg_raw_weather_2years` changed to tables with explicit `MergeTree` engine, partitioned by `toYYYYMM(timestamp)`, ordered by `(city_id, timestamp)`
- All models now include `post_hook="{{ apply_column_comments() }}"` to persist descriptions in ClickHouse

**Project structure**

```
models/
├── src/
│   └── schema.yml
├── stg/
│   ├── schema.yml
│   ├── stg_dim_city.sql               ← view + column comments
│   ├── stg_dim_country.sql            ← view + column comments
│   ├── stg_raw_weather_2years.sql     ← table, partitioned, tech_key
│   └── stg_raw_weather_hourly.sql     ← table, partitioned, tech_key
├── int/
│   ├── schema.yml
│   └── int_weather_observations.sql   ← incremental, partitioned, tech_key
└── marts/
    ├── schema.yml
    ├── mart_weather_daily.sql         ← incremental, partitioned, tech_key
    └── mart_weather_hourly.sql        ← incremental, partitioned, tech_key
```