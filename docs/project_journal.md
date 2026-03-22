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

Day 4 (2026-03-22) — dbt Transformation Layer

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