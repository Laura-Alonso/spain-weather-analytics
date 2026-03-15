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