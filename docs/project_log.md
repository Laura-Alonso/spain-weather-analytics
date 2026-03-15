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
