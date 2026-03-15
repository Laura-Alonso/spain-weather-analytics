# Project Setup

## 1. Environment

Create Python environment:

```
python -m venv env
source env/bin/activate
pip install -r requirements.txt
```

## 2. Configure credentials

Create `.env` file:

```
CLICKHOUSE_HOST= XXX
CLICKHOUSE_PORT= XXX
CLICKHOUSE_USER= XXX
CLICKHOUSE_PASSWORD= XXX
CLICKHOUSE_DATABASE= XXX
```
## 3. Create database schema

Run SQL files in order:

* `sql/schema/001_create_dim_country.sql`
* `sql/schema/002_create_dim_city.sql`
* `sql/schema/003_create_raw_weather_hourly.sql`

## 4. Populate reference tables

Insert country and city seeds.

## 5. Historical backfill

Run: `sql/quality_checks/weather_backfill_validation.sql`

## 6. Hourly ingestion pipeline

Run the incremental ingestion script: `scripts/update_weather_hourly.py`

The pipeline downloads the latest 24 hours of weather data for all cities and inserts them into `raw_weather_hourly`.
