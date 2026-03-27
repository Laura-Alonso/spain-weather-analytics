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
* `sql/schema/003_create_raw_weather_2years.sql`
* `sql/schema/004_create_raw_weather_hourly.sql`
* `sql/schema/005_create_ingestion_state.sql`

## 4. Populate reference tables

Insert country and city seeds:

* `sql/schema/001_seeds_country.sql`
* `sql/schema/002_seeds_city.sql`

## 5. Historical backfill

Run: `/scripts/backfill_weather_2years.py`
Run validation: `sql/quality_checks/weather_backfill_validation.sql`


## 6. Create ingestion state table. 

* `sql/schema/004_create_ingestion_state`

This table stores the execution state of the ingestion pipeline, including the last successful run and the processed time window, enabling monitoring and incremental control of the data flow.

## 7. Hourly ingestion pipeline

Run the incremental ingestion script: `scripts/update_weather_hourly.py`

---

## 8. Production deployment (Raspberry Pi)

The pipeline runs on a Raspberry Pi (Ubuntu) via cron.

1. Connect: `ubuntu@<IP>`
2. Install dependencies: `pip3 install --break-system-packages requests pandas clickhouse-connect python-dotenv`
3. Schedule execution: `crontab-e`:
    * `0 * * * * flock -n /tmp/weather_pipeline.lock -c 'cd /home/ubuntu/weather_pipeline && /usr/bin/python3 scripts/update_weather_hourly.py >> logs/pipeline.log 2>&1'`
4. Monitoring (logs)

---

## 9. dbt Transformation Layer

**Prerequisites**

`pip install "dbt-clickhouse = 1.7.2"`

** Configure dbt profile**

Create `~/.dbt/profiles.yml` (do NOT commit this file):

```yml
weather_project:
  target: dev
  outputs:
    dev:
      type: clickhouse
      schema: weather
      host: <CLICKHOUSE_HOST>
      port: <CLICKHOUSE_PORT>
      user: <CLICKHOUSE_USER>
      password: <CLICKHOUSE_PASSWORD>
      secure: True
```

**Project structure**

The dbt project lives under `dbt/weather_project/`. Models follow a layered architecture:

```
models/
├── src/
│   └── schema.yml          ← source declarations pointing to ClickHouse raw tables
└── stg/
    ├── schema.yml          ← staging model tests and descriptions
    ├── stg_dim_city.sql
    ├── stg_dim_country.sql
    ├── stg_raw_weather_2years.sql
    └── stg_raw_weather_hourly.sql
```

**Run dbt**

Verify connection:

```
cd dbt/weather_project
dbt debug
```

Run staging models:

```
dbt run --select stg
dbt test --select stg
```

Generate and serve documentation:

```
dbt docs generate
dbt docs serve
```

---

## 10. dbt Intermediate Layer

**Run intermediate models:**

```bash
cd dbt/weather_project
dbt run --select int
dbt test --select int
```

**Full refresh** (rebuilds the entire table from scratch):

```bash
dbt run --select int_weather_observations --full-refresh
```

**Validate no duplicates after run:**

```sql
SELECT city_id, timestamp, count(*) as cnt
FROM weather.int_weather_observations
GROUP BY city_id, timestamp
HAVING cnt > 1
```

**Project structure update:**

```
models/
├── src/
│   └── schema.yml
├── stg/
│   ├── schema.yml
│   ├── stg_dim_city.sql
│   ├── stg_dim_country.sql
│   ├── stg_raw_weather_2years.sql
│   └── stg_raw_weather_hourly.sql
└── int/
    ├── schema.yml
    └── int_weather_observations.sql
```