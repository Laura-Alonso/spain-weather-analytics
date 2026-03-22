# Libraries
import requests
import pandas as pd
import clickhouse_connect
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
load_dotenv()
import time

# ClickHouse configuration
client = clickhouse_connect.get_client(
    host=os.getenv("CLICKHOUSE_HOST"),
    port=os.getenv("CLICKHOUSE_PORT"),
    username=os.getenv("CLICKHOUSE_USER"),
    password=os.getenv("CLICKHOUSE_PASSWORD"),
    database=os.getenv("CLICKHOUSE_DATABASE")
    )


# API congiguration
URL = "https://api.open-meteo.com/v1/forecast"

## Get city information from ClickHouse
query = """
SELECT city_id, latitude, longitude
FROM dim_city
"""

cities  = client.query(query).result_rows

total_rows = 0

for city in cities:
    CITY_ID = city[0]
    LAT = city[1]
    LON = city[2]

    print(f"Updating city {CITY_ID}")
    
    try:
        params = {
            "latitude": LAT,
            "longitude": LON,
            "hourly": [
                "temperature_2m",
                "wind_speed_10m",
                "wind_gusts_10m",
                "precipitation",
                "relative_humidity_2m",
                "pressure_msl"
            ],
            "past_days": 1,
            "timezone": "Europe/Madrid"
        }

        r = requests.get(URL, params=params, timeout=60)
        r.raise_for_status()

        data = r.json()["hourly"]
        
        df = pd.DataFrame({
            "timestamp": data["time"],
            "temperature": data["temperature_2m"],
            "wind_speed": data["wind_speed_10m"],
            "wind_gusts": data["wind_gusts_10m"],
            "precipitation": data["precipitation"],
            "humidity": data["relative_humidity_2m"],
            "pressure": data["pressure_msl"]
        })

        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["city_id"] = CITY_ID
        df["ingestion_ts"] = datetime.utcnow()

        # Filter future timestamps
        now = datetime.now()
        df = df[df["timestamp"] <= now]

        rows = len(df)
        total_rows += rows

        client.insert_df("raw_weather_hourly", df)

        print(f"Inserted {rows} rows for city {CITY_ID}")

    except Exception as e:
        print(f"ERROR city {CITY_ID}: {e}")

    time.sleep(1)

client.command("""
ALTER TABLE weather.ingestion_state
UPDATE
    last_successful_run = now(),
    window_start = now() - INTERVAL 1 DAY,
    window_end = now()
WHERE pipeline = 'weather_hourly_ingestion'
""")