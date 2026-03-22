# Libraries
import requests
import pandas as pd
import clickhouse_connect
from datetime import datetime
from dateutil.relativedelta import relativedelta
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
URL = "https://archive-api.open-meteo.com/v1/archive"

END_DATE = datetime.utcnow().date()
START_DATE = END_DATE - relativedelta(years=2)

START_DATE = START_DATE.strftime("%Y-%m-%d")
END_DATE = END_DATE.strftime("%Y-%m-%d")

## Get city information from ClickHouse
query = """
SELECT city_id, latitude, longitude
FROM dim_city
"""

cities  = client.query(query).result_rows

for city in cities:
    CITY_ID = city[0]
    LAT = city[1]
    LON = city[2]

    print(f"Downloading data for city {CITY_ID}")
    
    # Request parameters
    params = {
        "latitude": LAT,
        "longitude": LON,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "hourly": [
            "temperature_2m",
            "wind_speed_10m",
            "wind_gusts_10m",
            "precipitation",
            "relative_humidity_2m",
            "pressure_msl"
        ],
        "timezone": "Europe/Madrid"
    }

    # Make the API request
    r = requests.get(URL, params=params, timeout=60)
    r.raise_for_status()

    data = r.json()["hourly"]
    
    # Create a DataFrame from the API response
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

    print("Rows downloaded:", len(df))

    # Insert the data into ClickHouse
    client.insert_df(
        "raw_weather_2years",
        df
    )
    time.sleep(1)

    print(f"Inserted city {CITY_ID}")