'''
forecast_weather.py
-------------------
Loads the saved LightGBM models and generates a 168-hour (7-day) forecast
for all cities. Inserts results into weather.mart_forecast.

Strategy: recursive forecasting
  - Start from the last known observation per city
  - Predict hour 1, feed that prediction as lag_1h for hour 2, and so on
  - Repeat 168 times → full 7-day forecast

Run: daily via cron on Raspberry Pi
Cron example: 30 4 * * * cd /home/ubuntu/weather_pipeline && python3 ml/forecast_weather.py

Output: rows inserted into weather.mart_forecast
'''

# ── Libraries ──────────────────────────────────────────────────────────────────
import os
import json
import pickle
import warnings
import numpy as np
import pandas as pd
import clickhouse_connect

from datetime import datetime
from dotenv import load_dotenv

warnings.filterwarnings("ignore")
load_dotenv()

# ── Config ─────────────────────────────────────────────────────────────────────
MODEL_DIR      = "ml/models"
FORECAST_HOURS = 168   # 7 days

# ── 1. Connect to ClickHouse ───────────────────────────────────────────────────
print("Connecting to ClickHouse...")

client = clickhouse_connect.get_client(
    host=os.getenv("CLICKHOUSE_HOST"),
    port=os.getenv("CLICKHOUSE_PORT"),
    username=os.getenv("CLICKHOUSE_USER"),
    password=os.getenv("CLICKHOUSE_PASSWORD"),
    database=os.getenv("CLICKHOUSE_DATABASE")
)

print("Connected!")

# ── 2. Load models ────────────────────────────────────────────────────────────
print("Loading models from disk...")

with open(f"{MODEL_DIR}/lgbm_regressor.pkl", "rb") as f:
    regressor = pickle.load(f)

with open(f"{MODEL_DIR}/lgbm_classifier.pkl", "rb") as f:
    classifier = pickle.load(f)

with open(f"{MODEL_DIR}/feature_cols.json") as f:
    FEATURE_COLS = json.load(f)

with open(f"{MODEL_DIR}/model_meta.json") as f:
    meta = json.load(f)

MODEL_VERSION = meta["model_version"]
print(f"Models loaded. Version: {MODEL_VERSION}")

# ── 3. Load recent history per city ──────────────────────────────────────────
# We need the last 168 hours of real observations to build the initial lag features.
# Without this history, lag_168h would be NaN and the forecast would fail.

print("Loading recent history from ClickHouse...")

query = """
SELECT
    observation_timestamp   AS timestamp,
    city,
    latitude,
    longitude,
    country,
    temperature,
    precipitation,
    humidity,
    pressure
FROM weather.mart_weather_hourly
WHERE observation_timestamp >= now() - INTERVAL 8 DAY
ORDER BY city, observation_timestamp
"""

history = client.query_df(query)
history["timestamp"] = pd.to_datetime(history["timestamp"])

print(f"Loaded {len(history):,} rows of recent history.")

# ── 4. Forecast loop (one city at a time) ─────────────────────────────────────
# For each city we:
#   a) Build a buffer with the last 168 hours of real data
#   b) Iteratively predict the next hour, append it to the buffer, repeat

forecast_run_ts = datetime.utcnow()
all_forecasts   = []

for city, city_history in history.groupby("city"):

    city_history = city_history.sort_values("timestamp").copy()

    # Grab city metadata (constant across rows)
    latitude  = city_history["latitude"].iloc[0]
    longitude = city_history["longitude"].iloc[0]
    country   = city_history["country"].iloc[0]

    # The buffer holds real + predicted rows — we slide it forward each step
    # We keep at least 168 rows (needed for lag_168h)
    buffer = city_history[["timestamp", "temperature", "precipitation",
                            "humidity", "pressure"]].copy().tail(200)

    last_ts = buffer["timestamp"].max()

    print(f"\nForecasting {city} — starting from {last_ts}")

    city_forecasts = []

    for h in range(1, FORECAST_HOURS + 1):

        forecast_ts = last_ts + pd.Timedelta(hours=h)

        # ── Build features from the buffer ──
        # We look at the last row of the buffer as "current hour - 1"
        # and go further back for longer lags.

        def lag(col, n):
            """Get the value n steps back from the end of the buffer."""
            series = buffer[col]
            if len(series) < n:
                return np.nan
            return series.iloc[-(n)]

        def rolling_mean(col, window):
            """Mean of the last `window` values (excluding current)."""
            series = buffer[col].iloc[:-0] if len(buffer) >= window else buffer[col]
            return series.tail(window).mean()

        row = {
            # Temperature lags
            "lag_1h":   lag("temperature", 1),
            "lag_2h":   lag("temperature", 2),
            "lag_3h":   lag("temperature", 3),
            "lag_12h":  lag("temperature", 12),
            "lag_24h":  lag("temperature", 24),
            "lag_48h":  lag("temperature", 48),
            "lag_168h": lag("temperature", 168),
            # Temperature rolling
            "roll_6h":  rolling_mean("temperature", 6),
            "roll_24h": rolling_mean("temperature", 24),
            "roll_72h": rolling_mean("temperature", 72),
            # Humidity
            "humidity_lag_1h":  lag("humidity", 1),
            "humidity_lag_24h": lag("humidity", 24),
            "humidity_roll_6h": rolling_mean("humidity", 6),
            # Pressure
            "pressure_lag_1h":  lag("pressure", 1),
            "pressure_lag_24h": lag("pressure", 24),
            "pressure_roll_6h": rolling_mean("pressure", 6),
            # Precipitation
            "precip_lag_1h":  lag("precipitation", 1),
            "precip_lag_24h": lag("precipitation", 24),
            "precip_roll_6h": rolling_mean("precipitation", 6),
            # Time features
            "hour":        forecast_ts.hour,
            "day_of_week": forecast_ts.dayofweek,
            "month":       forecast_ts.month,
            "day_of_year": forecast_ts.day_of_year,
            # Geography
            "latitude":  latitude,
            "longitude": longitude,
        }

        X_pred = pd.DataFrame([row])[FEATURE_COLS]

        # ── Predict ──
        predicted_temp  = float(regressor.predict(X_pred)[0])
        rain_probability = float(classifier.predict_proba(X_pred)[0][1])

        # ── Append prediction to buffer (so next iteration uses it as a lag) ──
        new_row = pd.DataFrame([{
            "timestamp":     forecast_ts,
            "temperature":   predicted_temp,
            # For future lags, we use rain_probability as a soft precipitation proxy
            "precipitation": rain_probability,
            "humidity":      buffer["humidity"].iloc[-1],   # carry forward last known
            "pressure":      buffer["pressure"].iloc[-1],   # carry forward last known
        }])

        buffer = pd.concat([buffer, new_row], ignore_index=True)

        city_forecasts.append({
            "forecast_timestamp":    forecast_ts,
            "city":                  city,
            "latitude":              latitude,
            "longitude":             longitude,
            "country":               country,
            "predicted_temperature": round(predicted_temp, 2),
            "rain_probability":      round(rain_probability, 4),
            "forecast_run_ts":       forecast_run_ts,
            "model_version":         MODEL_VERSION,
            "horizon_hours":         h,
        })

    print(f"  {len(city_forecasts)} hours forecasted for {city}")
    all_forecasts.extend(city_forecasts)

# ── 5. Build final DataFrame ──────────────────────────────────────────────────
df_forecast = pd.DataFrame(all_forecasts)

# tech_key: deterministic hash per city + forecast_timestamp + run_ts
# Using pandas string hashing as a simple surrogate (ClickHouse cityHash64 not available in Python)
df_forecast["tech_key"] = (
    df_forecast["city"].astype(str)
    + df_forecast["forecast_timestamp"].astype(str)
    + df_forecast["forecast_run_ts"].astype(str)
).apply(hash).abs()

df_forecast["ingestion_ts"] = datetime.utcnow()

print(f"\nTotal forecast rows: {len(df_forecast):,}")

# ── 6. Insert into ClickHouse ─────────────────────────────────────────────────
print("Inserting into weather.mart_forecast...")

# Delete today's existing forecast for these cities before inserting
# This avoids stacking multiple runs on the same day
client.command("""
    DELETE FROM weather.mart_forecast
    WHERE toDate(forecast_run_ts) = today()
""")

client.insert_df("mart_forecast", df_forecast)

print(f"Done! {len(df_forecast):,} rows inserted into weather.mart_forecast.")
print(f"Model version: {MODEL_VERSION}")
print(f"Forecast run:  {forecast_run_ts.strftime('%Y-%m-%d %H:%M:%S')} UTC")
