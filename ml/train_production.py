'''
train_production.py
-------------------
Trains two LightGBM models on the full historical dataset and saves them to disk.

Models:
  1. LightGBM Regressor  → predicted_temperature (°C)
  2. LightGBM Classifier → rain_probability (0.0 – 1.0)

Run: manually on laptop or via cron on Raspberry Pi (monthly)
Cron example: 0 3 1 * * cd /home/ubuntu/weather_pipeline && python3 ml/train_production.py

Output:
  ml/models/lgbm_regressor.pkl
  ml/models/lgbm_classifier.pkl
  ml/models/feature_cols.json   ← list of feature names (used by forecast_weather.py)
  ml/models/model_meta.json     ← version, training date, metrics
'''

# ── Libraries ──────────────────────────────────────────────────────────────────
import os
import json
import pickle
import warnings
import numpy as np
import pandas as pd
import clickhouse_connect
import lightgbm as lgb

from datetime import datetime
from dotenv import load_dotenv
from sklearn.metrics import mean_squared_error, mean_absolute_error, log_loss, roc_auc_score

warnings.filterwarnings("ignore")
load_dotenv()

# ── Config ─────────────────────────────────────────────────────────────────────
MODEL_DIR     = "ml/models"
MODEL_VERSION = f"lgbm_v1_{datetime.utcnow().strftime('%Y-%m')}"

os.makedirs(MODEL_DIR, exist_ok=True)

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

# ── 2. Load data ───────────────────────────────────────────────────────────────
# We use mart_weather_hourly because it already has city names + coordinates joined.
# int_weather_observations would also work but requires an extra join here.

print("Loading data from ClickHouse...")

query = """
SELECT
    observation_timestamp   AS timestamp,
    city,
    latitude,
    longitude,
    temperature,
    precipitation,
    humidity,
    pressure
FROM weather.mart_weather_hourly
ORDER BY city, observation_timestamp
"""

df = client.query_df(query)

print(f"Loaded {len(df):,} rows across {df['city'].nunique()} cities.")
print(f"Date range: {df['timestamp'].min()} → {df['timestamp'].max()}")

# ── 3. Feature engineering ────────────────────────────────────────────────────
# Identical to train_experiment.py so the model sees the same input structure.

def build_features(df: pd.DataFrame) -> pd.DataFrame:

    all_cities = []

    for city, city_df in df.groupby("city"):

        city_df = city_df.sort_values("timestamp").copy()

        # Temperature lags
        city_df["lag_1h"]   = city_df["temperature"].shift(1)
        city_df["lag_2h"]   = city_df["temperature"].shift(2)
        city_df["lag_3h"]   = city_df["temperature"].shift(3)
        city_df["lag_12h"]  = city_df["temperature"].shift(12)
        city_df["lag_24h"]  = city_df["temperature"].shift(24)
        city_df["lag_48h"]  = city_df["temperature"].shift(48)
        city_df["lag_168h"] = city_df["temperature"].shift(168)

        # Temperature rolling
        city_df["roll_6h"]  = city_df["temperature"].shift(1).rolling(6,  min_periods=1).mean()
        city_df["roll_24h"] = city_df["temperature"].shift(1).rolling(24, min_periods=1).mean()
        city_df["roll_72h"] = city_df["temperature"].shift(1).rolling(72, min_periods=1).mean()

        # Humidity lags + rolling
        city_df["humidity_lag_1h"]  = city_df["humidity"].shift(1)
        city_df["humidity_lag_24h"] = city_df["humidity"].shift(24)
        city_df["humidity_roll_6h"] = city_df["humidity"].shift(1).rolling(6, min_periods=1).mean()

        # Pressure lags + rolling
        city_df["pressure_lag_1h"]  = city_df["pressure"].shift(1)
        city_df["pressure_lag_24h"] = city_df["pressure"].shift(24)
        city_df["pressure_roll_6h"] = city_df["pressure"].shift(1).rolling(6, min_periods=1).mean()

        # Precipitation lag (used by classifier)
        city_df["precip_lag_1h"]  = city_df["precipitation"].shift(1)
        city_df["precip_lag_24h"] = city_df["precipitation"].shift(24)
        city_df["precip_roll_6h"] = city_df["precipitation"].shift(1).rolling(6, min_periods=1).mean()

        all_cities.append(city_df)

    df = pd.concat(all_cities).sort_values(["city", "timestamp"]).reset_index(drop=True)

    # Time features
    df["hour"]        = df["timestamp"].dt.hour
    df["day_of_week"] = df["timestamp"].dt.dayofweek
    df["month"]       = df["timestamp"].dt.month
    df["day_of_year"] = df["timestamp"].dt.dayofyear

    # Rain label for classifier: 1 if it rained that hour, 0 otherwise
    df["rained"] = (df["precipitation"] > 0).astype(int)

    # Drop NaN rows from lag features
    lag_cols = [
        "lag_1h", "lag_2h", "lag_3h", "lag_12h", "lag_24h", "lag_48h", "lag_168h",
        "roll_6h", "roll_24h", "roll_72h",
        "humidity_lag_1h", "humidity_lag_24h", "humidity_roll_6h",
        "pressure_lag_1h", "pressure_lag_24h", "pressure_roll_6h",
        "precip_lag_1h", "precip_lag_24h", "precip_roll_6h"
    ]
    df = df.dropna(subset=lag_cols).reset_index(drop=True)

    print(f"Features built. Dataset size: {len(df):,} rows.")
    return df


df = build_features(df)

# ── 4. Define features ────────────────────────────────────────────────────────

FEATURE_COLS = [
    # Temperature lags
    "lag_1h", "lag_2h", "lag_3h",
    "lag_12h", "lag_24h", "lag_48h", "lag_168h",
    # Temperature rolling
    "roll_6h", "roll_24h", "roll_72h",
    # Humidity
    "humidity_lag_1h", "humidity_lag_24h", "humidity_roll_6h",
    # Pressure
    "pressure_lag_1h", "pressure_lag_24h", "pressure_roll_6h",
    # Precipitation (for classifier)
    "precip_lag_1h", "precip_lag_24h", "precip_roll_6h",
    # Time
    "hour", "day_of_week", "month", "day_of_year",
    # Geography
    "latitude", "longitude",
]

X = df[FEATURE_COLS]
y_temp = df["temperature"]   # regression target
y_rain = df["rained"]        # classification target

# ── 5. Train Model 1 — Temperature Regressor ──────────────────────────────────
print("\nTraining LightGBM Regressor (temperature)...")

regressor = lgb.LGBMRegressor(
    n_estimators=300,
    learning_rate=0.05,
    max_depth=6,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=42,
    verbose=-1
)

regressor.fit(X, y_temp)

# Quick in-sample metrics (just to confirm training worked — not for evaluation)
y_pred_temp = regressor.predict(X)
rmse = np.sqrt(mean_squared_error(y_temp, y_pred_temp))
mae  = mean_absolute_error(y_temp, y_pred_temp)

print(f"  Regressor in-sample RMSE: {rmse:.4f}°C  MAE: {mae:.4f}°C")
print("  (Use cross-val results from train_experiment.py for real evaluation)")

# ── 6. Train Model 2 — Rain Probability Classifier ────────────────────────────
print("\nTraining LightGBM Classifier (rain probability)...")

rain_rate = y_rain.mean()
print(f"  Rain base rate: {rain_rate:.1%} of hours had precipitation")

classifier = lgb.LGBMClassifier(
    n_estimators=300,
    learning_rate=0.05,
    max_depth=6,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=42,
    verbose=-1
)

classifier.fit(X, y_rain)

y_pred_rain_proba = classifier.predict_proba(X)[:, 1]
auc  = roc_auc_score(y_rain, y_pred_rain_proba)
loss = log_loss(y_rain, y_pred_rain_proba)

print(f"  Classifier in-sample AUC: {auc:.4f}  Log-loss: {loss:.4f}")

# ── 7. Save models and metadata ───────────────────────────────────────────────
print("\nSaving models...")

with open(f"{MODEL_DIR}/lgbm_regressor.pkl", "wb") as f:
    pickle.dump(regressor, f)

with open(f"{MODEL_DIR}/lgbm_classifier.pkl", "wb") as f:
    pickle.dump(classifier, f)

with open(f"{MODEL_DIR}/feature_cols.json", "w") as f:
    json.dump(FEATURE_COLS, f, indent=2)

meta = {
    "model_version":   MODEL_VERSION,
    "trained_at":      datetime.utcnow().isoformat(),
    "n_rows":          len(df),
    "date_range_from": str(df["timestamp"].min()),
    "date_range_to":   str(df["timestamp"].max()),
    "regressor": {
        "type":      "LGBMRegressor",
        "insample_rmse": round(rmse, 6),
        "insample_mae":  round(mae, 6)
    },
    "classifier": {
        "type":         "LGBMClassifier",
        "rain_base_rate": round(rain_rate, 4),
        "insample_auc": round(auc, 6),
        "insample_logloss": round(loss, 6)
    }
}

with open(f"{MODEL_DIR}/model_meta.json", "w") as f:
    json.dump(meta, f, indent=2)

print(f"\nDone! Models saved to {MODEL_DIR}/")
print(f"  lgbm_regressor.pkl")
print(f"  lgbm_classifier.pkl")
print(f"  feature_cols.json")
print(f"  model_meta.json")
print(f"\nModel version: {MODEL_VERSION}")
