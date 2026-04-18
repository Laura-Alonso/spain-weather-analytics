''' 
STRUCTURE:

1. Connect to ClickHouse
2. Load data
3. Feature engineering (temperature lags + humidity/pressure lags)
4. Train & evaluate: XGBoost, LightGBM, LinearRegression, CatBoost
5. Results table + feature importance
6. Save results to JSON
'''

# Libraries

import os
import json
import warnings
import numpy as np
import pandas as pd
import clickhouse_connect
 
from datetime import datetime
from dotenv import load_dotenv
 
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import StandardScaler
 
import xgboost as xgb
import lightgbm as lgb
from catboost import CatBoostRegressor
 
warnings.filterwarnings("ignore")
load_dotenv()


# 1. Connect to ClickHouse

'''
1. CONNECT TO CLICKHOUSE
'''

client = clickhouse_connect.get_client(
    host=os.getenv("CLICKHOUSE_HOST"),
    port=os.getenv("CLICKHOUSE_PORT"),
    username=os.getenv("CLICKHOUSE_USER"),
    password=os.getenv("CLICKHOUSE_PASSWORD"),
    database=os.getenv("CLICKHOUSE_DATABASE")
)
 
print("Connected!")


'''
2. LOAD DATA

* Principal table: int_weather_observations - Clean and deduplicated hourly data.
* Supplemental table: dim_city - City latitude and longitude.

'''

query = """
SELECT
    o.timestamp,
    o.city_id,
    c.city_name,
    c.latitude,
    c.longitude,
    o.temperature,
    o.wind_speed,
    o.wind_gusts,
    o.precipitation,
    o.humidity,
    o.pressure
FROM weather.int_weather_observations AS o
LEFT JOIN weather.dim_city AS c ON o.city_id = c.city_id
WHERE o.timestamp >= '2024-03-22 00:00:00'
  AND o.timestamp <= '2026-03-28 23:00:00'
ORDER BY o.city_id, o.timestamp
"""

df = client.query_df(query)
 
print(f"Loaded {len(df):,} rows across {df['city_id'].nunique()} cities.")
print(f"Date range: {df['timestamp'].min()} → {df['timestamp'].max()}")


'''
3. FEATURE ENGINEERING

TEMPERATURE:
    * Lag Features (lag_<n>_h) — past values shifted forward in time.
        - 1 hour ago, 2 hours ago, 3 hours ago, 12 hours ago, 24 hours ago, 48 hours ago, 168 hours ago.
    * Rolling Averages (roll_<n>_h) — mean values over a recent window.
        - Last 6 hours, 24 hours, 72 hours.

HUMIDITY:
    * Lag Features (humidity_lag_<n>_h) — past values shifted forward in time.
        - 1 hour ago, 24 hours ago.
    * Rolling Averages (humidity_roll_<n>_h) — mean values over a recent window.
        - Last 6 hours.

 PRESSURE:
    * Lag Features (pressure_lag_<n>_h) — past values shifted forward in time.
        - 1 hour ago, 24 hours ago.
    * Rolling Averages (pressure_roll_<n>_h) — mean values over a recent window.
        - Last 6 hours.
               
TIME FEATURES — cyclical patterns (day, week, year)
    - hour        = 0–23 (temperature peaks in afternoon, dips at night)
    - day_of_week = 0–6 (minor effect but included)
    - month       = 1–12 (strong seasonal effect)
    - day_of_year = 1–365 (fine-grained seasonality)

CITY FEATURES — latitude and longitude.
    - latitude  (northerly cities are colder)
    - longitude (minor effect for Spain but included for completeness)
'''

def build_features(df: pd.DataFrame) -> pd.DataFrame:
 
    all_cities = []
 
    for city_id, city_df in df.groupby("city_id"):
 
        # Sort by timestamp to ensure correct lagging
        city_df = city_df.sort_values("timestamp").copy()
 
        # Temperature lags
        city_df["lag_1h"]   = city_df["temperature"].shift(1)
        city_df["lag_2h"]   = city_df["temperature"].shift(2)
        city_df["lag_3h"]   = city_df["temperature"].shift(3)
        city_df["lag_12h"]  = city_df["temperature"].shift(12)
        city_df["lag_24h"]  = city_df["temperature"].shift(24)
        city_df["lag_48h"]  = city_df["temperature"].shift(48)
        city_df["lag_168h"] = city_df["temperature"].shift(168)
 
        # Temperature rolling - min_periods avoids NaN for first rows
        city_df["roll_6h"]  = city_df["temperature"].shift(1).rolling(6,   min_periods=1).mean()
        city_df["roll_24h"] = city_df["temperature"].shift(1).rolling(24,  min_periods=1).mean()
        city_df["roll_72h"] = city_df["temperature"].shift(1).rolling(72,  min_periods=1).mean()
 
        # Humidity lags
        city_df["humidity_lag_1h"]  = city_df["humidity"].shift(1)
        city_df["humidity_lag_24h"] = city_df["humidity"].shift(24)

        # Humidity rolling
        city_df["humidity_roll_6h"] = city_df["humidity"].shift(1).rolling(6, min_periods=1).mean()

        # Pressure lags
        city_df["pressure_lag_1h"]  = city_df["pressure"].shift(1)
        city_df["pressure_lag_24h"] = city_df["pressure"].shift(24)

        # Pressure rolling
        city_df["pressure_roll_6h"] = city_df["pressure"].shift(1).rolling(6, min_periods=1).mean()


        all_cities.append(city_df)
 
    df = pd.concat(all_cities).sort_values(["city_id", "timestamp"]).reset_index(drop=True)
 
    # Time features - Global
    df["hour"]        = df["timestamp"].dt.hour
    df["day_of_week"] = df["timestamp"].dt.dayofweek
    df["month"]       = df["timestamp"].dt.month
    df["day_of_year"] = df["timestamp"].dt.dayofyear
 
    # Drop rows with NaN from lag/roll features
    # The first rows per city will have NaN lags (not enough history). We drop them cleanly here.
    lag_cols = [
        "lag_1h", "lag_2h", "lag_3h", "lag_12h", "lag_24h", "lag_48h", "lag_168h",
        "roll_6h", "roll_24h", "roll_72h",
        "humidity_lag_1h", "humidity_lag_24h", "humidity_roll_6h",
        "pressure_lag_1h", "pressure_lag_24h", "pressure_roll_6h"
    ]
    df = df.dropna(subset=lag_cols).reset_index(drop=True)
 
    print(f"Features built. Dataset size after dropping NaN rows: {len(df):,}")
    return df
 
 
df = build_features(df)

print(df.head())


'''
4. DEFINE FEATURES COLUMNS AND TARGET
'''

feature_cols = [
    # Temperature lags
    "lag_1h", "lag_2h", "lag_3h",
    "lag_12h", "lag_24h", "lag_48h", "lag_168h",
    # Temperature rolling
    "roll_6h", "roll_24h", "roll_72h",
    # NEW: Humidity features
    "humidity_lag_1h", "humidity_lag_24h", "humidity_roll_6h",
    # NEW: Pressure features
    "pressure_lag_1h", "pressure_lag_24h", "pressure_roll_6h",
    # Time features
    "hour", "day_of_week", "month", "day_of_year",
    # City features
    "latitude", "longitude", "city_id"
]
 
target_col = "temperature"


'''
5. CROSS-VALIDATION SETUP
'''

# Sort by time globally for TimeSeriesSplit to make sense
df = df.sort_values("timestamp").reset_index(drop=True)
 
X = df[feature_cols]
y = df[target_col]
 
# 5 folds — enough to get a stable estimate without taking forever
tscv = TimeSeriesSplit(n_splits=5)
 
# For Linear Regression we need to scale features (tree models don't need this)
scaler = StandardScaler()

'''
6. EVALUATION FUNCTION

- RMSE (Root Mean Squared Error): average prediction error in °C.
    * Lower is better. RMSE of 1.5 means predictions are off by ~1.5°C on avg.
    * Penalises large errors more than MAE.

- MAE (Mean Absolute Error): simpler average of absolute errors in °C.
    * Lower is better. More intuitive than RMSE.
'''

def evaluate_model(model_name: str, model, X, y, tscv, scale=False):
    """
    Run time-series cross-validation for a given model.
    Returns mean RMSE and MAE across all folds.
    """
    rmse_scores = []
    mae_scores  = []

    # Record when training starts for this model
    train_start = datetime.utcnow()
    print(f"  Started at: {train_start.strftime('%H:%M:%S')}")    
 
    for fold, (train_idx, test_idx) in enumerate(tscv.split(X)):
        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]
 
        # Linear regression needs scaled features
        if scale:
            X_train = scaler.fit_transform(X_train)
            X_test  = scaler.transform(X_test)
 
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
 
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        mae  = mean_absolute_error(y_test, y_pred)
 
        rmse_scores.append(rmse)
        mae_scores.append(mae)
 
        print(f"  [{model_name}] Fold {fold+1}/5 — RMSE: {rmse:.3f}°C  MAE: {mae:.3f}°C")

    # Record when training ends and compute duration
    train_end      = datetime.utcnow()
    duration_secs  = (train_end - train_start).total_seconds()
    duration_str   = f"{int(duration_secs // 60)}m {int(duration_secs % 60)}s" 

    print(f"  Finished at: {train_end.strftime('%H:%M:%S')}  |  Duration: {duration_str}")       
 
    return np.mean(rmse_scores), np.mean(mae_scores), train_start, train_end, duration_str


'''
7. TRAIN AND EVALUATE ALL MODELS

- XGBoost — Gradient boosted trees, strong on tabular data with lag features.
    Builds trees sequentially, each correcting the previous one's errors.
    Handles non-linear interactions between lag features and time features well.
    Slightly slower than LightGBM on large datasets.

- LightGBM — Similar to XGBoost but faster; often competitive or better.
    Uses histogram-based splitting and leaf-wise tree growth.
    Winner in v1 (RMSE: 0.7223°C). Now tested with the expanded feature set
    including humidity and pressure lags — expected to benefit the most
    from these additions due to its ability to capture feature interactions.

- LinearRegression — Baseline. Scaled features required (StandardScaler applied).
    Simple linear combination of all features. Fast but assumes linearity,
    which limits its ability to capture temperature patterns like daily cycles
    or seasonal non-linearities. Included purely as a benchmark floor.

- CatBoost — Gradient boosted trees by Yandex, new in v2.
    Handles feature interactions differently from XGBoost and LightGBM
    using ordered boosting, which reduces prediction shift.
    Strong candidate when feature set is mixed (lags + time + geo).
    No scaling required. Benchmarked here for the first time.    
'''


results = {}
 
# --- XGBoost ---
print("\nEvaluating XGBoost...")
xgb_model = xgb.XGBRegressor(
    n_estimators=300,
    learning_rate=0.05,
    max_depth=6,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=42,
    verbosity=0
)
rmse, mae, t_start, t_end, duration = evaluate_model("XGBoost", xgb_model, X, y, tscv)
results["XGBoost"] = {"rmse": rmse, "mae": mae, "duration": duration}
 

# --- LightGBM ---
print("\nEvaluating LightGBM...")
lgb_model = lgb.LGBMRegressor(
    n_estimators=300,
    learning_rate=0.05,
    max_depth=6,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=42,
    verbose=-1
)
rmse, mae, t_start, t_end, duration = evaluate_model("LightGBM", lgb_model, X, y, tscv)
results["LightGBM"] = {"rmse": rmse, "mae": mae, "duration": duration}
 
 
# --- Linear Regression (baseline) ---
print("\nEvaluating Linear Regression (baseline)...")
lr_model = LinearRegression()
rmse, mae, t_start, t_end, duration = evaluate_model("LinearRegression", lr_model, X, y, tscv, scale=True)
results["LinearRegression"] = {"rmse": rmse, "mae": mae, "duration": duration}


# --- CatBoost ---
print("\nEvaluating CatBoost...")
cat_model = CatBoostRegressor(
    iterations=300,
    learning_rate=0.05,
    depth=6,
    random_seed=42,
    verbose=0
)
rmse, mae, t_start, t_end, duration = evaluate_model("CatBoost", cat_model, X, y, tscv)
results["CatBoost"] = {"rmse": rmse, "mae": mae, "duration": duration}


'''
8. RESULTS TABLE
'''

print("\n" + "="*72)
print("MODEL COMPARISON — Temperature Forecasting (Spain)")
print("Features: temperature lags + humidity lags + pressure lags")
print("="*72)
print(f"{'Model':<22} {'RMSE (°C)':>12} {'MAE (°C)':>12} {'Duration':>12}")
print("-"*72)
 
# Sort by RMSE ascending (best first)
sorted_results = sorted(results.items(), key=lambda x: x[1]["rmse"])
 
for model_name, scores in sorted_results:
    marker = "  ← WINNER" if model_name == sorted_results[0][0] else ""
    print(
        f"{model_name:<22} "
        f"{scores['rmse']:>12.4f} "
        f"{scores['mae']:>12.4f} "
        f"{scores['duration']:>12}"
        f"{marker}"
    )
 
print("="*72)
best_model_name = sorted_results[0][0]
best_rmse       = sorted_results[0][1]["rmse"]
best_mae        = sorted_results[0][1]["mae"]
best_duration   = sorted_results[0][1]["duration"]
 
print(f"\nWinner: {best_model_name}")
print(f"  RMSE:     {best_rmse:.4f}°C")
print(f"  MAE:      {best_mae:.4f}°C")
print(f"  Duration: {best_duration}")


'''
9. FEATURE IMPORTANCE 

No for linear regression.
Shows which features each model relies on most.
Higher value = more important for predicting temperature.
This helps validate that our engineered features are meaningful.
'''
 
print("\n--- Feature Importance: XGBoost ---")
xgb_imp = pd.Series(xgb_model.feature_importances_, index=feature_cols).sort_values(ascending=False)
print(xgb_imp.to_string())
 
print("\n--- Feature Importance: LightGBM ---")
lgb_imp = pd.Series(lgb_model.feature_importances_, index=feature_cols).sort_values(ascending=False)
print(lgb_imp.to_string())
 
print("\n--- Feature Importance: CatBoost ---")
cat_imp = pd.Series(cat_model.get_feature_importance(), index=feature_cols).sort_values(ascending=False)
print(cat_imp.to_string())


'''
10. SAVE RESULTS 

Save the winner and full results to files so train_production.py knows which model to use 
without needing to re-run the experiment.

File saved:
    - data/experiment_results.json — full scores, rmse winner, and production model

'''

# Create data directory if it doesn't exist yet
os.makedirs("data", exist_ok=True)

output = {
    "run_date":    datetime.utcnow().isoformat(),
    "rmse_winner": best_model_name,

    # !!! FILL IN MANUALLY after reviewing feature importance
    "production_model": "LightGBM",
    "production_model_selected_manually": True,
    "production_model_reason": (
        "RMSE difference vs LinearRegression is negligible (0.05°C) but "
        "LightGBM distributes importance across lag, time, humidity and pressure "
        "features — more robust for recursive 168-hour forecasting."
    ),

    "results": {
        model: {
            "rmse":     round(scores["rmse"], 6),
            "mae":      round(scores["mae"],  6),
            "duration": scores["duration"]
        }
        for model, scores in results.items()
    }
}

with open("data/experiment_results.json", "w") as f:
    json.dump(output, f, indent=2)

print(f"\nRMSE winner:      {best_model_name}")
print(f"Production model: {output['production_model']}  ← set manually")
print("\nExperiment complete. Results saved to data/experiment_results.json")