'''
STRUCTURE OF THIS FILE:

1. Loads 2 years of hourly weather data from ClickHouse
2. Engineers features (lags, rolling averages, time features)
3. Trains 4 models using time-series cross-validation
4. Evaluates each model with RMSE and MAE
5. Prints a comparison table and declares a winner
6. Saves all results to data/experiment_results.json
'''

# Libraries
import os
import json
import logging
import warnings
import numpy as np
import pandas as pd
import clickhouse_connect
 
from datetime import datetime
from dotenv import load_dotenv

# Suppress warnings
warnings.filterwarnings("ignore")

# Suppress Prophet's verbose output - before importing Prophet
logging.getLogger("prophet").setLevel(logging.ERROR)
logging.getLogger("cmdstanpy").setLevel(logging.ERROR)

# Scikit-learn metrics and cross-validation tools
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.model_selection import TimeSeriesSplit
from sklearn.preprocessing import StandardScaler

# Candidate models
import xgboost as xgb
import lightgbm as lgb
from prophet import Prophet

# ClickHouse variables
load_dotenv()


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
2. LOAD DATA FROM CLICKHOUSE

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

* Lag Features (lag_<n>_h) — past temperature values shifted forward in time.
    - 1 hour ago, 2 hours ago, 3 hours ago, 12 hours ago, 24 hours ago, 48 hours ago, 168 hours ago.

* Rolling Averages (roll_<n>_h) — mean temperature over a recent window.
    - Last 6 hours, 24 hours, 72 hours.

* Time Features — cyclical patterns (day, week, year)
    - hour        = 0–23 (temperature peaks in afternoon, dips at night)
    - day_of_week = 0–6 (minor effect but included)
    - month       = 1–12 (strong seasonal effect)
    - day_of_year = 1–365 (fine-grained seasonality)

* City Features - latitude and longitude.
    - latitude  (northerly cities are colder)
    - longitude (minor effect for Spain but included for completeness)
'''

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build all features for the model.
    Features are created per city to avoid leakage between cities.
    """
 
    all_cities = []
 
    for city_id, city_df in df.groupby("city_id"):
 
        # Sort by timestamp to ensure correct lagging
        city_df = city_df.sort_values("timestamp").copy()
 
        # --- Lag features ---
        city_df["lag_1h"]   = city_df["temperature"].shift(1)
        city_df["lag_2h"]   = city_df["temperature"].shift(2)
        city_df["lag_3h"]   = city_df["temperature"].shift(3)
        city_df["lag_12h"]  = city_df["temperature"].shift(12)
        city_df["lag_24h"]  = city_df["temperature"].shift(24)
        city_df["lag_48h"]  = city_df["temperature"].shift(48)
        city_df["lag_168h"] = city_df["temperature"].shift(168)
 
        # --- Rolling averages ---   min_periods avoids NaN for first rows
        city_df["roll_6h"]  = city_df["temperature"].shift(1).rolling(6,   min_periods=1).mean()
        city_df["roll_24h"] = city_df["temperature"].shift(1).rolling(24,  min_periods=1).mean()
        city_df["roll_72h"] = city_df["temperature"].shift(1).rolling(72,  min_periods=1).mean()
 
        all_cities.append(city_df)
 
    df = pd.concat(all_cities).sort_values(["city_id", "timestamp"]).reset_index(drop=True)
 
    # --- Time features ---   Global
    df["hour"]        = df["timestamp"].dt.hour
    df["day_of_week"] = df["timestamp"].dt.dayofweek
    df["month"]       = df["timestamp"].dt.month
    df["day_of_year"] = df["timestamp"].dt.dayofyear
 
    # Drop rows with NaN from lag features
    # The first rows per city will have NaN lags (not enough history). We drop them cleanly here.
    df = df.dropna(subset=[
        "lag_1h", "lag_2h", "lag_3h",
        "lag_12h", "lag_24h", "lag_48h", "lag_168h",
        "roll_6h", "roll_24h", "roll_72h"
    ]).reset_index(drop=True)
 
    print(f"Features built. Dataset size after dropping NaN rows: {len(df):,}")
    return df
 
 
df = build_features(df)

print(df.head())


'''
4. DEFINE FEATURES COLUMNS AND TARGET
'''

feature_cols = [
    # Lag features
    "lag_1h", "lag_2h", "lag_3h",
    "lag_12h", "lag_24h", "lag_48h", "lag_168h",
    # Rolling averages
    "roll_6h", "roll_24h", "roll_72h",
    # Time features
    "hour", "day_of_week", "month", "day_of_year",
    # City features
    "latitude", "longitude",
    # City ID as a categorical signal
    "city_id"
]
 
target_col = "temperature"


'''
5.TIME-SERIES CROSS-VALIDATION SETUP
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

- XGBoost  — Gradient boosted trees, strong on tabular data with lag features
- LightGBM — Similar to XGBoost but faster; often competitive or better
- Prophet  — Facebook's time-series model, requires a different interface
- Linear   — Simple baseline; everything should beat this

*Prophet notes:
    Prophet requires columns named 'ds' (timestamp) and 'y' (target).
    It doesn't use the engineered features — it models time patterns directly.
    We train one Prophet model per city and average the scores.
    This is why Prophet may score differently — it uses a fundamentally different approach (trend + seasonality decomposition).
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
results["XGBoost"] = {"rmse": rmse, "mae": mae, "train_start": t_start.isoformat(), "train_end": t_end.isoformat(), "duration": duration}
 

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
results["LightGBM"] = {"rmse": rmse, "mae": mae, "train_start": t_start.isoformat(), "train_end": t_end.isoformat(), "duration": duration}
 
 
# --- Linear Regression (baseline) ---
print("\nEvaluating Linear Regression (baseline)...")
lr_model = LinearRegression()
rmse, mae, t_start, t_end, duration = evaluate_model("LinearRegression", lr_model, X, y, tscv, scale=True)
results["LinearRegression"] = {"rmse": rmse, "mae": mae, "train_start": t_start.isoformat(), "train_end": t_end.isoformat(), "duration": duration}


# --- Prophet ---
print("\nEvaluating Prophet (one model per city)...")
 
prophet_rmse_all = []
prophet_mae_all  = []
 
prophet_start = datetime.utcnow()
print(f"  Started at: {prophet_start.strftime('%H:%M:%S')}")
 
for city_id, city_df in df.groupby("city_id"):
    city_name = city_df["city_name"].iloc[0]
    print(f"  [Prophet] City: {city_name}")
 
    # Prophet requires 'ds' and 'y' columns
    prophet_df = city_df[["timestamp", "temperature"]].rename(
        columns={"timestamp": "ds", "temperature": "y"}
    ).sort_values("ds").reset_index(drop=True)

    prophet_df["ds"] = prophet_df["ds"].dt.tz_localize(None)
 
    city_rmse = []
    city_mae  = []
 
    for fold, (train_idx, test_idx) in enumerate(tscv.split(prophet_df)):
        train_data = prophet_df.iloc[train_idx]
        test_data  = prophet_df.iloc[test_idx]
 
        m = Prophet(
            daily_seasonality=True,
            weekly_seasonality=True,
            yearly_seasonality=True,
            interval_width=0.95
        )
        m.fit(train_data)
 
        forecast = m.predict(test_data[["ds"]])
        y_pred = forecast["yhat"].values
        y_test = test_data["y"].values
 
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        mae  = mean_absolute_error(y_test, y_pred)
 
        city_rmse.append(rmse)
        city_mae.append(mae)
        print(f"    Fold {fold+1}/5 — RMSE: {rmse:.3f}°C  MAE: {mae:.3f}°C")
 
    prophet_rmse_all.append(np.mean(city_rmse))
    prophet_mae_all.append(np.mean(city_mae))
 
prophet_end     = datetime.utcnow()
prophet_secs    = (prophet_end - prophet_start).total_seconds()
prophet_dur_str = f"{int(prophet_secs // 60)}m {int(prophet_secs % 60)}s"
print(f"  Finished at: {prophet_end.strftime('%H:%M:%S')}  |  Duration: {prophet_dur_str}")
 
results["Prophet"] = {
    "rmse": np.mean(prophet_rmse_all),
    "mae":  np.mean(prophet_mae_all),
    "train_start": prophet_start.isoformat(),
    "train_end":   prophet_end.isoformat(),
    "duration":    prophet_dur_str
}

'''
8. RESULTS TABLE
'''

print("\n" + "="*72)
print("MODEL COMPARISON — Temperature Forecasting (Spain)")
print("="*72)
print(f"{'Model':<22} {'RMSE (°C)':>12} {'MAE (°C)':>12} {'Duration':>12}")
print("-"*72)
 
# Sort by RMSE ascending (best first)
sorted_results = sorted(results.items(), key=lambda x: x[1]["rmse"])
 
for model_name, scores in sorted_results:
    marker = " WINNER!!!" if model_name == sorted_results[0][0] else ""
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

XGBoost and LightGBM only 
Shows which features each model relies on most.
Higher value = more important for predicting temperature.
This helps validate that our engineered features are meaningful.
'''
 
# Print feature importances for XGBoost
print("\n--- Feature Importance: XGBoost ---")
xgb_importance = pd.Series(
    xgb_model.feature_importances_,
    index=feature_cols
).sort_values(ascending=False)
print(xgb_importance.to_string())
 
# Print feature importances for LightGBM
print("\n--- Feature Importance: LightGBM ---")
lgb_importance = pd.Series(
    lgb_model.feature_importances_,
    index=feature_cols
).sort_values(ascending=False)
print(lgb_importance.to_string())


'''
10. SAVE RESULTS 

Save the winner and full results to files so train_production.py knows which model to use 
without needing to re-run the experiment.

File saved:
    - data/experiment_results.json — full scores, rmse winner, and production model

'''

# Create data directory if it doesn't exist yet
os.makedirs("data", exist_ok=True)
 
# The model selected for production — change this manually if needed

production_model  = "LightGBM"                                                         
production_reason = (                                                                  
    "RMSE difference vs LinearRegression is negligible (0.05 C) but "
    "LightGBM handles non-linear patterns and extreme weather events better, "
    "making it more robust for production use."
)
 
# Save full results — single source of truth
with open("data/experiment_results.json", "w") as f:                                   
    json.dump({
        "run_date":                datetime.utcnow().isoformat(),
        "rmse_winner":             best_model_name,                                     
        "production_model":        production_model,                                    
        "production_model_reason": production_reason,                                  
        "results": {
            model: {
                "rmse":     round(scores["rmse"], 6),
                "mae":      round(scores["mae"],  6),
                "duration": scores["duration"]                                          
            }
            for model, scores in results.items()
        }
    }, f, indent=2)
 
print(f"\nRMSE winner:      {best_model_name}")
print(f"Production model: {production_model}")                                          
print(f"Reason:           {production_reason}")                                         
print("\nExperiment complete. Results saved to data/experiment_results.json")