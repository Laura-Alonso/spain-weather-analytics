-- 006. Forecast table — predicted temperature and rain probability per city
 
CREATE TABLE IF NOT EXISTS weather.mart_forecast
(
    `forecast_timestamp`    DateTime,
    `city`                  String,
    `latitude`              Float32,
    `longitude`             Float32,
    `country`               String,
    `predicted_temperature` Float32,
    `rain_probability`      Float32,
    `forecast_run_ts`       DateTime,
    `model_version`         String,
    `horizon_hours`         Int32,
    `tech_key`              UInt64,
    `ingestion_ts`          DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(forecast_timestamp)
ORDER BY (city, forecast_timestamp, forecast_run_ts);