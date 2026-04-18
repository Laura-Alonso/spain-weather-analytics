-- 003. Table with weather data directly from te API - Historic: 2 years of data, hourly
CREATE TABLE weather.raw_weather_2years (
  `timestamp` DateTime,
  `city_id` Int32,
  `temperature` Float32,
  `wind_speed` Float32,
  `wind_gusts` Float32,
  `precipitation` Float32,
  `humidity` Float32,
  `pressure` Float32,
  `ingestion_ts` DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (city_id, timestamp);