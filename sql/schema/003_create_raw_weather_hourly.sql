-- 003. Table with weather data directly from te API
CREATE TABLE IF NOT EXISTS raw_weather_hourly (
    timestamp DATETIME,
    city_name TEXT,
    latitude REAL,
    longitude REAL,
    temperature REAL,
    wind_speed REAL,
    wind_gusts REAL,
    precipitation REAL,
    humidity REAL,
    pressure REAL
);