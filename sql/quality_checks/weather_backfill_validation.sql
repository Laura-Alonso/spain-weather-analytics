-- 1. Validate temporal coverage
SELECT
    city_id,
    min(timestamp) AS min_ts,
    max(timestamp) AS max_ts,
    count() AS rows
FROM weather.raw_weather_hourly
GROUP BY city_id
ORDER BY city_id;

-- 2. Detect duplicated hours
SELECT
    city_id,
    count() AS rows,
    uniqExact(timestamp) AS unique_hours
FROM weather.raw_weather_hourly
GROUP BY city_id;

-- 3. Null values
SELECT
    city_id,
    countIf(temperature IS NULL) AS temp_nulls,
    countIf(wind_speed IS NULL) AS wind_speed_nulls,
    countIf(wind_gusts IS NULL) AS wind_gusts_nulls,
    countIf(precipitation IS NULL) AS precipitation_nulls,
    countIf(humidity IS NULL) AS humidity_nulls,
    countIf(pressure IS NULL) AS pressure_nulls
FROM weather.raw_weather_hourly
GROUP BY city_id;

-- 4. Basic sanity checks
SELECT
    city_id,

    min(temperature) AS min_temp,
    max(temperature) AS max_temp,

    min(wind_speed) AS min_wind_speed,
    max(wind_speed) AS max_wind_speed,

    min(wind_gusts) AS min_wind_gusts,
    max(wind_gusts) AS max_wind_gusts,

    min(precipitation) AS min_precip,
    max(precipitation) AS max_precip,

    min(humidity) AS min_humidity,
    max(humidity) AS max_humidity,

    min(pressure) AS min_pressure,
    max(pressure) AS max_pressure

FROM weather.raw_weather_hourly
GROUP BY city_id;