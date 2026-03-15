-- 002. Create table with city information

CREATE TABLE weather.dim_city (
  `city_id` Int32,
  `city_name` String,
  `latitude` Float32,
  `longitude` Float32,
  `country_id` Int32
)
ENGINE = MergeTree
PRIMARY KEY tuple(city_id)
ORDER BY tuple(city_id);
