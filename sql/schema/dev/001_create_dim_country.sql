-- 001. Create table with country information
CREATE TABLE weather_dev.dim_country (
  `country_id` Int32,
  `country_name` String
)
ENGINE = MergeTree
PRIMARY KEY tuple(country_id)
ORDER BY tuple(country_id);