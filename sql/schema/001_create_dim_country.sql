-- 001. Create table with country information
CREATE TABLE IF NOT EXISTS weather.dim_country (
	country_id INTEGER PRIMARY KEY,
	country_name TEXT NOT NULL
);
