# **spain-weather-analytics**

This project builds a weather data pipeline for Spain using:

- Open-Meteo API
- ClickHouse
- Python ingestion scripts
- dbt for transformation

The pipeline stores hourly weather observations and prepares them for analytics and forecasting.

---

## **Data Warehouse**

The project uses **ClickHouse** as the analytical database.

ClickHouse is accessed through **CH-UI**, which provides a lightweight interface for running SQL queries and managing tables.

Reasons for choosing ClickHouse:

- Columnar storage optimized for analytical workloads
- Very fast aggregation queries
- Suitable for time-series data
- Simple local deployment

---

## **Main Features**

* temperature_2m - Main temperature
* wind_speed_10m - Wind intensity
* wind_gust_10m - Extreme events
* surface_pressure - Preasure
* relative_humidity_2m - Humidity

---

## **Configuration**

* Historic: 2 years
* Ingestion: Hourly
* Cities:
    * Madrid
    * Barcelona
    * Alicante
    * Valladolid

---

## **Endpoint**

**Hisotric**: https://archive-api.open-meteo.com/v1/archive

Parameters:

* latitude
* longitude
* start_date
* end_date
* hourly
* timezone

---

## **Project Setup**

Follow these steps to initialize the project environment.

### 1. Create the SQLite database

Open DBeaver and create a new SQLite database file: `data/weather.db`

### 2. Create database schema

Run the SQL scripts located in: `sql/schema`. Execute them in order.

### 3. Insert seed data

Run the seed scripts: `sql/seeds`. Execute them in order.
