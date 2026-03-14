# **spain-weather-analytics**

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
