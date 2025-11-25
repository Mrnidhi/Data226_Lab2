## Data 226 – Lab 2: Stock Analytics Pipeline

This project is my solution for **Lab 2** in Data 226.  
I built a small end‑to‑end pipeline that:
- pulls daily stock prices from **Alpha Vantage**
- stores them in **Snowflake**
- transforms them with **dbt**
- automates everything with **Airflow**
- and visualizes the results in **Preset** (Superset).

### 1. What the pipeline does
- Calls the Alpha Vantage API for a few tickers (e.g. `AAPL`, `MSFT`, `GOOGL`).
- Writes the raw daily price data into a Snowflake table.
- Uses dbt to:
  - create a clean staging view: `stg_stock_prices`
  - build a technical indicators table: `stock_technical_indicators`  
    (simple moving averages, daily returns, and a basic bullish/bearish trend flag).
- Runs all steps in order through a single Airflow DAG so it can be scheduled.

### 2. Main files and folders
- **Airflow**
  - DAG file: `airflow/dags/etl_pipeline.py`  
    DAG id in the UI: `stock_analytics_lab2`
- **dbt project**
  - Root: `dbt/stock_analytics/`
  - Staging model: `models/staging/stg_stock_prices.sql`
  - Marts model: `models/marts/stock_technical_indicators.sql`
- **Docker / infrastructure**
  - Local stack for Airflow + Postgres: `docker-compose.yaml`

### 3. How ETL and ELT are implemented
- **ETL (Extract–Transform–Load) with Airflow + Snowflake**
  - Extract: a Python task in `etl_pipeline.py` calls the Alpha Vantage API and builds a clean `pandas` DataFrame.
  - Transform (light): the same task does basic type casting (dates, floats, ints).
  - Load: the task uses the Airflow `SnowflakeHook` to:
    - create the raw target table if it does not exist
    - stage data into a temp table
    - `MERGE` from the temp table into `USER_DB_MAGPIE.RAW.STOCK_DATA` (upsert by symbol + date).
- **ELT (Extract–Load–Transform) with dbt + Snowflake**
  - dbt reads directly from the raw Snowflake table as a **source**.
  - `stg_stock_prices` is a dbt **staging view** that standardizes column names and exposes a clean base.
  - `stock_technical_indicators` is a dbt **marts table** that does the heavier transforms:
    moving averages, daily returns, and a trend label.

### 4. How to run the pipeline
1. From the project root, start the containers:
   ```bash
   docker compose up -d
   ```
2. Open the Airflow web UI (see `docker-compose.yaml`, typically `http://localhost:8081`).
3. In Airflow:
   - Add a Variable `ALPHA_VANTAGE_KEY` with your API key.
   - Create a Connection `snowflake_conn` that points to your Snowflake account  
     (database `USER_DB_MAGPIE`, with the right user/role/warehouse).
4. Turn on and trigger the DAG **`stock_analytics_lab2`**.
5. In Snowflake, you should then see:
   - raw data in `USER_DB_MAGPIE.RAW.STOCK_DATA`
   - transformed data in `USER_DB_MAGPIE.ANALYTICS.STOCK_TECHNICAL_INDICATORS`

### 5. Dashboard in Preset (Superset)
- Preset is connected to the same Snowflake database.
- In Preset, I created a dataset on the dbt model  
  `USER_DB_MAGPIE.ANALYTICS.STOCK_TECHNICAL_INDICATORS`.
- On top of this dataset, I built a dashboard that includes:
  - price and volume trends by symbol
  - moving‑average trend signal (bullish vs. bearish)
  - daily returns over time

In summary, this repo shows a full but compact data pipeline:  
**API → Snowflake → dbt models → Airflow orchestration → Preset dashboard**.
# Data226_Lab2
