# Lab 2: Building an End-to-End Data Analytics Pipeline

**Authors:** Srinidhi Gowda Jayaramegowda, Bhavyasree Kondi  
**Course:** DATA 226 – Data Warehousing  
**Date:** Nov 25, 2025

---

## Problem Statement

Financial data is inherently volatile and high-volume, requiring robust systems to ingest, process, and analyze market trends without data loss or duplication. This project architects an end-to-end data pipeline that automates the daily extraction of stock prices (AAPL, MSFT, GOOGL), transforms raw records into meaningful technical indicators (RSI, Moving Averages), and visualizes the results for decision-making.

## Architecture

```
Alpha Vantage API → Airflow (ETL) → Snowflake (RAW) → dbt (Transform) → Snowflake (ANALYTICS) → Preset Dashboard
```

## Tech Stack

| Component | Tool |
|-----------|------|
| Data Source | Alpha Vantage API |
| Orchestration | Apache Airflow 2.10 |
| Warehouse | Snowflake |
| Transformation | dbt-core 1.8 |
| Visualization | Preset (Superset) |
| Infrastructure | Docker Compose |

## Project Structure

```
├── airflow/
│   ├── dags/etl_pipeline.py
│   ├── Dockerfile
│   └── requirements.txt
├── dbt/stock_analytics/
│   ├── models/
│   │   ├── staging/stg_stock_prices.sql
│   │   └── marts/stock_technical_indicators.sql
│   ├── snapshots/stock_prices_snapshot.sql
│   ├── tests/volume_is_positive.sql
│   ├── profiles.yml
│   └── dbt_project.yml
└── docker-compose.yaml
```

## Database Schema

### Raw Layer (`RAW.STOCK_DATA`)

| Field | Type | Description |
|-------|------|-------------|
| symbol | VARCHAR(10) | Stock ticker (PK) |
| date | DATE | Trading date (PK) |
| open | FLOAT | Opening price |
| high | FLOAT | Daily high |
| low | FLOAT | Daily low |
| close | FLOAT | Closing price |
| volume | BIGINT | Trading volume |

### Analytical Layer (`ANALYTICS.STOCK_TECHNICAL_INDICATORS`)

| Field | Type | Description |
|-------|------|-------------|
| symbol | VARCHAR(10) | Stock ticker |
| date | DATE | Trading date |
| prev_close | FLOAT | Previous day's close |
| price_momentum | FLOAT | Dollar change (close - prev_close) |
| daily_return | FLOAT | Percentage change |
| sma_5, sma_10, sma_20 | FLOAT | Simple moving averages |
| volume_sma_5 | FLOAT | 5-day volume moving average |
| avg_gain_14 | FLOAT | 14-day avg positive change |
| avg_loss_14 | FLOAT | 14-day avg negative change |
| rsi_14 | FLOAT | Relative Strength Index (0-100) |
| trend_signal | VARCHAR | BULLISH or BEARISH |

## Pipeline Flow

### ETL Phase (Airflow)
- **Extract:** Fetch daily prices from Alpha Vantage API
- **Load:** Insert into Snowflake using MERGE (upsert) for idempotency
- **Features:** Rate limiting (12s delay), transaction management, rollback on failure

### ELT Phase (dbt)
- **Staging:** `stg_stock_prices` – clean view over raw data
- **Marts:** `stock_technical_indicators` – calculated metrics using window functions
- **Tests:** Not null checks, volume >= 0 validation
- **Snapshots:** Historical tracking of price changes

### DAG Tasks
```
extract_load_raw_data → dbt_run → dbt_test → dbt_snapshot
```

## How to Run

```bash
# Start containers
docker compose up -d

# Access Airflow UI at http://localhost:8081
# Username: airflow / Password: airflow

# Configure in Airflow:
# - Variable: ALPHA_VANTAGE_KEY
# - Connection: snowflake_conn

# Trigger DAG: stock_analytics_lab2
```

## Dashboard (Preset)

The BI dashboard visualizes key technical indicators:

- **Price Trend Chart:** Close price with SMA overlays by symbol
- **RSI Indicator:** Line chart with overbought (70) / oversold (30) zones
- **Signal Distribution:** Bullish vs Bearish day ratio
- **Volume vs Price:** Correlation between volume spikes and price movements
- **Daily Returns:** Box plot showing volatility across stocks
- **Momentum Over Time:** Scatter plot of price momentum spikes

### Key Insights
- Rising moving averages confirm bullish momentum
- RSI levels indicate overbought/oversold conditions
- Volume spikes align with significant price movements
- GOOGL shows highest volatility, AAPL/MSFT more stable

## Conclusion

This project demonstrates a modern data stack separating Extraction (Airflow) from Transformation (dbt). The MERGE strategy ensures data reliability, while dbt enables version-controlled financial logic (RSI calculations). The modular architecture makes the system maintainable and extensible.

## References

- [GitHub Repository](https://github.com/Mrnidhi/Data226_Lab2)
- [Apache Airflow Documentation](https://airflow.apache.org/)
- [dbt Documentation](https://docs.getdbt.com/)
- [Alpha Vantage API](https://www.alphavantage.co/documentation/)

---

**Course:** SJSU Data 226 – Data Warehousing
