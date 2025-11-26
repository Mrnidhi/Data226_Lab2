# Data 226 – Lab 2: Stock Analytics Pipeline

End-to-end data pipeline for stock market analysis using Alpha Vantage, Snowflake, dbt, Airflow, and Preset.

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
│   ├── dags/etl_pipeline.py    # Main DAG
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

## Pipeline Flow

### ETL Phase (Airflow)
1. Extract daily prices from Alpha Vantage for AAPL, MSFT, GOOGL
2. Load into Snowflake using MERGE (upsert)

### ELT Phase (dbt)
1. `stg_stock_prices` – staging view over raw data
2. `stock_technical_indicators` – marts table with:
   - Moving averages (SMA 5/10/20)
   - Price momentum & daily returns
   - RSI (14-period)
   - Trend signal (BULLISH/BEARISH)

### Data Quality (dbt test)
- Not null checks on symbol, date, close, volume
- Volume must be >= 0

### Historical Tracking (dbt snapshot)
- Captures changes to close and volume over time
- Stored in `SNAPSHOTS.STOCK_PRICES_SNAPSHOT`

## Output Schema

| Column | Description |
|--------|-------------|
| symbol | Stock ticker |
| date | Trading date |
| open, high, low, close | Price data |
| volume | Trading volume |
| prev_close | Previous day close |
| price_momentum | Dollar change |
| daily_return | Percentage change |
| sma_5, sma_10, sma_20 | Moving averages |
| rsi_14 | Relative Strength Index |
| trend_signal | BULLISH or BEARISH |

## How to Run

```bash
# Start containers
docker compose up -d

# Access Airflow UI at http://localhost:8081
# Username: airflow / Password: airflow

# Set these in Airflow:
# - Variable: ALPHA_VANTAGE_KEY
# - Connection: snowflake_conn

# Trigger DAG: stock_analytics_lab2
```

## Dashboard

Preset connects to Snowflake and visualizes:
- Price trends by symbol
- RSI indicators with overbought/oversold zones
- Moving average crossovers
- Daily returns distribution

---

**Course:** SJSU Data 226 – Data Warehousing
