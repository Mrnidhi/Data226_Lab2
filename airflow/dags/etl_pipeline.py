from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import pandas as pd
import time

# --- Configuration ---
DBT_PROJECT_DIR = "/opt/airflow/dbt/stock_analytics"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

def load_raw_stock_data(**context):
    """
    Fetches stock data from Alpha Vantage and loads it into Snowflake.
    Uses SnowflakeHook for secure connection management.
    """
    print("--- Step 1: Extract from Alpha Vantage ---")
    
    api_key = Variable.get("ALPHA_VANTAGE_KEY")
    symbols = ["GOOGL", "MSFT", "AAPL"]
    data_frames = []
    
    for symbol in symbols:
        print(f"Fetching data for {symbol}...")
        try:
            url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}&outputsize=compact'
            r = requests.get(url)
            data = r.json()
            
            # Parse the JSON response
            ts_data = data.get("Time Series (Daily)")
            if not ts_data:
                print(f"WARNING: No data found for {symbol}. Skipping.")
                continue

            # Transform to list of dictionaries
            records = []
            for date_str, metrics in ts_data.items():
                records.append({
                    "symbol": symbol,  # Changed to lowercase for consistency
                    "date": date_str,
                    "open": float(metrics["1. open"]),
                    "high": float(metrics["2. high"]),
                    "low": float(metrics["3. low"]),
                    "close": float(metrics["4. close"]),
                    "volume": int(metrics["5. volume"])
                })
            
            df = pd.DataFrame(records)
            df['date'] = pd.to_datetime(df['date'])
            data_frames.append(df)
            
            print(f"Successfully fetched {len(df)} rows for {symbol}")
            
            # Respect API rate limits (5 calls/min for free tier)
            time.sleep(12)
            
        except Exception as e:
            print(f"ERROR fetching {symbol}: {str(e)}")
            raise e 

    if not data_frames:
        raise ValueError("ETL ERROR: No data fetched for ANY symbol. Failing task.")
        
    final_df = pd.concat(data_frames).sort_values(['symbol', 'date'])
    print(f"Total rows to load: {len(final_df)}")

    print("--- Step 2: Load to Snowflake ---")
    
    # Use the Airflow Connection (snowflake_conn)
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    target_table = "USER_DB_MAGPIE.RAW.STOCK_DATA"  
    temp_table = "USER_DB_MAGPIE.RAW.STOCK_DATA_TEMP"

    try:
        cursor.execute("BEGIN;")
        
        # 1. Create Target Table (Idempotent) - Enhanced schema
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                symbol VARCHAR(10) NOT NULL,
                date DATE NOT NULL,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume BIGINT,
                loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                PRIMARY KEY (symbol, date)
            );
        """)
        
        # 2. Create Temporary Staging Table
        cursor.execute(f"""
            CREATE OR REPLACE TEMPORARY TABLE {temp_table} (
                symbol VARCHAR(10),
                date DATE,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume BIGINT
            );
        """)
        
        # 3. Bulk Insert into Temp Table
        print(f"Staging {len(final_df)} rows...")
        insert_sql = f"""
            INSERT INTO {temp_table} (symbol, date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        data_tuples = []
        for _, row in final_df.iterrows():
            data_tuples.append((
                row['symbol'],
                row['date'].strftime('%Y-%m-%d'),
                float(row['open']),
                float(row['high']),
                float(row['low']),
                float(row['close']),
                int(row['volume'])
            ))
            
        cursor.executemany(insert_sql, data_tuples)
        print(f"Staged {len(data_tuples)} rows successfully")
        
        # 4. MERGE (Upsert) from Temp to Target
        print("Merging data into target table...")
        merge_sql = f"""
            MERGE INTO {target_table} AS target
            USING {temp_table} AS source
            ON target.symbol = source.symbol AND target.date = source.date
            WHEN MATCHED THEN
                UPDATE SET
                    open = source.open,
                    high = source.high,
                    low = source.low,
                    close = source.close,
                    volume = source.volume
            WHEN NOT MATCHED THEN
                INSERT (symbol, date, open, high, low, close, volume)
                VALUES (source.symbol, source.date, source.open, source.high, 
                        source.low, source.close, source.volume);
        """
        cursor.execute(merge_sql)
        
        # 5. Log the operation
        cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
        total_count = cursor.fetchone()[0]
        print(f"Total records in {target_table}: {total_count}")
        
        cursor.execute("COMMIT;")
        print("Transaction Committed. ETL Load Complete.")
        
    except Exception as e:
        cursor.execute("ROLLBACK;")
        print("Transaction Rolled Back due to error.")
        raise e
    finally:
        cursor.close()
        conn.close()

with DAG(
    'stock_analytics_lab2',
    default_args=default_args,
    description='Lab 2: End-to-End Stock Analytics Pipeline - Alpha Vantage -> Snowflake -> dbt -> BI',
    schedule_interval='@daily', 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['lab2', 'snowflake', 'dbt', 'stock-analysis'],
    doc_md="""
    # Stock Analytics Pipeline (Lab 2)
    
    ## Overview
    End-to-end data pipeline for stock price analysis and prediction.
    
    ## Pipeline Flow
    1. **ETL**: Extract stock data from Alpha Vantage API → Load to Snowflake raw table
    2. **ELT**: dbt transforms raw data into analytical models with technical indicators
    3. **Testing**: dbt data quality tests ensure model reliability
    4. **Snapshots**: dbt captures historical changes for trend analysis
    
    ## Tables Created
    - `STOCK_DATA_RAW`: Raw API data from Alpha Vantage
    - dbt models: Technical indicators, moving averages, trend analysis
    """
) as dag:

    # Task 1: Extract & Load (ETL Phase)
    extract_load_raw_data = PythonOperator(
        task_id='extract_load_raw_data',
        python_callable=load_raw_stock_data,
        doc_md="Extracts stock data from Alpha Vantage API and loads to Snowflake raw table"
    )

    # Task 2: dbt Run (ELT Phase)
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --full-refresh",
        doc_md="Runs dbt models to transform raw data into analytical models with technical indicators"
    )

    # Task 3: dbt Test (Data Quality)
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test",
        doc_md="Executes dbt data quality tests to ensure model reliability and data integrity"
    )

    # Task 4: dbt Snapshot (Historical Tracking)
    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt snapshot",
        doc_md="Creates dbt snapshots to track historical changes in key dimensions"
    )

    # Orchestration - Clear dependency chain
    extract_load_raw_data >> dbt_run >> dbt_test >> dbt_snapshot