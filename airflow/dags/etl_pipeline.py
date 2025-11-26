from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import pandas as pd
import time

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
    """Fetch stock data from Alpha Vantage and load into Snowflake."""
    
    api_key = Variable.get("ALPHA_VANTAGE_KEY")
    symbols = ["GOOGL", "MSFT", "AAPL"]
    data_frames = []
    
    for symbol in symbols:
        print(f"Fetching {symbol}...")
        try:
            url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}&outputsize=compact'
            r = requests.get(url)
            data = r.json()
            
            ts_data = data.get("Time Series (Daily)")
            if not ts_data:
                print(f"No data for {symbol}, skipping.")
                continue

            records = []
            for date_str, metrics in ts_data.items():
                records.append({
                    "symbol": symbol,
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
            print(f"Got {len(df)} rows for {symbol}")
            
            time.sleep(12)  # API rate limit
            
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
            raise

    if not data_frames:
        raise ValueError("No data fetched for any symbol.")
        
    final_df = pd.concat(data_frames).sort_values(['symbol', 'date'])
    print(f"Total rows: {len(final_df)}")

    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    target_table = "USER_DB_MAGPIE.RAW.STOCK_DATA"  
    temp_table = "USER_DB_MAGPIE.RAW.STOCK_DATA_TEMP"

    try:
        cursor.execute("BEGIN;")
        
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
        
        insert_sql = f"""
            INSERT INTO {temp_table} (symbol, date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        data_tuples = [
            (row['symbol'], row['date'].strftime('%Y-%m-%d'),
             float(row['open']), float(row['high']), float(row['low']),
             float(row['close']), int(row['volume']))
            for _, row in final_df.iterrows()
        ]
            
        cursor.executemany(insert_sql, data_tuples)
        
        merge_sql = f"""
            MERGE INTO {target_table} AS target
            USING {temp_table} AS source
            ON target.symbol = source.symbol AND target.date = source.date
            WHEN MATCHED THEN
                UPDATE SET
                    open = source.open, high = source.high, low = source.low,
                    close = source.close, volume = source.volume
            WHEN NOT MATCHED THEN
                INSERT (symbol, date, open, high, low, close, volume)
                VALUES (source.symbol, source.date, source.open, source.high, 
                        source.low, source.close, source.volume);
        """
        cursor.execute(merge_sql)
        
        cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
        total_count = cursor.fetchone()[0]
        print(f"Total records in table: {total_count}")
        
        cursor.execute("COMMIT;")
        
    except Exception as e:
        cursor.execute("ROLLBACK;")
        raise
    finally:
        cursor.close()
        conn.close()

with DAG(
    'stock_analytics_lab2',
    default_args=default_args,
    description='Stock Analytics Pipeline: Alpha Vantage -> Snowflake -> dbt',
    schedule_interval='@daily', 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['lab2', 'snowflake', 'dbt'],
) as dag:

    extract_load_raw_data = PythonOperator(
        task_id='extract_load_raw_data',
        python_callable=load_raw_stock_data,
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --full-refresh",
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test",
    )

    dbt_snapshot = BashOperator(
        task_id='dbt_snapshot',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt snapshot",
    )

    extract_load_raw_data >> dbt_run >> dbt_test >> dbt_snapshot
