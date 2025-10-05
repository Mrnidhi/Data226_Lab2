
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
import snowflake.connector
import requests
import logging

default_args = {
    'owner': 'srinidhigowda',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def return_snowflake_conn():
    conn = snowflake.connector.connect(
        user=Variable.get("snowflake_userid"),
        password=Variable.get("snowflake_password"),
        account=Variable.get("snowflake_account"),
        warehouse=Variable.get("snowflake_warehouse"),
        database=Variable.get("snowflake_database"),
        schema='RAW'
    )
    return conn.cursor()

with DAG(
    'stock_data_pipeline',
    default_args=default_args,
    description='Fetch AAPL stock data from Alpha Vantage and load to Snowflake',
    schedule_interval='@once',
    catchup=False,
    tags=['stock', 'alpha_vantage', 'snowflake', 'homework'],
) as dag:

    @task
    def fetch_stock_data():
        symbol = "AAPL"
        
        api_key = Variable.get("alpha_vantage_key")
        
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}&outputsize=compact'
        
        logging.info(f"Fetching data for symbol: {symbol}")
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            if "Time Series (Daily)" not in data:
                raise ValueError(f"Invalid response format: {data}")
            
            results = []
            dates = list(data["Time Series (Daily)"].keys())
            dates.sort(reverse=True)
            
            for date in dates[:90]:
                day_data = data["Time Series (Daily)"][date]
                day_data['date'] = date
                day_data['symbol'] = symbol
                results.append(day_data)
            
            logging.info(f"Successfully fetched {len(results)} records for {symbol}")
            return results
            
        except Exception as e:
            logging.error(f"Error fetching stock data: {str(e)}")
            raise

    @task
    def create_and_load_table(stock_data):
        symbol = "AAPL"
        target_table = "raw.stock_data"
        
        cur = return_snowflake_conn()
        try:
            cur.execute("BEGIN;")
            
            create_sql = f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                symbol VARCHAR(10),
                date DATE,
                open FLOAT,
                close FLOAT,
                high FLOAT,
                low FLOAT,
                volume NUMBER,
                PRIMARY KEY (symbol, date)
            )
            """
            cur.execute(create_sql)
            print("Table created/verified")
            
            cur.execute(f"DELETE FROM {target_table} WHERE symbol = '{symbol}'")
            print(f"Deleted existing records for {symbol}")
            
            for record in stock_data:
                date = record['date']
                open_price = record['1. open']
                high = record['2. high']
                low = record['3. low']
                close = record['4. close']
                volume = record['5. volume']
                
                sql = f"""
                INSERT INTO {target_table} (symbol, date, open, high, low, close, volume)
                VALUES ('{symbol}', '{date}', {open_price}, {high}, {low}, {close}, {volume})
                """
                cur.execute(sql)
                print(f"Inserted record for {symbol} on {date}")
            
            cur.execute("COMMIT;")
            print("Transaction committed successfully")
            
        except Exception as e:
            cur.execute("ROLLBACK;")
            print(f"Error occurred: {e}")
            raise e

    @task
    def verify_load():
        symbol = "AAPL"
        target_table = "raw.stock_data"
        
        cur = return_snowflake_conn()
        try:
            cur.execute(f"SELECT COUNT(*) FROM {target_table} WHERE symbol = '{symbol}'")
            count = cur.fetchone()[0]
            
            cur.execute(f"SELECT MAX(date) FROM {target_table} WHERE symbol = '{symbol}'")
            latest_date = cur.fetchone()[0]
            
            print(f"Verification complete: {count} records for {symbol}, latest date: {latest_date}")
            return f"Verification: {count} records, latest date: {latest_date}"
            
        except Exception as e:
            print(f"Error in verification: {e}")
            raise e

    stock_data = fetch_stock_data()
    load_result = create_and_load_table(stock_data)
    verification = verify_load()

    load_result >> verification
