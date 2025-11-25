from airflow.utils.dates import days_ago
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import logging
from airflow.decorators import dag, task

@dag(schedule_interval="@daily", start_date=days_ago(1), catchup=False)
def forecasting_dag():
    def return_snowflake_conn():
        # Establish a connection to Snowflake
        hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        conn = hook.get_conn()
        return conn.cursor()

    @task()
    def train(train_input_table, train_view, forecast_function_name):
        # Create a training view and forecast model using Snowflake ML
        cur = return_snowflake_conn()
        create_view_sql = f"""
        CREATE OR REPLACE VIEW {train_view} AS 
        SELECT SYMBOL, CLOSE, CAST(DATE AS TIMESTAMP_NTZ) AS DATE
        FROM {train_input_table};
        """
        create_model_sql = f"""
        CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
            INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
            SERIES_COLNAME => 'SYMBOL',
            TIMESTAMP_COLNAME => 'DATE',
            TARGET_COLNAME => 'CLOSE',
            CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
        );
        """
        try:
            cur.execute(create_view_sql)
            cur.execute(create_model_sql)
            cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
        except Exception as e:
            logging.error(f"Error in training process: {e}")
            raise
        finally:
            cur.close()

    @task()
    def predict(forecast_function_name, train_input_table, forecast_table, final_table):
        # Generate predictions and store results in a final table with union operation
        cur = return_snowflake_conn()
        make_prediction_sql = f"""
        BEGIN
            CALL {forecast_function_name}!FORECAST(
                FORECASTING_PERIODS => 7,
                CONFIG_OBJECT => {{'prediction_interval': 0.95}}
            );
            LET x := SQLID;
            CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
        END;
        """
        create_final_table_sql = f"""
        CREATE OR REPLACE TABLE {final_table} AS
        SELECT SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
        FROM {train_input_table}
        UNION ALL
        SELECT replace(series, '"', '') as SYMBOL, ts as DATE, NULL AS actual, forecast, lower_bound, upper_bound
        FROM {forecast_table};
        """
        try:
            cur.execute(make_prediction_sql)
            cur.execute(create_final_table_sql)
        except Exception as e:
            logging.error(f"Error in prediction process: {e}")
            raise
        finally:
            cur.close()

    train_input_table = "USER_DB_MAGPIE.STOCK_PREDICTOR.stock_data"
    train_view = "USER_DB_MAGPIE.STOCK_PREDICTOR.stock_data_view"
    forecast_table = "USER_DB_MAGPIE.STOCK_PREDICTOR.stock_data_forecast"
    forecast_function_name = "USER_DB_MAGPIE.STOCK_PREDICTOR.predict_stock_price"
    final_table = "USER_DB_MAGPIE.STOCK_PREDICTOR.predicted_stock_data"

    train_task = train(train_input_table, train_view, forecast_function_name)
    predict_task = predict(forecast_function_name, train_input_table, forecast_table, final_table)
    
    # Set task dependencies: predict should run after train completes
    train_task >> predict_task

dag_instance = forecasting_dag()
