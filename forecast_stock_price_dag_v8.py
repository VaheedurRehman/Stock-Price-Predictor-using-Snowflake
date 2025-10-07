from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta

# ------------------------------
# CONFIGURATION
# ------------------------------
default_args = {
    'owner': 'vaheed',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

SNOWFLAKE_CONN_ID = 'snowflake_conn'
DB_NAME = 'STOCK_DB'
RAW_SCHEMA = 'RAW'
ANALYTICS_SCHEMA = 'ANALYTICS'
FORECAST_MODEL = 'LAB1_STOCK_FORECAST'
FORECAST_TABLE = 'STOCK_FORECAST'
FINAL_VIEW = 'STOCK_FORECAST_UNION_VIEW'

# ------------------------------
# PYTHON FUNCTIONS
# ------------------------------
def read_from_snowflake():
    """Check if base stock data exists before forecasting."""
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    df = hook.get_pandas_df(f"SELECT COUNT(*) AS total FROM {RAW_SCHEMA}.STOCK_PRICES;")
    print(f"✅ Found {df.iloc[0,0]} records in {RAW_SCHEMA}.STOCK_PRICES.")


def forecast_stock_prices():
    """Create and run Snowflake ML Forecast inside Snowflake."""
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        sql_script = f"""
        USE ROLE ACCOUNTADMIN;
        USE WAREHOUSE COMPUTE_WH;
        USE DATABASE {DB_NAME};
        USE SCHEMA {RAW_SCHEMA};

        -- Step 1: Create a clean training view
        CREATE OR REPLACE VIEW {RAW_SCHEMA}.STOCK_PRICES_V1 AS
        SELECT 
            TO_TIMESTAMP_NTZ(TRADE_DATE) AS DATE_V1,
            CLOSE,
            SYMBOL
        FROM {RAW_SCHEMA}.STOCK_PRICES
        WHERE CLOSE IS NOT NULL;

        -- Step 2: Train model using Snowflake ML
        CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {FORECAST_MODEL}(
            INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{RAW_SCHEMA}.STOCK_PRICES_V1'),
            SERIES_COLNAME => 'SYMBOL',
            TIMESTAMP_COLNAME => 'DATE_V1',
            TARGET_COLNAME => 'CLOSE',
            CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
        );

        -- Step 3: Generate predictions
        BEGIN
            CALL {FORECAST_MODEL}!FORECAST(
                FORECASTING_PERIODS => 7,
                CONFIG_OBJECT => {{ 'prediction_interval': 0.95 }}
            );
            LET f_result := SQLID;
            CREATE OR REPLACE TABLE {ANALYTICS_SCHEMA}.{FORECAST_TABLE} AS
            SELECT 
                REPLACE(SERIES, '"', '') AS SYMBOL,
                TS AS TRADE_DATE,
                FORECAST AS FORECAST_CLOSE,
                UPPER_BOUND AS FORECAST_UPPER,
                LOWER_BOUND AS FORECAST_LOWER
            FROM TABLE(RESULT_SCAN(:f_result));
        END;
        """
        print("🚀 Running Snowflake ML forecast pipeline...")
        for statement in sql_script.split(";"):
            stmt = statement.strip()
            if stmt:
                cur.execute(stmt)
        conn.commit()
        print(f"✅ Forecast table created: {ANALYTICS_SCHEMA}.{FORECAST_TABLE}")

    except Exception as e:
        conn.rollback()
        print(f"❌ Error during Snowflake ML Forecast: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def write_forecast_to_snowflake():
    """Create a final view combining actuals and forecast results."""
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        union_sql = f"""
        USE DATABASE {DB_NAME};
        USE SCHEMA {ANALYTICS_SCHEMA};

        CREATE OR REPLACE VIEW {ANALYTICS_SCHEMA}.{FINAL_VIEW} AS
        SELECT 
            SYMBOL,
            TRADE_DATE,
            CLOSE AS ACTUAL,
            NULL AS FORECAST_CLOSE,
            NULL AS FORECAST_UPPER,
            NULL AS FORECAST_LOWER,
            'ACTUAL' AS SOURCE
        FROM {RAW_SCHEMA}.STOCK_PRICES
        UNION ALL
        SELECT 
            SYMBOL,
            TRADE_DATE,
            NULL AS ACTUAL,
            FORECAST_CLOSE,
            FORECAST_UPPER,
            FORECAST_LOWER,
            'FORECAST' AS SOURCE
        FROM {ANALYTICS_SCHEMA}.{FORECAST_TABLE};
        """

        cur.execute(union_sql)
        conn.commit()
        print(f"✅ Combined view {FINAL_VIEW} created successfully in {ANALYTICS_SCHEMA}.")

    except Exception as e:
        conn.rollback()
        print(f"❌ Error creating final union view: {e}")
        raise
    finally:
        cur.close()
        conn.close()

# ------------------------------
# DAG DEFINITION
# ------------------------------
with DAG(
    dag_id='forecast_stock_price_dag_v9',
    default_args=default_args,
    description='Forecast stock prices using Snowflake ML (3-task DAG)',
    schedule_interval='@daily',
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=['snowflake', 'forecast', 'lab1']
) as dag:

    read_data = PythonOperator(
        task_id='read_from_snowflake',
        python_callable=read_from_snowflake
    )

    forecast_data = PythonOperator(
        task_id='forecast_stock_prices',
        python_callable=forecast_stock_prices
    )

    write_data = PythonOperator(
        task_id='write_forecast_to_snowflake',
        python_callable=write_forecast_to_snowflake
    )

    read_data >> forecast_data >> write_data
