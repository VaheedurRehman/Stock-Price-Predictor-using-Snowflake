from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import os

default_args = {
    'owner': 'vaheed',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

DATA_DIR = '/opt/airflow/dags/data'
os.makedirs(DATA_DIR, exist_ok=True)

# ---------------- HELPER FUNCTIONS ----------------

def extract_stock_data(**context):
    symbols = ['AAPL', 'NVDA']  # ✅ two companies as per rubric
    file_paths = []
    end_date = datetime.today().date()
    start_date = end_date - timedelta(days=180)

    for symbol in symbols:
        print(f"📈 Fetching data for {symbol} from {start_date} to {end_date}")
        df = yf.download(symbol, start=start_date, end=end_date)
        df.reset_index(inplace=True)
        df.rename(columns={
            'Date': 'TRADE_DATE',
            'Open': 'OPEN',
            'High': 'HIGH',
            'Low': 'LOW',
            'Close': 'CLOSE',
            'Volume': 'VOLUME'
        }, inplace=True)
        df['SYMBOL'] = symbol
        df = df[['SYMBOL', 'TRADE_DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']]

        path = os.path.join(DATA_DIR, f'{symbol}_raw.csv')
        df.to_csv(path, index=False)
        file_paths.append(path)
        print(f"✅ Saved {symbol} data → {path}")

    context['ti'].xcom_push(key='raw_files', value=file_paths)


def transform_and_combine_data(**context):
    file_paths = context['ti'].xcom_pull(key='raw_files')
    combined_df = pd.concat([pd.read_csv(p) for p in file_paths], ignore_index=True)
    combined_df.dropna(inplace=True)
    combined_df.drop_duplicates(inplace=True)

    clean_path = os.path.join(DATA_DIR, 'combined_cleaned.csv')
    combined_df.to_csv(clean_path, index=False)
    print(f"✅ Combined clean data → {clean_path}")

    context['ti'].xcom_push(key='cleaned_path', value=clean_path)


def load_to_snowflake(**context):
    clean_path = context['ti'].xcom_pull(key='cleaned_path')
    df = pd.read_csv(clean_path)

    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute("CREATE OR REPLACE DATABASE STOCK_DB;")
    cur.execute("CREATE OR REPLACE SCHEMA RAW;")
    cur.execute("""
        CREATE OR REPLACE TABLE RAW.STOCK_PRICES (
            SYMBOL STRING,
            TRADE_DATE TIMESTAMP,
            OPEN FLOAT,
            HIGH FLOAT,
            LOW FLOAT,
            CLOSE FLOAT,
            VOLUME FLOAT
        );
    """)

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO RAW.STOCK_PRICES (SYMBOL, TRADE_DATE, OPEN, HIGH, LOW, CLOSE, VOLUME)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            row['SYMBOL'],
            row['TRADE_DATE'],
            float(row['OPEN']),
            float(row['HIGH']),
            float(row['LOW']),
            float(row['CLOSE']),
            float(row['VOLUME'])
        ))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Loaded {len(df)} rows into Snowflake → RAW.STOCK_PRICES")


# ---------------- DAG DEFINITION ----------------

with DAG(
    dag_id='fetch_stock_price_dag_v4',
    default_args=default_args,
    description='Fetch and load multiple stock prices into Snowflake',
    schedule_interval='@daily',
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=['stock', 'multi-company', 'ETL']
) as dag:

    extract_task = PythonOperator(
        task_id='extract_stock_data',
        python_callable=extract_stock_data
    )

    transform_task = PythonOperator(
        task_id='transform_and_combine_data',
        python_callable=transform_and_combine_data
    )

    load_task = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake
    )

    extract_task >> transform_task >> load_task
