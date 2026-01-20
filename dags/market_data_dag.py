from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
import pandas as pd
import logging
import os

# Assuming these are in your PYTHONPATH
from utils.market_calendar import is_market_open_today
from utils.data_fetchers import fetch_alpha_vantage_batch
from utils.db_utils import bulk_insert_ohclv, insert_metrics
from utils.redis_cache import cache_latest_prices
from utils.notifications import send_slack_alert

default_args = {
    'owner': 'quant_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,  # Reduced retries since Alpha Vantage has rate limits
    'retry_delay': timedelta(minutes=5),
}

def check_market_holiday(**context):
    if not is_market_open_today():
        send_slack_alert("Market is closed today. Skipping data collection.")
        raise AirflowSkipException("Market Holiday - No data to collect.")
    return True

def fetch_stock_universe(**context):
    from utils.stock_lists import get_sp500_tickers, get_additional_liquid_stocks
    tickers = get_sp500_tickers() + get_additional_liquid_stocks()
    # Pushing to XCom explicitly
    context['ti'].xcom_push(key='ticker_list', value=tickers)
    return len(tickers)

def fetch_market_data_batch(**context):
    ti = context['ti']
    tickers = ti.xcom_pull(key='ticker_list', task_ids='fetch_stock_universe')
    
    if not tickers:
        raise ValueError("No tickers found in XCom from fetch_stock_universe")

    logging.info(f"Fetching market data for {len(tickers)} tickers from Alpha Vantage...")
    
    # Get API key from environment
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    if not api_key:
        raise ValueError("ALPHA_VANTAGE_API_KEY not set in environment")
    
    try:
        # Use the Alpha Vantage batch fetcher
        data = fetch_alpha_vantage_batch(tickers, api_key)
        
        if data.empty:
            logging.warning("No market data returned. Will proceed with empty dataset.")
            ti.xcom_push(key='market_data', value='{}')
            return 0
        
        logging.info(f"Successfully fetched {len(data)} records for {data['ticker'].nunique()} tickers")
        ti.xcom_push(key='market_data', value=data.to_json())
        return len(data)
        
    except Exception as e:
        logging.error(f"Failed to fetch market data: {e}")
        ti.xcom_push(key='market_data', value='{}')
        return 0

def validate_and_load_data(**context):
    from utils.great_expectations_suite import validate_ohclv_data
    ti = context['ti']
    
    # Pull data
    data_json = ti.xcom_pull(key='market_data', task_ids='fetch_market_data')
    
    # GUARD: Check if data_json is None or empty
    if data_json is None or data_json == '{}':
        logging.warning("No data found in XCom 'market_data'. Skipping load.")
        raise AirflowSkipException("No data to validate/load.")

    try:
        df = pd.read_json(data_json)
    except:
        logging.warning("Could not parse market data JSON. Skipping load.")
        raise AirflowSkipException("Invalid data format.")
    
    if df.empty:
        logging.warning("DataFrame is empty. Skipping load.")
        raise AirflowSkipException("Empty DataFrame.")

    # Validate data quality
    validation_results = validate_ohclv_data(df)
    if not validation_results['success']:
        logging.warning(f"Data validation failed: {validation_results['errors']}")
        send_slack_alert(f"Data validation failed: {validation_results['errors']}")
        # Don't fail the DAG on validation errors, just skip insert
        raise AirflowSkipException("Data quality check failed.")
    
    # Insert into database
    records_inserted = bulk_insert_ohclv(df)
    logging.info(f"Inserted {records_inserted} records into database")
    
    # Update cache
    cache_latest_prices(df)
    
    ti.xcom_push(key='records_inserted', value=records_inserted)
    return records_inserted

def generate_daily_metrics(**context):
    from utils.metrics import calculate_daily_metrics
    ti = context['ti']
    
    # Pull record count to ensure previous step worked
    records = ti.xcom_pull(key='records_inserted', task_ids='validate_and_load')
    
    if not records or records == 0:
        logging.info("No records were inserted. Metrics might be zero.")

    # Note: Ensure you fixed the SQL typo in utils/metrics.py: 
    # Change COUNT(* FROM ... to COUNT(*) FROM ...
    metrics = calculate_daily_metrics(context['ds'])
    insert_metrics(metrics)
    return metrics

with DAG(
    'market_data_collection',
    default_args=default_args,
    schedule_interval='15 16 * * 1-5',
    catchup=False,
    max_active_runs=1,
    tags=['market_data', 'daily', 'critical']
) as dag:
    
    check_holiday = PythonOperator(
        task_id='check_market_holiday',
        python_callable=check_market_holiday
    )
    
    get_tickers = PythonOperator(
        task_id='fetch_stock_universe',
        python_callable=fetch_stock_universe
    )

    fetch_data = PythonOperator(
        task_id='fetch_market_data',
        python_callable=fetch_market_data_batch,
        execution_timeout=timedelta(minutes=20)
    )

    validate_load = PythonOperator(
        task_id='validate_and_load',
        python_callable=validate_and_load_data
    )

    metrics = PythonOperator(
        task_id='generate_daily_metrics',
        python_callable=generate_daily_metrics
    )

    # Simplified dependency flow
    check_holiday >> get_tickers >> fetch_data >> validate_load >> metrics