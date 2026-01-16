from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
import pandas as pd
import logging

# Assuming these are in your PYTHONPATH
from utils.market_calendar import is_market_open_today
from utils.data_fetchers import fetch_ohclv_batch
from utils.db_utils import bulk_insert_ohclv, insert_metrics
from utils.redis_cache import cache_latest_prices
from utils.notifications import send_slack_alert

default_args = {
    'owner': 'quant_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
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

    batch_size = 50
    all_data = []
    failed_tickers = []

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        try:
            data = fetch_ohclv_batch(batch, period='1d') # Changed 'id' to '1d' assuming standard yfinance
            if not data.empty:
                all_data.append(data)
        except Exception as e:
            failed_tickers.extend(batch)
            logging.error(f"Batch {i//batch_size} failed: {e}")
    
    # Always push something, even if empty, to prevent NoneType errors downstream
    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        ti.xcom_push(key='market_data', value=combined_data.to_json())
    else:
        ti.xcom_push(key='market_data', value=None)

    ti.xcom_push(key='failed_tickers', value=failed_tickers)
    
    if failed_tickers:
        send_slack_alert(f"Warning: {len(failed_tickers)} tickers failed to fetch")
    
    return len(all_data)

def validate_and_load_data(**context):
    from utils.great_expectations_suite import validate_ohclv_data
    ti = context['ti']
    
    # Pull data
    data_json = ti.xcom_pull(key='market_data', task_ids='fetch_market_data')
    
    # GUARD: Check if data_json is None or empty
    if data_json is None:
        logging.warning("No data found in XCom 'market_data'. Skipping load.")
        raise AirflowSkipException("No data to validate/load.")

    df = pd.read_json(data_json)
    
    if df.empty:
        raise AirflowSkipException("DataFrame is empty. Skipping.")

    validation_results = validate_ohclv_data(df)
    if not validation_results['success']:
        send_slack_alert(f"Data validation failed: {validation_results['errors']}")
        raise Exception("Data quality check failed.")
    
    records_inserted = bulk_insert_ohclv(df)
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