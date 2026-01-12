from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk.definitions.taskgroup import TaskGroup
from airflow.exceptions import AirflowSkipException
import pandas as pd
import yfinance as yf
from utils.market_calendar import is_market_open_today, get_next_trading_day
from utils.data_fetchers import fetch_ohlcv_batch
from utils.db_utils import bulk_insert_ohclv
from utils.redis_cache import cache_latest_prices
from utils.notifications import send_slack_alert


default_args = {
    'owner': 'quant_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 1 , 7),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30)
}


def check_market_holiday(**contet):
    if not is_market_open_today():
        send_slack_alert("Market is closed today. Skipping data collection.")
        raise AirflowSkipException("Market Holiday - No data to collect.")
    return True


def fetch_stock_universe(**context):
    from utils.stock_lists import get_sp500_tickers, get_additional_liquid_stocks
    tickers = get_sp500_tickers() + get_additional_liquid_stocks()
    context['ti'].xcom_push(key='ticker_list', value=tickers)  # pushes the list of tickers into the airflow database so the next task can pull it

    return len(tickers)

def fetch_market_data_batch(**context):
    ti = context['ti']
    tickers = ti.xcom_pull(key='ticker_list', task_ids = 'fetch_stock_universe')
    batch_size = 50

    all_data = []
    failed_tickers = []

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        try:
            data = fetch_ohlcv_batch(batch, period='id')
            all_data.append(data)
        except Exception as e:
            failed_tickers.extend(batch)
            print(f"Batch {i//batch_size} failed: {e}")
    
    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        ti.xcom_push(key='market_data', value=combined_data.to_json())
        ti.xcom_push(key='failed_tickers', value=failed_tickers)

    if failed_tickers:
        send_slack_alert(f"Warning: {len(failed_tickers)} tickers failed to fetch")
    
    return len(all_data)

def validate_and_load_data(**context):
    from utils.great_expectations_suite import validate_ohclv_data

    ti = context['ti']
    data_json = ti.xcom_pull(key='market_data', task_ids='fetch_market_data')
    df = pd.read_json(data_json)

    validation_results = validate_ohclv_data(df)

    if not validation_results['success']:
        send_slack_alert(f"Data validation failed: {validation_results['errors']}")
        raise Exception("Data quality check failed.")
    
    records_inserted = bulk_insert_ohclv(df)

    cache_latest_prices(df)

    ti.xcom_push(key='records_inserted', value=records_inserted)

    return records_inserted

def retry_failed_tickers(**context):
    ti = context['ti']

    failed_tickers = ti.xcom_pull(key='failed_tickers', task_ids='fetch_market_data')

    if not failed_tickers:
        return 0
    
    retry_data = []
    still_failed = []

    for ticker in failed_tickers:
        try:
            data = fetch_ohclv_batch([ticker], period='id')
            retry_data.append(data)
        except:
            still_failed.append(ticker)

    if retry_data:
        combined = pd.concat(retry_data, ignore_index=True)
        bulk_insert_ohclv(combined)

    if still_failed:
        send_slack_alert(f"Permanent failures for: {', '.join(still_failed)}")

    return len(retry_data)

def generate_daily_metrics(**context):
    from utils.metrics import calculate_daily_metrics

    ti = context['ti']
    records = ti.xcom_pull(key='records_inserted', task_ids='validate_and_load')

    metrics = calculate_daily_metrics(context['ds'])

    from utils.db_utils import insert_metrics
    insert_metrics(metrics)
    return metrics


with DAG(
    'market_data_collection',
    default_args = default_args,
    description = 'Daily collection of OHCLV data for 500+ stocks',
    schedule_interval = '15 16 * * 1-5',
    catchup = False,
    max_active_runs=1,
    tags=['market_data', 'daily', 'critical']
) as dag:
    
    check_holiday = PythonOperator(
        task_id='check_market_holiday',
        python_callable=check_market_holiday
    )
    
    get_tickers = PythonOperator(
        task_id = 'fetch_stock_universe',
        python_callable=fetch_stock_universe
    )

    fetch_data = PythonOperator(
        task_id = 'fetch_market_data',
        python_callable=fetch_market_data_batch,
        execution_timeout=timedelta(minutes=20)
    )

    validate_load = PythonOperator(
        task_id = 'validate_and_load',
        python_callable=validate_and_load_data
    )

    retry_failures = PythonOperator(
        task_id = 'rettry_failed_tickers',
        python_callable=retry_failed_tickers,
        trigger_rule='all_done'
    )

    metrics = PythonOperator(
        task_id='generate_daily_metrics',
        python_callable=generate_daily_metrics
    )


    check_holiday >> get_tickers >> fetch_data >> validate_load >> retry_failures >> metrics  



    











