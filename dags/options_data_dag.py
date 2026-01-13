from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator


default_args = {
    'owner': 'quant_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

def check_market_hours(**context):
    from utils.market_calendar import is_market_open_now

    if is_market_open_now():
        return 'fetch_options_chains'
    return 'skip_collection'

def fetch_options_chains(**context):
    from utils.data_fetchers import fetch_options_data
    from utils.stock_lists import get_liquid_options_tickers

    tickers = get_liquid_options_tickers()

    all_options = []
    for ticker in tickers:
        try:
            options_data = fetch_options_data(ticker)
            all_options.append(options_data)
        except Exception as e:
            print(f"Failed to fetch options for {ticker}: {e}")

    if all_options:
        import pandas as pd
        combined = pd.concat(all_options, ignore_index=True)
        context['ti'].xcom_push(key='options_data', value=combined.to_json())
        return len(all_options)
    return 0

def validate_options_data(**context):
    import pandas as pd
    from utils.great_expectations_suite import validate_options_data

    ti = context['ti']
    data_json = ti.xcom_pull(key='options_data', task_ids='fetch_options_chains')
    df = pd.read_json(data_json)

    validation_results = validate_options_data(df)

    if not validation_results['success']:
        from utils.notifications import send_slack_alert
        send_slack_alert(f"Options Validation failed: {validation_results['errors']}")
        raise Exception("Option data quality check failed")
    
    # load to database
    from utils.db_utils import bulk_insert_options
    records = bulk_insert_options(df)

    return records

with DAG(
    'options_data_collection',
    default_args=default_args,
    description="Real-time options chain data collection",
    schedule_interval='*/15 9-16 * * 1-5', # Every 15 min, 9 AM - 4 PM ET
    catchup = False,
    max_active_runs=1,
    tags=['options', 'realtime', 'market_hours']
) as dag:
    check_hours = BranchPythonOperator(
        task_id = 'check_market_hours',
        python_callable=check_market_hours,
    )

    skip = EmptyOperator(task_id='skip_collection')

    fetch = PythonOperator(
        task_id = 'validate_options',
        python_callable=validate_options_data,
    )

    validate = PythonOperator(
        task_id = 'validate_options',
        python_callable = validate_options_data
    )

    check_hours >> [skip, fetch]
    fetch >> validate





    
    