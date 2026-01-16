from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowSkipException
import io # Added for cleaner JSON reading

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
    import pandas as pd

    tickers = get_liquid_options_tickers()
    all_options = []
    
    for ticker in tickers:
        try:
            # options_data is likely a DataFrame from your utility
            df_ticker = fetch_options_data(ticker)
            
            if df_ticker is not None and not df_ticker.empty:
                # FIX: Explicitly add the ticker column so validation passes
                df_ticker['ticker'] = ticker
                # FIX: Ensure option_type exists (if your fetcher doesn't provide it)
                if 'option_type' not in df_ticker.columns:
                    # Logic here depends on if your fetcher returns calls/puts mixed
                    # This is a placeholder; adjust based on your utility's output
                    df_ticker['option_type'] = 'unknown' 
                
                all_options.append(df_ticker)
        except Exception as e:
            print(f"Failed to fetch options for {ticker}: {e}")

    if all_options:
        combined = pd.concat(all_options, ignore_index=True)
        context['ti'].xcom_push(key='options_data', value=combined.to_json())
        return len(all_options)
    
    # Push None so validate task knows to skip
    context['ti'].xcom_push(key='options_data', value=None)
    return 0

def validate_options_data(**context):
    import pandas as pd
    from utils.great_expectations_suite import validate_options_data as ge_validate

    ti = context['ti']
    data_json = ti.xcom_pull(key='options_data', task_ids='fetch_options_chains')
    
    # GUARD: Prevent NoneType crash
    if not data_json:
        raise AirflowSkipException("No options data received. Skipping validation.")

    # FIX: Use StringIO to resolve the FutureWarning
    df = pd.read_json(io.StringIO(data_json))

    validation_results = ge_validate(df)

    if not validation_results['success']:
        from utils.notifications import send_slack_alert
        msg = f"Options Validation failed: {validation_results['errors']}"
        # Log it so you can see it in Airflow logs even if Slack fails
        print(msg) 
        send_slack_alert(msg)
        raise Exception("Option data quality check failed")
    
    # load to database
    from utils.db_utils import bulk_insert_options
    records = bulk_insert_options(df)

    return records

with DAG(
    'options_data_collection',
    default_args=default_args,
    description="Real-time options chain data collection",
    schedule_interval='*/15 9-16 * * 1-5',
    catchup=False,
    max_active_runs=1,
    tags=['options', 'realtime', 'market_hours']
) as dag:

    check_hours = BranchPythonOperator(
        task_id='check_market_hours',
        python_callable=check_market_hours,
    )

    skip = EmptyOperator(task_id='skip_collection')

    fetch = PythonOperator(
        task_id='fetch_options_chains',
        python_callable=fetch_options_chains
    )

    validate = PythonOperator(
        task_id='validate_options',
        python_callable=validate_options_data
    )

    check_hours >> [skip, fetch]
    fetch >> validate