import io
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'quant_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

def fetch_corporate_actions(**context):
    from utils.data_fetchers import fetch_corporate_actions_batch
    from utils.stock_lists import get_sp500_tickers

    tickers = get_sp500_tickers()
    # Batch fetcher should return a combined DF with a 'ticker' column
    actions = fetch_corporate_actions_batch(tickers)

    if actions is None or actions.empty:
        print("No actions found for current batch.")
        return 0

    # Pushing to XCom as JSON
    context['ti'].xcom_push(key='corporate_actions', value=actions.to_json())
    return len(actions)

def process_dividends(**context):
    from utils.db_utils import bulk_insert_dividends

    ti = context['ti']
    actions_json = ti.xcom_pull(key='corporate_actions', task_ids='fetch_actions')
    
    if not actions_json:
        return "No data to process"

    # FIX: Wrap in StringIO to solve the literal JSON warning
    df = pd.read_json(io.StringIO(actions_json))

    # FIX: Map 'Dividends' column to 'action_type' logic
    if 'Dividends' in df.columns:
        # Filter for rows that actually have a dividend payment
        dividends = df[df['Dividends'] > 0].copy()
        dividends['action_type'] = 'dividend'
        
        # Ensure 'ex_date' is a column if it was the index
        if dividends.index.name == 'Date' or 'ex_date' not in dividends.columns:
            dividends = dividends.reset_index().rename(columns={'index': 'ex_date', 'Date': 'ex_date'})
    else:
        print("Required column 'Dividends' not found.")
        return 0

    if not dividends.empty:
        records = bulk_insert_dividends(dividends)
        return f"Inserted {records} dividend records"
    
    return "Zero dividends found"

def process_splits(**context):
    from utils.db_utils import bulk_insert_splits, adjust_historical_prices

    ti = context['ti']
    actions_json = ti.xcom_pull(key='corporate_actions', task_ids='fetch_actions')
    
    if not actions_json:
        return "No data to process"

    df = pd.read_json(io.StringIO(actions_json))

    # FIX: Map 'Stock Splits' column to 'action_type' logic
    if 'Stock Splits' in df.columns:
        splits = df[df['Stock Splits'] > 0].copy()
        splits['action_type'] = 'split'
        
        # Standardize split_ratio and ex_date
        splits['split_ratio'] = splits['Stock Splits']
        if splits.index.name == 'Date' or 'ex_date' not in splits.columns:
            splits = splits.reset_index().rename(columns={'index': 'ex_date', 'Date': 'ex_date'})
    else:
        print("Required column 'Stock Splits' not found.")
        return 0

    if not splits.empty:
        records = bulk_insert_splits(splits)

        # Apply price adjustments in DB for the split
        for _, row in splits.iterrows():
            adjust_historical_prices(row['ticker'], row['split_ratio'], row['ex_date'])
        return f"Processed {records} splits"
    
    return "Zero splits found"

with DAG(
    'corporate_actions_collection',
    default_args=default_args,
    description='Daily collection of corporate actions and dividends',
    schedule_interval='0 18 * * 1-5', 
    catchup=False,
    tags=['corporate_actions', 'daily']
) as dag:

    fetch = PythonOperator(
        task_id='fetch_actions',
        python_callable=fetch_corporate_actions,
    )

    divs = PythonOperator(
        task_id='process_dividends',
        python_callable=process_dividends,
    )

    split_task = PythonOperator(
        task_id="process_splits", # Fixed typo from 'process_spits'
        python_callable=process_splits,
    )

    fetch >> [divs, split_task]