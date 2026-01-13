from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

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
    actions = fetch_corporate_actions_batch(tickers)

    context['ti'].xcom_push(key='corporate_actions', value=actions.to_json())
    return len(actions)

def process_dividends(**context):
    import pandas as pd
    from utils.db_utils import bulk_insert_dividends

    ti = context['ti']
    actions_json = ti.xcom_pull(key='corporate_actions', task_ids='fetch_actions')
    df = pd.read_json(actions_json)

    # filter for dividends
    dividends = df[df['action_type'] == 'dividend']

    if not dividends.empty:
        records = bulk_insert_dividends(dividends)
        return records
    
    return 0

def process_splits(**context):
    import pandas as pd
    from utils.db_utils import bulk_insert_splits, adjust_historical_prices

    ti = context['ti']
    actions_json = ti.xcom_pull(key='corporate_actions', task_ids='fetch_actions')
    df = pd.read_json(actions_json)

    # filter for splits
    splits = df[df['action_type'] == 'split']

    if not splits.empty:
        records = bulk_insert_splits(splits)

        # Adjust historical prices for splits
        for _, split in splits.iterrows():
            adjust_historical_prices(split['ticker'], split['split_ratio'], split['ex_date'])
        return records
    
    return 0

with DAG(
    'corporate_actions_collection',
    default_args=default_args,
    description='Daily collection of corporate actions and dividends',
    schedule_interval='0 18 * * 1-5', # 6PM ET on weekdays
    catchup=False,
    tags=['corporate_acions', 'daily']
) as dag:

    fetch = PythonOperator(
        task_id='fetch_actions',
        python_callable=fetch_corporate_actions,
    )

    dividends = PythonOperator(
        task_id='process_dividends',
        python_callable=process_dividends,
    )

    splits = PythonOperator(
        task_id="process_spits",
        python_callable=process_splits,
    )

    fetch >> [dividends, splits]

