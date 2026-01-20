"""
Economic indicators collection - runs weekly/monthly based on relaease schedules
"""


from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'quant_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=10)
}

def fetch_fred_data(**context):
    from fredapi import Fred
    import os

    fred = Fred(api_key=os.getenv("FRED_API_KEY"))

    indicators = {
        'DFF': "Fed Funds Rate",
        'T10Y2Y': 'Treasure Yield Spread',
        'UNRATE': 'Unemployment Rate',
        'CPIAUVSL': 'CPI',
        'GDP': 'GDP',
    }

    data = {}
    for code, name in indicators.items():
        try:
            series = fred.get_series(code)
            data[code] = series.to_dict()
        except Exception as e:
            print(f"Failed to fetch {name}: {e}")
        
    context['ti'].xcom_push(key='fred_data', value=data)
    return len(data)

def fetch_vix_data(**context):
    import yfinance as yf

    vix = yf.download('^VIX', period='5d', interval='1d')
    vvix = yf.download('^VVIX', period='5d', interval='1d')

    context['ti'].xcom_push(key='vix_data', value={
        'VIX': vix.to_json(),
        'VVIX': vvix.to_json()
    })

    return True

def load_economic_data(**context):
    from utils.db_utils import bulk_insert_economic_data

    ti = context['ti']
    fred_data = ti.xcom_pull(key='fred_data', task_ids='fetch_fred')
    vix_data = ti.xcom_pull(key='vix_data', task_ids='fetch_vix')

    records = bulk_insert_economic_data(fred_data, vix_data)
    return records

with DAG(
    'economic_indicators_collections',
    default_args=default_args,
    description='Collection of economic indicators and volatility indices',
    schedule_interval='0 10 * * 1',
    catchup=False,
    tags=['economic_data', 'weekly'],  
) as dag:
    
    fetch_fred = PythonOperator(
        task_id='fetch_fred',
        python_callable=fetch_fred_data,
    )

    fetch_vix = PythonOperator(
        task_id = 'fetch_vix',
        python_callable=fetch_vix_data,
    )

    load = PythonOperator(
        task_id = 'load_economic_data',
        python_callable=load_economic_data,
    )

    [fetch_fred, fetch_vix] >> load
