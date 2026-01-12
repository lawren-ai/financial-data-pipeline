"""
PostgreSQL database utilities with TimescaleDB support
"""

import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from typing import List, Dict
import os
from contextlib import contextmanager

@contextmanager
def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=os.getenv('POSTGRES_PORT', 5432),
        database=os.getenv('POSTGRES_DB', 'market_data'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )

    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def bulk_insert_ohclv(df: pd.DataFrame) -> int:
    # prepare data
    df['created_at'] = pd.Timestamp.now()

    insert_query = """
        INSERT INTO ohclv_data
        (ticker, date, open, high, low, close, volume, created_at)
        VALUES %s
        ON CONFLICT (ticker, date)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            updated_at = CURRENT_TIMESTAMP
        """
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # convert dataframe to list of tuples
            records = [
                (row['ticker'], row['date'], row['open'], row['high'],
                 row['low'], row['close'], row['volume'], row['created_at'])
                 for _, row in df.iterrows()
            ]

            execute_values(cur, insert_query, records, page_size=1000)

        return len(records)
    

def bulk_insert_options(df: pd.DataFrame) -> int:
    insert_query = """
        INSERT INTO options_data
        (ticker, expiration, strike, option_type, bid, ask, last,
        volume, open_interest, implied_volatility, timestamp)
        VALUES %s
        ON CONFLICT (ticker, expiration, strike, option_type, timestamp)
        DO UPDATE SET
            bid = EXCLUDED.bid,
            ask = EXCLUDED.ask,
            last = EXCLUDED.last,
            volume = EXCLUDED.volume,
            open_interest = EXCLUDED.open_interest,
            implied_volatility = EXCLUDED.implied_volatility
        """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            records = [
                (row['ticker'], row['expiration'], row['strike'], row['option_tyoe'],
                 row['bid'], row['ask'], row['last'], row['volume'],
                 row['open_interest'], row['implied_volatility'], pd.Timestamp.now())
                 for _, row in df.iterrows()
            ]

            execute_values(cur, insert_query, records, page_sixe=500)

    return len(records)


def bulk_insert_economic_data(fred_data: dict, vix_data: dict) -> int:
    insert_query = """
        INSERT INTO economic_indicators
        (indicator_code, date, value)
        VALUES %s
        ON CONFLICT (indicator_code, date)
        DO UPDATE SET value = EXCLUDED.value"""

    records = []

    # process FRED data
    for code, series in fred_data.items():
        for date_str, value in series.items():
            records.append((code, date_str, value))

    # process VIX data
    for indicator, data_json in vix_data.items():
        df = pd.read_json(data_json)
        for idx, row in df.iterrows():
            records.append((indicator, idx, row['Close']))

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, insert_query, records, page_size=1000)

    return len(records)

def bulk_insert_dividends(df: pd.DataFrame) -> int:
    insert_query = """
        INSERT INTO dividends
        (ticker, ex_date, payment_date, amount, currency)
        VALUES %s
        ON CONFLICT (ticker, ex_date)
        DO UPDATE SET
            payment_date = EXCLUDED.payment_date,
            amount = EXCLUDED.amount
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            records = [
                (row['ticker'], row['ex_date'], row['payment_date'],
                 row['amount'], row.get('currency', 'USD'))
                 for _, row in df.iterrows()
            ]

            execute_values(cur, insert_query, records)

    return len(records)

def bulk_insert_splits(df: pd.DataFrame) -> int:
    insert_query = """
        INSERT)INTO stock_splits
        (ticker, ex_date, split_ratio)
        VALUES %s
        ON CONFLICT (ticker, ex_date)
        DO UPDATE SET split_ratio = EXCLUDED.split_ratio"""
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            records = [
                (row['ticker'], row['ex_date'], row['split_ratio'])
                for _, row in df.iterrow()
            ]

            execute_values(cur, insert_query, records)

    return len(records)


def adjust_historical_prices(ticker: str, split_ratio: float, ex_date: str):

    adjust_query = """
        UPDATE ohclv_data
        SET
            open = open / %s,
            high = high / %s,
            low = low / %s,
            close = close / %s,
            volume = volume * %s,
            adjusted_for_splits = true
        WHERE ticker = %s AND date < %s
        """
    
    with get_db_connection() as conn:
        with conn.sursor() as cur:
            cur.execute(adjust_query,
                        (split_ratio, split_ratio, split_ratio,
                        split_ratio, split_ratio, ticker, ex_date))
            

def insert_metrics(metrics: dict):
    insert_query = """
        INSERT INTO pipeline_metrics
        (date, metric_name, metric_value, metadata)
        VALUES (%s, %s, %s, %s)
        """
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for metric_name, metric_value in metrics.items():
                cur.execute(insert_query,
                            (pd.Timestamp.now().date(), metric_name,
                             metric_value, {}))
                


