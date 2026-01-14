from datetime import datetime, date
import pandas as pd
import psycopg2
from typing import Optional
import pytz

def get_db_connection():
    import os
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        database=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )

def is_market_open_today() -> bool:
    today = date.today()

    # check if weekend
    if today.weekday() >= 5:
        return False
    
    # check if holiday
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT is_trading_day
                FROM market_calendar
                WHERE date = %s
                """, (today, ))
            result = cur.fetchone()

            if result is None:
                # if not in calendar, assume it's a trading day
                return True

            return result[0]
        
def is_market_open_now() -> bool:
    """Check if market is currently open (9:30 AM - 4:00PM ET)"""
    if not is_market_open_today():
        return False
    
    # Get current time in ET
    et_tz = pytz.timezone('America/New_York')
    now_et = datetime.now(et_tz)


    # Market hours: 9:30 AM - 4:00PM
    market_open = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)

    # Check for easily close
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                    SELECT early_close
                    FROM market_calendar
                    WHERE date = %s AND early_close IS NOT NULL
                    """, (now_et.date(),))
            
            result = cur.fetchone()
            if result:
                close_time = result[0]
                market_close = now_et.replace(
                    hour=close_time.hour,
                    minute=close_time.minute,
                    second=0,
                    microsecond=0
                )

    return market_open <= now_et <= market_close

def get_next_trading_day(start_date: Optional[date] = None) -> date:
    """Get next trading day after given date"""
    if start_date is None:
        start_date = date.today()

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT date
                FROM market_calendar
                WHERE date > %s
                    AND is_trading_day = true
                    ORDER BY date ASC
                    LIMIT 1
                """, (start_date,))    
            result = cur.fetchone()

            if result:
                return result[0]

            # If not in calendar, find next weekday
            next_day = start_date + pd.Timedelta(days=1)
            while next_day.weekday() >= 5:
                next_day += pd.Timedelta(days=1)
            return next_day 
        
def get_trading_days_between(start_date: date, end_date:date) -> list:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT date
                FROM market_calendar
                WHERE date >= %s
                    AND date <= %s
                    AND is_trading_day = true
                ORDER BY date ASC
            """, (start_date, end_date))

            results = cur.fetchall()
            return [row[0] for row in results]

            