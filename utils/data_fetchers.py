import yfinance as yf
import pandas as pd
from typing import List, Dict
import requests
import time
from datetime import datetime, timedelta
import os

class CircuitBreaker:
    """Circuit breaker pattern for API failures"""
    def __init__(self, failure_threshold=3, timeout=300):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.last_failure_time = None
        self.state = 'closed'
    
    def call(self, func, *args, **kwargs):
        if self.state == 'open':
            if time.time() - self.last_failure_time > self.timeout:
                self.state = 'half_open'
            else:
                raise Exception("Circuit breaker is OPEN")
        
        try:
            result = func(*args, **kwargs)
            if self.state == 'half_open':
                self.state = 'closed'
                self.failure_count = 0
            return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.failure_count >= self.failure_threshold:
                self.state = 'open'

            raise e
        
# Global circuit breakers for each data source
yfinance_cb = CircuitBreaker()
alpha_vantage_cb = CircuitBreaker()


def fetch_ohclv_batch(tickers: List[str], period: str = 'id') -> pd.DataFrame:
    def _fetch():
        data  = yf.download(
            tickers,
            period=period,
            group_by='ticker',
            threads=True,
            progress=False
        )

        if data.empty:
            raise Exception("No data returned from yfinance")
        
        # Reshape data
        result = []

        if len(tickers) == 1:
            # single ticker data has different structure
            ticker = tickers[0]
            df = data.reset_index()
            df['ticker'] = ticker
            df.columns = [col.lower() for col in df.columns]
            result.append(df[['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']])

        else:
            # Multiple tickers
            for ticker in tickers:
                try:
                    ticker_data = data[ticker].reset_index()
                    ticker_data['ticker'] = ticker
                    ticker_data.columns = [col.lower() for col in ticker_data.columns]
                    result.append(ticker_data[['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']])
                except KeyError:
                    print(f"Warning: No data for {ticker}")
                    continue

        if not result:
            raise Exception("No valid data for any ticker")
        
        combined = pd.concat(result, ignore_index=True)

        # clean data
        combined = combined.dropna()
        combined['date'] = pd.to_datetime(combined['date']).dt.date

        return combined

    return yfinance_cb.call(_fetch)

def fetch_options_data(ticker: str) -> pd.DataFrame:
    try:
        stock = yf.Ticker(ticker)

        # Get available expiration dates
        expirations = stock.options

        if not expirations:
            return pd.DataFrame()
        
        all_options = []

        for exp_date in expirations[:3]:
            try:
                opt_chain = stock.option_chain(exp_date)

                # process calls
                calls = opt_chain.calls.copy()
                calls['option_type'] = 'call'
                calls['expiration'] = exp_date
                calls['ticker'] = ticker

                # process puts
                puts = opt_chain.puts.copy()
                puts['option_type'] = 'put'
                puts['expiration'] = exp_date
                puts['ticker'] = ticker

                all_options.append(calls)
                all_options.append(puts)

            except Exception as e:
                print(f"Error fetching options for {ticker} exp {exp_date}: {e}")
                continue

        if not all_options:
            return pd.DataFrame()
        
        combined = pd.concat(all_options, ignore_index=True)

        # Select and rename columns
        columns_map = {
            'strike': 'strike',
            'bid': 'bid',
            'ask': 'ask',
            'lastPrice': 'last',
            'volume': 'volume',
            'openInterest': 'open_interest',
            'impliedVolatility': 'implied_volatility'
        }

        result = combined[['ticker', 'expiration', 'option_type'] + list(columns_map.keys())].copy()
        result.rename(columns=columns_map, inplace=True)

        # clean data
        result = result.dropna(subset=['bid', 'ask'])
        result['expiration'] = pd.to_datetime(result['expiration']).dt.date

        return result
    
    except Exception as e:
        print(f"Error fetching options for {ticker}: {e}")
        return pd.DataFrame()
    

def fetch_corporate_actions_batch(tickers: List[str]) -> pd.DataFrame:
    all_actions = []


    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)

            # Get dividends
            dividends = stock.dividends
            if not dividends.empty:
                div_df = dividends.reset_index()
                div_df.columns = ['date', 'amount']
                div_df['ticker'] = ticker
                div_df['action_type'] = 'dividend'
                div_df['ex_date'] = div_df['date']
                div_df['payment_date'] = None
                all_actions.append(div_df[['ticker', 'action_type', 'ex_date', 'payment_date', 'amount']])

            # get splits
            splits = stock.splits
            if not splits.empty:
                split_df = splits.reset_index()
                split_df.columns = ['date', 'split_ratio']
                split_df['ticker'] = ticker
                split_df['action_type'] = 'split'
                split_df['ex_date'] = split_df['date']
                split_df['amount'] = None
                all_actions.append(split_df[['ticker', 'action_type', 'ex_date', 'split_ratio']])
            
        except Exception as e:
            print(f"Error fetching corporate actions for {ticker}: {e}")
            continue

    if not all_actions:
        return pd.DataFrame()
    
    combined = pd.concat(all_actions, ignore_index=True)
    combined['ex_date'] = pd.to_datetime(combined['ex_date']).dt.date

    # filter for recent actions only
    recent_date = datetime.now().date() - timedelta(days=30)
    combined = combined[combined['ex_date'] >= recent_date]

    return combined

def fetch_alpha_vantage_data(ticker: str, api_key: str) -> pd.DataFrame:
    def _fetch():
        url = "https://www/alphavantage.co/query"
        params = {
            'function': 'TIME_SERIES_DAILY',
            'symbol': ticker,
            'apikey': api_key,
            'outputsize': 'compact'
        }

        response = requests.get(url, params=params)
        response.raise_for_status()

        data = response.json()

        if 'Error Message' in data:
            raise Exception(f"Alpha Vantage error: {data['Error Message']}")
        
        if 'Time Series (Daily)' not in data:
            raise Exception("Unexpected response format from Alpha Vantage")
        
        # parse time series data
        time_series = data['TIme Series (Daily)']

        records = []
        for date_str, values in time_series.items():
            records.append({
                'ticker': ticker,
                'date': pd.to_datetime(date_str).date(),
                'open': float(values['1. open']),
                'high': float(values['2. high']),
                'low': float(values['3. low']),
                'close': float(values['4. close']),
                'volume': int(values['5. volume'])
            })

        df = pd.DataFrame(records)
        return df.sort_values('date')
    
    return alpha_vantage_cb.call(_fetch)




 
    





        



