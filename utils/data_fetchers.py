import yfinance as yf
import pandas as pd
from typing import List, Dict, Optional, Callable
import requests
import time
from datetime import datetime, timedelta
import os
from functools import wraps

class RateLimiter:
    """Rate limiter with token bucket algorithm"""
    def __init__(self, rate: float = 1.0, per: float = 1.0):
        """
        rate: number of requests
        per: per this many seconds
        """
        self.rate = rate
        self.per = per
        self.allowance = rate
        self.last_check = time.time()
    
    def acquire(self):
        """Wait if necessary to maintain rate limit"""
        now = time.time()
        time_passed = now - self.last_check
        self.last_check = now
        self.allowance += time_passed * (self.rate / self.per)
        
        if self.allowance > self.rate:
            self.allowance = self.rate
        
        if self.allowance < 1.0:
            sleep_time = (1.0 - self.allowance) * (self.per / self.rate)
            print(f"Rate limit: waiting {sleep_time:.2f}s before next request")
            time.sleep(sleep_time)
            self.allowance = 0.0
        else:
            self.allowance -= 1.0

def exponential_backoff_retry(max_retries: int = 3, base_delay: float = 1.0, backoff_factor: float = 2.0):
    """
    Decorator for exponential backoff retry logic
    
    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay in seconds
        backoff_factor: Multiplier for delay on each retry
    """
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    if attempt < max_retries:
                        delay = base_delay * (backoff_factor ** attempt)
                        print(f"Attempt {attempt + 1}/{max_retries + 1} failed: {e}. "
                              f"Retrying in {delay:.1f}s...")
                        time.sleep(delay)
                    else:
                        print(f"All {max_retries + 1} attempts failed for {func.__name__}")
            
            raise last_exception
        
        return wrapper
    return decorator

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
        
# Global circuit breakers and rate limiters for each data source
yfinance_cb = CircuitBreaker()
alpha_vantage_cb = CircuitBreaker()

# Rate limiter for yfinance: 1 batch request per 5 seconds (very conservative for stability)
yfinance_limiter = RateLimiter(rate=1.0, per=5.0)

# Rate limiter for Alpha Vantage: 5 requests per minute (API limit)
alpha_vantage_limiter = RateLimiter(rate=5.0, per=60.0)


def fetch_ohclv_batch(tickers: List[str], period: str = '1y', batch_size: int = None) -> pd.DataFrame:
    """
    Fetch OHLCV data ticker-by-ticker (yfinance batch mode has JSON issues)
    
    Args:
        tickers: List of ticker symbols
        period: Period for historical data ('1y', '5y', etc)
        batch_size: Ignored (kept for compatibility)
    """
    all_data = []
    total_tickers = len(tickers)
    success_count = 0
    failed_tickers = []
    
    print(f"\nFetching OHLCV data for {total_tickers} tickers individually...")
    
    for i, ticker in enumerate(tickers, 1):
        # Show progress every 50 tickers
        if i % 50 == 1:
            print(f"\nProgress: {i}/{total_tickers} tickers...")
        
        try:
            yfinance_limiter.acquire()
            
            # Fetch single ticker
            data = yf.download(
                ticker,
                period=period,
                progress=False,
                timeout=30
            )
            
            if data is None or data.empty:
                failed_tickers.append(ticker)
                continue
            
            # Format data
            df = data.reset_index()
            df['ticker'] = ticker
            df.columns = [col.lower() for col in df.columns]
            all_data.append(df[['ticker', 'date', 'open', 'high', 'low', 'close', 'volume']])
            success_count += 1
            
            # Rate limiting between requests
            if i < total_tickers:
                time.sleep(0.1)  # 100ms between requests
        
        except Exception as e:
            print(f"  ⚠️  {ticker}: {str(e)[:50]}")
            failed_tickers.append(ticker)
            continue
    
    if not all_data:
        print(f"\n❌ CRITICAL: Failed to fetch data for all tickers.")
        return pd.DataFrame()
    
    combined = pd.concat(all_data, ignore_index=True)
    combined = combined.dropna()
    combined['date'] = pd.to_datetime(combined['date']).dt.date
    
    success_rate = success_count / total_tickers * 100 if total_tickers > 0 else 0
    print(f"\n{'='*60}")
    print(f"OHLCV Fetch Summary:")
    print(f"  Total tickers: {total_tickers}")
    print(f"  Successful: {success_count} ({success_rate:.1f}%)")
    print(f"  Failed: {len(failed_tickers)}")
    print(f"  Records retrieved: {combined['ticker'].nunique()} unique tickers, {len(combined)} total records")
    print(f"  Date range: {combined['date'].min()} to {combined['date'].max()}")
    print(f"{'='*60}\n")
    
    return combined

def fetch_options_data(ticker: str) -> pd.DataFrame:
    try:
        yfinance_limiter.acquire()  # Apply rate limiting
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
    
def fetch_single_corporate_action(ticker: str) -> Optional[pd.DataFrame]:
    """Fetch corporate actions for a single ticker with rate limiting and retries"""
    yfinance_limiter.acquire()  # Apply rate limiting
    
    try:
        stock = yf.Ticker(ticker)
        actions = []

        # 1. Fetch Dividends
        dividends = stock.dividends
        if dividends is not None and hasattr(dividends, 'empty') and not dividends.empty:
            div_df = dividends.reset_index()
            div_df.columns = ['ex_date', 'amount']
            div_df['ticker'] = ticker
            div_df['action_type'] = 'dividend'
            div_df['split_ratio'] = 0.0
            actions.append(div_df[['ticker', 'action_type', 'ex_date', 'amount', 'split_ratio']])

        # 2. Fetch Splits
        splits = stock.splits
        if splits is not None and hasattr(splits, 'empty') and not splits.empty:
            split_df = splits.reset_index()
            split_df.columns = ['ex_date', 'split_ratio']
            split_df['ticker'] = ticker
            split_df['action_type'] = 'split'
            split_df['amount'] = 0.0
            actions.append(split_df[['ticker', 'action_type', 'ex_date', 'amount', 'split_ratio']])
        
        if actions:
            return pd.concat(actions, ignore_index=True)
        return None
        
    except Exception as e:
        print(f"ERROR fetching corporate actions for {ticker}: {e}")
        return None

def fetch_corporate_actions_batch(tickers: List[str], batch_size: int = 50, max_retries_per_ticker: int = 2) -> pd.DataFrame:
    """
    Fetch corporate actions in batches with exponential backoff retry
    
    Args:
        tickers: List of ticker symbols
        batch_size: Number of tickers to process before pause (default 50)
        max_retries_per_ticker: Max retries per individual ticker
    """
    all_actions = []
    error_tickers = []
    success_count = 0
    
    total_tickers = len(tickers)
    
    for i, ticker in enumerate(tickers):
        # Process in batches - add inter-batch delay
        if i > 0 and i % batch_size == 0:
            batch_num = (i // batch_size)
            print(f"Batch {batch_num} complete ({i}/{total_tickers}). "
                  f"Pausing 5s before next batch to avoid rate limiting...")
            time.sleep(5)
        
        # Retry with exponential backoff for individual ticker
        ticker_result = None
        for attempt in range(max_retries_per_ticker):
            try:
                ticker_result = fetch_single_corporate_action(ticker)
                if ticker_result is not None:
                    all_actions.append(ticker_result)
                    success_count += 1
                break
            except Exception as e:
                if attempt < max_retries_per_ticker - 1:
                    delay = 1.0 * (2.0 ** attempt)
                    print(f"Retry {attempt + 1} for {ticker}: waiting {delay:.1f}s")
                    time.sleep(delay)
                else:
                    error_tickers.append(ticker)
                    print(f"Failed to fetch {ticker} after {max_retries_per_ticker} attempts")

    # Calculate stats
    failure_rate = len(error_tickers) / total_tickers if total_tickers > 0 else 0
    success_rate = success_count / total_tickers if total_tickers > 0 else 0
    
    print(f"\n{'='*60}")
    print(f"Corporate Actions Batch Fetch Summary:")
    print(f"  Total tickers: {total_tickers}")
    print(f"  Successful: {success_count} ({success_rate*100:.1f}%)")
    print(f"  Failed: {len(error_tickers)} ({failure_rate*100:.1f}%)")
    print(f"{'='*60}\n")
    
    # Alert if too many failures (>50% failure rate)
    if failure_rate > 0.5:
        raise Exception(
            f"Corporate actions fetch: {failure_rate*100:.1f}% failure rate "
            f"({len(error_tickers)}/{total_tickers} tickers failed). "
            f"This may indicate API issues or network problems."
        )
    
    if not all_actions:
        print(f"WARNING: No corporate actions found for any of {total_tickers} tickers")
        return pd.DataFrame()
    
    combined = pd.concat(all_actions, ignore_index=True)
    
    # 3. Handle Timezones: yfinance dates are often UTC-aware
    combined['ex_date'] = pd.to_datetime(combined['ex_date'], utc=True).dt.date

    # Filter for recent actions
    recent_date = datetime.now().date() - timedelta(days=365)
    combined = combined[combined['ex_date'] >= recent_date]

    print(f"Successfully fetched corporate actions for {success_count} tickers. "
          f"Found {len(combined)} total actions.")

    return combined
def fetch_alpha_vantage_data(ticker: str, api_key: str) -> pd.DataFrame:
    def _fetch():
        url = "https://www.alphavantage.co/query"
        params = {
            'function': 'TIME_SERIES_DAILY',
            'symbol': ticker,
            'apikey': api_key,
            'outputsize': 'full'  # Get full history
        }

        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()

        if 'Error Message' in data:
            raise Exception(f"Alpha Vantage error: {data['Error Message']}")
        
        if 'Time Series (Daily)' not in data:
            raise Exception("Unexpected response format from Alpha Vantage")
        
        # parse time series data
        time_series = data['Time Series (Daily)']

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


def fetch_alpha_vantage_batch(tickers: List[str], api_key: str) -> pd.DataFrame:
    """
    Fetch OHLCV data from Alpha Vantage for multiple tickers
    
    Args:
        tickers: List of ticker symbols
        api_key: Alpha Vantage API key
    
    Note: Alpha Vantage has a limit of 5 requests per minute
    """
    all_data = []
    total_tickers = len(tickers)
    success_count = 0
    failed_tickers = []
    
    print(f"\nFetching OHLCV data from Alpha Vantage for {total_tickers} tickers...")
    print(f"Rate limit: 5 requests per minute (1 per 12s minimum)\n")
    
    for i, ticker in enumerate(tickers, 1):
        # Show progress every 25 tickers
        if i % 25 == 1:
            print(f"Progress: {i}/{total_tickers} tickers...")
        
        try:
            alpha_vantage_limiter.acquire()  # Rate limiting
            
            df = fetch_alpha_vantage_data(ticker, api_key)
            
            if df is not None and not df.empty:
                all_data.append(df)
                success_count += 1
            else:
                failed_tickers.append(ticker)
        
        except Exception as e:
            error_msg = str(e)[:60]
            if "Note: our standard API call frequency is 5 per minute" in error_msg or "thank you for using" in error_msg.lower():
                print(f"  ⚠️  {ticker}: Rate limit hit - pausing 70s...")
                time.sleep(70)  # Wait for rate limit to reset
                try:
                    alpha_vantage_limiter.acquire()
                    df = fetch_alpha_vantage_data(ticker, api_key)
                    if df is not None and not df.empty:
                        all_data.append(df)
                        success_count += 1
                    else:
                        failed_tickers.append(ticker)
                except Exception as retry_error:
                    print(f"    Retry failed: {str(retry_error)[:40]}")
                    failed_tickers.append(ticker)
            else:
                print(f"  ⚠️  {ticker}: {error_msg}")
                failed_tickers.append(ticker)
            continue
    
    if not all_data:
        print(f"\n❌ CRITICAL: Failed to fetch data for all tickers.")
        return pd.DataFrame()
    
    combined = pd.concat(all_data, ignore_index=True)
    combined = combined.dropna()
    
    success_rate = success_count / total_tickers * 100 if total_tickers > 0 else 0
    print(f"\n{'='*60}")
    print(f"Alpha Vantage OHLCV Fetch Summary:")
    print(f"  Total tickers: {total_tickers}")
    print(f"  Successful: {success_count} ({success_rate:.1f}%)")
    print(f"  Failed: {len(failed_tickers)}")
    if combined is not None and not combined.empty:
        print(f"  Records retrieved: {combined['ticker'].nunique()} unique tickers, {len(combined)} total records")
        print(f"  Date range: {combined['date'].min()} to {combined['date'].max()}")
    print(f"{'='*60}\n")
    
    return combined




 
    





        



