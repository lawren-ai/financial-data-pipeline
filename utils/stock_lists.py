import pandas as pd
import requests
from typing import List


import requests
from typing import List
import pandas as pd

def get_sp500_tickers() -> List[str]:
    """Fetch S&P 500 ticker list from Wikipedia with proper headers"""
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        # 1. Fetch the content with headers
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status() # This will catch the 403 error specifically
        
        # 2. Pass the text content to pandas
        tables = pd.read_html(response.text)
        df = tables[0]
        
        # 3. Clean tickers (Yahoo Finance uses '-' for '.' in symbols like BRK-B)
        tickers = df['Symbol'].str.replace('.', '-', regex=False).tolist()
        return tickers
        
    except Exception as e:
        print(f"Error fetching S&P 500 tickers: {e}")
        # Return cached list as fallback
        return _get_cached_sp500_tickers()
def get_nasdaq100_tickers() -> List[str]:
    """Fetch NASDAQ 100 ticker List"""
    url = 'https://en.wikipedia.org/wiki/NASDAQ-100'

    try:
        tables = pd.read_html(url)
        df = tables[4]
        tickers = df['Ticker'].tolist()
        return tickers
    except Exception as e:
        print(f"Error fetching NASDAQ 100 tickers: {e}")
        return []
    
def get_liquid_options_tickers() -> List[str]:
    """
    Get list of stocks with liquid options markets
    """
    liquid_tickers = [
        'AAPL', 'MSFT', 'AMZN', 'GOOGL', 'TSLA', 'META', 'NVDA', 'AMD',
        'SPY', 'QQQ', 'IWM', 'DIA',  # ETFs
        'NFLX', 'BABA', 'DIS', 'BA', 'GE', 'F', 'GM',
    ]
    return liquid_tickers

def get_additional_liquid_stocks() -> List[str]:
    return [
        'BRK.B', 'TSLA', 'TSM', 'V', 'UNH', 'JNJ', 'WMT', 'JPM',
        'MA', 'PG', 'NVDA', 'HD', 'DIS', 'PYPL', 'ADBE', 'CRM',
        'NFLX', 'CMCSA', 'PFE', 'ABT', 'TMO', 'COST', 'MRK', 'ABBV'
    ]

def _get_cached_sp500_tickers() -> List[str]:
    """Cached s&p 500 list as fallback"""
    return [
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRK.B',
        'V', 'UNH', 'JNJ', 'WMT', 'JPM', 'MA', 'PG', 'XOM', 'HD', 'CVX',
        'LLY', 'MRK', 'ABBV', 'KO', 'PEP', 'COST', 'AVGO', 'TMO', 'MCD',
        'CSCO', 'ACN', 'ABT', 'DHR', 'PFE', 'NKE', 'LIN', 'TXN', 'CRM'
    ]



    