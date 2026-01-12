"""
Redis cache layer for high-frequency data access
"""

import redis
import json
import os
from typing import Optional, List
import pandas as pd

# Initialize Redis connection
redis_client = redis.Redis(
    host=os.getenv('REDIS_HOST', 'localhost'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db=0,
    decode_responses=True
)


def cache_latest_prices(df: pd.DataFrame, ttl: int=900):
    """
    Cache latest prices in Redis with 15-minute TTL
    Format: ticker: latest -> {price, volume, timestamo}
    """
    pipe = redis_client.pipeline()

    for _, row in df.iterrows():
        key = f"price:latest:{row['ticker']}"
        value = {
            'close': float(row['close']),
            'open': float(row['open']),
            'high': float(row['high']),
            'low': float(row['low']),
            'volume': int(row['volume']),
            'date': str(row['date']),
            'timestamp': pd.Timestamp.now().isoformat()
        }

        pipe.setex(key, ttl, json.dumps(value))

    pipe.execute()

def get_cached_price(ticker: str) -> Optional[dict]:
    key = f"price:latest:{ticker}"
    data = redis_client.get(key)

    if data:
        return json.loads(data)
    return None

def cache_market_stats(stats: dict, ttl: int=300):
    key = "market:stats:latest"
    redis_client.setex(key, ttl, json.dumps(stats))

def get_market_stats() -> Optional[dict]:
    data = redis_client.get("market:stats:latest")
    if data:
        return json.loads(data)
    return None

def cache_options_snapshot(ticker: str, options_data: dict, ttl: int= 900):
    key = f"Options:{ticker}:snapshot"
    redis_client.setex(key, ttl, json.dumps(options_data))

def increment_rate_limit(identifier: str, window: int = 3600, limit: int=1000) -> bool:
    """
    Rete limiting using Redis
    Returns true if within limit, false if exceeded
    """
    key = f"ratelimit:{identifier}"

    pipe = redis_client.pipeline()
    pipe.incr(key)
    pipe.expire(key, window)
    current_count = pipe.execute()[0]

    return current_count <= limit

def cache_validation_results(dag_id: str, run_id: str, results: dict, ttl: int = 86400):
    """cache validation results for 24 hours"""
    key = f"validation:{dag_id}:{run_id}"
    redis_client.setex(key, ttl, json.dumps(results))

def get_validation_results(dag_id: str, run_id: str) -> Optional[dict]:
    """Retrieve cached validation results"""
    key = f"validation:{dag_id}:{run_id}"
    data = redis_client.get(key)
    if data:
        return json.loads(data)
    return None






