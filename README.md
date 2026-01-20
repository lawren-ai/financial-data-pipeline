# Financial Data Pipeline

A personal automated financial data collection and storage system. Fetches market data, options, dividends, stock splits, and economic indicators from multiple sources and stores them in PostgreSQL for analysis.

## 🎯 Project Overview

This project automates the collection of financial market data from multiple sources using Apache Airflow for orchestration. It's designed as a self-contained, Docker-based system that runs independently on your machine.

**Key Features:**
- 📊 Automatic daily data collection from financial APIs
- 🗄️ Centralized PostgreSQL database storage
- ⏰ Scheduled DAG execution via Apache Airflow
- 💾 Redis caching layer for performance
- 🔄 Error handling and retry logic
- 📈 Support for multiple data types (OHLCV, options, dividends, economic indicators)

## 📋 Table of Contents

1. [Architecture](#-architecture)
2. [Prerequisites](#-prerequisites)
3. [Installation](#-installation)
4. [Configuration](#-configuration)
5. [Running the Pipeline](#-running-the-pipeline)
6. [Database Schema](#-database-schema)
7. [Data Sources](#-data-sources)
8. [Usage Examples](#-usage-examples)
9. [Troubleshooting](#-troubleshooting)

---

## 🏗️ Architecture

### System Components

```
┌─────────────────────────────────────────────────────┐
│           Financial Data Pipeline                   │
├─────────────────────────────────────────────────────┤
│                                                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────┐ │
│  │   Airflow    │  │  PostgreSQL  │  │  Redis   │ │
│  │  Scheduler   │  │   Database   │  │  Cache   │ │
│  └──────┬───────┘  └──────┬───────┘  └────┬─────┘ │
│         │                 ▲                │       │
│         ├─────────────────┴────────────────┘       │
│         │                                          │
│  ┌──────▼────────────────────────────────────┐   │
│  │           DAG Executors                    │   │
│  ├─────────────────────────────────────────┬─┘   │
│  │                                         │      │
│  ├─ market_data_collection        ────────┤      │
│  ├─ options_data_collection       ────────┤      │
│  ├─ corporate_actions_collection  ────────┤      │
│  └─ economic_indicators_collection────────┘      │
│                                                    │
│  ┌──────────────────────────────────────────┐   │
│  │      Data Fetchers & Utils               │   │
│  ├──────────────────────────────────────────┤   │
│  │ • yfinance integration                   │   │
│  │ • Alpha Vantage integration              │   │
│  │ • FRED API integration                   │   │
│  │ • Rate limiting & retries                │   │
│  └──────────────────────────────────────────┘   │
│                                                    │
└─────────────────────────────────────────────────────┘
```

### Technology Stack

| Component | Purpose |
|-----------|---------|
| **Apache Airflow** | Workflow orchestration & scheduling |
| **PostgreSQL + TimescaleDB** | Time-series data storage |
| **Redis** | Caching & in-memory data store |
| **Python 3.10+** | Core application code |
| **Docker & Docker Compose** | Containerization & local deployment |

---

## 📋 Prerequisites

- **Docker Desktop** (Windows/Mac) or Docker Engine (Linux)
- **Docker Compose** v2.0+
- **PostgreSQL Client** (optional, for direct SQL access)
- **Python 3.10+** (for local development)
- **Git** (for version control)

### Required API Keys

Depending on your configuration, you may need:

- **yfinance** - Free, no key required
- **FRED API** - [Register here](https://research.stlouisfed.org/fred/) to get free API key
- **Alpha Vantage** - [Register here](https://www.alphavantage.co/api/) for free or premium API key

---

## 🚀 Installation

### Step 1: Clone or Setup Project

```bash
# Navigate to project directory
cd financial-data-pipeline
```

### Step 2: Create Environment File

Create a `.env` file with your configuration:

```env
# PostgreSQL Configuration
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_secure_password_here
POSTGRES_DB=market_data
POSTGRES_PORT=5433

# Airflow Configuration
AIRFLOW_UID=1000
AIRFLOW_FERNET_KEY=your_fernet_key_here
AIRFLOW_SECRET_KEY=your_secret_key_here

# API Keys
FRED_API_KEY=your_fred_api_key_here
ALPHA_VANTAGE_API_KEY=your_alpha_vantage_key_here
```

**Generate Fernet Key:**
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### Step 3: Start Docker Containers

```bash
# Start all services in background
docker-compose up -d

# Verify all containers are running
docker ps
```

Expected containers:
- `quant_postgres` - PostgreSQL database
- `quant_redis` - Redis cache
- `quant_airflow_webserver` - Airflow UI
- `quant_airflow_scheduler` - DAG scheduler

### Step 4: Verify Installation

```bash
# Check if Airflow is ready
docker exec quant_airflow_webserver airflow dags list

# Check if database is accessible
psql -h localhost -p 5433 -U postgres -d market_data -c "SELECT 1"
```

---

## ⚙️ Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_HOST` | `postgres` | Database hostname |
| `POSTGRES_PORT` | `5433` | Database port (external) |
| `POSTGRES_USER` | `postgres` | Database user |
| `POSTGRES_PASSWORD` | - | Database password (required) |
| `POSTGRES_DB` | `market_data` | Database name |
| `FRED_API_KEY` | - | FRED API key for economic data |
| `ALPHA_VANTAGE_API_KEY` | - | Alpha Vantage API key |

### DAG Configuration

Each DAG has a schedule interval defined in its Python file:

**market_data_collection**
```python
schedule_interval='0 16 * * 1-5'  # 4 PM EST, Mon-Fri
```

**options_data_collection**
```python
schedule_interval='0 */4 * * 1-5'  # Every 4 hours, Mon-Fri
```

**corporate_actions_collection**
```python
schedule_interval='0 18 * * 1-5'  # 6 PM EST, Mon-Fri
```

**economic_indicators_collection**
```python
schedule_interval='0 10 * * MON'  # 10 AM EST, Mondays
```

To modify schedules, edit the DAG files in `dags/` directory.

---

## ▶️ Running the Pipeline

### Via Airflow Web UI

1. **Access Airflow Dashboard**
   ```
   http://localhost:8080
   ```
   - Username: `admin`
   - Password: `admin`

2. **Trigger a DAG**
   - Click on a DAG name
   - Click "Trigger DAG" button
   - Monitor execution in the UI

3. **View Logs**
   - Click on a task run
   - View logs in the bottom panel

### Via Command Line

```bash
# Test a specific DAG (runs once, no scheduling)
docker exec quant_airflow_webserver airflow dags test market_data_collection 2026-01-20

# Trigger a DAG
docker exec quant_airflow_webserver airflow dags trigger market_data_collection

# View DAG status
docker exec quant_airflow_webserver airflow dags list-runs --dag-id market_data_collection

# View task logs
docker exec quant_airflow_webserver airflow tasks logs market_data_collection fetch_market_data 2026-01-20
```

### Manual Data Insertion (for Testing)

If you want to populate the database with sample data without running the full pipeline:

```python
# Python script to insert sample data
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import random

conn = psycopg2.connect(
    host='localhost',
    port=5433,
    database='market_data',
    user='postgres',
    password='your_password'
)

# Insert sample OHLCV data
cur = conn.cursor()
tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

for ticker in tickers:
    for days_ago in range(365):
        date = (datetime.now() - timedelta(days=days_ago)).date()
        price = 100 + random.uniform(-5, 5)
        
        cur.execute("""
            INSERT INTO ohclv_data 
            (ticker, date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (ticker, date, price-1, price+1, price-2, price, 1000000))

conn.commit()
cur.close()
conn.close()
print("Sample data inserted!")
```

---

## 📊 Database Schema

### Tables

#### `ohclv_data` - Historical Price Data
```sql
CREATE TABLE ohclv_data (
    id BIGSERIAL,
    ticker VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open NUMERIC(12, 4),
    high NUMERIC(12, 4),
    low NUMERIC(12, 4),
    close NUMERIC(12, 4),
    volume BIGINT,
    adjusted_for_splits BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, date)
);
```

#### `options_data` - Options Contracts
```sql
CREATE TABLE options_data (
    id BIGSERIAL,
    ticker VARCHAR(10) NOT NULL,
    expiration DATE NOT NULL,
    strike NUMERIC(12, 4),
    option_type VARCHAR(4), -- 'call' or 'put'
    bid NUMERIC(12, 4),
    ask NUMERIC(12, 4),
    implied_volatility NUMERIC(8, 6),
    volume INTEGER,
    open_interest INTEGER,
    timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, expiration, strike, option_type, timestamp)
);
```

#### `dividends` - Dividend Payments
```sql
CREATE TABLE dividends (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    ex_date DATE NOT NULL,
    payment_date DATE,
    amount NUMERIC(12, 4),
    dividend_type VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ticker, ex_date)
);
```

#### `stock_splits` - Stock Split Events
```sql
CREATE TABLE stock_splits (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    ex_date DATE NOT NULL,
    split_ratio NUMERIC(10, 6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ticker, ex_date)
);
```

#### `economic_indicators` - Economic Data
```sql
CREATE TABLE economic_indicators (
    id SERIAL PRIMARY KEY,
    indicator_code VARCHAR(20),
    indicator_name VARCHAR(100),
    date DATE NOT NULL,
    value NUMERIC(18, 6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(indicator_code, date)
);
```

### Querying Data

```bash
# Connect to database
psql -h localhost -p 5433 -U postgres -d market_data

# View recent OHLCV data
SELECT * FROM ohclv_data 
WHERE ticker = 'AAPL' 
ORDER BY date DESC 
LIMIT 10;

# Check data counts
SELECT 
    'ohclv_data' as table_name, COUNT(*) as records
FROM ohclv_data
UNION ALL
SELECT 'dividends', COUNT(*) FROM dividends
UNION ALL
SELECT 'stock_splits', COUNT(*) FROM stock_splits
UNION ALL
SELECT 'options_data', COUNT(*) FROM options_data
UNION ALL
SELECT 'economic_indicators', COUNT(*) FROM economic_indicators;

# Get unique tickers
SELECT DISTINCT ticker FROM ohclv_data ORDER BY ticker;

# Date range of data
SELECT 
    MIN(date) as earliest_date,
    MAX(date) as latest_date,
    COUNT(DISTINCT ticker) as num_tickers
FROM ohclv_data;
```

---

## 📡 Data Sources

### OHLCV Data (Stock Prices)
- **Primary Source**: yfinance (Yahoo Finance API)
- **Frequency**: Daily (configurable)
- **Tickers**: S&P 500 (500 tickers)
- **Period**: 1 year of historical data
- **Fallback**: Alpha Vantage

### Options Data
- **Source**: yfinance
- **Frequency**: Daily/Hourly (configurable)
- **Content**: Call & put options, bid/ask, implied volatility
- **Coverage**: Major US options symbols

### Corporate Actions
- **Source**: yfinance
- **Frequency**: Daily
- **Content**: Dividends, stock splits
- **Coverage**: All US-listed companies

### Economic Indicators
- **Source**: FRED (Federal Reserve Economic Data)
- **Frequency**: Weekly/Monthly (data-dependent)
- **Indicators**:
  - Unemployment rate
  - GDP
  - Inflation
  - Interest rates
  - And 400+ more

---

## 💡 Usage Examples

### Example 1: Query AAPL Prices

```python
import psycopg2
import pandas as pd

conn = psycopg2.connect(
    host='localhost', 
    port=5433, 
    database='market_data',
    user='postgres',
    password='your_password'
)

# Get last 30 days of AAPL
df = pd.read_sql("""
    SELECT date, open, high, low, close, volume
    FROM ohclv_data
    WHERE ticker = 'AAPL'
    AND date >= CURRENT_DATE - INTERVAL '30 days'
    ORDER BY date
""", conn)

print(df)
```

### Example 2: Check Latest Dividends

```bash
psql -h localhost -p 5433 -U postgres -d market_data -c "
SELECT ticker, ex_date, amount
FROM dividends
WHERE ex_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY ex_date DESC;"
```

### Example 3: Analyze Returns

```python
import pandas as pd

# Calculate daily returns
df['returns'] = df['close'].pct_change()

# Summary statistics
print(f"Mean return: {df['returns'].mean():.4%}")
print(f"Volatility: {df['returns'].std():.4%}")
print(f"Sharpe ratio: {df['returns'].mean() / df['returns'].std():.2f}")
```

### Example 4: Export Data to CSV

```bash
# Export OHLCV data
psql -h localhost -p 5433 -U postgres -d market_data -c "
COPY (
    SELECT * FROM ohclv_data 
    WHERE date >= '2025-01-01'
) TO STDOUT WITH CSV HEADER" > market_data.csv
```

---

## 🔧 Troubleshooting

### Database Connection Issues

**Problem**: `psql: could not connect to server`

**Solution**:
```bash
# Check if PostgreSQL container is running
docker ps | grep postgres

# Restart PostgreSQL
docker-compose restart postgres

# Verify port is correct (5433 for external)
psql -h localhost -p 5433 -U postgres -d market_data
```

### Airflow DAGs Not Showing

**Problem**: DAGs list is empty in Airflow UI

**Solution**:
```bash
# Check if DAGs are in correct location
docker exec quant_airflow_webserver ls -la /opt/airflow/dags/

# Restart Airflow scheduler
docker-compose restart airflow-scheduler

# Trigger DAG parsing
docker exec quant_airflow_webserver airflow dags reserialize
```

### API Rate Limiting

**Problem**: "Failed to get ticker" errors or JSON decode errors

**Solution**:
- Reduce batch size in `utils/data_fetchers.py`
- Increase delay between requests
- Use Alpha Vantage instead of yfinance
- Check if API service is experiencing outages

### Out of Memory

**Problem**: Docker container crashes or slows down

**Solution**:
```bash
# Increase Docker memory allocation (Docker Desktop settings)
# Or limit container memory in docker-compose.yml:
services:
  postgres:
    mem_limit: 2g
```

### Data Not Inserting

**Problem**: DAGs run successfully but no data in tables

**Solution**:
```bash
# Check DAG logs
docker logs quant_airflow_webserver

# Verify table exists
psql -h localhost -p 5433 -U postgres -d market_data -c "\dt"

# Check for constraint violations
psql -h localhost -p 5433 -U postgres -d market_data -c "SELECT * FROM ohclv_data LIMIT 1;"
```

---

## 📚 Project Structure

```
financial-data-pipeline/
├── dags/                          # Airflow DAG definitions
│   ├── market_data_dag.py        # OHLCV data fetching
│   ├── options_data_dag.py       # Options data fetching
│   ├── economic_data_dag.py      # Economic indicators
│   └── corporate_actions_dag.py  # Dividends & splits
├── utils/                         # Utility modules
│   ├── data_fetchers.py          # API data fetching
│   ├── db_utils.py               # Database operations
│   ├── stock_lists.py            # Ticker lists
│   ├── market_calendar.py        # Market holidays
│   ├── metrics.py                # Monitoring metrics
│   ├── redis_cache.py            # Caching layer
│   └── notifications.py          # Alert notifications
├── schema/
│   └── init.sql                  # Database schema
├── .env                          # Environment variables
├── docker-compose.yml            # Service definitions
├── Dockerfile                    # Container image
├── requirements.txt              # Python dependencies
└── README.md                     # This file
```

---

## 🛠️ Development

### Adding a New Data Source

1. **Create fetcher function in `utils/data_fetchers.py`**:
```python
def fetch_my_data(symbols: List[str]) -> pd.DataFrame:
    # Implement API call
    # Return DataFrame with columns: symbol, date, value
    pass
```

2. **Create database table in `schema/init.sql`**:
```sql
CREATE TABLE my_data (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10),
    date DATE,
    value NUMERIC(18, 6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

3. **Create DAG in `dags/`**:
```python
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG('my_data_collection', ...) as dag:
    fetch_task = PythonOperator(
        task_id='fetch',
        python_callable=fetch_my_data
    )
```

4. **Add database insert function to `utils/db_utils.py`**

### Running Tests

```bash
# Run Airflow DAG test
docker exec quant_airflow_webserver airflow dags test market_data_collection 2026-01-20

# Check database integrity
psql -h localhost -p 5433 -U postgres -d market_data -c "SELECT COUNT(*) FROM ohclv_data;"
```

---

## 📝 Notes

- **Market Hours**: US market operates 9:30 AM - 4:00 PM EST, Mon-Fri
- **Data Delays**: Some data providers have ~15-20 minute delays
- **Historical Data**: yfinance provides up to 20 years of historical data
- **Rate Limits**: Free API tiers have request limits (check provider docs)
- **Timezone**: All times in UTC internally, configured as US/Eastern in market_calendar.py

---

## 📄 License

This is a personal project. Use as-is for learning and personal finance tracking.

---

## 🤝 Contributing

This is a personal project, but improvements are welcome. For any issues or suggestions, refer to the troubleshooting section.

---

## 📞 Support

For issues with:
- **Docker**: See [Docker documentation](https://docs.docker.com/)
- **PostgreSQL**: See [PostgreSQL documentation](https://www.postgresql.org/docs/)
- **Airflow**: See [Airflow documentation](https://airflow.apache.org/docs/)
- **Data Sources**: 
  - yfinance: https://github.com/ranaroussi/yfinance
  - FRED: https://fred.stlouisfed.org/
  - Alpha Vantage: https://www.alphavantage.co/

---

**Last Updated**: January 2026  
**Version**: 1.0
