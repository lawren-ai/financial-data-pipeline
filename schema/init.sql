CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;


-- OHCLV Data Table (Main market data)
CREATE TABLE IF NOT EXISTS ohclv_data (
    id BIGSERIAL,
    ticker VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open NUMERIC(12, 4) NOT NULL,
    high NUMERIC(12, 4) NOT NULL,
    low NUMERIC(12, 4) NOT NULL,
    close NUMERIC(12, 4) NOT NULL,
    volume BIGINT NOT NULL,
    adjusted_for_splits BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, date)
);

-- Convert to TimescaleDB hypertable for time-series optimization
SELECT create_hypertable('ohclv_data', 'date',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE);

-- Create indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_ohclv_ticker ON ohclv_data(ticker);
CREATE INDEX IF NOT EXISTS idx_ohclv_date ON ohclv_data(date DESC);
CREATE INDEX IF NOT EXISTS idx_ohclv_ticker_date ON ohclv_data(ticker, date DESC);

-- Creaye continuous aggregate for daily market statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_market_stats
WITH (timescaledb.continuous) AS 
SELECT
    time_bucket('1 day', date) AS bucket,
    COUNT(DISTINCT ticker) as ticker_count,
    AVG(close) as avg_close,
    SUM(volume) as total_volume,
    PERCCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY close) as median_close

FROM ohclv_data
GROUP BY bucket 
WITH NO DATA;

-- Refresh policy for comtinuous aggregate
SELECT add_continuous_aggregate_policy('daily_market_stats',
start_offset => INTERVAL '3 days',
end_offset => INTERVAL '1 hour',
schedule_interval => INTERVAL '1 hour',
if_not_exists => TRUE);

-- Options Data table 
CREATE TABLE IF NOT EXIST options_data (
    id BIGSERIAL,
    ticker VARCHAR(10) NOT NULL,
    expiration DATE NOT NULL,
    strike NUMERIC(12, 4) NOT NULL,
    option_type VARCHAR(4) NOT NULL CHECK (option_type IN ('call', 'put')),
    bid NUMERIC(12, 4),
    ask NUMERIC(12, 4),
    last NUMERIC(12, 4),
    volume INTEGER,
    open_interest INTEGER,
    implied_volatility NUMERIC(8, 6),
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, expiration, strike, option_type, timestamp)
);

-- Convert to hypertable
SELECT create_hyper_table('options_data', 'timestamp',
chunk_time_interval => INTERVAL '1 day',
if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_options_ticker_exp ON options_data(ticker, expiration);
CREATE INDEX IF NOT EXISTS idx_options_timestamp ON options_data(timestamp DESC);


-- Economic Indicators Table
CREATE TABLE IF NOT EXISTS economic_indicators (
    id SERIAL PRIMARY KEY,
    indicator_code VARCHAR(20) NOT NULL,
    indicator_name VARCHAR(100),
    date DATE NOT NULL,
    value NUMERIC(18, 6),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(indicator_code, date)
);


CREATE INDEX IF NOT EXISTS idx_econ_code_date ON economic_indicators(indicator_code, date DESC);


-- Corporate Actions Table

-- Dividends
CREATE TABLE IF NOT EXISTS dividends (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    ex_date DATE NOT NULL,
    payment_date DATE,
    amount NUMERIC(12, 4) NOT NULL,
    currency VARCHAR(3) DEFAULT 'USD',
    dividend_type VARCHAR(20),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ticker, ex_date)
);

CREATE INDEX IF NOT EXISTS idx_dividends_ticker ON dividends(ticker);
CREATE INDEX IF NOT EXISTS idx_dividends_ex_date ON dividends(ex_date DESC);

-- Stock Splits
CREATE TABLE IF NOT EXISTS stock_splits (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    ex_date DATE NOT NULL,
    split_ratio NUMERIC(10, 6) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ticker, ex_date)
);

CREATE INDEX IF NOT EXISTS idx_splits_ticker ON stock_splits(ticker);
CREATE INDEX IF NOT EXISTS idx_splits_ex_date ON stock_splits(ex_date DESC);

-- Stock Universe table
CREATE TABLE IF NOT EXISTS stock_universe (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) UNIQUE NOT NULL,
    company_name VARCHAR(200),
    sector VARCHAR(200),
    industry VARCHAR(100),
    market_cap BIGINT,
    is_active BOOLEAN DEFAULT TRUE,
    added_date DATE NOT NULL DEFAULT CURRENT_DATE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_universe_ticker ON stock_universe(ticker);
CREATE INDEX IF NOT EXISTS idx_universe_sector ON stock_universe(sector);
CREATE INDEX IF NOT EXISTS idx_universe_active ON stock_universe(is_active);


-- Pipeline Metrics Table (for monitoring)
CREATE TABLE IF NOT EXISTS pipeine_metrics (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    metric_name VARCHAR(50) NOT NULL,
    metric_value NUMERIC(18, 4),
    metadata JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_metrics_date ON pipeline_metrics(date DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_name ON pipeline_metrics(metric_name);

-- Data Quality Logs
CREATE TABLE IF NOT EXISTS data_quality_logs (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100) NOT NULL,
    run_id VARCHAR(100) NOT NULL,
    task_id VARCHAR(100),
    validation_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    data_type VARCHAR(50),
    validation_success BOOLEAN,
    error_count INTEGER,
    warning_count INTEGER,
    errors_detail JSONB,
    warnings_detail JSONB,
    records_validated INTEGER
);

CREATE INDEX IF NOT EXISTS idx_quality_dag_run ON data_quality_logs(dag_id, run_id);
CREATE INDEX IF NOT EXISTS idx_quality_timestamp ON data_quality_logs(validation_timestamp DESC);


-- Market calendar (for holiday tracking)
CREATE TABLE IF NOT EXISTS market_calendar (
    id SERIAL PRIMARY KEY,
    date DATE UNIQUE NOT NULL,
    is_trading_day BOOLEAN NOT NULL,
    market VARCHAR(10) DEFAULT 'NYSE',
    holiday_name VARCHAR(100),
    early_close TIME,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_calendar_date ON market_calendar(date);

-- Populate with common US market holidays (example for 2026-2028)
INSERT INTO market_calendar (date, is_trading_day, holiday_name) VALUES 
('2024-01-01', FALSE, 'New Year''s Day'),
('2024-01-15', FALSE, 'Martin Luther King Jr. Day'),
('2024-02-19', FALSE, 'Presidents'' Day'),
('2024-03-29', FALSE, 'Good Friday'),
('2024-05-27', FALSE, 'Memorial Day'),
('2024-06-19', FALSE, 'Juneteenth'),
('2024-07-04', FALSE, 'Independence Day'),
('2024-09-02', FALSE, 'Labor Day'),
('2024-11-28', FALSE, 'Thanksgiving'),
('2024-12-25', FALSE, 'Christmas Day'),
('2025-01-01', FALSE, 'New Year''s Day'),
('2025-01-20', FALSE, 'Martin Luther King Jr. Day'),
('2025-02-17', FALSE, 'Presidents'' Day'),
('2025-04-18', FALSE, 'Good Friday'),
('2025-05-26', FALSE, 'Memorial Day'),
('2025-06-19', FALSE, 'Juneteenth'),
('2025-07-04', FALSE, 'Independence Day'),
('2025-09-01', FALSE, 'Labor Day'),
('2025-11-27', FALSE, 'Thanksgiving'),
('2025-12-25', FALSE, 'Christmas Day')
ON CONFLICT (date) DO NOTHING;


-- Helper Functions

-- Funtion to get latest price for a ticker
CREATE OR REPLACE FUNCTION get_latest_price(p_ticer VARCHAR)
RETURNS TABLE (
    ticker VARCHAR,
    date DATE,
    close NUMERIC,
    volume BIGINT
) AS $$
BEGIN 
    RETURN QUERY
    SELECT o.ticker, o.date, o.close, o.volume
    FROM ohclv_data o,
    WHERE o.ticker = p.ticker
    ORDER BY o.date DESC
    LIMIT 1;

END;
$$ LANGUAGE plpgsql;

-- Function to calculate returns
CREATE OR REPLACE FUNCTION calculate_returns(
    p_ticker VARCHAR,
    p_start_date DATE,
    p_end_date date
)

RETURNS NUMERIC AS $$
DECLARE
    start_price NUMERIC;
    end_price NUMERIC;
    returns NUMERIC;
BEGIN
    SELECT close INTO start_price
    FROM ohclv_data
    WHERE ticker = p_ticker AND data >= p_start_date
    ORDER BY date ASC 
    LIMIT 1;

    SELECT close INTO end_price
    FROM ohclv_data
    WHERE ticker = p_ticker AND date <= p_end_date
    ORDER BY date DESC
    LIMIT 1;

    IF start_price IS NULL OR end_price IS NULL OR start_price = 0 THEN
        RETURN NULL;
    END IF;

    returns := ((end_price - start_price) / start_price) * 100;
    RETURN returns

END;
$$ LANGUAGE plpgsql;

-- Data Retention Policies (TimescaleDB)
-- Retain detailed options data for 90 days

SELECT add_retention_policy('options_data',
    INTERVAL '90 days',
    if_not_exists => TRUE
    );

-- Archive old OHCLV data to compressed chunks after 2 years
SELECT add_compression_policy('ohclv_data',
INTERVAL '730 days',
if_not_exists => TRUE
);


-- Views for common queries
-- Latest prices view
CREATE OR REPLACE VIEW v_latest_prices AS
SELECT DISTINCT ON (ticker)
    ticker,
    date,
    close as price,
    volume,
    ((close - open) / open * 100) as day_change_pct
FROM ohclv_data
ORDER BY ticker, date DESC

-- Top movers view (daily)
CREATE OR REPLACE VIEW v_top_movers AS
SELECT 
    ticker,
    date,
    close,
    ((close - open) / open * 100) as change_pct,
    volume
FROM ohclv_data
WHERE date = CURRENT_DATE - INTERVAL '1 day'
ORDER BY ABS((close - open) / open) DESC
LIMIT 50;


-- Pipeline health view
CREATE OR REPLACE VIEW v_pipeline_health AS
SELECT
    date,
    metric_name,
    metric_value,
    created_at
FROM pipeline_metrics
WHERE data >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY date DESC, created_at DESC;
