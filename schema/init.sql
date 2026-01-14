-- 1. Enable TimescaleDB (Only if available)
DO $$ 
BEGIN
    CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
EXCEPTION WHEN others THEN 
    RAISE NOTICE 'TimescaleDB extension not found. Skipping hypertable conversions.';
END $$;

-- --- OHCLV Data Table ---
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

-- Convert to Hypertable (Conditional)
DO $$ 
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        PERFORM create_hypertable('ohclv_data', 'date', chunk_time_interval => INTERVAL '1 month', if_not_exists => TRUE);
    END IF;
END $$;

-- --- Options Data Table ---
-- FIXED: Added 'S' to EXISTS
CREATE TABLE IF NOT EXISTS options_data (
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

-- FIXED: create_hypertable (removed underscore)
DO $$ 
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        PERFORM create_hypertable('options_data', 'timestamp', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);
    END IF;
END $$;

-- --- Economic Indicators ---
CREATE TABLE IF NOT EXISTS economic_indicators (
    id SERIAL PRIMARY KEY,
    indicator_code VARCHAR(20) NOT NULL,
    indicator_name VARCHAR(100),
    date DATE NOT NULL,
    value NUMERIC(18, 6),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(indicator_code, date)
);

-- --- Corporate Actions ---
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

CREATE TABLE IF NOT EXISTS stock_splits (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL,
    ex_date DATE NOT NULL,
    split_ratio NUMERIC(10, 6) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ticker, ex_date)
);

-- --- Monitoring Tables ---
-- FIXED: Typo in table name 'pipeine_metrics' -> 'pipeline_metrics'
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    metric_name VARCHAR(50) NOT NULL,
    metric_value NUMERIC(18, 4),
    metadata JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- --- Market Calendar ---
CREATE TABLE IF NOT EXISTS market_calendar (
    id SERIAL PRIMARY KEY,
    date DATE UNIQUE NOT NULL,
    is_trading_day BOOLEAN NOT NULL,
    market VARCHAR(10) DEFAULT 'NYSE',
    holiday_name VARCHAR(100),
    early_close TIME,
    notes TEXT
);

-- --- FIXED FUNCTIONS ---

-- FIXED: Typo in p_ticer, and comma before WHERE
CREATE OR REPLACE FUNCTION get_latest_price(p_ticker VARCHAR)
RETURNS TABLE (ticker VARCHAR, date DATE, close NUMERIC, volume BIGINT) AS $$
BEGIN 
    RETURN QUERY
    SELECT o.ticker, o.date, o.close, o.volume
    FROM ohclv_data o
    WHERE o.ticker = p_ticker
    ORDER BY o.date DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- FIXED: Missing semicolon and typo in 'data' column
CREATE OR REPLACE FUNCTION calculate_returns(p_ticker VARCHAR, p_start_date DATE, p_end_date DATE)
RETURNS NUMERIC AS $$
DECLARE
    start_price NUMERIC;
    end_price NUMERIC;
BEGIN
    SELECT close INTO start_price FROM ohclv_data WHERE ticker = p_ticker AND date >= p_start_date ORDER BY date ASC LIMIT 1;
    SELECT close INTO end_price FROM ohclv_data WHERE ticker = p_ticker AND date <= p_end_date ORDER BY date DESC LIMIT 1;
    IF start_price IS NULL OR end_price IS NULL OR start_price = 0 THEN RETURN NULL; END IF;
    RETURN ((end_price - start_price) / start_price) * 100;
END;
$$ LANGUAGE plpgsql;

-- --- FIXED VIEWS ---
-- FIXED: Missing semicolon before new view
CREATE OR REPLACE VIEW v_latest_prices AS
SELECT DISTINCT ON (ticker)
    ticker, date, close as price, volume,
    ((close - open) / NULLIF(open,0) * 100) as day_change_pct
FROM ohclv_data
ORDER BY ticker, date DESC;

CREATE OR REPLACE VIEW v_top_movers AS
SELECT ticker, date, close, ((close - open) / NULLIF(open,0) * 100) as change_pct, volume
FROM ohclv_data
WHERE date = (SELECT MAX(date) FROM ohclv_data)
ORDER BY ABS((close - open) / NULLIF(open,0)) DESC
LIMIT 50;

-- --- DATA POPULATION ---
INSERT INTO market_calendar (date, is_trading_day, holiday_name) VALUES 
('2026-01-01', FALSE, 'New Year''s Day'),
('2026-01-14', TRUE, 'Live Testing Day'),
('2026-01-19', FALSE, 'MLK Day')
ON CONFLICT (date) DO NOTHING;