-- Raw market ticks from Kafka
CREATE TABLE IF NOT EXISTS market_raw (
    id          SERIAL PRIMARY KEY,
    symbol      VARCHAR(10) NOT NULL,
    ts          TIMESTAMP NOT NULL,
    open        NUMERIC(12,4),
    high        NUMERIC(12,4),
    low         NUMERIC(12,4),
    close       NUMERIC(12,4),
    volume      BIGINT,
    created_at  TIMESTAMP DEFAULT NOW()
);

-- Processed ticks with indicators
CREATE TABLE IF NOT EXISTS market_processed (
    id            SERIAL PRIMARY KEY,
    symbol        VARCHAR(10) NOT NULL,
    ts            TIMESTAMP NOT NULL,
    close         NUMERIC(12,4),
    volume        BIGINT,
    sma_20        NUMERIC(12,4),
    sma_50        NUMERIC(12,4),
    rsi_14        NUMERIC(8,4),
    volume_spike  BOOLEAN DEFAULT FALSE,
    signal        VARCHAR(10) DEFAULT 'NEUTRAL',
    created_at    TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_symbol_ts       ON market_raw(symbol, ts DESC);
CREATE INDEX IF NOT EXISTS idx_processed_symbol_ts ON market_processed(symbol, ts DESC);
