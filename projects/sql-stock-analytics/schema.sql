-- Schema for the stock market analytics database (SQLite dialect).
-- Three normalized tables + indexes tuned for the analysis queries.

DROP TABLE IF EXISTS trades;
DROP TABLE IF EXISTS daily_prices;
DROP TABLE IF EXISTS companies;

CREATE TABLE companies (
    ticker        TEXT PRIMARY KEY,
    company_name  TEXT NOT NULL,
    sector        TEXT NOT NULL
);

CREATE TABLE daily_prices (
    ticker      TEXT    NOT NULL REFERENCES companies (ticker),
    trade_date  DATE    NOT NULL,
    open        REAL    NOT NULL,
    high        REAL    NOT NULL,
    low         REAL    NOT NULL,
    close       REAL    NOT NULL,
    volume      INTEGER NOT NULL,
    PRIMARY KEY (ticker, trade_date)
);

CREATE TABLE trades (
    trade_id    INTEGER PRIMARY KEY,
    trade_date  DATE    NOT NULL,
    ticker      TEXT    NOT NULL REFERENCES companies (ticker),
    side        TEXT    NOT NULL CHECK (side IN ('BUY', 'SELL')),
    quantity    INTEGER NOT NULL CHECK (quantity > 0),
    fill_price  REAL    NOT NULL
);

CREATE INDEX idx_prices_date   ON daily_prices (trade_date);
CREATE INDEX idx_trades_ticker ON trades (ticker, trade_date);
