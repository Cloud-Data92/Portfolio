-- Star schema for the retail sales data warehouse (SQLite dialect).
-- Classic dimensional model: one fact table surrounded by dimensions,
-- joined on surrogate keys. This is the Kimball-style structure used
-- for BI/reporting workloads (and what Tableau connects to happily).

DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_store;
DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_date (
    date_key     INTEGER PRIMARY KEY,      -- YYYYMMDD surrogate, e.g. 20250314
    full_date    DATE NOT NULL UNIQUE,
    year         INTEGER NOT NULL,
    quarter      INTEGER NOT NULL,
    month        INTEGER NOT NULL,
    month_name   TEXT    NOT NULL,
    day_of_week  TEXT    NOT NULL,
    is_weekend   INTEGER NOT NULL CHECK (is_weekend IN (0, 1))
);

CREATE TABLE dim_store (
    store_key    INTEGER PRIMARY KEY AUTOINCREMENT,
    store_name   TEXT NOT NULL UNIQUE      -- normalized casing, e.g. 'Atlanta'
);

CREATE TABLE dim_product (
    product_key  INTEGER PRIMARY KEY AUTOINCREMENT,
    sku          TEXT NOT NULL UNIQUE,
    product_name TEXT NOT NULL,
    category     TEXT NOT NULL
);

CREATE TABLE fact_sales (
    sale_key     INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id     INTEGER NOT NULL UNIQUE,  -- natural key from source; enforces idempotent loads
    date_key     INTEGER NOT NULL REFERENCES dim_date (date_key),
    store_key    INTEGER NOT NULL REFERENCES dim_store (store_key),
    product_key  INTEGER NOT NULL REFERENCES dim_product (product_key),
    quantity     INTEGER NOT NULL CHECK (quantity > 0),
    unit_price   REAL    NOT NULL CHECK (unit_price > 0),
    revenue      REAL    NOT NULL          -- quantity * unit_price, precomputed for BI
);

CREATE INDEX idx_fact_date    ON fact_sales (date_key);
CREATE INDEX idx_fact_store   ON fact_sales (store_key);
CREATE INDEX idx_fact_product ON fact_sales (product_key);
