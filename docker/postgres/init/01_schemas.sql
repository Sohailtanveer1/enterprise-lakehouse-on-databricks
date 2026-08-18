-- NorthPeak source database
--
-- Two schemas standing in for two separate operational systems:
--   erp      -> orders, order_items, payments   (captured by Debezium)
--   commerce -> customers, promotions           (served by the REST API)
--
-- In production these would be separate databases on separate hosts. One
-- Postgres instance keeps the local footprint to ~300 MB; the pipeline cannot
-- tell the difference because it only ever sees files in GCS.

CREATE SCHEMA IF NOT EXISTS erp;
CREATE SCHEMA IF NOT EXISTS commerce;

-- ---------------------------------------------------------------- ERP
CREATE TABLE erp.orders (
    order_id         TEXT PRIMARY KEY,
    customer_id      TEXT        NOT NULL,
    order_date       TIMESTAMPTZ NOT NULL,
    order_status     TEXT        NOT NULL,
    payment_status   TEXT        NOT NULL,
    shipping_status  TEXT        NOT NULL,
    store_id         TEXT,
    region           TEXT,
    promotion_id     TEXT,
    currency         TEXT        NOT NULL DEFAULT 'USD',
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE erp.order_items (
    order_id          TEXT        NOT NULL,
    order_line_number INT         NOT NULL,
    product_id        TEXT        NOT NULL,
    quantity          INT         NOT NULL,
    unit_price        NUMERIC(10,2) NOT NULL,
    discount_amount   NUMERIC(10,2) NOT NULL DEFAULT 0,
    tax_amount        NUMERIC(10,2) NOT NULL DEFAULT 0,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, order_line_number)
);

CREATE TABLE erp.payments (
    payment_id     TEXT PRIMARY KEY,
    order_id       TEXT        NOT NULL,
    payment_method TEXT        NOT NULL,
    payment_amount NUMERIC(10,2) NOT NULL,
    payment_status TEXT        NOT NULL,
    attempt_number INT         NOT NULL DEFAULT 1,
    paid_at        TIMESTAMPTZ,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- REPLICA IDENTITY FULL makes Postgres write the complete pre-image of every
-- updated or deleted row into the WAL. Without it Debezium's `before` field
-- contains only the primary key, and SCD Type 2 cannot see what changed.
-- Cost: larger WAL volume. At this scale that is the right trade.
ALTER TABLE erp.orders      REPLICA IDENTITY FULL;
ALTER TABLE erp.order_items REPLICA IDENTITY FULL;
ALTER TABLE erp.payments    REPLICA IDENTITY FULL;

CREATE INDEX ON erp.orders (updated_at);
CREATE INDEX ON erp.orders (customer_id);
CREATE INDEX ON erp.order_items (order_id);
CREATE INDEX ON erp.payments (order_id);

-- ----------------------------------------------------------- Commerce
CREATE TABLE commerce.customers (
    customer_id      TEXT PRIMARY KEY,
    first_name       TEXT,
    last_name        TEXT,
    email            TEXT,
    phone            TEXT,
    address_line1    TEXT,
    city             TEXT,
    state            TEXT,
    country          TEXT DEFAULT 'US',
    postal_code      TEXT,
    signup_date      DATE,
    customer_segment TEXT,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE commerce.promotions (
    promotion_id   TEXT PRIMARY KEY,
    promotion_name TEXT,
    promo_type     TEXT,
    discount_value NUMERIC(10,2),
    start_date     DATE,
    end_date       DATE,
    channel        TEXT,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The API pages by updated_at, so this index is load-bearing, not decorative.
CREATE INDEX ON commerce.customers (updated_at);
CREATE INDEX ON commerce.promotions (updated_at);
