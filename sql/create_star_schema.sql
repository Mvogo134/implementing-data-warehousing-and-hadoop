-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS FactSales;
DROP TABLE IF EXISTS DimDate;
DROP TABLE IF EXISTS DimCustomer;
DROP TABLE IF EXISTS DimProduct;
DROP TABLE IF EXISTS DimStore;

CREATE TABLE DimDate (
    date_id       INTEGER      PRIMARY KEY,                 -- e.g. 20250401
    full_date     DATE         NOT NULL UNIQUE,
    day_name      VARCHAR(9)   NOT NULL,                    -- Monday, Tuesday...
    day_of_week   SMALLINT     NOT NULL CHECK (day_of_week BETWEEN 1 AND 7),
    day_of_month  SMALLINT     NOT NULL CHECK (day_of_month BETWEEN 1 AND 31),
    week_of_year  SMALLINT     NOT NULL CHECK (week_of_year BETWEEN 1 AND 53),
    month         SMALLINT     NOT NULL CHECK (month BETWEEN 1 AND 12),
    month_name    VARCHAR(9)   NOT NULL,
    quarter       SMALLINT     NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    year          SMALLINT     NOT NULL,
    is_weekend    BOOLEAN      NOT NULL,
    holiday_name  VARCHAR(40)                               -- NULL when none
);

-- ---------------------------------------------------------------------------
-- Dimension: DimCustomer
-- One row per customer who has ever registered with the company.
-- ---------------------------------------------------------------------------
CREATE TABLE DimCustomer (
    customer_id    VARCHAR(10)  PRIMARY KEY,                -- C001..C020
    full_name      VARCHAR(60)  NOT NULL,
    gender         CHAR(1)      CHECK (gender IN ('M', 'F')),
    email          VARCHAR(100) NOT NULL UNIQUE,
    city           VARCHAR(40)  NOT NULL,
    state_province VARCHAR(40)  NOT NULL,
    country        VARCHAR(40)  NOT NULL,
    region         VARCHAR(30)  NOT NULL,                   -- e.g. Europe
    signup_date    DATE         NOT NULL
);

-- ---------------------------------------------------------------------------
-- Dimension: DimProduct
-- One row per product sold in the catalogue.
-- ---------------------------------------------------------------------------
CREATE TABLE DimProduct (
    product_id     VARCHAR(10)   PRIMARY KEY,               -- P001..P020
    product_name   VARCHAR(80)   NOT NULL,
    category       VARCHAR(30)   NOT NULL,
    subcategory    VARCHAR(30)   NOT NULL,
    brand          VARCHAR(30)   NOT NULL,
    unit_price_USD DECIMAL(10,2) NOT NULL CHECK (unit_price_USD > 0),
    is_active      BOOLEAN       NOT NULL DEFAULT TRUE
);

-- ---------------------------------------------------------------------------
-- Dimension: DimStore
-- One row per sales channel / physical store.
-- Global online channels use city/state/country/region = 'Global'.
-- ---------------------------------------------------------------------------
CREATE TABLE DimStore (
    store_id       VARCHAR(10)  PRIMARY KEY,                -- S001..S010
    store_name     VARCHAR(60)  NOT NULL,
    channel        VARCHAR(15)  NOT NULL
                   CHECK (channel IN ('Online', 'Mobile App',
                                      'Retail Store', 'Marketplace')),
    city           VARCHAR(40)  NOT NULL,
    state_province VARCHAR(40)  NOT NULL,
    country        VARCHAR(40)  NOT NULL,
    region         VARCHAR(30)  NOT NULL,
    open_date      DATE         NOT NULL
);

-- ---------------------------------------------------------------------------
-- Fact table: FactSales
-- GRAIN: one row = one product line within one customer order.
--   (An order with three products produces three FactSales rows.)
-- ---------------------------------------------------------------------------
CREATE TABLE FactSales (
    sale_id        BIGINT        PRIMARY KEY,               -- surrogate
    order_id       VARCHAR(20)   NOT NULL,                  -- ORD-2025-0001
    order_line_no  SMALLINT      NOT NULL,                  -- 1, 2, 3...
    date_id        INTEGER       NOT NULL REFERENCES DimDate(date_id),
    customer_id    VARCHAR(10)   NOT NULL REFERENCES DimCustomer(customer_id),
    product_id     VARCHAR(10)   NOT NULL REFERENCES DimProduct(product_id),
    store_id       VARCHAR(10)   NOT NULL REFERENCES DimStore(store_id),
    quantity       SMALLINT      NOT NULL CHECK (quantity > 0),
    unit_price     DECIMAL(10,2) NOT NULL CHECK (unit_price > 0),
    discount_rate  DECIMAL(3,2)  NOT NULL DEFAULT 0.00
                                 CHECK (discount_rate BETWEEN 0 AND 1),
    sales_amount   DECIMAL(12,2) NOT NULL CHECK (sales_amount > 0),
    CONSTRAINT uq_order_line UNIQUE (order_id, order_line_no)
);

-- ---------------------------------------------------------------------------
-- Post-load structural checks
-- ---------------------------------------------------------------------------
SELECT 'fact sales foreign keys satisfied: ' || COUNT(*) AS check_msg
FROM FactSales fs
WHERE fs.customer_id NOT IN (SELECT customer_id FROM DimCustomer)
   OR fs.product_id  NOT IN (SELECT product_id  FROM DimProduct)
   OR fs.store_id    NOT IN (SELECT store_id    FROM DimStore)
   OR fs.date_id     NOT IN (SELECT date_id     FROM DimDate);
