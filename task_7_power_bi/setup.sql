CREATE DATABASE superstore;

CREATE SCHEMA stage;
CREATE SCHEMA core;
CREATE SCHEMA mart;

CREATE TABLE stage.superstore_raw (
    row_id          INTEGER,
    order_id        VARCHAR(20),
    order_date      VARCHAR(50), 
    ship_date       VARCHAR(50),
    ship_mode       VARCHAR(20),
    customer_id     VARCHAR(20),
    customer_name   VARCHAR(100),
    segment         VARCHAR(50),
    country         VARCHAR(50),
    city            VARCHAR(50),
    state           VARCHAR(50),
    postal_code     VARCHAR(20),
    region          VARCHAR(50),
    product_id      VARCHAR(20),
    category        VARCHAR(50),
    sub_category    VARCHAR(50),
    product_name    VARCHAR(200),
    sales           NUMERIC,
    quantity        INTEGER,
    discount        NUMERIC,
    profit          NUMERIC
);

CREATE TABLE core.customers_dim (
    customer_pk         SERIAL PRIMARY KEY,  
    customer_id         VARCHAR(20) NOT NULL, 
    customer_name       VARCHAR(100) NOT NULL,
    segment             VARCHAR(50) NOT NULL,
    valid_from          DATE NOT NULL,
    valid_to            DATE,
    is_current          BOOLEAN NOT NULL,
    UNIQUE (customer_id, valid_from)
);

CREATE TABLE core.products_dim (
    product_pk          SERIAL PRIMARY KEY,
    product_id          VARCHAR(20) UNIQUE NOT NULL,
    category            VARCHAR(50) NOT NULL,
    sub_category        VARCHAR(50) NOT NULL,
    product_name        VARCHAR(200) NOT NULL
);

ALTER TABLE core.geography_dim
    ADD CONSTRAINT geography_dim_uniq
        UNIQUE (country, region, state, city, valid_from);

CREATE TABLE core.geography_dim (
    geo_pk              SERIAL PRIMARY KEY,
    country             VARCHAR(50) NOT NULL,
    region              VARCHAR(50) NOT NULL,
    state               VARCHAR(50) NOT NULL,
    city                VARCHAR(50) NOT NULL,
    postal_code         VARCHAR(20),
    UNIQUE (country, region, state, city)
);

CREATE TABLE core.orders_fact (
    order_id            VARCHAR(20) NOT NULL,
    row_id              INTEGER, 
    order_date          DATE NOT NULL,
    ship_date           DATE NOT NULL,
    ship_mode           VARCHAR(20) NOT NULL,
    customer_fk         INTEGER REFERENCES core.customers_dim(customer_pk),
    product_fk          INTEGER REFERENCES core.products_dim(product_pk),
    geo_fk              INTEGER REFERENCES core.geography_dim(geo_pk),
    sales               NUMERIC NOT NULL,
    quantity            INTEGER NOT NULL,
    discount            NUMERIC NOT NULL,
    profit              NUMERIC NOT NULL,

    PRIMARY KEY (order_id, row_id)
);

COPY stage.superstore_raw 
FROM 'G:\task_9_power_bi\SampleSuperstore.csv' 
WITH (
    FORMAT CSV,
    HEADER TRUE,
    DELIMITER ',',
    QUOTE '"',
    ENCODING 'WIN1251'
);

GRANT USAGE ON SCHEMA mart TO postgres;
GRANT SELECT ON ALL TABLES IN SCHEMA mart TO postgres;
GRANT SELECT ON mart.superstore_sales_mart to postgres;
GRANT USAGE ON SCHEMA stage, core, mart TO PUBLIC;
