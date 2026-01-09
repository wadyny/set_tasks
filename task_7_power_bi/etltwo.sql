TRUNCATE TABLE stage.superstore_raw;

COPY stage.superstore_raw 
FROM 'G:\task_9_power_bi\SecondaryLoad.csv' 
WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', QUOTE '"', ENCODING 'WIN1251');

INSERT INTO core.customers_dim (customer_id, customer_name, segment, valid_from, valid_to, is_current)
SELECT DISTINCT ON (s.customer_id)
    s.customer_id,
    s.customer_name,
    s.segment,
    MIN(TO_DATE(s.order_date, 'MM/DD/YYYY')) OVER (PARTITION BY s.customer_id) AS valid_from,
    NULL AS valid_to,
    TRUE AS is_current
FROM stage.superstore_raw s
LEFT JOIN core.customers_dim c ON c.customer_id = s.customer_id
WHERE c.customer_id IS NULL
ORDER BY s.customer_id, TO_DATE(s.order_date, 'MM/DD/YYYY');

UPDATE core.customers_dim c
SET customer_name = s.customer_name
FROM stage.superstore_raw s
WHERE c.customer_id = s.customer_id
  AND c.is_current = TRUE
  AND c.customer_name <> s.customer_name; 


WITH changed_customers AS (
    SELECT DISTINCT 
        s.customer_id,
        MIN(TO_DATE(s.order_date, 'MM/DD/YYYY')) AS change_date
    FROM stage.superstore_raw s
    JOIN core.customers_dim c 
        ON c.customer_id = s.customer_id AND c.is_current = TRUE
    WHERE c.segment <> s.segment
    GROUP BY s.customer_id
),
closed_old_versions AS (
    UPDATE core.customers_dim c
    SET 
        valid_to = cc.change_date - INTERVAL '1 day', 
        is_current = FALSE
    FROM changed_customers cc
    WHERE c.customer_id = cc.customer_id 
      AND c.is_current = TRUE
    RETURNING c.customer_id 
),
new_versions_to_insert AS (
    SELECT DISTINCT ON (s.customer_id)
        s.customer_id,
        s.customer_name,
        s.segment,
        cc.change_date AS valid_from,
        NULL::DATE AS valid_to,
        TRUE AS is_current
    FROM stage.superstore_raw s
    JOIN changed_customers cc ON s.customer_id = cc.customer_id
    WHERE TO_DATE(s.order_date, 'MM/DD/YYYY') >= cc.change_date
    ORDER BY s.customer_id, TO_DATE(s.order_date, 'MM/DD/YYYY') ASC 
)

INSERT INTO core.customers_dim (customer_id, customer_name, segment, valid_from, valid_to, is_current)
SELECT * FROM new_versions_to_insert
ON CONFLICT (customer_id, valid_from) DO NOTHING;

WITH new_geo AS (
    SELECT DISTINCT 
        s.country, s.region, s.state, s.city, s.postal_code,
        MIN(TO_DATE(s.order_date, 'MM/DD/YYYY')) AS valid_from
    FROM stage.superstore_raw s
    GROUP BY 1,2,3,4,5
)
INSERT INTO core.geography_dim (country, region, state, city, postal_code, valid_from, valid_to, is_current)
SELECT n.country, n.region, n.state, n.city, n.postal_code, n.valid_from, NULL, TRUE
FROM new_geo n
ON CONFLICT (country, region, state, city, valid_from) DO NOTHING;


INSERT INTO core.orders_fact (
    order_id, 
    row_id, 
    order_date, 
    ship_date, 
    ship_mode, 
    sales, 
    quantity, 
    discount, 
    profit,
    customer_fk, 
    product_fk, 
    geo_fk
)
SELECT
    s.order_id,
    s.row_id,
    TO_DATE(s.order_date, 'MM/DD/YYYY') AS order_date,
    TO_DATE(s.ship_date, 'MM/DD/YYYY') AS ship_date,
    s.ship_mode,
    s.sales,
    s.quantity,
    s.discount,
    s.profit,
    (SELECT customer_pk 
     FROM core.customers_dim c 
     WHERE c.customer_id = s.customer_id 
       AND c.is_current = TRUE),

    (SELECT product_pk 
     FROM core.products_dim p 
     WHERE p.product_id = s.product_id),
     

    (SELECT geo_pk 
     FROM core.geography_dim g
     WHERE g.country = s.country 
       AND g.region = s.region 
       AND g.state = s.state 
       AND g.city = s.city
     LIMIT 1) 
FROM
    stage.superstore_raw s
ON CONFLICT (order_id, row_id) DO NOTHING;


TRUNCATE TABLE mart.superstore_sales_mart;

INSERT INTO mart.superstore_sales_mart
SELECT
    f.order_id,
    f.order_date,
    f.ship_date,
    f.ship_mode,
    f.sales,
    f.quantity,
    f.discount,
    f.profit,
    c.customer_id,
    c.customer_name,
    c.segment,
    p.product_id,
    p.category,
    p.sub_category,
    p.product_name,
    g.country,
    g.region,
    g.state,
    g.city,
    g.postal_code,
    c.valid_from AS customer_version_from,
    c.valid_to AS customer_version_to
FROM core.orders_fact f
JOIN core.customers_dim c ON f.customer_fk = c.customer_pk 
    AND f.order_date >= c.valid_from 
    AND (f.order_date <= c.valid_to OR c.valid_to IS NULL)
JOIN core.products_dim p ON f.product_fk = p.product_pk
JOIN core.geography_dim g ON f.geo_fk = g.geo_pk;