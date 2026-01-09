INSERT INTO core.products_dim (product_id, category, sub_category, product_name)
SELECT DISTINCT
    s.product_id,
    s.category,
    s.sub_category,
    s.product_name
FROM
    stage.superstore_raw s
ON CONFLICT (product_id) DO NOTHING; 


INSERT INTO core.geography_dim (country, region, state, city, postal_code)
SELECT DISTINCT
    s.country,
    s.region,
    s.state,
    s.city,
    s.postal_code
FROM
    stage.superstore_raw s
ON CONFLICT (country, region, state, city) DO NOTHING; 


INSERT INTO core.customers_dim (customer_id, customer_name, segment, valid_from, valid_to, is_current)
SELECT DISTINCT
    s.customer_id,
    s.customer_name,
    s.segment,
    MIN(TO_DATE(s.order_date, 'MM/DD/YYYY')) AS first_seen_date,
    NULL::DATE AS valid_to,
    TRUE AS is_current
FROM
    stage.superstore_raw s
GROUP BY 1, 2, 3
ON CONFLICT (customer_id, valid_from) DO NOTHING;


INSERT INTO core.orders_fact (
    order_id, row_id, order_date, ship_date, ship_mode, sales, quantity, discount, profit,
    customer_fk, product_fk, geo_fk
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
    (SELECT customer_pk FROM core.customers_dim c WHERE c.customer_id = s.customer_id AND c.is_current = TRUE),
    (SELECT product_pk FROM core.products_dim p WHERE p.product_id = s.product_id),
    (SELECT geo_pk FROM core.geography_dim g
     WHERE g.country = s.country AND g.region = s.region AND g.state = s.state AND g.city = s.city)
FROM
    stage.superstore_raw s
ON CONFLICT (order_id, row_id) DO NOTHING;