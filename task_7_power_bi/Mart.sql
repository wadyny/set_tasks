CREATE TABLE mart.superstore_sales_mart AS
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
FROM
    core.orders_fact f
JOIN
    core.customers_dim c ON f.customer_fk = c.customer_pk
JOIN
    core.products_dim p ON f.product_fk = p.product_pk
JOIN
    core.geography_dim g ON f.geo_fk = g.geo_pk
WHERE
    f.order_date >= c.valid_from
    AND (f.order_date < c.valid_to OR c.valid_to IS NULL);


