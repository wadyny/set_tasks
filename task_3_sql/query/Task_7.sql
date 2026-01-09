/*
Вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах
 (customer.address_id в этом city), и которые начинаются на букву “a”. То же самое сделать
  для городов в которых есть символ “-”. Написать все в одном запросе.
 */
WITH city_groups AS (
    SELECT
        city.city,
        CASE 
            WHEN city.city ILIKE 'a%' THEN 'starts_with_a'
            WHEN city.city LIKE '%-%' THEN 'has_dash'
        END AS city_group,
        customer.customer_id
    FROM city 
    JOIN address ON city.city_id = address.city_id
    JOIN customer ON address.address_id = customer.address_id
    WHERE city.city ILIKE 'a%' OR city.city LIKE '%-%'
),
category_rentals AS (
    SELECT
        cg.city_group,
        category .name AS category_name,
        SUM(film.rental_duration) AS total_hours
    FROM city_groups cg
    JOIN rental ON cg.customer_id = rental.customer_id
    JOIN inventory i ON rental.inventory_id = i.inventory_id
    JOIN film ON i.film_id = film.film_id
    JOIN film_category fc ON film.film_id = fc.film_id
    JOIN category  ON fc.category_id = category.category_id
    GROUP BY cg.city_group, category.name
),
ranked_categories AS (
    SELECT *, RANK() OVER (PARTITION BY city_group ORDER BY total_hours DESC) AS rnk
    FROM category_rentals
)
SELECT city_group, category_name, total_hours
FROM ranked_categories
WHERE rnk = 1;


DROP TABLE IF EXISTS ranked_categories;
DROP TABLE IF EXISTS category_rentals;
DROP TABLE IF EXISTS city_groups;

CREATE TEMPORARY TABLE city_groups AS
SELECT
    city.city,
    CASE 
        WHEN city.city ILIKE 'a%' THEN 'starts_with_a'
        WHEN city.city LIKE '%-%' THEN 'has_dash'
    END AS city_group,
    customer.customer_id
FROM city 
JOIN address ON city.city_id = address.city_id
JOIN customer ON address.address_id = customer.address_id
WHERE city.city ILIKE 'a%' OR city.city LIKE '%-%';

CREATE TEMPORARY TABLE category_rentals AS
SELECT
    cg.city_group,
    category.name AS category_name,
    SUM(film.rental_duration) AS total_hours
FROM city_groups cg
JOIN rental ON cg.customer_id = rental.customer_id
JOIN inventory i ON rental.inventory_id = i.inventory_id
JOIN film ON i.film_id = film.film_id
JOIN film_category fc ON film.film_id = fc.film_id
JOIN category ON fc.category_id = category.category_id
GROUP BY cg.city_group, category.name;


CREATE TEMPORARY TABLE ranked_categories AS
SELECT
    city_group,
    category_name,
    total_hours,
    RANK() OVER (PARTITION BY city_group ORDER BY total_hours DESC) AS rnk
FROM category_rentals;

SELECT city_group, category_name, total_hours
FROM ranked_categories
WHERE rnk = 1;

select * from city_groups				# пример переиспользования 

