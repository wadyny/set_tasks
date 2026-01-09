/*
 Вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). 
 Отсортировать по количеству неактивных клиентов по убыванию.
 */
SELECT 
    city.city,
    SUM(customer.active) AS active_customers,
    SUM(1 - customer.active) AS inactive_customers
FROM city 
JOIN address ON city.city_id = address.city_id
JOIN customer ON address.address_id = customer.address_id
GROUP BY city.city
ORDER BY inactive_customers DESC;
