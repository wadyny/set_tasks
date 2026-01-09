/*
 *Вывести категорию фильмов, на которую потратили больше всего денег.
 */
SELECT 
    category.name AS category_name,
    SUM(payment.amount) AS total_revenue
FROM 
    payment  JOIN rental ON payment.rental_id = rental.rental_id
    JOIN inventory i ON rental.inventory_id = i.inventory_id
    JOIN film ON i.film_id = film.film_id
    JOIN film_category fc ON film.film_id = fc.film_id
    JOIN category ON fc.category_id = category.category_id
GROUP BY  category.name
ORDER BY  total_revenue DESC
LIMIT 1;
