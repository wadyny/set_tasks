/*
 * Вывести количество фильмов в каждой категории, отсортировать по убыванию.
 */

SELECT 
    category.name AS category_name,
    COUNT(film.film_id) AS film_count
FROM 
    category 
    JOIN film_category fc ON category.category_id = fc.category_id
    JOIN film  ON fc.film_id = film.film_id
GROUP BY  category.name
ORDER BY  film_count DESC;
