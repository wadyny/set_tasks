/*
 *Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. 
 *Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.
 */
WITH actor_film_counts AS (
    SELECT 
        actor.actor_id,
        actor.first_name,
        actor.last_name,
        COUNT(fa.film_id) AS film_count
    FROM actor JOIN film_actor fa ON actor.actor_id = fa.actor_id
    JOIN film_category fc ON fa.film_id = fc.film_id
    JOIN category ON fc.category_id = category.category_id
    WHERE category.name = 'Children'
    GROUP BY actor.actor_id, actor.first_name, actor.last_name
),
ranked_actors AS (
    SELECT *, RANK() OVER (ORDER BY film_count DESC) AS rnk
    FROM actor_film_counts
)
SELECT first_name, last_name, film_count
FROM ranked_actors
WHERE rnk <= 3
ORDER BY film_count DESC;
