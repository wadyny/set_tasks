/*
 *Вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.
 */
SELECT 
    film.title
FROM  film LEFT JOIN inventory ON film.film_id = inventory.film_id
WHERE inventory.film_id IS NULL;
