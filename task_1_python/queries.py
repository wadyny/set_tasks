QUERIES = {
    "list_rooms_with_student_counts": """
    SELECT
        room.id,
        room.name,
        COUNT(student.id) AS student_count_in_room
    FROM room LEFT JOIN student on room.id = student.room
    GROUP BY room.id, room.name
    ORDER BY room.id
    """,

    "list_of_five_rooms_with_smallest_avg_age": """
        SELECT
            room.id,
            room.name,
            AVG(EXTRACT(YEAR FROM AGE(CURRENT_DATE, student.birthday))) AS avg_age
        FROM room
        LEFT JOIN student on room.id = student.room
        GROUP BY room.id, room.name
        ORDER BY avg_age
        LIMIT 5
    """,

    "list_of_five_rooms_with_max_age_difference": """
        SELECT
            room.id,
            room.name,
            MAX(EXTRACT(YEAR FROM AGE(CURRENT_DATE, student.birthday))) -
            MIN(EXTRACT(YEAR FROM AGE(CURRENT_DATE, student.birthday))) AS age_difference
        FROM room INNER JOIN student on room.id = student.room
        GROUP BY room.id, room.name
        ORDER BY age_difference DESC
        LIMIT 5
    """,

    "list_of_rooms_with_mixed_sex": """
        SELECT
            room.id,
            room.name
        FROM room INNER JOIN student on room.id = student.room
        GROUP BY room.id, room.name
        HAVING COUNT(DISTINCT student.sex) > 1
    """
}
