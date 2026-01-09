import json
from executor_query import ExecutorQuery

executor = ExecutorQuery()

def load_rooms(filepath: str):
    with open(filepath, 'r', encoding='utf-8') as f:
        rooms = json.load(f)

    for room in rooms:
        executor.execute_update(
            "INSERT INTO room (id, name) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING",
            (room['id'], room['name'])
        )

def load_students(filepath: str):
    with open(filepath, 'r', encoding='utf-8') as f:
        students = json.load(f)

    for student in students:
        executor.execute_update(
            """
            INSERT INTO student (id, name, birthday, sex, room) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            """,
            (
                student['id'],
                student['name'],
                student['birthday'],
                student['sex'],
                student['room']
            )
        )
