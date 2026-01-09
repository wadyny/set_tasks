from data_loader import load_rooms, load_students
from executor_query import ExecutorQuery
from data_exporter import DataExporter
from dotenv import load_dotenv
from queries import QUERIES
import os


load_dotenv()


def main():
    students_path = os.getenv("STUDENTS_PATH")
    rooms_path = os.getenv("ROOMS_PATH")
    output_format = os.getenv("UPLOAD_DATA_FORMAT")
    output_path = os.getenv("UPLOAD_PATH")

    print("Loading rooms...")
    load_rooms(rooms_path)
    print("Loading students...")
    load_students(students_path)

    executor = ExecutorQuery()

    for query_name, query_sql in QUERIES.items():
        print(f"Executing query: {query_name}")
        results = executor.execute_query(query_sql)

        exporter = DataExporter(results)
        output_file = f"{output_path}{query_name}.{output_format}"
        exporter.export(output_format, output_file)
        print(f"Exported results to {output_file}")


if __name__ == '__main__':
    main()
