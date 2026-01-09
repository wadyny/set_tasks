from db_connection import get_connection

class ExecutorQuery:
    def execute_query(self, query, params=None):
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                columns = [desc[0] for desc in cur.description]
                results = [dict(zip(columns, row)) for row in cur.fetchall()]
        return results

    def execute_update(self, query, params=None):
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
