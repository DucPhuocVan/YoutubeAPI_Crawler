from psycopg2 import sql
from airflow.providers.postgres.hooks.postgres import PostgresHook

class CheckPoint:
    def __init__(self, conn_id = 'postgres_conn'):
        self.conn_id = conn_id
        self.connection = None
        self.cursor = None

    def connect(self):
        hook = PostgresHook(self.conn_id)
        self.connection = hook.get_conn()
        self.cursor = self.connection.cursor()

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def create_checkpoint_table(self):
        self.connect()
        query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS checkpoint (
                table_name VARCHAR(255) PRIMARY KEY,
                last_updated TIMESTAMP                     
            );
        """)
        self.cursor.execute(query)
        self.connection.commit()
        self.close()

    def get_last_checkpoint(self, table_name):
        self.connect()
        query = sql.SQL("""
            SELECT last_updated 
            FROM checkpoint
            WHERE table_name = {table}
        """).format(
            table=sql.Literal(table_name),
        )
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        print(result)
        if result:
            return result[0]
        else:
            return None
        
    def update_checkpoint(self, table_name, last_updated):
        self.connect()
        query = sql.SQL("""
            INSERT INTO checkpoint (table_name, last_updated)
            VALUES ({table}, {last_updated})
            ON CONFLICT (table_name) DO UPDATE SET last_updated = EXCLUDED.last_updated;
        """).format(
            table=sql.Literal(table_name),
            last_updated=sql.Literal(last_updated),
        )
        self.cursor.execute(query)
        self.connection.commit()
        self.close()
