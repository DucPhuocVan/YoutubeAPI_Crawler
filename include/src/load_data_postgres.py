from psycopg2 import sql
from airflow.providers.postgres.hooks.postgres import PostgresHook

class Postgres:
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

    def create_table(self, table_name: str, columns: dict):
        self.connect()
        column_definitions = ', '.join([f"{col} TEXT" for col in columns])
        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {table} (
                {columns}
            )
        """).format(
            table=sql.Identifier(table_name),
            columns=sql.SQL(column_definitions)
        )
        self.cursor.execute(create_table_query)
        self.connection.commit()
        self.close()

    def insert_data(self, table_name: str, df):
        self.connect()
        column_names = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = sql.SQL("""
            INSERT INTO {table} ({columns})
            VALUES ({placeholders})
        """).format(
            table=sql.Identifier(table_name),
            columns=sql.SQL(column_names),
            placeholders=sql.SQL(placeholders)
        )
        for i, row in df.iterrows():
            self.cursor.execute(insert_query, tuple(row))
        self.connection.commit()
        self.close()

    def truncate_table(self, table_name: str):
        self.connect()
        truncate_query = sql.SQL("TRUNCATE TABLE {table}").format(
            table=sql.Identifier(table_name)
        )
        self.cursor.execute(truncate_query)
        self.connection.commit()
        self.close()

    def load_to_postgres(self, tables: dict):
        for table_name, df in tables.items():
            # Create or replace table
            self.create_table(table_name, df.columns.to_list())
            # Truncate the table before inserting new data
            self.truncate_table(table_name)
            # Insert data into the table
            self.insert_data(table_name, df)