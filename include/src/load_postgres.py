from psycopg2 import sql
from airflow.providers.postgres.hooks.postgres import PostgresHook
import hashlib
from datetime import datetime, timedelta
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

    def create_table_overwrite(self, table_name: str, columns: dict, unique_key: list):
        self.connect()
        column_definitions = ', '.join([f"{col} TEXT" for col in columns])
        key_definitions = ', '.join(unique_key)

        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {column_definitions},
                row_hash TEXT,
                CONSTRAINT {table_name}_PK PRIMARY KEY ({key_definitions})
            )
        """
        
        self.cursor.execute(create_table_query)
        self.connection.commit()
        self.close()

    def create_table_overwrite_daily(self, table_name: str, columns: dict, unique_key: list):
        self.connect()
        column_definitions = ', '.join([f"{col} TEXT" for col in columns])
        key_definitions = ', '.join(unique_key) + ', export_date'

        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {column_definitions},
                row_hash TEXT,
                CONSTRAINT {table_name}_PK PRIMARY KEY ({key_definitions})
            )
        """
        
        self.cursor.execute(create_table_query)
        self.connection.commit()
        self.close()

    def create_table_scd_type2(self, table_name: str, columns: dict):
        self.connect()
        column_definitions = ', '.join([f"{col} TEXT" for col in columns])
        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {table} (
                {columns},
                row_hash TEXT,
                start_date DATE DEFAULT CURRENT_DATE,
                end_date DATE,
                is_active BOOLEAN DEFAULT TRUE,
                CONSTRAINT {constraint} UNIQUE (row_hash)                           
            )
            """).format(
            table=sql.Identifier(table_name),
            columns=sql.SQL(column_definitions),
            constraint=sql.Identifier(f"{table_name}_row_hash")
        )
        self.cursor.execute(create_table_query)
        self.connection.commit()
        self.close()

    def row_hash(self, row):
        hash_input = ''.join(str(val) for val in row)
        return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()
    
    # insert append
    def insert_append(self, table_name: str, df):
        self.connect()

        df['row_hash'] = df.apply(lambda x: self.row_hash(x), axis=1)

        column_names = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))

        insert_overwrite_query = sql.SQL("""
                INSERT INTO {table} ({columns})
                VALUES ({placeholders})
            """).format(
            table=sql.Identifier(table_name),
            columns=sql.SQL(column_names),
            placeholders=sql.SQL(placeholders)
        )

        for _, row in df.iterrows():
            self.cursor.execute(insert_overwrite_query, tuple(row))
        
        self.connection.commit()
        self.close()

    # insert overwrite
    def insert_overwrite(self, table_name: str, df, unique_key: list):
        self.connect()

        df['row_hash'] = df.apply(lambda x: self.row_hash(x), axis=1)

        column_names = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        key_definitions = ', '.join(unique_key)
        column_update = ', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns if col not in unique_key])

        insert_overwrite_query = sql.SQL("""
                INSERT INTO {table} ({columns})
                VALUES ({placeholders})
                ON CONFLICT ({primary_key}) DO UPDATE SET {updated}
            """).format(
            table=sql.Identifier(table_name),
            columns=sql.SQL(column_names),
            placeholders=sql.SQL(placeholders),
            primary_key=sql.SQL(key_definitions),
            updated=sql.SQL(column_update)
        )

        for _, row in df.iterrows():
            self.cursor.execute(insert_overwrite_query, tuple(row))
        
        self.connection.commit()
        self.close()

    # insert overwrite daily
    def insert_overwrite_daily(self, table_name: str, df, unique_key: list):
        self.connect()

        df['row_hash'] = df.apply(lambda x: self.row_hash(x), axis=1)

        # overwrite 
        column_names = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        key_definitions = ', '.join(unique_key) + ', export_date'
        column_update = ', '.join([f"{col} = EXCLUDED.{col}" for col in df.columns if col not in unique_key])

        insert_overwrite_query = sql.SQL("""
                INSERT INTO {table} ({columns})
                VALUES ({placeholders})
                ON CONFLICT ({primary_key}) DO UPDATE SET {updated}
            """).format(
            table=sql.Identifier(table_name),
            columns=sql.SQL(column_names),
            placeholders=sql.SQL(placeholders),
            primary_key=sql.SQL(key_definitions),
            updated=sql.SQL(column_update)
        )

        for _, row in df.iterrows():
            self.cursor.execute(insert_overwrite_query, tuple(row))
        
        self.connection.commit()
        self.close()

    # upsert scd type 2
    def insert_scd_type2(self, table_name: str, df, unique_key: list):
        self.connect()

        current_date = datetime.now().date()
        df['row_hash'] = df.apply(lambda x: self.row_hash(x), axis=1)

        # Check if the table is empty
        self.cursor.execute(sql.SQL("SELECT COUNT(*) FROM {table}").format(
            table=sql.Identifier(table_name)
        ))
        table_empty = self.cursor.fetchone()[0] == 0

        # if the table is empty
        if table_empty:
            column_names = ', '.join(df.columns) + ', start_date, end_date, is_active'
            placeholders = ', '.join(['%s'] * len(df.columns)) + ', %s, NULL, TRUE'
            
            insert_query = sql.SQL("""
                INSERT INTO {table} ({columns})
                VALUES ({placeholders})
                """).format(
                table=sql.Identifier(table_name),
                columns=sql.SQL(column_names),
                placeholders=sql.SQL(placeholders)
            )

            for _, row in df.iterrows():
                self.cursor.execute(insert_query, tuple(row) + (current_date,))

        # if the table is not empty
        else:
            update_query = sql.SQL("""
                UPDATE {table}
                SET end_date = %s,
                    is_active = FALSE
                WHERE row_hash <> %s
                AND ({list_unique_key})
                """).format(
                table=sql.Identifier(table_name),
                list_unique_key=sql.SQL(' AND '.join(f"{key} = %s" for key in unique_key))
            )

            column_names = ', '.join(df.columns) + ', start_date, end_date, is_active'
            placeholders = ', '.join(['%s'] * len(df.columns)) + ', %s, NULL, TRUE'

            insert_query = sql.SQL("""
                INSERT INTO {table} ({columns})
                VALUES ({placeholders})
                ON CONFLICT (row_hash) DO NOTHING
                """).format(
                table=sql.Identifier(table_name),
                columns=sql.SQL(column_names),
                placeholders=sql.SQL(placeholders)
            )

            update_end_date_query = sql.SQL("""
                UPDATE {table}
                SET end_date = start_date
                WHERE end_date < start_date
                """).format(
                table=sql.Identifier(table_name)
            )

            for _, row in df.iterrows():
                unique_key_values = tuple(row[key] for key in unique_key)
                    
                self.cursor.execute(update_query, (current_date - timedelta(days=1), row['row_hash'], unique_key_values))
                self.cursor.execute(insert_query, tuple(row) + (current_date,))
                self.cursor.execute(update_end_date_query)
            
        self.connection.commit()
        self.close()

    def load_to_postgres_scd_type2(self, tables: dict, unique_key: list):
        for table_name, df in tables.items():
            self.create_table_scd_type2(table_name, df.columns.to_list())
            self.upsert_data_scd_type2(table_name, df, unique_key)

    def load_to_postgres_overwrite(self, tables: dict, unique_key: list):
        for table_name, df in tables.items():
            self.create_table_overwrite(table_name, df.columns.to_list(), unique_key)
            self.insert_overwrite(table_name, df, unique_key)

    def load_to_postgres_overwrite_daily(self, tables: dict, unique_key: list):
        for table_name, df in tables.items():
            self.create_table_overwrite_daily(table_name, df.columns.to_list(), unique_key)
            self.insert_overwrite_daily(table_name, df, unique_key)

    def load_to_postgres_append(self, tables: dict, unique_key: list):
        for table_name, df in tables.items():
            self.create_table_overwrite(table_name, df.columns.to_list(), unique_key)
            self.insert_append(table_name, df)