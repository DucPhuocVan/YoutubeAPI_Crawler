from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud.exceptions import GoogleCloudError
import hashlib
from datetime import datetime, timedelta
from pandas_gbq import to_gbq
import os
from dotenv import load_dotenv
load_dotenv()

class BigQuery:
    def __init__(self, conn_id = 'google_cloud_default'):
        self.conn_id = conn_id
        self.connection = None

    def connect(self):
        hook = BigQueryHook(self.conn_id)
        self.connection = hook.get_client()

    def close(self):
        self.connection = None

    def create_table_overwrite(self, table_name: str, columns: dict, unique_key: list):
        self.connect()
        column_definitions = ', '.join([f"{col} STRING" for col in columns])
        key_definitions = ', '.join(unique_key)

        schema = os.environ.get("schema_staging")
        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                {column_definitions},
                row_hash STRING,
                PRIMARY KEY ({key_definitions}) NOT ENFORCED
            )
        """
        
        query_job = self.connection.query(create_table_query)
        query_job.result()
        self.close()

    def create_table_snapshot(self, table_name: str, columns: dict, unique_key: list):
        self.connect()
        column_definitions = ', '.join([f"{col} STRING" for col in columns])
        schema = os.environ.get("schema_staging")
        key_definitions = ', '.join(unique_key) + ', export_date'

        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                {column_definitions},
                row_hash STRING,
                PRIMARY KEY ({key_definitions}) NOT ENFORCED
            )
        """
        query_job = self.connection.query(create_table_query)
        query_job.result()
        self.close()

    def row_hash(self, row):
        hash_input = ''.join(str(val) for val in row)
        return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()
    
    def insert_append(self, table_name: str, df):
        self.connect()

        df['row_hash'] = df.apply(lambda x: self.row_hash(x), axis=1)
        table = f"""{os.environ.get("schema_staging")}.{table_name}"""
        to_gbq(df, destination_table=table, project_id={os.environ.get("project_id")}, if_exists='append')

        self.close()

    def insert_overwrite(self, table_name: str, df, unique_key: list):
        self.connect()

        df['row_hash'] = df.apply(lambda x: self.row_hash(x), axis=1)
        schema = os.environ.get("schema_staging")

        temp_table = f"""{schema}.{table_name}_temp"""
        df.to_gbq(destination_table=temp_table, project_id={os.environ.get("project_id")}, if_exists='replace')
        
        unique_key_conditions = ' AND '.join([f"target.{key} = source.{key}" for key in unique_key])
        column_update = ', '.join([f"target.{col} = source.{col}" for col in df.columns if col not in unique_key])

        merge_query = f"""
                        MERGE {schema}.{table_name} AS target
                        USING {schema}.{temp_table} AS source
                        ON {unique_key_conditions}
                        WHEN MATCHED THEN
                        UPDATE SET {column_update}
                        WHEN NOT MATCHED THEN
                        INSERT ROW
                        """

        print(merge_query)
        query_job = self.connection.query(merge_query)
        query_job.result()

        self.client.delete_table(temp_table)

        self.close()

    def insert_snapshot(self, table_name: str, df, unique_key: list):
        self.connect()

        df['row_hash'] = df.apply(lambda x: self.row_hash(x), axis=1)
        df['export_date'] = datetime.utcnow().strftime('%Y-%m-%d')
        schema = os.environ.get("schema_staging")

        temp_table = f"""{schema}.{table_name}_temp"""
        df.to_gbq(destination_table=temp_table, project_id={os.environ.get("project_id")}, if_exists='replace')
        
        unique_key_conditions = ' AND '.join([f"target.{key} = source.{key}" for key in unique_key])
        column_update = ', '.join([f"target.{col} = source.{col}" for col in df.columns if col not in unique_key])

        merge_query = f"""
                        MERGE {schema}.{table_name} AS target
                        USING {temp_table} AS source
                        ON {unique_key_conditions}
                        WHEN MATCHED THEN
                        UPDATE SET {column_update}
                        WHEN NOT MATCHED THEN
                        INSERT ROW
                        """

        print(merge_query)
        query_job = self.connection.query(merge_query)
        query_job.result()

        self.client.delete_table(temp_table)

        self.close()

    def load_overwrite(self, tables: dict, unique_key: list):
        for table_name, df in tables.items():
            self.create_table_overwrite(table_name, df.columns.to_list(), unique_key)
            self.insert_overwrite(table_name, df, unique_key)

    def load_snapshot(self, tables: dict, unique_key: list):
        for table_name, df in tables.items():
            self.create_table_snapshot(table_name, df.columns.to_list(), unique_key)
            self.insert_snapshot(table_name, df, unique_key)

    def load_append(self, tables: dict, unique_key: list):
        for table_name, df in tables.items():
            self.create_table_overwrite(table_name, df.columns.to_list(), unique_key)
            self.insert_append(table_name, df)