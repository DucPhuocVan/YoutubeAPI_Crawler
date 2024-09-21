from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import re
import pandas as pd
from io import BytesIO
from .checkpoint import CheckPoint
from .load_postgres import Postgres
from datetime import datetime
import io
from dotenv import load_dotenv
import os

load_dotenv()

class S3:
    def __init__(self, aws_conn_id='s3_conn'):
        self.s3_hook = S3Hook(aws_conn_id=aws_conn_id)
        self.checkpoint = CheckPoint()
        self.postgres = Postgres()

    def df_to_parquet(self, df):
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow', compression='snappy')
        parquet_buffer.seek(0)
        return parquet_buffer

    def upload_to_s3(self, df, file_name):
        now = datetime.now()
        year = now.strftime('%Y')
        month = now.strftime('%m')
        day = now.strftime('%d')

        s3_path = f'{file_name}/{year}/{month}/{day}/{file_name}{year}{month}{day}.parquet'
        s3_bucket = os.environ.get("s3_bucket")
        parquet_buffer = self.df_to_parquet(df)

        self.s3_hook.load_file_obj(
            file_obj=parquet_buffer,
            key=s3_path,
            bucket_name=s3_bucket,
            replace=True
        )
        print(f"{file_name} uploaded to s3://{s3_bucket}/{file_name}")

    def get_files_from_s3(self, bucket_name, folder_name, checkpoint_date):
        files = self.s3_hook.list_keys(bucket_name=bucket_name, prefix=folder_name)

        files_to_ingest = []
        
        for file_key in files:
            if file_key.endswith('.parquet'):
                file_obj = self.s3_hook.get_key(file_key, bucket_name)
                file_last_modified = file_obj.last_modified

                if file_last_modified.replace(tzinfo=None) > checkpoint_date:
                    files_to_ingest.append(file_key)
        
        return files_to_ingest

    def extract_export_date(self, file_key):
        match = re.search(r'/(\d{4})/(\d{2})/(\d{2})/.*?(\d{8})\.parquet$', file_key)
        
        if match:
            return match.group(4)
        return None
    
    def read_parquet_from_s3(self, bucket_name, file_key, export_date):
        file_obj = self.s3_hook.get_key(file_key, bucket_name)
        parquet_data = BytesIO(file_obj.get()["Body"].read())

        parquet_df = pd.read_parquet(parquet_data)

        parquet_df['export_date'] = export_date

        return parquet_df
    
    def load_file_into_posgres(self, folder_name, unique_key: list, type_load):
        self.checkpoint.create_checkpoint_table()
        checkpoint_date = self.checkpoint.get_last_checkpoint(folder_name)
        bucket_name = os.environ.get("s3_bucket")

        if checkpoint_date is None:
            checkpoint_date = datetime(2000, 1, 1)
        
        files_to_ingest = self.get_files_from_s3(bucket_name, folder_name, checkpoint_date)
        
        for file_key in files_to_ingest:
            export_date = self.extract_export_date(file_key)
            df = self.read_parquet_from_s3(bucket_name, file_key, export_date)

            if type_load == 'scd_type2':
                self.postgres.load_to_postgres_scd_type2({folder_name: df}, unique_key)
            elif type_load == 'overwrite_daily':
                self.postgres.load_to_postgres_overwrite_daily({folder_name: df}, unique_key)
            elif type_load == 'overwrite':
                self.postgres.load_to_postgres_overwrite({folder_name: df}, unique_key)
            elif type_load == 'append':
                self.postgres.load_to_postgres_append({folder_name: df}, unique_key)
        
        self.checkpoint.update_checkpoint(folder_name, datetime.now())

