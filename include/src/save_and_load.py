from .load_csv_s3 import S3
from .load_data_postgres import Postgres
import os
from dotenv import load_dotenv

load_dotenv()

class SaveAndLoad:
    def __init__(self):
        self.s3 = S3()
        self.postgres = Postgres()

    def save_to_csv(self, df, filename):
        df.to_csv(filename, index=False)
        print(f"DataFrame saved to {filename}")

    def save_and_load(self, df, filename):
        path = os.environ.get('data_path')+filename+'.csv'
        self.save_to_csv(df, path)
        self.s3.upload_to_s3(path, 'youtubeapi', filename+'.csv')
        self.postgres.load_to_postgres({filename: df})
