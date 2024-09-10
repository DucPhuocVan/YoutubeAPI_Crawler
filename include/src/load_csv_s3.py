from airflow.providers.amazon.aws.hooks.s3 import S3Hook

class S3:
    def __init__(self, aws_conn_id='s3_conn'):
        self.s3_hook = S3Hook(aws_conn_id=aws_conn_id)

    def upload_to_s3(self, filename, s3_bucket, s3_key):
        self.s3_hook.load_file(
            filename=filename,
            key=s3_key,
            bucket_name=s3_bucket,
            replace=True
        )
        print(f"{filename} uploaded to s3://{s3_bucket}/{s3_key}")

