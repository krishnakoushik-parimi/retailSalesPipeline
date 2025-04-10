import boto3
import os
s3 = boto3.client('s3',
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=''  # change if needed
)

bucket_name = 'retail-sales-data-pipeline'

# Upload files to S3
for file in ['sales.csv', 'products.csv', 'stores.csv']:
    s3.upload_file(f'data/{file}', bucket_name, f'raw-data/{file}')
    print(f'{file} uploaded!')
