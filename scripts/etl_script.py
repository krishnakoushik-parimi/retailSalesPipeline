from dotenv import load_dotenv
import os
import boto3
import psycopg2
import pandas as pd
from io import BytesIO

# Load environment variables from .env file
load_dotenv()

# AWS S3 Client
def read_csv_from_s3(file_name, bucket):
    s3 = boto3.client('s3',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-2')
    )
    obj = s3.get_object(Bucket=bucket, Key=f'raw-data/{file_name}')
    return pd.read_csv(BytesIO(obj['Body'].read()))

def insert_df(df, table_name, cur, conn):
    for row in df.itertuples(index=False):
        try:
            # Clean row values by stripping any leading/trailing spaces
            row = tuple(str(val).strip() if isinstance(val, str) else val for val in row)
            
            placeholders = ','.join(['%s'] * len(row))
            sql = f"INSERT INTO {table_name} VALUES ({placeholders}) ON CONFLICT DO NOTHING"
            cur.execute(sql, row)
        except Exception as e:
            print(f"Error inserting row {row}: {e}")
    conn.commit()

def run():
    bucket = 'retail-sales-data-pipeline'  # Update this if the bucket is renamed

    print("Reading CSVs from S3...")
    sales_df = read_csv_from_s3('sales.csv', bucket)
    products_df = read_csv_from_s3('products.csv', bucket)
    stores_df = read_csv_from_s3('stores.csv', bucket)

    # Clean column names and values to avoid extra spaces
    sales_df.columns = sales_df.columns.str.strip()
    products_df.columns = products_df.columns.str.strip()
    stores_df.columns = stores_df.columns.str.strip()

    sales_df = sales_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    products_df = products_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    stores_df = stores_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(
        dbname='sales_data',
        user='postgres',
        password=os.environ.get('PG_PASSWORD', 'yourpassword'),   # Use environment variable for password
        host='localhost',
        port='5432'
    )
    cur = conn.cursor()

    print("Inserting data...")
    insert_df(products_df, 'products', cur, conn)
    insert_df(stores_df, 'stores', cur, conn)
    insert_df(sales_df, 'sales', cur, conn)

    print("ETL complete!")
    cur.close()
    conn.close()

# Run the ETL process
if __name__ == "__main__":
    run()
