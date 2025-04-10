from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Now you can use os.environ.get() to access the variables
aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
aws_region = os.environ.get('AWS_DEFAULT_REGION')
pg_password = os.environ.get('PG_PASSWORD')

print(f"AWS Access Key: {aws_access_key_id}")
