import os
import requests
import boto3
from botocore.client import Config

MINIO_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000")
MINIO_ACCESS = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
BUCKET = 'ecommerce-bronze'

FILES = {
    "orders": "https://raw.githubusercontent.com/owid/ecommerce-data/main/orders.csv",
    "customers": "https://raw.githubusercontent.com/owid/ecommerce-data/main/customers.csv",
    "products": "https://raw.githubusercontent.com/owid/ecommerce-data/main/products.csv",
    "inventory": "https://raw.githubusercontent.com/owid/ecommerce-data/main/inventory.csv"
}

def ensure_bucket(s3):
    buckets = [b['Name'] for b in s3.list_buckets().get('Buckets', [])]
    if BUCKET not in buckets:
        s3.create_bucket(Bucket=BUCKET)

def upload_to_minio(s3, key, data):
    s3.put_object(Bucket=BUCKET, Key=key, Body=data)

def main():
    s3 = boto3.client('s3',
                      endpoint_url=MINIO_ENDPOINT,
                      aws_access_key_id=MINIO_ACCESS,
                      aws_secret_access_key=MINIO_SECRET,
                      config=Config(signature_version='s3v4'),
                      region_name='us-east-1')
    ensure_bucket(s3)

    for name, url in FILES.items():
        print(f"Downloading {name} from {url}")
        r = requests.get(url)
        if r.status_code == 200:
            key = f"bronze/{name}.csv"
            print(f"Uploading to MinIO as {key}")
            upload_to_minio(s3, key, r.content)
        else:
            print(f"Failed to download {url}")

if __name__ == "__main__":
    main()
