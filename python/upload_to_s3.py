import os
from pathlib import Path

import boto3


bucket_name = os.environ.get("RETAIL_BUCKET")

if not bucket_name:
    raise ValueError(
        "RETAIL_BUCKET environment variable is not set."
    )

s3 = boto3.client("s3")

files = {
    "data/customers.csv": "raw/customers/customers.csv",
    "data/products.csv": "raw/products/products.csv",
    "data/orders.csv": "raw/orders/orders.csv",
    "data/order_items.csv": "raw/order_items/order_items.csv",
    "data/payments.csv": "raw/payments/payments.csv",
}

for local_file, s3_key in files.items():

    file_path = Path(local_file)

    if not file_path.exists():
        raise FileNotFoundError(
            f"File does not exist: {local_file}"
        )

    print(f"Uploading {local_file}...")

    s3.upload_file(
        local_file,
        bucket_name,
        s3_key
    )

    print(
        f"Uploaded successfully -> "
        f"s3://{bucket_name}/{s3_key}"
    )

print("All files uploaded successfully!")