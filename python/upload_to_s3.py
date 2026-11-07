"""Upload the generated raw CSVs to s3://$RETAIL_BUCKET/raw/."""
import logging
import os

import boto3

from generator_config import DATA_DIR

log = logging.getLogger("upload_to_s3")

DATASETS = ["customers", "products", "orders", "order_items", "payments"]


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    bucket_name = os.environ.get("RETAIL_BUCKET")
    if not bucket_name:
        raise ValueError("RETAIL_BUCKET environment variable is not set.")

    files = {DATA_DIR / f"{name}.csv": f"raw/{name}/{name}.csv" for name in DATASETS}

    # Check every file first so a missing one never leaves S3 half-updated.
    missing = [str(path) for path in files if not path.is_file() or path.stat().st_size == 0]
    if missing:
        raise FileNotFoundError(f"Missing or empty input files: {missing}")

    s3 = boto3.client("s3")
    for local_path, s3_key in files.items():
        s3.upload_file(str(local_path), bucket_name, s3_key)
        log.info("Uploaded %s -> s3://%s/%s", local_path, bucket_name, s3_key)

    log.info("All %d files uploaded.", len(files))


if __name__ == "__main__":
    main()
