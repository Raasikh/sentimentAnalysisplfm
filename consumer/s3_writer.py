import io
from datetime import datetime, timezone
import structlog
import boto3
import polars as pl
from sentiment_platform.config import settings

logger= structlog.get_logger()

def create_s3_client():
    """
    Create an S3 client using the AWS credentials from the configuration settings.
    """
    return boto3.client(
        "s3",
        region_name=settings.s3.region,
    )


def upload_parquet_to_s3(df: pl.DataFrame, s3_client, key: str):
    """
    writing dataframe to parquet and uploading it to S3.
     - df: The Polars DataFrame to be uploaded.
     - s3_client: The S3 client instance to use for uploading.
     - key: The S3 object key (path) where the Parquet file will be
    """

    client= create_s3_client()
    now = datetime.now(timezone.utc)
    s3_key = (
        f"raw/year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"hn_posts_{now.strftime('%H%M%S')}.parquet"
    )
    buffer = io.BytesIO()
    df.write_parquet(buffer, compression="snappy", statistics=True)
    buffer.seek(0)


    client.put_object(
        Bucket=settings.s3.bucket,
        Key=s3_key,
        Body=buffer.getvalue(),
    )

    logger.info(
        "Uploaded to S3",
        bucket=settings.s3.bucket,
        key=s3_key,
        rows=df.height,
        size_bytes=buffer.getbuffer().nbytes,
    )

    return f"s3://{settings.s3.bucket}/{s3_key}"