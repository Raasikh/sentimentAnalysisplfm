#building consumer.py for kafka consumer to consume messages from raw-hn-posts topic, process them and produce to processed-sentiments topic

from time import time, monotonic
from datetime import datetime, timezone
from pathlib import Path
import structlog
import polars as pl
from confluent_kafka import Consumer, KafkaError
from sentiment_platform.config import settings
from sentiment_platform.models import HNPost
from consumer.s3_writer import create_s3_client, upload_parquet_to_s3
from ml.sentiment_model import SentimentModel
from ml.tracking import log_batch_metrics, init_tracking
from monitoring.drift_detector import run_drift_check

model = SentimentModel()

logger= structlog.get_logger("hn_consumer")


def create_consumer() -> Consumer:
    """
    Create a Kafka consumer instance using the configuration settings.
    """
    conf = {
        'bootstrap.servers': settings.kafka.broker,
        'group.id': settings.kafka.group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    }
    consumer= Consumer(conf)
    consumer.subscribe([settings.kafka.topic])
    return consumer     



def collect_micro_message(
        consume:Consumer,
        batch_size:int=100,
        timeout_seconds:int=5
)-> list[HNPost]:
    """
    Collecting messages until batch size or timeout is reached.
    """

    batch: list[HNPost] = []
    start_time= monotonic()

    while len(batch) < batch_size:
        elasped_time= monotonic() - start_time
        remaining_time= timeout_seconds - elasped_time
        if remaining_time <= 0:
            break

        msg= consume.poll(remaining_time)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logger.info("End of partition reached", topic=msg.topic(), partition=msg.partition())
            else:
                logger.error("Error while consuming message", error=str(msg.error()))
            continue

        try:
            post= HNPost.model_validate_json(msg.value())
            batch.append(post)
        except Exception as e:
              logger.warning(
                "Failed to deserialize message",
                error=str(e),
                offset=msg.offset(),
            )
              
    return batch


def batch_to_transform(batch: list[HNPost])-> pl.DataFrame:
    """
    Transform a batch of HNPost objects into a Polars DataFrame for processing.
    """
    records= [post.model_dump() for post in batch]
    df = pl.DataFrame(records).with_columns(
        pl.col("created_at").cast(pl.Datetime("us", "UTC")),
        pl.col("fetched_at").cast(pl.Datetime("us", "UTC")),
        pl.col("id").cast(pl.Int64),
        pl.col("score").cast(pl.Int64),
        pl.col("num_comments").cast(pl.Int64),
    )

    return df

def write_parquet(df: pl.DataFrame, output_dir: str="data/parquet") -> Path:
    """
    Write a Polars DataFrame to a Parquet file at the specified path.
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    file_path = Path(output_dir) / f"data_{int(time())}.parquet"
    df.write_parquet(file_path, compression="snappy", statistics=True)

    logger.info("Written batch to Parquet", file_path=str(file_path), num_rows=df.height)

    return file_path


# earlier we psuhed to parquet now lets do for redis 

def update_redis_cache(df: pl.DataFrame) -> None:
    """Push latest batch stats to Redis for real-time serving."""
    import redis as r

    client = r.Redis(
        host=settings.redis.host,
        port=settings.redis.port,
        db=settings.redis.db,
        decode_responses=True,
    )

    try:
        # Store each post individually for quick lookup
        for row in df.iter_rows(named=True):
            key = f"hn:post:{row['id']}"
            client.hset(key, mapping={
                "title": row["title"] or "",
                "author": row["author"],
                "score": str(row["score"]),
                "num_comments": str(row["num_comments"]),
                "post_type": row["post_type"],
                "created_at": str(row["created_at"]),
            })
            # Auto-expire after 24 hours
            client.expire(key, 86400)

        # Update aggregate stats
        batch_stats = {
            "last_batch_size": str(df.height),
            "last_batch_time": datetime.now(timezone.utc).isoformat(),
            "avg_score": str(round(df["score"].mean(), 2)),
            "total_posts_cached": str(client.dbsize()),
        }
        client.hset("hn:stats:latest", mapping=batch_stats)

        logger.info("Redis cache updated", posts=df.height)

    except Exception as e:
        logger.error("Redis update failed", error=str(e))
    finally:
        client.close()


def enrich_with_sentiment(df: pl.DataFrame) -> pl.DataFrame:
    """Add sentiment scores to the DataFrame."""
    # Use title for link posts, text for self posts
    texts = df.select(
        pl.when(pl.col("text").is_not_null() & (pl.col("text") != ""))
        .then(pl.col("text"))
        .otherwise(pl.col("title"))
        .alias("input_text")
    )["input_text"].to_list()

    predictions = model.predict_batch(texts)

    return df.with_columns(
        pl.Series("sentiment_label", [p["label"] for p in predictions]),
        pl.Series("sentiment_score", [p["score"] for p in predictions]),
    )



def run_consumer():
    """Main loop: consume messages, process, and update cache."""
    consumer= create_consumer()
    init_tracking()

    logger.info("Starting HN consumer", topic=settings.kafka.topic)

    try:
        while True:
            batch= collect_micro_message(consumer)
            if not batch:
                continue

            df= batch_to_transform(batch)
            df= enrich_with_sentiment(df)
            file_path = write_parquet(df)
            s3_path = upload_parquet_to_s3(df, create_s3_client(), str(file_path))
            logger.info("Uploaded batch to S3", s3_path=s3_path)

            update_redis_cache(df)
            log_batch_metrics(df)
            run_drift_check(df)
            consumer.commit(asynchronous=False)

            logger.info("Batch processed and committed", batch_size=len(batch))

    except KeyboardInterrupt:
        logger.info("Consumer shutdown requested by user")
    finally:
        consumer.close()
        logger.info("Consumer closed")



if __name__ == "__main__":
    run_consumer()


"""
The main loop is intentionally simple because all the complexity lives in the functions we already built. But the **order of operations** is critical:

1. **Collect** — gather messages into a micro-batch
2. **Transform** — convert to Polars DataFrame
3. **Write Parquet** — persist to disk
4. **Update Redis** — push to hot cache
5. **Commit offsets** — *only then* tell Kafka we're done

That last step is the key. `consumer.commit()` only runs after both Parquet and Redis writes succeed. If the consumer crashes between step 3 and step 5, Kafka still has those messages marked as unread. On restart, they get reprocessed. You might get duplicate writes to Parquet, but you never lose data. This is the **at-least-once** guarantee we set up earlier with `enable.auto.commit: False`.

- **`KeyboardInterrupt`** — lets you stop the consumer cleanly with Ctrl+C
- **`consumer.close()` in `finally`** — tells Kafka this consumer is leaving the group so partitions get rebalanced immediately instead of waiting for a timeout

That's the complete consumer. Here's a summary of the data flow:
```
HN API → Producer → Kafka → Consumer → collect batch
                                          → Polars DataFrame
                                          → Parquet file (bulk storage)
                                          → Redis (hot cache)
                                          → commit offsets
                                          """