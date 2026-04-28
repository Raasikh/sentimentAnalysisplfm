import time
from datetime import datetime, timezone

import httpx
import structlog
from confluent_kafka import Producer

from sentiment_platform.config import settings
from sentiment_platform.models import HNPost


logger = structlog.get_logger("hn_producer")


HN_API_URL = "https://hacker-news.firebaseio.com/v0"


def create_producer()-> Producer:
    """
    Create a Kafka producer instance using the configuration settings.
    """
    conf={
        'bootstrap.servers': settings.kafka.broker,
        'client.id': 'hn-producer',
        "acks": "all",
    }
    return Producer(conf)


def delivery_callback(err, msg):
    """Called once per message to confirm delivery or log failure."""
    if err:
        logger.error("Message delivery failed", error=str(err))
    else:
        logger.info(
            "Message delivered",
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
        )


def fetch_news_story(client: httpx.Client)->list[int]:
    """
    Fetch the latest news story IDs from the Hacker News API.
    """
    response = client.get(f"{HN_API_URL}/newstories.json")
    response.raise_for_status()
    return response.json()


def fetch_item(client: httpx.Client, item_id: int)->HNPost:
    """
    Fetch a specific news story by ID and return it as an HNPost object.
    """
    response = client.get(f"{HN_API_URL}/item/{item_id}.json")
    response.raise_for_status()
    data = response.json()
    if data is None:
        raise ValueError(f"Item with ID {item_id} not found")
    return HNPost(
        id=data["id"],
        title=data.get("title", ""),
        text=data.get("text"),
        url=data.get("url"),
        author=data.get("by", ""),
        score=data.get("score", 0),
        num_comments=data.get("descendants", 0),
        created_at=datetime.fromtimestamp(data["time"], tz=timezone.utc),
        fetched_at=datetime.now(timezone.utc),
    )


def run_producer(poll_interval: int=30, batch_size: int=50):
    """Main loop: poll HN for new stories and push to Kafka."""

    producer= create_producer()
    seen_ids:set[int]= set()

    logger.info("Starting HN producer", poll_interval=poll_interval, topic=settings.kafka.topic)


    with httpx.Client(timeout=10) as client:
        while True:
            try:
                story_ids= fetch_news_story(client)
                new_ids= [id for id in story_ids if id not in seen_ids][:batch_size] if id not in seen_ids else []
                logger.info("Fetched story IDs", total=len(story_ids), new=len(new_ids))


                for story_id in new_ids:
                        post= fetch_item(client, story_id)
                        if post is None:
                            logger.warning("Story not found or invalid", story_id=story_id)
                            continue
                        producer.produce(
                            settings.kafka.topic,
                            key=str(post.id).encode("utf-8"),
                            value=post.model_dump_json().encode("utf-8"),
                            callback=delivery_callback,
                        )
                        seen_ids.add(story_id)

                producer.flush(timeout=10)

                if len(seen_ids) > 10_000:
                    seen_ids= set(list(seen_ids)[-5_000:])
            except Exception as e:
                        logger.error("Failed to fetch or produce story", story_id=story_id, error=str(e))



            time.sleep(poll_interval)


if __name__ == "__main__":
    run_producer()