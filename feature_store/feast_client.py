from datetime import datetime, timezone
import polars as pl

from feast import FeatureStore

store= FeatureStore(repo_path="feature_store/")



def get_training_features(post_ids: list[int]) -> pl.DataFrame:
    """Fetch historical features for model training."""
    entity_df = pl.DataFrame({
        "post_id": post_ids,
        "event_timestamp": [datetime.now(timezone.utc)] * len(post_ids),
    }).to_pandas()


    training_df= store.get_historical_features(
        features=[
            "post_features:score",
            "post_features:num_comments",
            "post_features:post_type",
            "post_features:author",
        ],).to_df()
    
    return pl.from_pandas(training_df)



def get_online_features(post_ids: list[int]) -> pl.DataFrame:
    """Fetch latest features for real-time inference."""

    features= store.get_online_features(
        entity_rows=[{"post_id": post_id} for post_id in post_ids],
        features=[
            "post_features:score",
            "post_features:num_comments",
            "post_features:post_type",
            "post_features:author",
        ],).to_df()
    
    return pl.from_pandas(features)


def materialize_features():
    """Sync latest features from offline store to online store (Redis)."""
    store.materialize_incremental(
        end_date=datetime.now(timezone.utc),
    )

"""

`materialize_incremental` looks at what's changed since the last sync and pushes only the new data to Redis. You'd run this on a schedule — say every 5 minutes — or trigger it after each consumer batch completes.

Here's the full data flow with Feast now included:
```
Kafka → Consumer → Parquet (S3) → Feast offline store
                                        ↓ materialize
                                   Feast online store (Redis)
                                        ↓
                                   FastAPI → user requests → Feast online store (Redis) → model inference
"""