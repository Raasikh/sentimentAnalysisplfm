from datetime import timedelta

from feast import Entity, Feature, FeatureView, FileSource
from feast.types import Int64, String
from feast import Field


# Define the data source for the feature view

post= Entity(name="post_id", description="Unique identifier for a Hacker News post")


author= Feature(name="author", dtype=String, description="Author of the post")


post_source = FileSource(
    path="data/parquet/",
    timestamp_field="created_at",
)

# ============================================
# FEATURE VIEWS - groups of related features
# ============================================


post_features= FeatureView(
    name="post_features",
    entities=[post],
    schema=[
        Field(name="title", dtype=String),
        Field(name="score", dtype=Int64),
        Field(name="num_comments", dtype=Int64),
        Field(name="post_type", dtype=String),
        Field(name="author", dtype=String),
    ],
    source=post_source,
    ttl=timedelta(days=7),
)


