# sentiment_platform/config.py

from pydantic_settings import BaseSettings


class KafkaSettings(BaseSettings):
    broker: str = "localhost:29092"
    topic: str = "raw-hn-posts"
    processed_topic: str = "processed-sentiments"
    group_id: str = "sentiment-consumer-group"

    model_config = {"env_prefix": "KAFKA_"}


class RedisSettings(BaseSettings):
    host: str = "localhost"
    port: int = 6379
    db: int = 0

    model_config = {"env_prefix": "REDIS_"}


class MLflowSettings(BaseSettings):
    tracking_uri: str = "http://localhost:5000"

    model_config = {"env_prefix": "MLFLOW_"}


class APISettings(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8000

    model_config = {"env_prefix": "API_"}

    
class S3Settings(BaseSettings):
    access_key_id: str
    secret_access_key: str
    region: str
    s3_bucket: str

    model_config = {"env_prefix": "AWS_"}

class Settings(BaseSettings):
    kafka: KafkaSettings = KafkaSettings()
    redis: RedisSettings = RedisSettings()
    mlflow: MLflowSettings = MLflowSettings()
    s3: S3Settings = S3Settings()
    api: APISettings = APISettings()

    model_config = {"env_file": ".env"}


settings = Settings()

