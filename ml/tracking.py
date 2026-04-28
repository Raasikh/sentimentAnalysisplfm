from datetime import datetime, timezone
import structlog

import mlflow
import polars as pl
from sentiment_platform.config import settings

logger= structlog.get_logger("tracking")


def init_tracking():
    mlflow.set_tracking_uri(settings.mlflow.tracking_uri)
    mlflow.set_experiment(settings.mlflow.experiment_name)
    logger.info("MLflow tracking initialized", tracking_uri=settings.mlflow.tracking_uri, experiment_name=settings.mlflow.experiment_name)




def log_batch_metrics(df:pl.DataFrame):
    """ log metrics for a batch of data to MLflow """

    with mlflow.start_run(run_name=f"batch_{datetime.now(timezone.utc).isoformat()}"):
        label_counts= df["sentiment_label"].value_counts().to_dict()
        for row in label_counts.iter_rows(named=True):
            label= row["sentiment_label"].lower()
            mlflow.log_metric(f"count_{label}", row["count"])


        mlflow.log_metric("batch_size", df.height)
        mlflow.log_metric("avg_confidence", df["sentiment_score"].mean())
        mlflow.log_metric("min_confidence", df["sentiment_score"].min())
        mlflow.log_metric("avg_post_score", df["score"].mean())

        # Log parameters
        mlflow.log_param("model_name", "distilbert-base-uncased-finetuned-sst-2-english")
        mlflow.log_param("timestamp", datetime.now(timezone.utc).isoformat())

        logger.info(
            "Logged batch metrics to MLflow",
            batch_size=df.height,
            avg_confidence=round(df["sentiment_score"].mean(), 4),
        )