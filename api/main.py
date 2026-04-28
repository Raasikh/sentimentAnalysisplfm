from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI

from ml.sentiment_model import SentimentModel
from api.metrics import MetricsMiddleware, metrics_endpoint
from api.routes import router

logger= structlog.get_logger("api")

model= SentimentModel()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up API")
    model.load_model()
    yield
    logger.info("Shutting down API")


app= FastAPI(title="Hacker News Sentiment Analysis API",
              description="API for analyzing sentiment of Hacker News posts", 
              version="1.0.0", lifespan=lifespan)


app.add_middleware(MetricsMiddleware)
app.include_router(router, prefix="/api/v1")
app.add_api_route("/metrics", metrics_endpoint)


