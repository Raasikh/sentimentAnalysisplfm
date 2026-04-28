from datetime import datetime, timezone

from pydantic import BaseModel

import redis 
import structlog
from fastapi import APIRouter, HTTPException
from sentiment_platform.config import settings
from api.metrics import PREDICTION_COUNT
from ml.sentiment_model import SentimentModel


logger= structlog.get_logger("api_router")
router= APIRouter()



# ============================================
# REQUEST / RESPONSE SCHEMAS
# ============================================


class PredictRequest(BaseModel):
    text: str


class PredictResponse(BaseModel):
    text:str
    sentiment_label: str
    sentiment_score: float
    timestamp: str

class BatchPredictRequest(BaseModel):
    texts: list[str]


class BatchPredictResponse(BaseModel):
    results: list[PredictResponse]



class PostResponse(BaseModel):
    id: int
    title: str
    author: str
    score: int
    num_comments: int
    post_type: str
    created_at: str



class StatsResponse(BaseModel):
    last_batch_size: int
    last_batch_time: str
    avg_score: float
    total_posts_cached: int



model= SentimentModel()

@router.post("/predict", response_model=PredictResponse)
def predict_sentiment(request: PredictRequest):
    result= model.predict_sentiment(request.text)

    PREDICTION_COUNT.labels(label=result["label"]).inc()

    return PredictResponse(
        text=request.text,
        sentiment_label=result["label"],
        sentiment_score=result["score"],
        timestamp=datetime.now(timezone.utc).isoformat(),
    )

@router.post("/batch_predict", response_model=BatchPredictResponse)
def batch_predict(request: BatchPredictRequest):
    """Predict sentiment for a batch of texts."""

    if len(request.texts) > 100:
        raise HTTPException(status_code=400, detail="Batch size cannot exceed 100")
    


    results= model.predict_batch(request.texts)
    now = datetime.now(timezone.utc).isoformat()

    responses=[]
    for text, res in zip(request.texts, results):
        PREDICTION_COUNT.labels(label=res["label"]).inc()
        responses.append(PredictResponse(
            text=text,
            sentiment_label=res["label"],
            sentiment_score=res["score"],
            timestamp=now,
        ))
    return BatchPredictResponse(results=responses)




def get_redis_client():
    return redis.Redis(
        host=settings.redis.host,
        port=settings.redis.port,
        db=settings.redis.db,
        decode_responses=True,
    )

@router.get("/posts/{post_id}", response_model=PostResponse)
def get_post(post_id: int):
    redis_client = get_redis_client()
    try:
        data= redis_client.hgetall(f"hn:post:{post_id}")
        if not data:
            raise HTTPException(status_code=404, detail="Post not found")
        return PostResponse(
            id=post_id,
            **data)
    finally:
        redis_client.close()



@router.get("/stats", response_model=StatsResponse)
def get_stats():
    redis_client = get_redis_client()
    try:
        data= redis_client.hgetall("hn:stats:latest")
        if not data:
            raise HTTPException(status_code=404, detail="Stats not found")
        return StatsResponse(
            **data
        )
    finally:
        redis_client.close()