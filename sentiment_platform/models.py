from datetime import datetime
from pydantic import BaseModel

class HNPost(BaseModel):
    id: int
    title: str
    text: str | None = None
    url: str | None = None
    author: str
    score: int
    num_comments: int
    created_at: datetime
    fetched_at: datetime 
    