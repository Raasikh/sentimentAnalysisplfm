# api/Dockerfile

FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --extra api --extra ml --no-dev --frozen

COPY sentiment_platform/ ./sentiment_platform/
COPY api/ ./api/
COPY ml/ ./ml/
COPY .env ./

EXPOSE 8000

CMD ["uv", "run", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]