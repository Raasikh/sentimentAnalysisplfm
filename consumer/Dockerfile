# consumer/Dockerfile

FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
RUN uv sync --extra streaming --extra storage --extra ml --extra monitoring --no-dev --frozen

COPY sentiment_platform/ ./sentiment_platform/
COPY consumer/ ./consumer/
COPY ml/ ./ml/
COPY monitoring/ ./monitoring/
COPY .env ./

# Create directory for local parquet fallback
RUN mkdir -p data/parquet

CMD ["uv", "run", "python", "-m", "consumer.hn_consumer"]