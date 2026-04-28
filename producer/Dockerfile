# producer/Dockerfile

FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
RUN uv sync --extra streaming --no-dev --frozen

COPY sentiment_platform/ ./sentiment_platform/
COPY producer/ ./producer/
COPY .env ./

CMD ["uv", "run", "python", "-m", "producer.hn_producer"]