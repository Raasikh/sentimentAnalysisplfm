# Sentiment Platform

Real-time sentiment analytics platform that ingests Hacker News posts,
analyzes sentiment using NLP, and serves insights through a REST API.

## Architecture
```
HN API → Producer → Kafka → Consumer → Polars DataFrame
                                          → S3 (Parquet)
                                          → Redis (cache)
                                          → Feast (features)
                                          → MLflow (tracking)
                                          → Drift detection
                                                ↓
                              FastAPI ← Redis ← Feast
                                ↓
                            Prometheus → Grafana
```

## Tech Stack

| Component        | Technology                          |
|-----------------|-------------------------------------|
| Message broker   | Apache Kafka                       |
| In-memory cache  | Redis                              |
| Bulk storage     | Amazon S3 (Parquet)                |
| ML model         | DistilBERT (HuggingFace)           |
| Feature store    | Feast                              |
| Experiment tracking | MLflow                          |
| API framework    | FastAPI                            |
| Data processing  | Polars (micro-batching)            |
| Drift detection  | Evidently                          |
| Monitoring       | Prometheus + Grafana               |
| Dependencies     | uv                                 |
| Containerization | Docker Compose                     |

## Prerequisites

- Docker and Docker Compose
- Python 3.12+
- [uv](https://github.com/astral-sh/uv)
- AWS account with S3 access

## Quick Start

1. Clone the repository:
```bash
   git clone <your-repo-url>
   cd sentiment-platform
```

2. Set up environment variables:
```bash
   cp .env.example .env
   # Edit .env with your AWS credentials and passwords
```

3. Launch all services:
```bash
   docker-compose up --build
```

4. Verify services are running:
   - API docs: http://localhost:8000/docs
   - MLflow UI: http://localhost:5000
   - Grafana: http://localhost:3000 (admin/admin)
   - Prometheus: http://localhost:9090

## Local Development

Install all dependencies:
```bash
uv sync --extra all
```

Install only what you need:
```bash
uv sync --extra streaming        # Producer/Consumer
uv sync --extra storage           # S3 + Redis
uv sync --extra ml                # ML pipeline
uv sync --extra api               # FastAPI
uv sync --extra monitoring        # Drift detection
```

Run services individually:
```bash
uv run python -m producer.hn_producer
uv run python -m consumer.hn_consumer
uv run uvicorn api.main:app --reload
```

## API Endpoints

| Endpoint               | Method | Description              |
|------------------------|--------|--------------------------|
| `/api/v1/predict`      | POST   | Single text sentiment    |
| `/api/v1/predict/batch`| POST   | Batch sentiment (max 100)|
| `/api/v1/posts/{id}`   | GET    | Cached HN post by ID     |
| `/api/v1/stats`        | GET    | Pipeline health stats    |
| `/metrics`             | GET    | Prometheus metrics       |
| `/docs`                | GET    | Interactive API docs     |

## Project Structure
```
sentiment-platform/
├── docker-compose.yml
├── pyproject.toml
├── .env.example
├── sentiment_platform/
│   ├── config.py              # Shared Pydantic settings
│   └── models.py              # HNPost data schema
├── producer/
│   ├── Dockerfile
│   └── hn_producer.py         # HN API → Kafka
├── consumer/
│   ├── Dockerfile
│   ├── hn_consumer.py         # Kafka → Polars → S3/Redis
│   └── s3_writer.py           # Parquet upload to S3
├── ml/
│   ├── sentiment_model.py     # DistilBERT wrapper
│   └── tracking.py            # MLflow integration
├── api/
│   ├── Dockerfile
│   ├── main.py                # FastAPI app
│   ├── routes.py              # API endpoints
│   └── metrics.py             # Prometheus middleware
├── feature_store/
│   ├── feature_store.yaml     # Feast config
│   ├── features.py            # Entity + feature views
│   └── feast_client.py        # Feature retrieval
└── monitoring/
    ├── drift_detector.py      # Evidently drift checks
    ├── prometheus/
    │   └── prometheus.yml     # Scrape config
    └── grafana/
        └── provisioning/      # Auto-configured datasources
```

## Monitoring

Grafana dashboards track:
- API request rate and latency (p95)
- Sentiment distribution over time
- Data drift score with alert thresholds
- Per-feature drift status

## License

MIT