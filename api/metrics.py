import time
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from prometheus_client import Counter, Histogram, generate_latest

REQUEST_COUNT= Counter(
    "api_request_count",
    "Total number of API requests",
    ["endpoint", "method", "status_code"],
)

REQUEST_LATENCY= Histogram(
    "api_request_latency_seconds",
    "Latency of API requests in seconds",
    ["endpoint", "method"],
)

PREDICTION_COUNT= Counter(
    "model_prediction_count",
    "Total number of model predictions",
    ["model_name"],
)


class MetricsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start = time.monotonic()
        response = await call_next(request)
        duration = time.monotonic() - start

        if request.url.path != "/metrics":
            REQUEST_COUNT.labels(
                method=request.method,
                endpoint=request.url.path,
                status_code=response.status_code,
            ).inc()

            REQUEST_LATENCY.labels(
                method=request.method,
                endpoint=request.url.path,
            ).observe(duration)

        return response
async def metrics_endpoint(request: Request) -> Response:
    """Endpoint to expose Prometheus metrics."""
    data = generate_latest()
    return Response(content=data, media_type="text/plain")



