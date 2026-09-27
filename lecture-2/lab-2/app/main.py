import asyncio
import logging
import os
import random
import time
import uuid

import httpx
import structlog
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import Counter, Histogram
from prometheus_fastapi_instrumentator import Instrumentator

OTEL_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://jaeger:4318")
SERVICE_NAME = os.getenv("OTEL_SERVICE_NAME", "orders-api")

resource = Resource.create({"service.name": SERVICE_NAME})
provider = TracerProvider(resource=resource)
provider.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{OTEL_ENDPOINT}/v1/traces"))
)
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(__name__)

structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    cache_logger_on_first_use=True,
)
log = structlog.get_logger()

ORDERS_CREATED = Counter("orders_created_total", "Total orders created", ["status"])
ORDER_PROCESSING_TIME = Histogram(
    "order_processing_seconds",
    "Time spent processing an order",
    buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0],
)

CATALOG = [
    {"id": 1, "name": "Яблоки Антоновка", "price": 120.0},
    {"id": 2, "name": "Груша Конференция", "price": 180.0},
    {"id": 3, "name": "Клубника", "price": 350.0},
    {"id": 4, "name": "Голубика", "price": 420.0},
]
ORDERS: dict[str, dict] = {}

app = FastAPI(title="Пальчики оближешь — Orders API")

FastAPIInstrumentor.instrument_app(app)
Instrumentator().instrument(app).expose(app)


def get_trace_id() -> str:
    """Возвращает trace_id текущего спана в hex или '-'."""
    span = trace.get_current_span()
    ctx = span.get_span_context()
    return format(ctx.trace_id, "032x") if ctx.is_valid else "-"


@app.get("/", response_class=HTMLResponse)
async def index():
    return """
    <html><head><title>Пальчики оближешь — тест</title></head>
    <body style="font-family: sans-serif; max-width: 600px; margin: 40px auto;">
      <h1>Пальчики оближешь — Orders API</h1>
      <p>Кнопки для провокации ситуаций:</p>
      <button onclick="fetch('/api/orders/fail', {method:'POST'}).then(r=>alert('status: '+r.status))">Создать ошибку</button>
      <button onclick="fetch('/api/orders/slow', {method:'POST'}).then(r=>alert('status: '+r.status))">Создать задержку</button>
      <button onclick="fetch('/api/orders/load', {method:'POST'}).then(r=>alert('status: '+r.status))">Нагрузка</button>
      <p><a href="/docs">Swagger</a> · <a href="/metrics">Метрики</a></p>
    </body></html>
    """


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/api/catalog")
async def get_catalog():
    await asyncio.sleep(random.uniform(0.02, 0.15))
    log.info("catalog_viewed", items=len(CATALOG), trace_id=get_trace_id())
    return {"items": CATALOG}


@app.post("/api/orders")
async def create_order(payload: dict):
    start = time.perf_counter()
    order_id = str(uuid.uuid4())
    await asyncio.sleep(random.uniform(0.1, 0.8))
    ORDERS[order_id] = {
        "id": order_id,
        "items": payload.get("items", []),
        "status": "created",
    }
    ORDERS_CREATED.labels(status="success").inc()
    ORDER_PROCESSING_TIME.observe(time.perf_counter() - start)
    log.info("order_created", order_id=order_id, trace_id=get_trace_id())
    return {"order_id": order_id, "status": "created"}


@app.get("/api/orders/{order_id}")
async def get_order(order_id: str):
    order = ORDERS.get(order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    log.info("order_viewed", order_id=order_id, trace_id=get_trace_id())
    return order


@app.post("/api/orders/fail")
async def fail_endpoint():
    span = trace.get_current_span()
    span.set_status(trace.Status(trace.StatusCode.ERROR, "Intentional failure"))
    ORDERS_CREATED.labels(status="failed").inc()
    log.error("order_failed", reason="intentional", trace_id=get_trace_id())
    raise HTTPException(status_code=500, detail="Intentional failure for testing")


@app.post("/api/orders/slow")
async def slow_endpoint():
    delay = random.uniform(1.0, 3.0)
    with tracer.start_as_current_span("slow-dependency") as span:
        span.set_attribute("delay_seconds", delay)
        await asyncio.sleep(delay)
    log.info("slow_request_done", delay=delay, trace_id=get_trace_id())
    return {"status": "ok", "delay": delay}


@app.post("/api/orders/load")
async def load_endpoint(count: int = 50):
    async with httpx.AsyncClient(base_url="http://orders-api:8000") as client:
        tasks = [client.post("/api/orders", json={"items": [1]}) for _ in range(count)]
        await asyncio.gather(*tasks, return_exceptions=True)
    log.info("load_generated", count=count, trace_id=get_trace_id())
    return {"status": "ok", "requests_sent": count}