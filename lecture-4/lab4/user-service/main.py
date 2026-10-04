import json
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders")
INSTANCE_NAME = os.getenv("INSTANCE_NAME", "user-service-1")

producer: AIOKafkaProducer | None = None
sent_orders: list[dict] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    try:
        yield
    finally:
        await producer.stop()


app = FastAPI(title="user-service", lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTML_PAGE.replace("__INSTANCE__", INSTANCE_NAME)


@app.post("/api/orders")
async def create_order():
    order = {
        "order_id": str(uuid.uuid4()),
        "items": ["apples", "pears", "strawberries"],
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    metadata = await producer.send_and_wait(KAFKA_TOPIC, value=order)
    record = {
        **order,
        "partition": metadata.partition,
        "offset": metadata.offset,
        "sent_by": INSTANCE_NAME,
    }
    sent_orders.append(record)
    return JSONResponse(record)


@app.get("/api/orders")
async def list_orders():
    return list(reversed(sent_orders[-20:]))


@app.get("/health")
async def health():
    return {"status": "ok", "instance": INSTANCE_NAME}


HTML_PAGE = """
<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <title>user-service — издатель заказов</title>
  <style>
    body { font-family: sans-serif; max-width: 900px; margin: 30px auto; }
    h1 { font-size: 20px; }
    button { padding: 10px 18px; font-size: 15px; cursor: pointer; }
    table { border-collapse: collapse; width: 100%; margin-top: 20px; font-size: 13px; }
    th, td { border: 1px solid #ccc; padding: 6px 10px; text-align: left; }
    th { background: #f0f0f0; }
    .meta { color: #555; font-size: 13px; }
    code { background: #eee; padding: 1px 5px; border-radius: 3px; }
  </style>
</head>
<body>
  <h1>user-service — издатель заказов</h1>
  <p class="meta">Инстанс: <code>__INSTANCE__</code> · Топик: <code>orders</code></p>
  <button onclick="createOrder()">Создать заказ</button>
  <p id="status" class="meta"></p>

  <h2 style="font-size:16px; margin-top:30px;">Отправленные заказы</h2>
  <table>
    <thead>
      <tr><th>order_id</th><th>partition</th><th>offset</th><th>отправлено</th></tr>
    </thead>
    <tbody id="rows"></tbody>
  </table>

<script>
async function createOrder() {
  const r = await fetch('/api/orders', {method: 'POST'});
  const j = await r.json();
  document.getElementById('status').textContent =
    `Отправлено: partition=${j.partition}, offset=${j.offset}`;
  refresh();
}
async function refresh() {
  const r = await fetch('/api/orders');
  const rows = await r.json();
  document.getElementById('rows').innerHTML = rows.map(o =>
    `<tr><td>${o.order_id.slice(0,8)}…</td><td>${o.partition}</td><td>${o.offset}</td><td>${o.sent_by}</td></tr>`
  ).join('');
}
setInterval(refresh, 2000);
refresh();
</script>
</body>
</html>
"""