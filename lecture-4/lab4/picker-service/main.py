import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "picker-group")
INSTANCE_NAME = os.getenv("INSTANCE_NAME") or os.getenv("HOSTNAME", "picker-unknown")

processed: list[dict] = []


async def consume_loop():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    await consumer.start()
    try:
      async for msg in consumer:
        await asyncio.sleep(1.5)  # имитация «сборки заказа»
        print(
          f"[{INSTANCE_NAME}] picked order "
          f"{msg.value.get('order_id')} "
          f"partition={msg.partition} offset={msg.offset}",
          flush=True,
        )
        record = {
          "order_id": msg.value.get("order_id"),
          "items": msg.value.get("items", []),
          "partition": msg.partition,
          "offset": msg.offset,
          "picked_by": INSTANCE_NAME,
          "picked_at": datetime.now(timezone.utc).isoformat(),
        }
        processed.append(record)
    finally:
        await consumer.stop()


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(consume_loop())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


app = FastAPI(title="picker-service", lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTML_PAGE.replace("__INSTANCE__", INSTANCE_NAME)


@app.get("/api/processed")
async def list_processed():
    return list(reversed(processed[-20:]))


@app.get("/health")
async def health():
    return {"status": "ok", "instance": INSTANCE_NAME}


HTML_PAGE = """
<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <title>picker-service — сборщик заказов</title>
  <style>
    body { font-family: sans-serif; max-width: 900px; margin: 30px auto; }
    h1 { font-size: 20px; }
    table { border-collapse: collapse; width: 100%; margin-top: 20px; font-size: 13px; }
    th, td { border: 1px solid #ccc; padding: 6px 10px; text-align: left; }
    th { background: #f0f0f0; }
    .meta { color: #555; font-size: 13px; }
    code { background: #eee; padding: 1px 5px; border-radius: 3px; }
  </style>
</head>
<body>
  <h1>picker-service — сборщик заказов</h1>
  <p class="meta">Инстанс: <code>__INSTANCE__</code> · Группа: <code>picker-group</code></p>

  <h2 style="font-size:16px; margin-top:30px;">Обработанные заказы</h2>
  <table>
    <thead>
      <tr><th>order_id</th><th>partition</th><th>offset</th><th>собрал</th></tr>
    </thead>
    <tbody id="rows"></tbody>
  </table>

<script>
async function refresh() {
  const r = await fetch('/api/processed');
  const rows = await r.json();
  document.getElementById('rows').innerHTML = rows.map(o =>
    `<tr><td>${o.order_id.slice(0,8)}…</td><td>${o.partition}</td><td>${o.offset}</td><td>${o.picked_by}</td></tr>`
  ).join('');
}
setInterval(refresh, 2000);
refresh();
</script>
</body>
</html>
"""