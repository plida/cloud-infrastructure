# Лаба 2 — Полный мониторинг сервиса «Пальчики оближешь»

## Что это

Продолжение Лабы 1: для сервиса заказов из интернет-магазина «Пальчики оближешь» поднят полный стек observability — метрики, логи, трейсы и алерты.

## Что поднято

| Компонент | Роль | Порт |
|-----------|------|------|
| orders-api | Заглушка сервиса на FastAPI | 8000 |
| Prometheus | Сбор метрик (pull-модель) | 9090 |
| Grafana | Визуализация метрик и логов | 3000 |
| Loki | Хранилище логов | 3100 |
| Promtail | Агент сбора логов с Docker-контейнеров | 9080 |
| Jaeger | Хранилище и UI трейсов (OTLP) | 16686 |
| Alertmanager | Маршрутизация и рассылка алертов | 9093 |
| node-exporter | Метрики хоста (CPU, память, диск) | 9100 |

Всё развёрнуто через Docker Compose на домашнем сервере. Сервисы общаются через общую Docker-сеть `monitoring-net`.
## Подключение к серверу
```
ssh -L 19090:localhost:9090 `
    -L 13000:localhost:3000 `
    -L 19093:localhost:9093 `
    -L 18000:localhost:8000 `
    -L 19100:localhost:9100 `
    -L 13100:localhost:3100 `
    -L 19080:localhost:9080 `
    -L 16686:localhost:16686 `
    prj@cloud-infrastructure.yetti
```
Позволяет открыть в браузере сервисы.
## Что делает сервис-заглушка

Мини-сервис заказов с эндпоинтами:

- `GET /` — HTML-страница с тремя кнопками для провокации ситуаций
- `GET /api/catalog` — каталог товаров
- `POST /api/orders` — оформить заказ (успешно)
- `GET /api/orders/{id}` — статус заказа
- `POST /api/orders/fail` — возвращает 500, помечает спан как error, увеличивает `orders_created_total{status="failed"}`
- `POST /api/orders/slow` — отвечает медленно (1–3 сек), оборачивает операцию во вложенный спан `slow-dependency`
- `POST /api/orders/load` — делает 50 запросов к себе для всплеска RPS
- `GET /metrics` — метрики в формате Prometheus
- `GET /health` — health check

## Метрики и дашборд

Дашборд построен по методологии RED:

| Панель | Запрос | Что показывает |
|--------|--------|----------------|
| RPS | `sum(rate(http_requests_total{job="orders-api"}[1m]))` | Интенсивность запросов |
| Error rate | `sum(rate(http_requests_total{job="orders-api", status="5xx"}[5m])) / sum(rate(http_requests_total{job="orders-api"}[5m]))` | Доля ошибок 5xx |
| Latency p95 | `histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{job="orders-api"}[5m])) by (le))` | 95-й перцентиль времени ответа |
| Orders created | `sum(rate(orders_created_total{job="orders-api"}[5m])) by (status)` | Скорость создания заказов по статусу |

Почему RED: RED покрывает всё, что важно для сервисов, обрабатывающих запросы — Rate, Errors, Duration. Для инфраструктуры (хост) можно добавить USE, но в рамках лабы достаточно RED.

## Логи

Логи пишутся в структурированном JSON-формате через `structlog`. Каждая запись содержит:
- `level` — уровень (info/error)
- `event` — событие (order_created, order_failed, slow_request_done и т.д.)
- `trace_id` — ID трейса для связи с Jaeger
- `timestamp` — ISO-8601

Promtail собирает логи с Docker-контейнеров и отправляет в Loki. В Grafana можно фильтровать по сервису, уровню и тексту.

Пример запроса в Grafana -> Loki:

```logql
{service="orders-api"} | json | event != ""
```

Пример поиска ошибок:

```logql
{service="orders-api"} | json | level = "error"
```

## Трейсы

Сервис инструментирован OpenTelemetry SDK:
- Авто-инструментирование FastAPI -> корневой спан на каждый HTTP-запрос
- Вложенный спан `slow-dependency` в обработчике задержки
- Error-статус на спане при ошибке
- Экспорт по OTLP в Jaeger

Связка лог <-> трейс: по полю `trace_id` в JSON-логе можно найти тот же трейс в Jaeger UI.

## Алерты

Три правила в Prometheus (`prometheus/alerts.yml`).

### 1. HighErrorRate — высокая доля ошибок 5xx

```promql
sum(rate(http_requests_total{job="orders-api", status="5xx"}[5m]))
/
sum(rate(http_requests_total{job="orders-api", handler!="/metrics", handler!="none"}[5m])) > 0.05
```

Что ловит: доля ошибок 5xx среди пользовательских запросов превышает 5% в течение 1 минуты.

Чем грозит: клиенты не могут оформить заказы. Бизнес теряет деньги. Требуется немедленное вмешательство.

Почему порог 5%: достаточно низкий, чтобы заметить проблему рано, но достаточно высокий, чтобы не срабатывать на единичные ошибки. Из знаменателя исключены служебные эндпоинты (`/metrics`, `none`), чтобы фоновые скрейпы Prometheus не разбавляли долю ошибок.

### 2. HighLatencyP95 — высокий p95 ответа

```promql
histogram_quantile(0.95,
  sum(rate(http_request_duration_seconds_bucket{job="orders-api"}[5m])) by (le)
) > 0.3
```

Что ловит: 95-й перцентиль времени ответа превышает 0.3 секунды.

Чем грозит: сервис «тормозит» для большинства пользователей. Даже если ошибок нет, UX страдает, клиенты уходят. :(

Почему p95, а не среднее: среднее скрывает хвост медленных запросов. p95 показывает реальный опыт 5% самых неудачливых пользователей.


### 3. RequestRateSpike — всплеск RPS

```promql
sum(rate(http_requests_total{job="orders-api"}[1m])) > 0.5
```

Что ловит: RPS превышает 0.5 в течение 30 секунд.

Чем грозит: возможен DDoS, баг в клиенте или неожиданный всплеск популярности. Нужно проверить, справляется ли сервис.

## Скриншоты

- [`screenshots/dashboard.png`](screenshots/dashboard.png) — Grafana с RED-дашбордом после нагрузки
- [`screenshots/logs.png`](screenshots/logs.png) — Loki с логами orders-api
- [`screenshots/logs-error.png`](screenshots/logs-error.png) — Loki с логами ошибок
- [`screenshots/jaeger-slow.png`](screenshots/jaeger-slow.png) — Jaeger с трейсом задержки и спаном `slow-dependency`
- [`screenshots/jaeger-error.png`](screenshots/jaeger-error.png) — Jaeger с трейсом ошибки (красный спан)
- [`screenshots/alerts-prometheus.png`](screenshots/alerts-prometheus.png) — Prometheus `/alerts`, три алерта в Firing
- [`screenshots/alerts-alertmanager.png`](screenshots/alerts-alertmanager.png) — Alertmanager с тремя алертами

## Как запустить

```bash
# 1. Общая сеть (один раз)
docker network create monitoring-net

# 2. Запуск
cd lab-2
docker compose up -d --build

# 3. Проверка
docker compose ps
```

Сервисы доступны по адресам:
- Grafana: `http://localhost:3000` (admin/admin)
- Prometheus: `http://localhost:9090`
- Alertmanager: `http://localhost:9093`
- Jaeger: `http://localhost:16686`
- orders-api: `http://localhost:8000`
