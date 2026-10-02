# Lab 2 - Monitoring and observability

Полный стек observability для сервиса заказов «Пальчики оближешь»: метрики, логи, трейсы и алерты.

## Что это

Продолжение Лабы 1. Для сервиса заказов поднят полный мониторинг:
- **Метрики** - Prometheus + Grafana (дашборд по методологии RED).
- **Логи** - Loki + Promtail + Grafana.
- **Трейсы** - OpenTelemetry + Jaeger.
- **Алерты** - Prometheus + Alertmanager (три правила).

Всё развёрнуто через Docker Compose на домашнем сервере, сервисы общаются через общую Docker-сеть `monitoring-net`.

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

## Структура репозитория
```
lab-2/
├── README.md
├── report.md
├── docker-compose.yml
├── app/
│   ├── main.py
│   ├── requirements.txt
│   └── Dockerfile
├── prometheus/
│   ├── prometheus.yml
│   └── alerts.yml
├── alertmanager/
│   └── alertmanager.yml
├── loki/
│   └── loki-config.yml
├── promtail/
│   └── promtail-config.yml
└── screenshots/
    ├── dashboard.png
    ├── logs.png
    ├── logs-error.png
    ├── jaeger-slow.png
    ├── jaeger-error.png
    ├── alerts-prometheus.png
    └── alerts-alertmanager.png
```
