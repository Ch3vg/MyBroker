# EVS Task Broker

HTTP-брокер задач на Python (FastAPI + SQLite). At-least-once доставка, блокировка, heartbeat, retry и Dead Letter Queue — без Redis, RabbitMQ и других внешних очередей.

Проект в стадии активной разработки (pre-1.0). Текущая версия: **v0.0.0** (спецификация и документация).

## Возможности

- **Публикация задач** с произвольным JSON-payload и отложенным запуском (`delay_seconds`)
- **Pull-модель** для воркеров с long polling и атомарной блокировкой (`FOR UPDATE SKIP LOCKED`)
- **Heartbeat** для продления блокировки и автоматического requeue при падении воркера
- **Retry и DLQ** — per-task лимит повторов, задачи с исчерпанными попытками переходят в `DEAD`
- **Заменяемое хранилище** — SQLite по умолчанию, PostgreSQL через DSN
- **Observability** — JSON-логи (structlog), метрики Prometheus (v0.5.0)

## Как это работает

```
Producer ──POST /tasks──► Broker (SQLite) ◄──GET /tasks/pull── Worker
                               │                      │
                               │              heartbeat / ack / nack
                               ▼
                          PENDING → PROCESSING → COMPLETED
                                            ↘ retry → DEAD
```

1. Producer публикует задачу → статус `PENDING`
2. Worker делает long poll → получает задачу, статус `PROCESSING`
3. Worker периодически шлёт heartbeat → продлевает `lock_until`
4. Worker завершает работу → `ack` (успех) или `nack` (ошибка + retry)

## API

| Метод | Эндпоинт | Описание | Статус |
|-------|----------|----------|--------|
| `POST` | `/api/v1/tasks` | Публикация задачи | v0.2.0 |
| `GET` | `/api/v1/tasks/pull` | Получение задачи (long polling) | v0.3.0 |
| `POST` | `/api/v1/tasks/{id}/heartbeat` | Продление блокировки | v0.4.0 |
| `POST` | `/api/v1/tasks/{id}/ack` | Подтверждение выполнения | v0.4.0 |
| `POST` | `/api/v1/tasks/{id}/nack` | Ошибка / retry | v0.4.0 |
| `GET` | `/api/v1/tasks/{id}/status` | Статус задачи | v0.2.0 |
| `GET` | `/api/v1/tasks` | Список с фильтрами | v0.6.0 |
| `GET` | `/api/v1/health` | Liveness | v0.1.0 |
| `GET` | `/api/v1/metrics` | Prometheus | v0.5.0 |

### Пример: публикация

```json
POST /api/v1/tasks
{
  "task_type": "config.regenerate",
  "payload": {"config_id": "123e4567-..."},
  "delay_seconds": 0,
  "max_retries": 3
}
```

### Пример: pull

```
GET /api/v1/tasks/pull?worker_id=w-1&task_types=config.regenerate&timeout=30
```

Опциональный параметр `task_types` — список типов, которые воркер готов обрабатывать. Если не указан — воркер получает задачи любого типа.

## Конфигурация

Файл `broker_config.yaml` или переменные окружения с префиксом `BROKER_`:

```yaml
storage:
  dsn: "sqlite:///./broker.db"
  journal_mode: "WAL"

server:
  host: "0.0.0.0"
  port: 8001
  workers: 4

queues:
  default_lock_ttl_seconds: 60
  default_max_retries: 3
  retry_delay_seconds: 5
  dead_letter_enabled: true

polling:
  default_timeout_seconds: 30
  max_timeout_seconds: 120
  interval_seconds: 1

list:
  default_limit: 50
  max_limit: 200
```

## Быстрый старт

> Код брокера находится в разработке. После релиза v0.1.0:

```bash
pip install -e .
uvicorn broker.main:app --reload --host 0.0.0.0 --port 8001
```

## Дорожная карта

| Версия | Содержание |
|--------|------------|
| **v0.1.0** | Каркас, конфиг, БД, health, structlog |
| **v0.2.0** | Publish, status |
| **v0.3.0** | Pull, long polling, блокировка |
| **v0.4.0** | Heartbeat, ack, nack, retry, DLQ |
| **v0.5.0** | Метрики Prometheus |
| **v0.6.0** | List API, PostgreSQL smoke-тест |

## Документация

Подробная спецификация API, модель данных, алгоритмы и руководство для разработчиков:

**[docs/DEVELOPER.md](docs/DEVELOPER.md)**

## Лицензия

[MIT](LICENSE)
