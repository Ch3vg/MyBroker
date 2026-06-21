# Task Broker

HTTP-брокер задач на Python (FastAPI + SQLite). At-least-once доставка, блокировка, heartbeat, retry и Dead Letter Queue — без Redis, RabbitMQ и других внешних очередей.

Брокер оформлен как **Python-библиотека**: все параметры передаются в конструктор класса `Broker`, затем сервер запускается программно.

Текущая версия: **v1.0.0**.

Требуется **Python 3.10+**.

## Возможности

- **Python-библиотека** — настройка через конструктор `Broker`, запуск через `run()` или ASGI `app`
- **Публикация задач** с произвольным JSON-payload и отложенным запуском (`delay_seconds`)
- **Pull-модель** для воркеров с long polling и атомарной блокировкой (`FOR UPDATE SKIP LOCKED`)
- **Heartbeat** для продления блокировки и автоматического requeue при падении воркера
- **Retry и DLQ** — per-task лимит повторов, задачи с исчерпанными попытками переходят в `DEAD`
- **Заменяемое хранилище** — SQLite по умолчанию, PostgreSQL через DSN
- **Observability** — JSON-логи (structlog), метрики Prometheus

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

## Использование

```bash
pip install task-broker
```

```python
from broker import Broker

broker = Broker(
    dsn="sqlite+aiosqlite:///./broker.db",
    host="0.0.0.0",
    port=8001,
    default_lock_ttl_seconds=60,
    default_max_retries=3,
    retry_delay_seconds=5,
    default_pull_timeout_seconds=30,
    max_pull_timeout_seconds=120,
    pull_interval_seconds=1,
)

broker.run()  # блокирующий запуск HTTP-сервера
```

Для интеграции в существующее приложение доступно ASGI-приложение:

```python
app = broker.app  # монтирование в FastAPI / запуск через uvicorn
```

### Параметры `Broker`

| Параметр | По умолчанию | Описание |
|----------|--------------|----------|
| `dsn` | `sqlite+aiosqlite:///./broker.db` | DSN SQLAlchemy (SQLite, PostgreSQL и др.) |
| `host` | `"0.0.0.0"` | Адрес HTTP-сервера |
| `port` | `8001` | Порт HTTP-сервера |
| `default_lock_ttl_seconds` | `60` | TTL блокировки задачи |
| `default_max_retries` | `3` | Лимит повторов по умолчанию (per-task override при publish) |
| `retry_delay_seconds` | `5` | Задержка перед повторной выдачей после nack |
| `dead_letter_enabled` | `True` | Перевод в DEAD при исчерпании попыток |
| `default_pull_timeout_seconds` | `30` | Long poll timeout по умолчанию |
| `max_pull_timeout_seconds` | `120` | Максимально допустимый pull timeout |
| `pull_interval_seconds` | `1` | Интервал опроса БД при long polling |
| `list_default_limit` | `50` | Размер страницы list API по умолчанию *(v0.6.0)* |
| `list_max_limit` | `200` | Максимальный limit list API *(v0.6.0)* |
| `api_key` | `None` | Статичный API-ключ; если задан — доступ к API только с ключом *(v0.7.0)* |
| `log_level` | `"INFO"` | Уровень логирования |

Полный список и описание — в [docs/DEVELOPER.md](docs/DEVELOPER.md).

## Дорожная карта

| Версия | Содержание |
|--------|------------|
| **v0.1.0** | Класс `Broker`, БД, health, structlog, `run()` |
| **v0.2.0** | Publish, status |
| **v0.3.0** | Pull, long polling, блокировка |
| **v0.4.0** | Heartbeat, ack, nack, retry, DLQ |
| **v0.5.0** | Метрики Prometheus |
| **v0.6.0** | List API, PostgreSQL smoke-тест |
| **v0.7.0** | Статичный API-ключ (`Broker.api_key`) — проверка на каждом запросе, чтобы закрыть брокер от посторонних |
| **v1.0.0** | Первый стабильный релиз: полный API, PostgreSQL, CI, документация |

## Документация

Подробная спецификация API, модель данных, алгоритмы и руководство для разработчиков:

- **[docs/DEVELOPER.md](docs/DEVELOPER.md)** — внутреннее устройство и спецификация
- **[docs/API_EXAMPLES.md](docs/API_EXAMPLES.md)** — готовые примеры HTTP-запросов (curl, Python)

## Лицензия

[MIT](LICENSE)
