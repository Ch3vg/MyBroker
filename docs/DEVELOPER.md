# Task Broker – Developer Guide

> Пользовательская документация: [README.md](../README.md)

## Обзор

Брокер задач, написанный на Python с использованием FastAPI и SQLite (aiosqlite). Обеспечивает at‑least‑once доставку, блокировку задач, heartbeat, retry и Dead Letter Queue. Брокер работает как отдельный HTTP-сервис, не требует внешних очередей (Redis, RabbitMQ).

Брокер поставляется как **Python-библиотека**. Все настройки передаются в конструктор класса `Broker`; YAML-файлы и переменные окружения не используются. По умолчанию — SQLite, но хранилище заменяемо через параметр `dsn`.

Этот документ предназначен для разработчиков, которые хотят понять внутреннее устройство, дорабатывать или отлаживать брокер.

---

## Класс Broker

Точка входа библиотеки — класс `Broker`. Он принимает все параметры явно, создаёт FastAPI-приложение и управляет жизненным циклом БД.

```python
from broker import Broker

broker = Broker(
    dsn="sqlite+aiosqlite:///./broker.db",
    host="0.0.0.0",
    port=8001,
    default_lock_ttl_seconds=60,
    default_max_retries=3,
    retry_delay_seconds=5,
    dead_letter_enabled=True,
    default_pull_timeout_seconds=30,
    max_pull_timeout_seconds=120,
    pull_interval_seconds=1,
    list_default_limit=50,
    list_max_limit=200,
    log_level="INFO",
)

broker.run()       # блокирующий запуск (uvicorn)
broker.app         # ASGI-приложение для встраивания
```

### Методы

| Метод / свойство | Описание |
|------------------|----------|
| `run()` | Запуск HTTP-сервера на `host:port` |
| `app` | FastAPI ASGI-приложение (для uvicorn, hypercorn, встраивания) |

При первом запуске выполняется инициализация БД (создание таблицы, индексов). Для SQLite файл создаётся по пути из DSN.

---

## Модель данных

Таблица `tasks`:

    Колонка       | Тип              | Описание
    ---------------|------------------|---------------------------------------------------------------------------
    id             | TEXT (PK)        | UUID задачи
    task_type      | TEXT NOT NULL    | Тип задачи (например, config.regenerate)
    payload        | JSON NOT NULL    | Пользовательские данные (произвольный JSON)
    status         | TEXT             | PENDING, PROCESSING, COMPLETED, DEAD
    max_retries    | INTEGER NOT NULL | Максимальное число повторных попыток (per-task, см. publish)
    retries        | INTEGER          | Текущее число попыток
    available_at   | TIMESTAMP NOT NULL | Время, когда задача становится доступна для pull
    lock_until     | TIMESTAMP        | Время блокировки задачи (NULL если нет)
    worker_id      | TEXT             | Идентификатор воркера (NULL если свободна)
    created_at     | TIMESTAMP        | Время создания
    updated_at     | TIMESTAMP        | Время последнего изменения

### Статусы задачи

    PENDING     – ожидает выдачи воркеру
    PROCESSING  – заблокирована воркером
    COMPLETED   – успешно выполнена
    DEAD        – исчерпаны попытки (Dead Letter Queue)

Индекс для быстрого поиска задач при pull:

    CREATE INDEX idx_tasks_pull ON tasks(status, task_type, available_at, lock_until, created_at);

---

## API эндпоинты

### POST /api/v1/tasks – публикация задачи

Запрос:

    {
      "task_type": "config.regenerate",
      "payload": {"config_id": "123e4567-..."},
      "delay_seconds": 0,
      "max_retries": 3
    }

Поля:
- `task_type` – обязательный
- `payload` – обязательный, произвольный JSON
- `delay_seconds` – опционально, по умолчанию 0; отложенный запуск задачи
- `max_retries` – опционально; per-task лимит повторов. Если не передан – берётся из `Broker.default_max_retries`

Ответ (201 Created):

    {"task_id": "550e8400-..."}

Логика:
- сохранение задачи со статусом PENDING
- `available_at = NOW() + delay_seconds` – воркер не увидит задачу раньше этого момента
- `max_retries` сохраняется в строке задачи

---

### GET /api/v1/tasks/pull – получение задачи воркером (Long Polling)

Параметры запроса:
- `task_types` – опционально, список типов задач, которые воркер готов обрабатывать (повторяющийся query-параметр или через запятую). Если не передан или пуст – воркер получает задачи любого типа
- `worker_id` – обязательный, уникальный ID воркера
- `timeout` – опционально, максимальное время ожидания (сек), по умолчанию `Broker.default_pull_timeout_seconds`

Пример: `GET /api/v1/tasks/pull?worker_id=w-1&task_types=config.regenerate&task_types=email.send`

Алгоритм:
1. Брокер атомарно ищет задачу, удовлетворяющую условиям:
   - `task_type` входит в `task_types`, если список задан; иначе без фильтра по типу
   - статус PENDING (или PROCESSING с истёкшим `lock_until`)
   - `available_at <= NOW()`
   - используется `SELECT ... FOR UPDATE SKIP LOCKED`
2. Если задача найдена – переводит её в PROCESSING, устанавливает `worker_id` и `lock_until = NOW() + TTL`, коммитит транзакцию и возвращает задачу.
3. Если задач нет – повторяет попытку с интервалом `Broker.pull_interval_seconds` до истечения `timeout`, затем возвращает 204 No Content. Транзакция на время ожидания не удерживается.

Ответ (200 OK):

    {
      "task_id": "550e8400-...",
      "task_type": "config.regenerate",
      "payload": {"config_id": "123e4567-..."},
      "lock_ttl_seconds": 60
    }

Ответ (204 No Content) при отсутствии задач.

---

### POST /api/v1/tasks/{task_id}/heartbeat – продление блокировки

Запрос:

    {"worker_id": "worker-001"}

Логика:
- проверяет, что задача существует, имеет статус PROCESSING, и её `worker_id` совпадает с переданным
- обновляет `lock_until = NOW() + TTL`
- если задача уже не принадлежит воркеру – возвращает 409 Conflict с сообщением `STALE_TASK`; дальнейшие heartbeat от этого воркера игнорируются (ответственность на клиенте)

Ответы:
- 200 OK – блокировка продлена
- 409 Conflict – задача уже не принадлежит этому воркеру

---

### POST /api/v1/tasks/{task_id}/ack – подтверждение успешного выполнения

Запрос:

    {"worker_id": "worker-001"}

Логика:
- проверяет статус PROCESSING и совпадение `worker_id`
- переводит задачу в COMPLETED, сбрасывает `lock_until` и `worker_id`
- возвращает 200 OK или 409 Conflict при несовпадении (ответственность за прекращение работы – на клиенте)

---

### POST /api/v1/tasks/{task_id}/nack – сообщение об ошибке

Запрос:

    {"worker_id": "worker-001", "reason": "Xray API timeout"}

Логика:
- проверяет статус PROCESSING и совпадение `worker_id`
- увеличивает счётчик `retries`:
  - если `retries < max_retries` – переводит задачу в PENDING, устанавливает `available_at = NOW() + retry_delay`, сбрасывает `worker_id` и `lock_until`
  - если `retries >= max_retries` – переводит в DEAD (Dead Letter Queue)
- возвращает 200 OK или 409 Conflict

---

### GET /api/v1/tasks/{task_id}/status – проверка статуса

Ответ:

    {
      "id": "550e8400-...",
      "status": "PROCESSING",
      "retries": 0,
      "max_retries": 3,
      "available_at": "2025-01-01T12:00:00Z",
      "created_at": "2025-01-01T12:00:00Z"
    }

---

### GET /api/v1/tasks – список задач с фильтрами *(v0.6.0)*

Параметры запроса:
- `status` – опционально, фильтр по статусу (например, `DEAD`)
- `task_type` – опционально, фильтр по типу
- `limit` – опционально, размер страницы (по умолчанию `Broker.list_default_limit`)
- `offset` – опционально, смещение для пагинации

Ответ (200 OK):

    {
      "items": [
        {
          "id": "550e8400-...",
          "task_type": "config.regenerate",
          "status": "DEAD",
          "retries": 3,
          "max_retries": 3,
          "available_at": "2025-01-01T12:00:00Z",
          "created_at": "2025-01-01T12:00:00Z"
        }
      ],
      "total": 1,
      "limit": 50,
      "offset": 0
    }

---

### GET /api/v1/health – liveness-проверка

Возвращает `{"status": "ok"}` (200).

---

### GET /api/v1/metrics – метрики Prometheus *(v0.5.0)*

Эндпоинт в формате Prometheus text exposition. Основные метрики:

    broker_tasks_pending{task_type="..."}       – gauge, задачи в ожидании
    broker_tasks_processing{task_type="..."}    – gauge, задачи в обработке
    broker_tasks_dead{task_type="..."}          – gauge, задачи в DLQ
    broker_tasks_published_total{task_type="..."}  – counter
    broker_tasks_completed_total{task_type="..."}   – counter
    broker_tasks_nacked_total{task_type="..."}       – counter
    broker_pull_duration_seconds                  – histogram
    broker_pull_empty_total                       – counter, pull без задачи (204)

---

## Ключевые алгоритмы

### Атомарный pull (предотвращение гонок)

Используется `SELECT ... FOR UPDATE SKIP LOCKED` в рамках короткой транзакции. Это гарантирует, что даже при одновременных запросах от нескольких воркеров каждая задача будет выбрана только один раз. В SQLite эта возможность доступна с версии 3.37.0; в более старых версиях можно использовать `UPDATE ... WHERE ... RETURNING` с атомарным обновлением.

### Отложенный запуск (delay_seconds)

При publish брокер устанавливает `available_at = NOW() + delay_seconds`. Pull выдаёт только задачи с `available_at <= NOW()`. При nack с retry `available_at` сдвигается на `NOW() + retry_delay`.

### Long polling

Ожидание реализовано циклом коротких транзакций: попытка pull → sleep на `pull_interval_seconds` → повтор до `timeout`. Интервал и таймауты задаются при создании `Broker`.

### Heartbeat и TTL

- воркер рекомендуется отправлять heartbeat не реже чем каждые TTL/3 секунд
- если heartbeat не пришёл до `lock_until`, задача становится доступной для других воркеров (при следующем pull)
- если heartbeat/ack/nack приходит от воркера, которому задача уже не принадлежит, брокер отвечает 409 Conflict и не меняет состояние задачи. Если воркер продолжит работу – это его ответственность; брокер последующие запросы от «устаревшего» воркера просто отклоняет

### Dead Letter Queue

- при достижении `retries >= max_retries` задача получает статус DEAD
- такие задачи не выдаются воркерам и не перевыполняются автоматически
- мониторинг DLQ: метрики Prometheus (v0.5.0), list API `GET /api/v1/tasks?status=DEAD` (v0.6.0)

---

## Параметры Broker

Все настройки передаются в конструктор `Broker`. Значения по умолчанию указаны в таблице.

### Хранилище

| Параметр | По умолчанию | Описание |
|----------|--------------|----------|
| `dsn` | `sqlite+aiosqlite:///./broker.db` | DSN SQLAlchemy |

### HTTP-сервер

| Параметр | По умолчанию | Описание |
|----------|--------------|----------|
| `host` | `"0.0.0.0"` | Адрес bind |
| `port` | `8001` | Порт |

### Очередь

| Параметр | По умолчанию | Описание |
|----------|--------------|----------|
| `default_lock_ttl_seconds` | `60` | TTL блокировки; используется при pull и heartbeat |
| `default_max_retries` | `3` | Default для publish, если клиент не передал `max_retries` |
| `retry_delay_seconds` | `5` | Задержка перед повторной выдачей после nack |
| `dead_letter_enabled` | `True` | Перевод в DEAD при исчерпании попыток |

### Long polling

| Параметр | По умолчанию | Описание |
|----------|--------------|----------|
| `default_pull_timeout_seconds` | `30` | Timeout long poll по умолчанию |
| `max_pull_timeout_seconds` | `120` | Максимально допустимый timeout в query-параметре |
| `pull_interval_seconds` | `1` | Интервал опроса БД между попытками pull |

### List API *(v0.6.0)*

| Параметр | По умолчанию | Описание |
|----------|--------------|----------|
| `list_default_limit` | `50` | Размер страницы по умолчанию |
| `list_max_limit` | `200` | Максимальный limit |

### Логирование

| Параметр | По умолчанию | Описание |
|----------|--------------|----------|
| `log_level` | `"INFO"` | Уровень structlog |

`default_max_retries` при publish сохраняется в строку задачи как per-task значение.

---

## План версий

Разработка ведётся итерациями с тегами SemVer (pre-1.0).

### v0.1.0 – Каркас и инфраструктура
- структура проекта, зависимости
- класс `Broker` с параметрами конструктора
- слой БД, миграции, таблица `tasks`
- `GET /health`, `run()`, structlog

### v0.2.0 – Публикация и статус
- `POST /tasks` (`delay_seconds`, `max_retries`, `available_at`)
- `GET /tasks/{id}/status`
- тесты publish и status

### v0.3.0 – Pull и блокировка
- атомарный pull с фильтром `task_types` (опционально) и учётом `available_at`, expired lock
- long polling (цикл с `interval_seconds`)
- тесты конкурентного pull

### v0.4.0 – Жизненный цикл задачи
- heartbeat, ack, nack
- retry и переход в DEAD
- тесты stale/conflict/retry/DLQ

### v0.5.0 – Observability
- метрики Prometheus
- `GET /metrics`

### v0.6.0 – List API и PostgreSQL
- `GET /tasks` с фильтрами и пагинацией
- документация и smoke-тест для PostgreSQL DSN

---

## Запуск для разработки

Минимальная версия Python: **3.10**. Разработка ведётся только в venv:

    python3.10 -m venv .venv
    .venv\Scripts\activate        # Windows
    source .venv/bin/activate     # Linux / macOS

    pip install -e ".[dev]"

    python -c "
    from broker import Broker
    Broker(dsn='sqlite+aiosqlite:///./broker.db').run()
    "

При первом запуске создаётся файл `broker.db` (если не указан другой путь в DSN). Миграции выполняются автоматически при старте.

---

## Тестирование

Тесты используют отдельную тестовую БД (`file:memdb1?mode=memory&cache=shared` для SQLite). Запуск из активированного venv:

    pytest

По умолчанию pytest проверяет coverage пакета `broker` (порог — 90%). Перед коммитом всегда прогоняйте тесты с coverage.

Покрытие v0.1.0:
- класс `Broker` (настройки, `run()`, `app`)
- инициализация схемы БД и индексов
- `GET /health`
- настройка structlog

Будущие версии добавят тесты publish, pull, ack, nack, heartbeat, retry и DLQ.

---

## Расширение и доработка

### Добавление нового типа задачи

Тип задачи не требует регистрации в брокере – это просто строка в `task_type`. Воркер сам решает, как обрабатывать каждый тип на основе payload.

### Изменение логики ретраев

Реализовано в методе `nack()` репозитория. Можно переопределить, задав другую стратегию (экспоненциальная задержка, приоритезация и т.д.) в том же методе или через параметры `Broker`.

### Использование другого хранилища (например, PostgreSQL)

Передайте DSN PostgreSQL в конструктор: `Broker(dsn="postgresql+asyncpg://...")`. SQLAlchemy поддерживает PostgreSQL – `FOR UPDATE SKIP LOCKED` работает аналогично. Параметры long polling задаются независимо от СУБД.

---

## Логирование

Логи выводятся в stdout в формате JSON (structlog). Уровень задаётся параметром `log_level` при создании `Broker`. Основные события: создание задачи, pull, ack, nack, heartbeat, ошибки, DLQ.

---

## Лицензия

MIT

---
