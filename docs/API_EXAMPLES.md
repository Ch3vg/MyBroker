# Примеры HTTP-запросов к брокеру

Готовые примеры для producer, worker и отладки. Базовый URL по умолчанию: `http://localhost:8001`.

Переменные для копирования в терминал:

```bash
export BROKER_URL="http://localhost:8001"
export API_KEY="my-secret"          # только если Broker(api_key=...) задан
export AUTH_HEADER="Authorization: Bearer ${API_KEY}"
```

В PowerShell:

```powershell
$env:BROKER_URL = "http://localhost:8001"
$env:API_KEY = "my-secret"
$headers = @{ Authorization = "Bearer $($env:API_KEY)" }
```

Если `api_key` не задан — заголовок `Authorization` не нужен (кроме раздела про auth).

---

## Health (без ключа)

Liveness-проверка. Работает **всегда**, даже при включённом `api_key`.

```bash
curl -s "${BROKER_URL}/api/v1/health"
```

Ответ:

```json
{"status": "ok"}
```

---

## Аутентификация (v0.7.0)

При `Broker(api_key="my-secret")` все эндпоинты ниже требуют Bearer-токен. `GET /health` — исключение.

```bash
# Успех
curl -s -H "${AUTH_HEADER}" "${BROKER_URL}/api/v1/metrics"

# 401 без ключа
curl -s -w "\nHTTP %{http_code}\n" "${BROKER_URL}/api/v1/tasks"
```

---

## Producer: публикация задачи

### Минимальный запрос

```bash
curl -s -X POST "${BROKER_URL}/api/v1/tasks" \
  -H "Content-Type: application/json" \
  -H "${AUTH_HEADER}" \
  -d '{
    "task_type": "config.regenerate",
    "payload": {"config_id": "123e4567-e89b-12d3-a456-426614174000"}
  }'
```

Ответ `201`:

```json
{"task_id": "550e8400-e29b-41d4-a716-446655440000"}
```

### С отложенным стартом и лимитом повторов

```bash
curl -s -X POST "${BROKER_URL}/api/v1/tasks" \
  -H "Content-Type: application/json" \
  -H "${AUTH_HEADER}" \
  -d '{
    "task_type": "email.send",
    "payload": {"to": "user@example.com", "template": "welcome"},
    "delay_seconds": 300,
    "max_retries": 5
  }'
```

### Ошибки валидации (422)

```bash
# Пустой task_type
curl -s -X POST "${BROKER_URL}/api/v1/tasks" \
  -H "Content-Type: application/json" \
  -H "${AUTH_HEADER}" \
  -d '{"task_type": "", "payload": {}}'
```

---

## Worker: pull (long polling)

### Ожидание задачи до 30 секунд

```bash
curl -s -w "\nHTTP %{http_code}\n" \
  "${BROKER_URL}/api/v1/tasks/pull?worker_id=worker-001&timeout=30" \
  -H "${AUTH_HEADER}"
```

Ответ `200` (задача получена):

```json
{
  "task_id": "550e8400-e29b-41d4-a716-446655440000",
  "task_type": "config.regenerate",
  "payload": {"config_id": "123e4567-e89b-12d3-a456-426614174000"},
  "lock_ttl_seconds": 60
}
```

Ответ `204` — задач нет (таймаут long poll истёк).

### Pull только определённых типов

Несколько значений `task_types` (повтор параметра):

```bash
curl -s "${BROKER_URL}/api/v1/tasks/pull" \
  -G \
  --data-urlencode "worker_id=worker-001" \
  --data-urlencode "timeout=0" \
  --data-urlencode "task_types=config.regenerate" \
  --data-urlencode "task_types=email.send" \
  -H "${AUTH_HEADER}"
```

Мгновенный опрос (`timeout=0`) — одна попытка без ожидания.

---

## Worker: жизненный цикл задачи

Подставьте `TASK_ID` из ответа publish или pull.

```bash
export TASK_ID="550e8400-e29b-41d4-a716-446655440000"
export WORKER_ID="worker-001"
```

### Heartbeat — продление блокировки

```bash
curl -s -X POST "${BROKER_URL}/api/v1/tasks/${TASK_ID}/heartbeat" \
  -H "Content-Type: application/json" \
  -H "${AUTH_HEADER}" \
  -d "{\"worker_id\": \"${WORKER_ID}\"}"
```

Ответ `200` — блокировка продлена.  
Ответ `409` + `{"detail": "STALE_TASK"}` — задача уже у другого воркера.

### Ack — успешное выполнение

```bash
curl -s -X POST "${BROKER_URL}/api/v1/tasks/${TASK_ID}/ack" \
  -H "Content-Type: application/json" \
  -H "${AUTH_HEADER}" \
  -d "{\"worker_id\": \"${WORKER_ID}\"}"
```

### Nack — ошибка и retry

```bash
curl -s -X POST "${BROKER_URL}/api/v1/tasks/${TASK_ID}/nack" \
  -H "Content-Type: application/json" \
  -H "${AUTH_HEADER}" \
  -d "{\"worker_id\": \"${WORKER_ID}\", \"reason\": \"upstream timeout\"}"
```

После исчерпания `max_retries` задача переходит в `DEAD`.

---

## Статус одной задачи

```bash
curl -s "${BROKER_URL}/api/v1/tasks/${TASK_ID}/status" \
  -H "${AUTH_HEADER}"
```

Ответ `200`:

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "PROCESSING",
  "retries": 0,
  "max_retries": 3,
  "available_at": "2026-06-19T12:00:00+00:00",
  "created_at": "2026-06-19T12:00:00+00:00"
}
```

Ответ `404` — задача не найдена.

---

## Список задач (list API)

### Все задачи (первая страница)

```bash
curl -s "${BROKER_URL}/api/v1/tasks" \
  -H "${AUTH_HEADER}"
```

### Фильтр по статусу и типу (мониторинг DLQ)

```bash
curl -s -G "${BROKER_URL}/api/v1/tasks" \
  --data-urlencode "status=DEAD" \
  --data-urlencode "task_type=config.regenerate" \
  --data-urlencode "limit=20" \
  --data-urlencode "offset=0" \
  -H "${AUTH_HEADER}"
```

Ответ `200`:

```json
{
  "items": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440000",
      "task_type": "config.regenerate",
      "status": "DEAD",
      "retries": 3,
      "max_retries": 3,
      "available_at": "2026-06-19T12:00:00+00:00",
      "created_at": "2026-06-19T11:55:00+00:00"
    }
  ],
  "total": 1,
  "limit": 20,
  "offset": 0
}
```

### Неверный статус (422)

```bash
curl -s -G "${BROKER_URL}/api/v1/tasks" \
  --data-urlencode "status=INVALID" \
  -H "${AUTH_HEADER}"
```

---

## Метрики Prometheus

```bash
curl -s "${BROKER_URL}/api/v1/metrics" \
  -H "${AUTH_HEADER}"
```

Текстовый формат Prometheus (`text/plain`). Примеры метрик: `broker_tasks_pending`, `broker_tasks_dead`, `broker_tasks_completed_total`.

---

## Полный сценарий: publish → pull → heartbeat → ack

```bash
BROKER_URL="${BROKER_URL:-http://localhost:8001}"
AUTH=()
[ -n "${API_KEY:-}" ] && AUTH=(-H "Authorization: Bearer ${API_KEY}")

# 1. Публикация
TASK_ID=$(curl -s -X POST "${BROKER_URL}/api/v1/tasks" \
  -H "Content-Type: application/json" \
  "${AUTH[@]}" \
  -d '{"task_type": "demo.job", "payload": {"step": 1}}' \
  | python -c "import sys, json; print(json.load(sys.stdin)['task_id'])")

echo "Published: ${TASK_ID}"

# 2. Pull
PULL_CODE=$(curl -s -o /tmp/pull.json -w "%{http_code}" \
  "${BROKER_URL}/api/v1/tasks/pull?worker_id=demo-worker&timeout=5&task_types=demo.job" \
  "${AUTH[@]}")

echo "Pull HTTP ${PULL_CODE}"
cat /tmp/pull.json
echo

# 3. Heartbeat
curl -s -X POST "${BROKER_URL}/api/v1/tasks/${TASK_ID}/heartbeat" \
  -H "Content-Type: application/json" \
  "${AUTH[@]}" \
  -d '{"worker_id": "demo-worker"}'
echo

# 4. Ack
curl -s -X POST "${BROKER_URL}/api/v1/tasks/${TASK_ID}/ack" \
  -H "Content-Type: application/json" \
  "${AUTH[@]}" \
  -d '{"worker_id": "demo-worker"}'
echo

# 5. Проверка статуса
curl -s "${BROKER_URL}/api/v1/tasks/${TASK_ID}/status" "${AUTH[@]}"
echo
```

---

## Python (httpx)

Минимальный producer и worker:

```python
import httpx

BASE = "http://localhost:8001"
HEADERS = {"Authorization": "Bearer my-secret"}  # убрать, если api_key не задан

with httpx.Client(base_url=BASE, headers=HEADERS, timeout=60.0) as client:
    # Producer
    created = client.post(
        "/api/v1/tasks",
        json={
            "task_type": "config.regenerate",
            "payload": {"config_id": "abc"},
        },
    )
    created.raise_for_status()
    task_id = created.json()["task_id"]

    # Worker: long poll
    pull = client.get(
        "/api/v1/tasks/pull",
        params={"worker_id": "py-worker-1", "task_types": "config.regenerate", "timeout": 10},
    )
    if pull.status_code == 204:
        print("no tasks")
    else:
        pull.raise_for_status()
        task = pull.json()
        assert task["task_id"] == task_id

        client.post(
            f"/api/v1/tasks/{task_id}/heartbeat",
            json={"worker_id": "py-worker-1"},
        ).raise_for_status()

        client.post(
            f"/api/v1/tasks/{task_id}/ack",
            json={"worker_id": "py-worker-1"},
        ).raise_for_status()

    status = client.get(f"/api/v1/tasks/{task_id}/status")
    status.raise_for_status()
    print(status.json())  # status: COMPLETED
```

Асинхронный воркер — `httpx.AsyncClient` с теми же путями и телами запросов.

---

## Сводка кодов ответов

| Код | Где | Значение |
|-----|-----|----------|
| `200` | pull, status, list, lifecycle | Успех |
| `201` | publish | Задача создана |
| `204` | pull | Задач нет (таймаут long poll) |
| `401` | любой (кроме health) | Нет или неверный API-ключ |
| `404` | status, lifecycle | Задача не найдена |
| `409` | heartbeat, ack, nack | `STALE_TASK` — воркер потерял блокировку |
| `422` | publish, list | Ошибка валидации параметров |

Подробная спецификация: [DEVELOPER.md](DEVELOPER.md).
