from prometheus_client import Counter, Gauge, Histogram

TASKS_PENDING = Gauge(
    "broker_tasks_pending",
    "Tasks waiting to be pulled",
    ["task_type"],
)
TASKS_PROCESSING = Gauge(
    "broker_tasks_processing",
    "Tasks being processed",
    ["task_type"],
)
TASKS_DEAD = Gauge(
    "broker_tasks_dead",
    "Tasks in the dead letter queue",
    ["task_type"],
)
TASKS_PUBLISHED = Counter(
    "broker_tasks_published_total",
    "Tasks published",
    ["task_type"],
)
TASKS_COMPLETED = Counter(
    "broker_tasks_completed_total",
    "Tasks completed successfully",
    ["task_type"],
)
TASKS_NACKED = Counter(
    "broker_tasks_nacked_total",
    "Tasks nacked by workers",
    ["task_type"],
)
PULL_DURATION = Histogram(
    "broker_pull_duration_seconds",
    "Duration of pull requests in seconds",
)
PULL_EMPTY = Counter(
    "broker_pull_empty_total",
    "Pull requests that returned no task",
)

_STATUS_GAUGES: dict[str, Gauge] = {
    "PENDING": TASKS_PENDING,
    "PROCESSING": TASKS_PROCESSING,
    "DEAD": TASKS_DEAD,
}
_last_gauge_labels: dict[str, set[str]] = {status: set() for status in _STATUS_GAUGES}


def record_published(task_type: str) -> None:
    TASKS_PUBLISHED.labels(task_type=task_type).inc()


def record_completed(task_type: str) -> None:
    TASKS_COMPLETED.labels(task_type=task_type).inc()


def record_nacked(task_type: str) -> None:
    TASKS_NACKED.labels(task_type=task_type).inc()


def record_pull_empty() -> None:
    PULL_EMPTY.inc()


def observe_pull_duration(seconds: float) -> None:
    PULL_DURATION.observe(seconds)


def refresh_status_gauges(counts: list[tuple[str, str, int]]) -> None:
    new_labels = {status: set() for status in _STATUS_GAUGES}
    for status, task_type, count in counts:
        gauge = _STATUS_GAUGES.get(status)
        if gauge is None:
            continue
        gauge.labels(task_type=task_type).set(count)
        new_labels[status].add(task_type)
    for status, gauge in _STATUS_GAUGES.items():
        for task_type in _last_gauge_labels[status] - new_labels[status]:
            gauge.labels(task_type=task_type).set(0)
        _last_gauge_labels[status] = new_labels[status]
