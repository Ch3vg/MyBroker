def parse_task_types(task_types: list[str] | None) -> list[str] | None:
    if not task_types:
        return None
    parsed: list[str] = []
    for value in task_types:
        parsed.extend(part.strip() for part in value.split(",") if part.strip())
    return parsed or None
