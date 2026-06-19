import pytest

from broker.api.query_params import parse_task_types


def test_parse_task_types_none() -> None:
    assert parse_task_types(None) is None


def test_parse_task_types_empty_list() -> None:
    assert parse_task_types([]) is None


def test_parse_task_types_repeated_params() -> None:
    assert parse_task_types(["a", "b"]) == ["a", "b"]


def test_parse_task_types_comma_separated() -> None:
    assert parse_task_types(["a,b", "c"]) == ["a", "b", "c"]


def test_parse_task_types_strips_whitespace() -> None:
    assert parse_task_types([" a , b "]) == ["a", "b"]
