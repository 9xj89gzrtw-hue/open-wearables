"""Tests for the unique keys handle_duplicates looks an existing row up by."""

from sqlalchemy import Column, inspect
from sqlalchemy.sql.elements import ColumnElement

from app.models import HealthScore
from app.utils.duplicates import _unique_keys

_TABLE = inspect(HealthScore).local_table


def _names(keys: list[tuple[list[Column], ColumnElement[bool] | None]]) -> list[list[str]]:
    return [[column.name for column in columns] for columns, _ in keys]


def test_the_violated_index_alone_with_its_where_clause() -> None:
    keys = _unique_keys(_TABLE, "uq_health_score_source_time")

    assert _names(keys) == [["user_id", "provider", "data_source_id", "category", "recorded_at"]]
    assert str(keys[0][1]) == "data_source_id IS NOT NULL"


def test_every_unique_key_when_postgres_names_none() -> None:
    keys = _unique_keys(_TABLE, None)

    assert sorted(_names(keys)) == [
        ["event_record_id", "provider", "category"],
        ["user_id", "provider", "category", "recorded_at"],
        ["user_id", "provider", "data_source_id", "category", "recorded_at"],
    ]
