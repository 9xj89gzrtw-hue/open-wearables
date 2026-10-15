from functools import wraps
from typing import Callable, TypeVar

from psycopg.errors import UniqueViolation
from sqlalchemy import Column, Table, UniqueConstraint
from sqlalchemy.exc import IntegrityError as SQLAIntegrityError
from sqlalchemy.inspection import inspect
from sqlalchemy.sql.elements import ColumnElement

T = TypeVar("T")


def _unique_keys(table: Table, violated: str | None) -> list[tuple[list[Column], ColumnElement[bool] | None]]:
    """Columns of the table's unique constraints and unique indexes, with a partial index's WHERE.

    Only the violated one when Postgres names it: a partial index must not match a row it doesn't cover.
    """
    keys = [
        (constraint.name, list(constraint.columns), None)
        for constraint in table.constraints
        if isinstance(constraint, UniqueConstraint)
    ]
    keys += [
        (index.name, list(index.columns), index.dialect_options["postgresql"]["where"])
        for index in table.indexes
        if index.unique
    ]
    named = [key for key in keys if key[0] == violated]
    return [(columns, where) for _, columns, where in named or keys]


def handle_duplicates(func: Callable[..., T]) -> Callable[..., T | None]:
    """
    Decorator that handles duplicate inserts by querying for existing records
    based on unique constraints, unique indexes or primary keys.

    When a unique violation occurs, this decorator looks the existing record up by the
    constraint or index Postgres reports, or tries each of them when it reports none.
    """

    @wraps(func)
    def wrapper(*args, **kwargs) -> T | None:
        try:
            return func(*args, **kwargs)
        except SQLAIntegrityError as e:
            if isinstance(e.orig, UniqueViolation):
                model = args[0].model
                db_session = args[1]
                creator = args[2]

                db_session.rollback()

                creator_data = creator.model_dump()
                mapper = inspect(model)
                table = mapper.local_table

                for columns, where in _unique_keys(table, e.orig.diag.constraint_name):
                    query = db_session.query(model)
                    if where is not None:
                        query = query.filter(where)

                    all_columns_present = True
                    for column in columns:
                        if column.name in creator_data:
                            query = query.filter(
                                getattr(model, column.name) == creator_data[column.name],
                            )
                        else:
                            all_columns_present = False
                            break

                    if all_columns_present:
                        existing = query.one_or_none()
                        if existing:
                            return existing

                # fallback to primary key
                query = db_session.query(model)
                for key in mapper.primary_key:
                    if key.name in creator_data:
                        query = query.filter(getattr(model, key.name) == creator_data[key.name])

                existing = query.one_or_none()
                if existing:
                    return existing

            raise

    return wrapper
