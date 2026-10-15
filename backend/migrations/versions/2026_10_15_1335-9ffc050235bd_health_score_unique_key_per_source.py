"""health score unique key per source

Revision ID: 9ffc050235bd
Revises: b679f6226f99

A score with a data source (OW computes one per source) is unique per source, so two sources
scored at the same time no longer collide. Scores without one (provider scores) keep the old key:
NULLs are distinct in a unique index, so adding data_source_id to it would stop deduplicating them.

downgrade restores the old constraint and fails if scores of two sources share a time by then.
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "9ffc050235bd"
down_revision: Union[str, None] = "b679f6226f99"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Concurrently, so building them on a large health_score does not block writes. The old
    # constraint keeps rows unique until both indexes are in place.
    with op.get_context().autocommit_block():
        op.create_index(
            "uq_health_score_unsourced_time",
            "health_score",
            ["user_id", "provider", "category", "recorded_at"],
            unique=True,
            postgresql_where=sa.text("data_source_id IS NULL"),
            postgresql_concurrently=True,
        )
        op.create_index(
            "uq_health_score_source_time",
            "health_score",
            ["user_id", "provider", "data_source_id", "category", "recorded_at"],
            unique=True,
            postgresql_where=sa.text("data_source_id IS NOT NULL"),
            postgresql_concurrently=True,
        )
    op.drop_constraint("uq_health_score_user_provider_category_time", "health_score", type_="unique")


def downgrade() -> None:
    op.create_unique_constraint(
        "uq_health_score_user_provider_category_time",
        "health_score",
        ["user_id", "provider", "category", "recorded_at"],
    )
    with op.get_context().autocommit_block():
        op.drop_index(
            "uq_health_score_source_time",
            table_name="health_score",
            postgresql_where=sa.text("data_source_id IS NOT NULL"),
            postgresql_concurrently=True,
        )
        op.drop_index(
            "uq_health_score_unsourced_time",
            table_name="health_score",
            postgresql_where=sa.text("data_source_id IS NULL"),
            postgresql_concurrently=True,
        )
