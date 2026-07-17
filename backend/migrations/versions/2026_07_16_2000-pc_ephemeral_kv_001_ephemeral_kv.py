"""ephemeral_kv for Redis-free deployments

Revision ID: pc_ephemeral_kv_001
Revises: 9f0940493a9b

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "pc_ephemeral_kv_001"
down_revision: Union[str, None] = "9f0940493a9b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "ephemeral_kv",
        sa.Column("key", sa.Text(), primary_key=True),
        sa.Column("kind", sa.String(length=16), nullable=False),
        sa.Column("value_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_ephemeral_kv_expires_at", "ephemeral_kv", ["expires_at"])

    op.create_table(
        "ephemeral_pubsub",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("channel", sa.Text(), nullable=False),
        sa.Column("payload", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_ephemeral_pubsub_channel_id", "ephemeral_pubsub", ["channel", "id"])
    op.create_index("ix_ephemeral_pubsub_created_at", "ephemeral_pubsub", ["created_at"])


def downgrade() -> None:
    op.drop_table("ephemeral_pubsub")
    op.drop_table("ephemeral_kv")
