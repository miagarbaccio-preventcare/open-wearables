"""covering index for timeseries keyset pagination

The only index on data_point_series was the uniqueness constraint
(data_source_id, series_type_definition_id, recorded_at). The timeseries read
path filters on data_source_id + series_type_definition_id + a recorded_at
range but orders by (recorded_at, id), so Postgres had to sort the entire
filtered window for every page. A 180-day raw crawl walks hundreds of pages,
which made each page an O(window) sort instead of an O(limit) index scan.

Trailing `id` matches the keyset tuple `(recorded_at, id)` so the ORDER BY and
the cursor comparison are both satisfied by the index.

Revision ID: pc_dps_keyset_idx_001
Revises: pc_ephemeral_kv_001

"""

from typing import Sequence, Union

from alembic import op

revision: str = "pc_dps_keyset_idx_001"
down_revision: Union[str, None] = "pc_ephemeral_kv_001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

INDEX_NAME = "ix_dps_source_type_recorded_id"


def upgrade() -> None:
    # CONCURRENTLY cannot run inside a transaction — this table is large and
    # actively written by SDK imports, so an ACCESS EXCLUSIVE lock is not safe.
    with op.get_context().autocommit_block():
        op.execute(
            f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {INDEX_NAME} "
            "ON data_point_series "
            "(data_source_id, series_type_definition_id, recorded_at, id)"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {INDEX_NAME}")
