"""Add an ownership token to job leases.

Revision ID: 0002_add_job_lease_token
Revises: 0001_create_jobs_tables
Create Date: 2026-11-17
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "0002_add_job_lease_token"
down_revision: str | None = "0001_create_jobs_tables"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "transcription_jobs", sa.Column("lease_token", sa.String(length=36), nullable=True)
    )


def downgrade() -> None:
    op.drop_column("transcription_jobs", "lease_token")
