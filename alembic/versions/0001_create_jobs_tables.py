"""Create job persistence tables.

Revision ID: 0001_create_jobs_tables
Revises:
Create Date: 2026-10-24
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "0001_create_jobs_tables"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "transcription_jobs",
        sa.Column("job_id", sa.String(length=255), primary_key=True),
        sa.Column("material_id", sa.String(length=255), nullable=False),
        sa.Column("user_id", sa.String(length=255), nullable=False),
        sa.Column("correlation_id", sa.String(length=255), nullable=False),
        sa.Column("source_event_id", sa.String(length=255), nullable=False),
        sa.Column("source_bucket", sa.String(length=255)),
        sa.Column("source_key", sa.Text()),
        sa.Column("source_url", sa.Text()),
        sa.Column("detected_type", sa.String(length=64)),
        sa.Column("summary_bucket", sa.String(length=255)),
        sa.Column("summary_key", sa.Text()),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("current_stage", sa.String(length=32)),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("next_retry_at", sa.DateTime(timezone=True)),
        sa.Column("locked_until", sa.DateTime(timezone=True)),
        sa.Column("error_code", sa.String(length=128)),
        sa.Column("error_message", sa.Text()),
        sa.Column("retryable", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("pipeline_version", sa.String(length=64), nullable=False),
        sa.Column("models", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint(
            "(source_bucket IS NOT NULL AND source_key IS NOT NULL AND source_url IS NULL) "
            "OR (source_bucket IS NULL AND source_key IS NULL AND source_url IS NOT NULL)",
            name="ck_transcription_jobs_exactly_one_source",
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'processing', 'retry_wait', 'completed', 'failed')",
            name="ck_transcription_jobs_status",
        ),
    )
    op.create_index(
        "ix_transcription_jobs_status_retry_lease",
        "transcription_jobs",
        ["status", "next_retry_at", "locked_until"],
    )

    op.create_table(
        "processed_events",
        sa.Column("event_id", sa.String(length=255), primary_key=True),
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
    )

    op.create_table(
        "outbox_events",
        sa.Column("id", sa.String(length=255), primary_key=True),
        sa.Column(
            "job_id",
            sa.String(length=255),
            sa.ForeignKey("transcription_jobs.job_id"),
            nullable=False,
        ),
        sa.Column("topic", sa.String(length=255), nullable=False),
        sa.Column("partition_key", sa.String(length=255), nullable=False),
        sa.Column("event_type", sa.String(length=128), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("next_retry_at", sa.DateTime(timezone=True)),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.CheckConstraint(
            "status IN ('pending', 'published')", name="ck_outbox_events_status"
        ),
        sa.UniqueConstraint("job_id", "event_type", name="uq_outbox_events_job_id_event_type"),
    )
    op.create_index(
        "ix_outbox_events_status_retry",
        "outbox_events",
        ["status", "next_retry_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_events_status_retry", table_name="outbox_events")
    op.drop_table("outbox_events")
    op.drop_table("processed_events")
    op.drop_index(
        "ix_transcription_jobs_status_retry_lease", table_name="transcription_jobs"
    )
    op.drop_table("transcription_jobs")
