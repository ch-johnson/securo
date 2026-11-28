"""account_fingerprint and account_connection_links (#1341)

Adds ``accounts.account_fingerprint`` and creates ``account_connection_links``
for cross-connection joint account feeds (backfilling existing connected
accounts).

Revision ID: 148
Revises: 147
"""

import uuid
from datetime import datetime, timezone

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "148"
down_revision = "147"
branch_labels = None
depends_on = None


def _check_duplicate_connection_external_ids(bind: sa.Connection) -> None:
    """Fail loudly if two accounts share a ``(connection_id, external_id)``.

    Before migration 148, ``accounts`` had no unique constraint on
    ``(connection_id, external_id)``. Silently dropping one of the primary
    links during backfill would detach an active account from its feed.
    """
    dup_pairs = bind.execute(
        sa.text(
            """
            SELECT a.connection_id, COALESCE(a.external_id, CAST(a.id AS TEXT)) AS ext_id
            FROM accounts a
            JOIN bank_connections bc ON bc.id = a.connection_id
            WHERE a.connection_id IS NOT NULL
            GROUP BY a.connection_id, COALESCE(a.external_id, CAST(a.id AS TEXT))
            HAVING COUNT(*) > 1
            """
        )
    ).fetchall()
    if not dup_pairs:
        return
    conflicts: list[str] = []
    for conn_id, ext_id in dup_pairs:
        acc_rows = bind.execute(
            sa.text(
                """
                SELECT a.id
                FROM accounts a
                WHERE a.connection_id = :conn_id
                  AND COALESCE(a.external_id, CAST(a.id AS TEXT)) = :ext_id
                ORDER BY a.id
                """
            ),
            {"conn_id": conn_id, "ext_id": ext_id},
        ).fetchall()
        acc_ids = [str(row[0]) for row in acc_rows]
        conflicts.append(
            f"(connection_id={conn_id}, external_id={ext_id!r}, account_ids={acc_ids})"
        )
    raise RuntimeError(
        "Cannot backfill account_connection_links: duplicate (connection_id, external_id) "
        "rows exist in accounts; deduplicate or merge these accounts before running "
        f"migration 148: {'; '.join(conflicts)}"
    )


def _backfill_account_connection_links(bind: sa.Connection) -> None:
    _check_duplicate_connection_external_ids(bind)
    if bind.dialect.name == "postgresql":
        bind.execute(
            sa.text(
                """
                INSERT INTO account_connection_links (
                    id, workspace_id, account_id, connection_id,
                    external_id, stable_external_id, account_fingerprint,
                    is_primary_feed, status, last_synced_at, created_at
                )
                SELECT
                    gen_random_uuid(),
                    a.workspace_id,
                    a.id,
                    a.connection_id,
                    COALESCE(a.external_id, CAST(a.id AS TEXT)),
                    a.stable_external_id,
                    NULL,
                    true,
                    'active',
                    bc.last_sync_at,
                    now()
                FROM accounts a
                JOIN bank_connections bc ON bc.id = a.connection_id
                WHERE a.connection_id IS NOT NULL
                ON CONFLICT (account_id, connection_id) DO NOTHING
                """
            )
        )
    else:
        rows = bind.execute(
            sa.text(
                """
                SELECT a.workspace_id, a.id, a.connection_id,
                       a.external_id, a.stable_external_id, bc.last_sync_at
                FROM accounts a
                JOIN bank_connections bc ON bc.id = a.connection_id
                WHERE a.connection_id IS NOT NULL
                """
            )
        ).fetchall()
        now = datetime.now(timezone.utc)
        for ws_id, acc_id, conn_id, ext_id, stable_id, last_sync in rows:
            bind.execute(
                sa.text(
                    """
                    INSERT INTO account_connection_links (
                        id, workspace_id, account_id, connection_id,
                        external_id, stable_external_id, account_fingerprint,
                        is_primary_feed, status, last_synced_at, created_at
                    ) VALUES (
                        :id, :ws_id, :acc_id, :conn_id,
                        :ext_id, :stable_id, NULL,
                        1, 'active', :last_sync, :created_at
                    )
                    ON CONFLICT (account_id, connection_id) DO NOTHING
                    """
                ),
                {
                    "id": uuid.uuid4().hex,
                    "ws_id": ws_id,
                    "acc_id": acc_id,
                    "conn_id": conn_id,
                    "ext_id": ext_id if ext_id is not None else str(acc_id),
                    "stable_id": stable_id,
                    "last_sync": last_sync,
                    "created_at": now,
                },
            )


def upgrade() -> None:
    op.add_column(
        "accounts",
        sa.Column("account_fingerprint", sa.String(length=64), nullable=True),
    )
    op.create_index(
        "ix_accounts_account_fingerprint",
        "accounts",
        ["account_fingerprint"],
        unique=False,
    )

    op.create_table(
        "account_connection_links",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("workspace_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("account_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("connection_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("external_id", sa.String(length=255), nullable=False),
        sa.Column("stable_external_id", sa.String(length=255), nullable=True),
        sa.Column("account_fingerprint", sa.String(length=64), nullable=True),
        sa.Column(
            "is_primary_feed",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        sa.Column(
            "status",
            sa.String(length=32),
            nullable=False,
            server_default="active",
        ),
        sa.Column("last_synced_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["workspace_id"],
            ["workspaces.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["account_id"],
            ["accounts.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["connection_id"],
            ["bank_connections.id"],
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "account_id",
            "connection_id",
            name="uq_account_connection_links_account_conn",
        ),
    )
    op.create_index(
        "ix_account_connection_links_workspace_id",
        "account_connection_links",
        ["workspace_id"],
        unique=False,
    )
    op.create_index(
        "ix_account_connection_links_account_id",
        "account_connection_links",
        ["account_id"],
        unique=False,
    )
    op.create_index(
        "ix_account_connection_links_connection_id",
        "account_connection_links",
        ["connection_id"],
        unique=False,
    )
    op.create_index(
        "ix_account_connection_links_account_fingerprint",
        "account_connection_links",
        ["account_fingerprint"],
        unique=False,
    )
    op.create_index(
        "uq_account_connection_links_conn_ext_active",
        "account_connection_links",
        ["connection_id", "external_id"],
        unique=True,
        postgresql_where=sa.text("status != 'rejected'"),
        sqlite_where=sa.text("status != 'rejected'"),
    )
    op.create_index(
        "uq_account_connection_links_one_active_primary",
        "account_connection_links",
        ["account_id"],
        unique=True,
        postgresql_where=sa.text("is_primary_feed IS TRUE AND status = 'active'"),
        sqlite_where=sa.text("is_primary_feed IS TRUE AND status = 'active'"),
    )

    _backfill_account_connection_links(op.get_bind())


def downgrade() -> None:
    op.drop_index(
        "uq_account_connection_links_one_active_primary",
        table_name="account_connection_links",
    )
    op.drop_index(
        "uq_account_connection_links_conn_ext_active",
        table_name="account_connection_links",
    )
    op.drop_index(
        "ix_account_connection_links_account_fingerprint",
        table_name="account_connection_links",
    )
    op.drop_index(
        "ix_account_connection_links_connection_id",
        table_name="account_connection_links",
    )
    op.drop_index(
        "ix_account_connection_links_account_id",
        table_name="account_connection_links",
    )
    op.drop_index(
        "ix_account_connection_links_workspace_id",
        table_name="account_connection_links",
    )
    op.drop_table("account_connection_links")

    op.drop_index("ix_accounts_account_fingerprint", table_name="accounts")
    op.drop_column("accounts", "account_fingerprint")
