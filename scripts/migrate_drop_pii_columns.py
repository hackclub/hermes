#!/usr/bin/env python3
"""
Migration script to drop leftover PII columns from the letters table.

The application no longer stores PII (name, address, email) in the database,
but the columns were never removed. This causes IntegrityError on INSERT
because the old columns have NOT NULL constraints.

Usage:
    # Preview changes (dry run):
    python scripts/migrate_drop_pii_columns.py --dry-run

    # Apply migration:
    python scripts/migrate_drop_pii_columns.py

    # With explicit database URL:
    python scripts/migrate_drop_pii_columns.py --database-url "postgresql://..."
"""

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

# Columns that the SQLAlchemy model defines (keep these)
MODEL_COLUMNS = {
    "id",
    "letter_id",
    "event_id",
    "slack_message_ts",
    "slack_channel_id",
    "country",
    "mail_type",
    "weight_grams",
    "rubber_stamps_raw",
    "rubber_stamps_formatted",
    "notes",
    "cost_cents",
    "billing_paid",
    "status",
    "created_at",
    "mailed_at",
}


async def drop_pii_columns(database_url: str, dry_run: bool = False) -> None:
    """Drop PII columns from the letters table."""
    engine = create_async_engine(database_url, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        # Discover ALL columns in the table, then drop any not in the model
        result = await session.execute(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'letters'"
            ),
        )
        all_columns = {row[0] for row in result.fetchall()}
        existing = sorted(all_columns - MODEL_COLUMNS)

        if not existing:
            print("\n✅ No PII columns found in letters table. Nothing to do.")
            return

        print(f"\nFound {len(existing)} PII column(s) to drop from letters:\n")
        for col in existing:
            print(f"  - {col}")

        if dry_run:
            print(f"\n🔍 DRY RUN - Would drop {len(existing)} column(s). Remove --dry-run to apply.")
            return

        drop_clauses = ", ".join(f"DROP COLUMN IF EXISTS {col}" for col in existing)
        await session.execute(text(f"ALTER TABLE letters {drop_clauses}"))
        await session.commit()

        print(f"\n✅ Dropped {len(existing)} PII column(s) successfully!")


def main():
    parser = argparse.ArgumentParser(
        description="Drop leftover PII columns from the letters table"
    )
    parser.add_argument(
        "--database-url",
        help="PostgreSQL database URL (or set DATABASE_URL env var)",
        default=os.environ.get("DATABASE_URL"),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without applying them",
    )

    args = parser.parse_args()

    if not args.database_url:
        print("Error: Database URL is required. Set DATABASE_URL env var or use --database-url")
        sys.exit(1)

    database_url = args.database_url
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql+asyncpg://", 1)
    elif database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)

    try:
        asyncio.run(drop_pii_columns(database_url, dry_run=args.dry_run))
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
