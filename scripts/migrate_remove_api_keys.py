#!/usr/bin/env python3
"""
Migration script to remove all API keys from events.

This script clears the api_key_hash column for all events, requiring
users to regenerate their API keys.

Usage:
    # Preview changes (dry run):
    python scripts/migrate_remove_api_keys.py --dry-run

    # Apply migration:
    python scripts/migrate_remove_api_keys.py

    # With explicit database URL:
    python scripts/migrate_remove_api_keys.py --database-url "postgresql://..."
"""

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker


async def remove_api_keys(database_url: str, dry_run: bool = False) -> None:
    """Remove all API keys from events."""
    engine = create_async_engine(database_url, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        # Count events with API keys
        result = await session.execute(
            text("SELECT id, name, api_key_hash FROM events WHERE api_key_hash IS NOT NULL ORDER BY id")
        )
        events = result.fetchall()

        print(f"\nFound {len(events)} events with API keys:\n")
        print(f"{'ID':<6} {'Name':<40} {'API Key Hash (truncated)'}")
        print("-" * 80)

        for event_id, event_name, api_key_hash in events:
            hash_display = f"{api_key_hash[:16]}..." if api_key_hash else "NULL"
            print(f"{event_id:<6} {event_name:<40} {hash_display}")

        print("-" * 80)

        if not events:
            print("\n✅ No API keys to remove.")
            return

        if dry_run:
            print(f"\n🔍 DRY RUN - Would remove {len(events)} API keys. Remove --dry-run to apply.")
            return

        # Remove all API keys
        await session.execute(
            text("UPDATE events SET api_key_hash = NULL")
        )
        await session.commit()

        print(f"\n✅ Removed {len(events)} API keys successfully!")
        print("   Users will need to regenerate their API keys.")


def main():
    parser = argparse.ArgumentParser(
        description="Remove all API keys from events table"
    )
    parser.add_argument(
        "--database-url",
        help="PostgreSQL database URL (or set DATABASE_URL env var)",
        default=os.environ.get("DATABASE_URL")
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview changes without applying them"
    )

    args = parser.parse_args()

    if not args.database_url:
        print("Error: Database URL is required. Set DATABASE_URL env var or use --database-url")
        sys.exit(1)

    database_url = args.database_url
    if database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)

    try:
        asyncio.run(remove_api_keys(database_url, dry_run=args.dry_run))
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
