#!/usr/bin/env python3
"""
CLI script to delete all API keys by removing all events from the database.

Usage:
    python scripts/delete_all_api_keys.py

Or with explicit database URL:
    python scripts/delete_all_api_keys.py --database-url "postgresql://..."
"""

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from app.models import Event


async def delete_all_api_keys(database_url: str, force: bool = False) -> int:
    """Delete all events (and their API keys) from the database."""
    engine = create_async_engine(database_url, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        result = await session.execute(select(func.count(Event.id)))
        count = result.scalar() or 0

        if count == 0:
            print("No API keys found in the database.")
            return 0

        if not force:
            print(f"\n⚠️  This will delete {count} event(s) and their API keys.")
            print("This action cannot be undone!\n")
            confirm = input("Type 'DELETE' to confirm: ")
            if confirm != "DELETE":
                print("Aborted.")
                return 0

        await session.execute(delete(Event))
        await session.commit()

        return count


def main():
    parser = argparse.ArgumentParser(description="Delete all API keys from the database")
    parser.add_argument(
        "--database-url",
        help="PostgreSQL database URL (or set DATABASE_URL env var)",
        default=os.environ.get("DATABASE_URL")
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Skip confirmation prompt"
    )

    args = parser.parse_args()

    if not args.database_url:
        print("Error: Database URL is required. Set DATABASE_URL env var or use --database-url")
        sys.exit(1)

    database_url = args.database_url
    if database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)

    try:
        deleted_count = asyncio.run(delete_all_api_keys(database_url, args.force))

        if deleted_count > 0:
            print(f"\n✅ Deleted {deleted_count} API key(s)")

    except Exception as e:
        print(f"\n❌ Error deleting API keys: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
