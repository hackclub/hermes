#!/usr/bin/env python3
"""
CLI script to list all API keys (events) in the database.

Usage:
    python scripts/list_api_keys.py

Or with explicit database URL:
    python scripts/list_api_keys.py --database-url "postgresql://..."

Note: API keys are stored hashed and cannot be retrieved. This script shows
the events that have API keys configured.
"""

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from app.models import Event


async def list_events(database_url: str) -> list[dict]:
    """List all events with API keys."""
    engine = create_async_engine(database_url, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        result = await session.execute(select(Event).order_by(Event.created_at.desc()))
        events = result.scalars().all()

        return [
            {
                "id": event.id,
                "name": event.name,
                "theseus_queue": event.theseus_queue,
                "letter_count": event.letter_count,
                "balance_due_cents": event.balance_due_cents,
                "is_paid": event.is_paid,
                "created_at": event.created_at.isoformat() if event.created_at else None,
                "api_key_hash": event.api_key_hash[:16] + "..." if event.api_key_hash else None,
            }
            for event in events
        ]


def main():
    parser = argparse.ArgumentParser(description="List all API keys (events)")
    parser.add_argument(
        "--database-url",
        help="PostgreSQL database URL (or set DATABASE_URL env var)",
        default=os.environ.get("DATABASE_URL")
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output as JSON"
    )

    args = parser.parse_args()

    if not args.database_url:
        print("Error: Database URL is required. Set DATABASE_URL env var or use --database-url")
        sys.exit(1)

    database_url = args.database_url
    if database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+asyncpg://", 1)

    try:
        events = asyncio.run(list_events(database_url))

        if args.json:
            import json
            print(json.dumps(events, indent=2))
        else:
            if not events:
                print("No API keys found.")
                return

            print(f"\n{'ID':<6} {'Event Name':<30} {'Queue':<30} {'Letters':<10} {'Paid':<6} {'Created'}")
            print("-" * 110)
            for event in events:
                paid = "Yes" if event["is_paid"] else "No"
                created = event["created_at"][:10] if event["created_at"] else "N/A"
                print(f"{event['id']:<6} {event['name']:<30} {event['theseus_queue']:<30} {event['letter_count']:<10} {paid:<6} {created}")

            print(f"\nTotal: {len(events)} API key(s)")
            print("\nNote: API keys are stored hashed and cannot be retrieved.")

    except Exception as e:
        print(f"\n❌ Error listing API keys: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
