#!/usr/bin/env python3
"""
Migration script to fix org_slug column and backfill missing values.

This script:
1. Alters org_slug column to be nullable (if not already)
2. Sets empty string org_slugs to NULL
3. Optionally backfills org_slugs from a mapping you provide

Usage:
    # Preview changes (dry run):
    python scripts/migrate_org_slug.py --dry-run

    # Apply migration:
    python scripts/migrate_org_slug.py

    # With explicit database URL:
    python scripts/migrate_org_slug.py --database-url "postgresql://..."

Before running, edit the BACKFILL_MAPPING below to specify the correct org_slug
for each event that needs one.
"""

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

# =============================================================================
# BACKFILL MAPPING - Edit this before running!
# =============================================================================
# Map event IDs or names to their correct org_slug.
# You can use either event_id (int) or event_name (str) as keys.
#
# Example:
#   BACKFILL_MAPPING = {
#       1: "hackclub",           # Event ID 1 -> org_slug "hackclub"
#       "Haxmas 2024": "haxmas", # Event name "Haxmas 2024" -> org_slug "haxmas"
#   }
#
BACKFILL_MAPPING: dict[int | str, str] = {
    # Add your mappings here:
    # 1: "hackclub",
    # "My Event Name": "my-org-slug",
}
# =============================================================================


async def migrate_org_slug(database_url: str, dry_run: bool = False) -> None:
    """Run the org_slug migration."""
    engine = create_async_engine(database_url, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        # Step 1: Find events with empty or null org_slug
        result = await session.execute(
            text("SELECT id, name, org_slug FROM events ORDER BY id")
        )
        events = result.fetchall()

        print(f"\nFound {len(events)} total events:\n")
        print(f"{'ID':<6} {'Name':<40} {'Current org_slug':<30} {'Action'}")
        print("-" * 100)

        events_to_update = []
        events_missing_slug = []

        for event_id, event_name, org_slug in events:
            action = "OK"
            new_slug = None

            # Check if needs update from mapping
            if event_id in BACKFILL_MAPPING:
                new_slug = BACKFILL_MAPPING[event_id]
                action = f"UPDATE -> '{new_slug}'"
            elif event_name in BACKFILL_MAPPING:
                new_slug = BACKFILL_MAPPING[event_name]
                action = f"UPDATE -> '{new_slug}'"
            elif not org_slug or org_slug == "":
                action = "MISSING - add to BACKFILL_MAPPING"
                events_missing_slug.append((event_id, event_name))

            if new_slug:
                events_to_update.append((event_id, new_slug))

            current_display = f"'{org_slug}'" if org_slug else "NULL/empty"
            print(f"{event_id:<6} {event_name:<40} {current_display:<30} {action}")

        print("-" * 100)

        # Step 2: Convert empty strings to NULL
        empty_count_result = await session.execute(
            text("SELECT COUNT(*) FROM events WHERE org_slug = ''")
        )
        empty_count = empty_count_result.scalar()

        if empty_count > 0:
            print(f"\n📋 Will convert {empty_count} empty string org_slugs to NULL")
            if not dry_run:
                await session.execute(
                    text("UPDATE events SET org_slug = NULL WHERE org_slug = ''")
                )
                print(f"   ✅ Converted {empty_count} empty strings to NULL")

        # Step 3: Apply backfill mapping
        if events_to_update:
            print(f"\n📋 Will update {len(events_to_update)} events from BACKFILL_MAPPING:")
            for event_id, new_slug in events_to_update:
                print(f"   Event {event_id} -> '{new_slug}'")
                if not dry_run:
                    await session.execute(
                        text("UPDATE events SET org_slug = :slug WHERE id = :id"),
                        {"slug": new_slug, "id": event_id}
                    )
            if not dry_run:
                print(f"   ✅ Updated {len(events_to_update)} events")

        # Step 4: Commit changes
        if not dry_run:
            await session.commit()
            print("\n✅ Migration completed successfully!")
        else:
            print("\n🔍 DRY RUN - no changes made. Remove --dry-run to apply.")

        # Step 5: Warn about missing slugs
        if events_missing_slug:
            print(f"\n⚠️  WARNING: {len(events_missing_slug)} events still need org_slug:")
            print("   Add them to BACKFILL_MAPPING in this script and re-run.")
            for event_id, event_name in events_missing_slug:
                print(f"   - Event {event_id}: '{event_name}'")
            print("\n   These events will be skipped during billing until org_slug is set.")


def main():
    parser = argparse.ArgumentParser(
        description="Migrate org_slug column to nullable and backfill values"
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
        asyncio.run(migrate_org_slug(database_url, dry_run=args.dry_run))
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
