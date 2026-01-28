import logging
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import func, select

from app.config import get_settings
from app.database import AsyncSessionLocal
from app.hcb_client import HCBAPIError, hcb_client
from app.models import Event, Letter, LetterStatus
from app.slack_bot import slack_bot
from app.theseus_client import TheseusAPIError, theseus_client

logger = logging.getLogger(__name__)
settings = get_settings()

scheduler = AsyncIOScheduler()


async def check_all_pending_letters() -> dict:
    """
    Checks status of all pending letters and updates accordingly.

    Runs every hour:
    1. Query all letters where status != 'shipped'
    2. For each, call Theseus API to get current status
    3. If status changed, update DB and Slack message
    4. If now 'shipped', mark as mailed and remove button

    Returns:
        Dict with checked, updated, and mailed counts
    """
    logger.info("Starting hourly letter status check")

    checked = 0
    updated = 0
    mailed = 0

    async with AsyncSessionLocal() as session:
        stmt = select(Letter).where(
            Letter.status.notin_([LetterStatus.SHIPPED, LetterStatus.FAILED])
        )
        result = await session.execute(stmt)
        letters = result.scalars().all()

        logger.info(f"Found {len(letters)} pending letters to check")

        for letter in letters:
            checked += 1

            try:
                theseus_response = await theseus_client.get_letter_status(letter.letter_id)
                new_status_str = theseus_response.get("status", "").lower()

                try:
                    new_status = LetterStatus(new_status_str)
                except ValueError:
                    logger.warning(f"Unknown status '{new_status_str}' for letter {letter.letter_id}")
                    continue

                if new_status != letter.status:
                    old_status = letter.status
                    letter.status = new_status
                    updated += 1

                    logger.info(f"Letter {letter.letter_id} status changed: {old_status} -> {new_status}")

                    if new_status == LetterStatus.SHIPPED:
                        letter.mailed_at = datetime.utcnow()
                        mailed += 1

                        if letter.slack_message_ts and letter.slack_channel_id:
                            event_stmt = select(Event).where(Event.id == letter.event_id)
                            event_result = await session.execute(event_stmt)
                            event = event_result.scalar_one_or_none()

                            if event:
                                await slack_bot.update_letter_shipped(
                                    channel_id=letter.slack_channel_id,
                                    message_ts=letter.slack_message_ts,
                                    event_name=event.name,
                                    queue_name=event.theseus_queue,
                                    recipient_name=f"{letter.first_name} {letter.last_name}",
                                    country=letter.country,
                                    rubber_stamps_raw=letter.rubber_stamps_raw,
                                    cost_cents=letter.cost_cents,
                                    letter_id=letter.letter_id,
                                    mailed_at=letter.mailed_at
                                )

                    await session.commit()

            except TheseusAPIError as e:
                logger.error(f"Failed to check status for letter {letter.letter_id}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error checking letter {letter.letter_id}: {e}")

    logger.info(f"Status check complete: checked={checked}, updated={updated}, mailed={mailed}")
    return {"checked": checked, "updated": updated, "mailed": mailed}


async def process_billing_disbursements() -> dict:
    """
    Processes billing for all events with unbilled letters.

    Runs every hour:
    1. Group unbilled letters by event
    2. For each event with unbilled letters:
       - Calculate total amount
       - Create HCB disbursement from event's org to hermes-fulfillment
       - Mark letters as billing_paid=True

    Returns:
        Dict with events_processed, letters_billed, and total_amount_cents
    """
    if not settings.hcb_api_key:
        logger.warning("HCB API key not configured - skipping billing disbursements")
        return {"events_processed": 0, "letters_billed": 0, "total_amount_cents": 0, "skipped": True}

    logger.info("Starting hourly billing disbursement processing")

    events_processed = 0
    letters_billed = 0
    total_amount_cents = 0
    errors = []

    async with AsyncSessionLocal() as session:
        stmt = (
            select(
                Event.id,
                Event.name,
                Event.org_slug,
                func.count(Letter.id).label("letter_count"),
                func.sum(Letter.cost_cents).label("total_cost")
            )
            .join(Letter, Letter.event_id == Event.id)
            .where(Letter.billing_paid == False)  # noqa: E712
            .group_by(Event.id, Event.name, Event.org_slug)
        )
        result = await session.execute(stmt)
        event_billing = result.all()

        logger.info(f"Found {len(event_billing)} events with unbilled letters")

        for event_id, event_name, org_slug, letter_count, total_cost in event_billing:
            try:
                memo = f"Hermes Fulfillment // {letter_count} Letters"

                logger.info(f"Creating disbursement for {event_name}: {letter_count} letters, ${total_cost/100:.2f}")

                disbursement = await hcb_client.create_disbursement(
                    source_org_slug=org_slug,
                    destination_org_slug=settings.hcb_fulfillment_org_slug,
                    amount_cents=total_cost,
                    name=memo
                )

                logger.info(f"Disbursement created for {event_name}: {disbursement.get('id')}")

                update_stmt = (
                    Letter.__table__.update()
                    .where(Letter.event_id == event_id)
                    .where(Letter.billing_paid == False)  # noqa: E712
                    .values(billing_paid=True)
                )
                await session.execute(update_stmt)
                await session.commit()

                events_processed += 1
                letters_billed += letter_count
                total_amount_cents += total_cost

                logger.info(f"Marked {letter_count} letters as paid for {event_name}")

            except HCBAPIError as e:
                logger.error(f"Failed to create disbursement for {event_name} ({org_slug}): {e.message}")
                errors.append({"event": event_name, "error": e.message})
            except Exception as e:
                logger.error(f"Unexpected error processing billing for {event_name}: {e}")
                errors.append({"event": event_name, "error": str(e)})

    logger.info(
        f"Billing complete: events_processed={events_processed}, "
        f"letters_billed={letters_billed}, total=${total_amount_cents/100:.2f}"
    )

    return {
        "events_processed": events_processed,
        "letters_billed": letters_billed,
        "total_amount_cents": total_amount_cents,
        "errors": errors
    }


def start_scheduler():
    """Starts the background scheduler."""
    scheduler.add_job(
        check_all_pending_letters,
        'interval',
        hours=1,
        id='check_letter_status',
        replace_existing=True
    )
    scheduler.add_job(
        process_billing_disbursements,
        'interval',
        hours=1,
        id='process_billing',
        replace_existing=True
    )
    scheduler.start()
    logger.info("Background scheduler started - checking letter status and processing billing every hour")


def stop_scheduler():
    """Stops the background scheduler."""
    scheduler.shutdown()
    logger.info("Background scheduler stopped")
