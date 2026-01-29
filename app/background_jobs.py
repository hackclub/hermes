import logging
import uuid
from collections import defaultdict
from datetime import datetime
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import select

from app.config import get_settings
from app.database import AsyncSessionLocal
from app.hcb_client import HCBAPIError, hcb_client
from app.models import Disbursement, DisbursementStatus, Event, Letter, LetterStatus
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
       - Generate unique idempotency key
       - Create Disbursement record with SENDING status
       - Commit to DB
       - Create HCB disbursement from event's org to hermes-fulfillment
       - Update Disbursement to COMPLETED with HCB transfer ID
       - Mark letters as billing_paid=True
       - Log to Slack

    If any disbursement fails, it is marked FAILED and a Slack notification is sent
    to Jenin for manual resolution. No automatic retries are attempted.

    Returns:
        Dict with events_processed, letters_billed, and total_amount_cents
    """
    if not settings.hcb_client_id or not settings.hcb_client_secret:
        logger.warning("HCB OAuth credentials not configured - skipping billing disbursements")
        return {"events_processed": 0, "letters_billed": 0, "total_amount_cents": 0, "skipped": True}

    logger.info("Starting hourly billing disbursement processing")

    events_processed = 0
    letters_billed = 0
    total_amount_cents = 0
    errors: list[dict[str, Any]] = []

    async with AsyncSessionLocal() as session:
        # Process new unbilled letters
        # First, get all unbilled letters with their IDs to avoid race conditions
        letter_stmt = (
            select(Letter.id, Letter.event_id, Letter.cost_cents)
            .where(Letter.billing_paid == False)  # noqa: E712
        )
        letter_result = await session.execute(letter_stmt)
        unbilled_letters = letter_result.all()

        # Group letters by event_id with their IDs and costs
        event_letter_map: dict[int, list[int]] = defaultdict(list)
        event_cost_map: dict[int, int] = defaultdict(int)
        for letter_id, event_id, cost_cents in unbilled_letters:
            event_letter_map[event_id].append(letter_id)
            event_cost_map[event_id] += cost_cents or 0

        # Get event details for events with unbilled letters
        if not event_letter_map:
            event_billing: list[tuple[int, str, str]] = []
        else:
            stmt = (
                select(Event.id, Event.name, Event.org_slug)
                .where(Event.id.in_(event_letter_map.keys()))
            )
            result = await session.execute(stmt)
            event_billing = result.all()

        logger.info(f"Found {len(event_billing)} events with unbilled letters")

        for event_id, event_name, org_slug in event_billing:
            # Skip events with empty or null org_slug to avoid 404 errors
            if not org_slug:
                logger.warning(f"Skipping billing for event '{event_name}' (id={event_id}): org_slug is empty or null")
                errors.append({"event": event_name, "error": "Missing org_slug - cannot bill", "will_retry": False})
                continue

            letter_ids = event_letter_map[event_id]
            letter_count = len(letter_ids)
            total_cost = event_cost_map[event_id]
            idempotency_key = str(uuid.uuid4())

            try:
                # Step 3a: Build memo with unique reference for reconciliation
                memo = f"Hermes // {letter_count} Letters // ref:{idempotency_key}"

                # Step 3b: Create Disbursement record with SENDING status
                # This is committed BEFORE the API call so we can reconcile if crash occurs
                disbursement_record = Disbursement(
                    idempotency_key=idempotency_key,
                    event_id=event_id,
                    amount_cents=total_cost,
                    letter_count=letter_count,
                    status=DisbursementStatus.SENDING,
                    hcb_memo=memo,
                    last_attempt_at=datetime.utcnow()
                )
                session.add(disbursement_record)
                await session.commit()

                logger.info(f"Persisted disbursement {idempotency_key} for {event_name}: {letter_count} letters, ${total_cost/100:.2f}")

                hcb_response = await hcb_client.create_disbursement(
                    source_org_slug=org_slug,
                    destination_org_slug=settings.hcb_fulfillment_org_slug,
                    amount_cents=total_cost,
                    name=memo
                )

                disbursement_record.status = DisbursementStatus.COMPLETED
                disbursement_record.hcb_transfer_id = hcb_response.get("id")
                disbursement_record.completed_at = datetime.utcnow()

                update_stmt = (
                    Letter.__table__.update()
                    .where(Letter.id.in_(letter_ids))
                    .values(billing_paid=True)
                )
                await session.execute(update_stmt)
                await session.commit()

                logger.info(f"Disbursement completed for {event_name}: {hcb_response.get('id')}")

                # Step 6: Log to Slack
                await slack_bot.send_disbursement_notification(
                    event_name=event_name,
                    org_slug=org_slug,
                    letter_count=letter_count,
                    amount_cents=total_cost,
                    hcb_transfer_id=hcb_response.get("id"),
                    idempotency_key=idempotency_key
                )

                events_processed += 1
                letters_billed += letter_count
                total_amount_cents += total_cost

            except HCBAPIError as e:
                # API failed - mark as FAILED and notify Jenin for manual resolution
                logger.error(f"HCB API failed for {event_name} ({org_slug}): {e.message} - requires manual disbursement")
                disbursement_record.status = DisbursementStatus.FAILED
                disbursement_record.error_message = e.message
                await session.commit()

                await slack_bot.send_disbursement_failure_notification(
                    event_name=event_name,
                    org_slug=org_slug,
                    letter_count=letter_count,
                    amount_cents=total_cost,
                    error_message=e.message,
                    idempotency_key=idempotency_key
                )

                errors.append({"event": event_name, "error": e.message})

            except Exception as e:
                # Unexpected error - mark as FAILED and notify Jenin
                logger.error(f"Unexpected error processing billing for {event_name}: {e} - requires manual disbursement")
                disbursement_record.status = DisbursementStatus.FAILED
                disbursement_record.error_message = str(e)
                await session.commit()

                await slack_bot.send_disbursement_failure_notification(
                    event_name=event_name,
                    org_slug=org_slug,
                    letter_count=letter_count,
                    amount_cents=total_cost,
                    error_message=str(e),
                    idempotency_key=idempotency_key
                )

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
        minutes=30,
        id='process_billing',
        replace_existing=True
    )
    scheduler.start()
    logger.info("Background scheduler started - checking letter status every hour, processing billing every 30 minutes")


def stop_scheduler():
    """Stops the background scheduler."""
    scheduler.shutdown()
    logger.info("Background scheduler stopped")
