import logging
import uuid
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
    Processes billing for all events with unbilled letters using idempotent transactions.

    Runs every hour with idempotency protection:
    1. First, retry any PENDING disbursements from previous failed runs
    2. Group unbilled letters by event
    3. For each event with unbilled letters:
       - Generate unique idempotency key
       - Create Disbursement record with PENDING status (persisted first!)
       - Mark letters as billing_paid=True (before API call)
       - Commit to DB (ensures state is saved)
       - Create HCB disbursement from event's org to hermes-fulfillment
       - Update Disbursement to COMPLETED with HCB transfer ID
       - Log to Slack

    This flow prevents double billing: if the DB commit succeeds but API fails,
    the letters are already marked as paid. If DB commit fails, nothing is charged.

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
    errors: list[dict[str, Any]] = []

    async with AsyncSessionLocal() as session:
        # Step 1: Retry any PENDING disbursements from previous runs
        pending_stmt = (
            select(Disbursement)
            .where(Disbursement.status == DisbursementStatus.PENDING)
        )
        pending_result = await session.execute(pending_stmt)
        pending_disbursements = pending_result.scalars().all()

        for pending in pending_disbursements:
            logger.info(f"Retrying pending disbursement {pending.idempotency_key} for event {pending.event_id}")
            try:
                event_stmt = select(Event).where(Event.id == pending.event_id)
                event_result = await session.execute(event_stmt)
                event = event_result.scalar_one_or_none()

                if not event:
                    logger.error(f"Event {pending.event_id} not found for pending disbursement")
                    pending.status = DisbursementStatus.FAILED
                    pending.error_message = "Event not found"
                    await session.commit()
                    continue

                memo = f"Hermes Fulfillment // {pending.letter_count} Letters"

                hcb_response = await hcb_client.create_disbursement(
                    source_org_slug=event.org_slug,
                    destination_org_slug=settings.hcb_fulfillment_org_slug,
                    amount_cents=pending.amount_cents,
                    name=memo
                )

                pending.status = DisbursementStatus.COMPLETED
                pending.hcb_transfer_id = hcb_response.get("id")
                pending.completed_at = datetime.utcnow()
                await session.commit()

                logger.info(f"Completed pending disbursement {pending.idempotency_key}: {pending.hcb_transfer_id}")

                await slack_bot.send_disbursement_notification(
                    event_name=event.name,
                    org_slug=event.org_slug,
                    letter_count=pending.letter_count,
                    amount_cents=pending.amount_cents,
                    hcb_transfer_id=pending.hcb_transfer_id,
                    idempotency_key=pending.idempotency_key
                )

                events_processed += 1
                letters_billed += pending.letter_count
                total_amount_cents += pending.amount_cents

            except HCBAPIError as e:
                logger.error(f"Failed to retry disbursement {pending.idempotency_key}: {e.message}")
                permanent_error_codes = {400, 403, 404}
                if e.status_code is not None and e.status_code in permanent_error_codes:
                    pending.status = DisbursementStatus.FAILED
                    pending.error_message = e.message
                    await session.commit()
                else:
                    logger.info(f"Transient error for {pending.idempotency_key}, will retry later")
                errors.append({"event_id": pending.event_id, "error": e.message})
            except Exception as e:
                logger.error(f"Unexpected error retrying disbursement {pending.idempotency_key}: {e}")
                errors.append({"event_id": pending.event_id, "error": str(e)})

        # Step 2: Process new unbilled letters
        # First, get all unbilled letters with their IDs to avoid race conditions
        letter_stmt = (
            select(Letter.id, Letter.event_id, Letter.cost_cents)
            .where(Letter.billing_paid == False)  # noqa: E712
        )
        letter_result = await session.execute(letter_stmt)
        unbilled_letters = letter_result.all()

        # Group letters by event_id with their IDs and costs
        from collections import defaultdict
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
                # Step 3a: Create Disbursement record with PENDING status FIRST
                disbursement_record = Disbursement(
                    idempotency_key=idempotency_key,
                    event_id=event_id,
                    amount_cents=total_cost,
                    letter_count=letter_count,
                    status=DisbursementStatus.PENDING
                )
                session.add(disbursement_record)

                # Step 3b: Mark letters as billing_paid=True BEFORE API call
                # Use specific letter IDs captured earlier to avoid race condition
                update_stmt = (
                    Letter.__table__.update()
                    .where(Letter.id.in_(letter_ids))
                    .values(billing_paid=True)
                )
                await session.execute(update_stmt)

                # Step 3c: Commit to DB - this is the critical point
                # If this fails, nothing is charged (letters remain unbilled)
                # If this succeeds, letters are marked paid and disbursement is tracked
                await session.commit()

                logger.info(f"Persisted disbursement {idempotency_key} for {event_name}: {letter_count} letters, ${total_cost/100:.2f}")

                # Step 4: Now make the HCB API call
                memo = f"Hermes Fulfillment // {letter_count} Letters"

                hcb_response = await hcb_client.create_disbursement(
                    source_org_slug=org_slug,
                    destination_org_slug=settings.hcb_fulfillment_org_slug,
                    amount_cents=total_cost,
                    name=memo
                )

                # Step 5: Update disbursement record with success
                disbursement_record.status = DisbursementStatus.COMPLETED
                disbursement_record.hcb_transfer_id = hcb_response.get("id")
                disbursement_record.completed_at = datetime.utcnow()
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
                # API failed but letters are already marked as paid
                # Disbursement record stays PENDING for retry next hour
                logger.error(f"HCB API failed for {event_name} ({org_slug}): {e.message} - will retry next hour")
                errors.append({"event": event_name, "error": e.message, "will_retry": True})

            except Exception as e:
                logger.error(f"Unexpected error processing billing for {event_name}: {e}")
                await session.rollback()
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
