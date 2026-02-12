-- One-time migration: Remove PII from the letters table.
-- PII is now sent directly to Theseus/Slack at creation time and never stored.
--
-- Run with: psql $DATABASE_URL -f scripts/remove_letter_pii.sql

BEGIN;

-- Step 1: NULL out existing PII data
UPDATE letters SET
    first_name = NULL,
    last_name = NULL,
    address_line_1 = NULL,
    address_line_2 = NULL,
    city = NULL,
    state = NULL,
    postal_code = NULL,
    recipient_email = NULL;

-- Step 2: Drop the columns entirely
ALTER TABLE letters
    DROP COLUMN first_name,
    DROP COLUMN last_name,
    DROP COLUMN address_line_1,
    DROP COLUMN address_line_2,
    DROP COLUMN city,
    DROP COLUMN state,
    DROP COLUMN postal_code,
    DROP COLUMN recipient_email;

COMMIT;
