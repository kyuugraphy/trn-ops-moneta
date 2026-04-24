-- =============================================================================
-- Migration 001 — manual_acc_data_changes_new: add audit columns for Screen 1
-- =============================================================================
-- Purpose
--   Screen 1 (Manual Accounts) of the Streamlit app needs to track:
--     * CREATED_AT — when the active SCD-2 row was written
--     * UPDATED_AT — when the active SCD-2 row was last touched (close event)
--     * CREATED_BY — email/username of the operator (from Databricks Apps
--                    X-Forwarded-Email header or local OS user)
--
--   The table already carries SCD-2 columns (VALID_FROM, VALID_TO, IS_ACTIVE)
--   which semantically cover "when". The new timestamp columns are kept as a
--   convenience mirror so downstream consumers that grew up on CREATED_AT /
--   UPDATED_AT do not break, and so the UI can display a stable "last update"
--   timestamp regardless of SCD-2 internals.
--
-- Before running
--   * Replace `dtl_dev.sol_risk_solution_demo` with the correct catalog.schema
--     for your environment if it differs.
--   * The table must be a Delta table (it is, by virtue of the existing
--     VALID_FROM / VALID_TO / IS_ACTIVE columns being populated by an SCD-2
--     pipeline).
--
-- Idempotency
--   `ADD COLUMN IF NOT EXISTS` makes this migration safe to re-run.
-- =============================================================================

ALTER TABLE dtl_dev.sol_risk_solution_demo.manual_acc_data_changes_new
    ADD COLUMN IF NOT EXISTS (
        CREATED_AT TIMESTAMP COMMENT 'Timestamp the active row was inserted (mirrors VALID_FROM)',
        UPDATED_AT TIMESTAMP COMMENT 'Timestamp of the last change to the row (insert or SCD-2 close)',
        CREATED_BY STRING    COMMENT 'Operator identity captured from app (email or OS user)'
    );

-- Backfill existing rows so queries that ORDER BY UPDATED_AT do not see NULLs
UPDATE dtl_dev.sol_risk_solution_demo.manual_acc_data_changes_new
   SET CREATED_AT = COALESCE(CREATED_AT, VALID_FROM),
       UPDATED_AT = COALESCE(UPDATED_AT, VALID_TO, VALID_FROM)
 WHERE CREATED_AT IS NULL
    OR UPDATED_AT IS NULL;
