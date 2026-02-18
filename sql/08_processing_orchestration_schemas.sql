-- Purpose:
-- Create dedicated schemas for stream processing logic and orchestration tasks.
-- Dependencies:
-- - PLATFORM_DB database exists.
-- Execution order:
-- 1) Run before creating processing tables, procedures, and tasks.

CREATE SCHEMA IF NOT EXISTS PLATFORM_DB.PROCESSING;
CREATE SCHEMA IF NOT EXISTS PLATFORM_DB.ORCHESTRATION;
