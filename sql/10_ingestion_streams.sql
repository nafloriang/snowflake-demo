-- Purpose:
-- Create streams on ingestion landing tables to capture incremental change data.
-- Dependencies:
-- - PLATFORM_DB.INGESTION schema exists.
-- - PLATFORM_DB.INGESTION.LANDING_USERS and PLATFORM_DB.INGESTION.LANDING_POSTS tables exist.
-- Execution order:
-- 1) Run after landing tables and pipes are created.
-- 2) Run before stored procedures and tasks.

CREATE OR REPLACE STREAM PLATFORM_DB.INGESTION.LANDING_USERS_STREAM
ON TABLE PLATFORM_DB.INGESTION.LANDING_USERS;

CREATE OR REPLACE STREAM PLATFORM_DB.INGESTION.LANDING_POSTS_STREAM
ON TABLE PLATFORM_DB.INGESTION.LANDING_POSTS;
