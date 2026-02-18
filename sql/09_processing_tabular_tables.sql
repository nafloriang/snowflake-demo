-- Purpose:
-- Create normalized tabular processing tables for users and posts prior to Raw Vault loading.
-- Dependencies:
-- - PLATFORM_DB.PROCESSING schema exists.
-- Execution order:
-- 1) Run after processing schema creation.
-- 2) Run before stored procedures and tasks.

CREATE OR REPLACE TABLE PLATFORM_DB.PROCESSING.USERS_TABULAR (
    user_id NUMBER,
    name STRING,
    username STRING,
    email STRING,
    phone STRING,
    website STRING,
    load_ts TIMESTAMP,
    source_file STRING
);

CREATE OR REPLACE TABLE PLATFORM_DB.PROCESSING.POSTS_TABULAR (
    post_id NUMBER,
    user_id NUMBER,
    title STRING,
    body STRING,
    load_ts TIMESTAMP,
    source_file STRING
);
