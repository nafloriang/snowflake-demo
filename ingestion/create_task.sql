CREATE OR REPLACE TASK PLATFORM_DB.INGESTION.TASK_DBT_PLACEHOLDER
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '5 MINUTE'
AS
-- Placeholder: replace this statement with dbt execution orchestration.
SELECT CURRENT_TIMESTAMP() AS task_heartbeat;
