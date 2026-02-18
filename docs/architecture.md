# Platform PoC Architecture

## Flow: ingestion -> staging -> raw vault

1. **Ingestion (Snowpipe + Landing)**
   - Files arrive in `landing/` on GCS.
   - Entity-specific Snowpipes copy JSON rows into `PLATFORM_DB.LANDING` tables with metadata.
2. **Staging (dbt views)**
   - dbt staging models parse `VARIANT` payloads into typed, analytics-ready columns.
   - Staging is intentionally thin and focused on field extraction.
3. **DV Raw (dbt tables)**
   - Hubs capture business keys.
   - Links capture relationships.
   - Satellites capture descriptive attributes.

## Entity-based pipes

This PoC uses one Snowpipe per entity (for example, users and posts) so ingestion remains explicit and easy to operate. It supports simple onboarding by adding a new landing table and matching pipe.

## Why no complex stored procedure

A complex onboarding stored procedure was intentionally excluded to keep this PoC minimal:

- less operational overhead,
- easier review and debugging,
- faster iteration while validating the ingestion-to-vault pattern.

The task is a lightweight scheduler placeholder that can later call an orchestrated dbt execution path.
