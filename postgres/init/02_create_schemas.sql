-- =============================================================================
-- Schema creation for projet_energie database
-- Executed only when PostgreSQL volume is empty (first run)
-- =============================================================================

\connect projet_energie;

-- Schema for reference tables (from Silver layer)
CREATE SCHEMA IF NOT EXISTS ref;

-- Schema for analytical tables (from Gold layer)
CREATE SCHEMA IF NOT EXISTS gold;

-- Grant usage to default user (same as POSTGRES_USER)
-- Note: In production, create a dedicated application user with limited permissions
