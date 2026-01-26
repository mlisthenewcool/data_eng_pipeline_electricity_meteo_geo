-- =============================================================================
-- Schema creation for projet_energie database
-- Executed only when Postgres volume is empty (first run)
-- =============================================================================

-- \ is a meta command to execute psql
\connect projet_energie;

-- Schema for reference tables (from Silver layer)
CREATE SCHEMA IF NOT EXISTS silver;
COMMENT ON SCHEMA silver IS 'Données nettoyées (Source of Truth)';

-- Schema for analytical tables (from Gold layer)
CREATE SCHEMA IF NOT EXISTS gold;
COMMENT ON SCHEMA gold IS 'Données agrégées et calculées pour analyse (Business Layer)';

-- Grant usage to default user (same as POSTGRES_USER)
-- Note: In production, create a dedicated application user with limited permissions
