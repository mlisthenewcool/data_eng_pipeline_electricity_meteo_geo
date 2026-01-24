-- =============================================================================
-- Schema creation for projet_energie database
-- Can be executed multiple times safely (IF NOT EXISTS)
-- =============================================================================

-- Schema for reference tables (from Silver layer)
CREATE SCHEMA IF NOT EXISTS ref;

-- Schema for analytical tables (from Gold layer)
CREATE SCHEMA IF NOT EXISTS gold;
