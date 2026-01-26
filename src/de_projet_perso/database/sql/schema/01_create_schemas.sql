-- =============================================================================
-- Schema creation for projet_energie database
-- Can be executed multiple times safely (IF NOT EXISTS)
-- =============================================================================

-- Schema for Silver layer tables (dimensions and facts from Silver parquet)
CREATE SCHEMA IF NOT EXISTS silver;

-- Schema for Gold layer tables (analytical/aggregated data)
CREATE SCHEMA IF NOT EXISTS gold;

-- Legacy schema (kept for backward compatibility during migration)
CREATE SCHEMA IF NOT EXISTS ref;
