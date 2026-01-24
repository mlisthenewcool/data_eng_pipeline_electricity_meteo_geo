-- =============================================================================
-- Database initialization script
-- Executed only when PostgreSQL volume is empty (first run)
-- =============================================================================

-- Airflow metadata database
CREATE DATABASE airflow;

-- Project data warehouse database
CREATE DATABASE projet_energie;

-- Note: Default database (same name as POSTGRES_USER) is created automatically
-- if POSTGRES_DB is not defined in docker-compose