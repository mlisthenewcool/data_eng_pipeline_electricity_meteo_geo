-- =============================================================================
-- Database initialization script
-- Executed only when Postgres volume is empty (first run)

-- Note: Default database (same name as POSTGRES_USER) is created automatically
-- if POSTGRES_DB is not defined in docker-compose
-- =============================================================================

-- Airflow metadata database
-- Note: That database is already created by the docker container with POSTGRES_DB
-- CREATE DATABASE airflow;

-- Project data warehouse database
CREATE DATABASE projet_energie;

-- Note: if for any reason the CREATE DATABASE fails, use the following code :
-- SELECT 'CREATE DATABASE projet_energie'
-- WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'projet_energie')\gexec