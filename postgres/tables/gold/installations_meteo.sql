-- =============================================================================
-- Gold table: installations_meteo
-- Renewable installations linked to nearest compatible weather station
-- Can be executed multiple times safely (IF NOT EXISTS)
-- =============================================================================

CREATE TABLE IF NOT EXISTS gold.installations_meteo (
    id_peps                 VARCHAR(50) PRIMARY KEY,
    nom_installation        VARCHAR(500),
    type_energie            VARCHAR(20),  -- 'solaire' or 'eolien'
    code_filiere            VARCHAR(10),
    filiere                 VARCHAR(100),
    technologie             VARCHAR(100),
    puissance_max_installee DOUBLE PRECISION,  -- MW
    commune                 VARCHAR(255),
    departement             VARCHAR(100),
    region                  VARCHAR(100),
    code_iris               VARCHAR(9),
    centroid_lat            DOUBLE PRECISION,
    centroid_lon            DOUBLE PRECISION,
    date_mise_en_service    DATE,
    station_meteo_id        VARCHAR(20),
    station_meteo_nom       VARCHAR(255),
    station_meteo_lat       DOUBLE PRECISION,
    station_meteo_lon       DOUBLE PRECISION,
    distance_station_km     DOUBLE PRECISION,
    updated_at              TIMESTAMP DEFAULT NOW()
);

-- Index for filtering by energy type
CREATE INDEX IF NOT EXISTS idx_installations_type_energie
    ON gold.installations_meteo(type_energie);

-- Index for geographic queries
CREATE INDEX IF NOT EXISTS idx_installations_coords
    ON gold.installations_meteo(centroid_lat, centroid_lon);

-- Index for regional analysis
CREATE INDEX IF NOT EXISTS idx_installations_region
    ON gold.installations_meteo(region);

-- Index for station lookups
CREATE INDEX IF NOT EXISTS idx_installations_station
    ON gold.installations_meteo(station_meteo_id);
