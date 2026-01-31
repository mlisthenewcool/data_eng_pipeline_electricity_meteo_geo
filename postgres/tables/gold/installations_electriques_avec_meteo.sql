-- =============================================================================
-- Table creation for projet_energie database (V1 - Minimal)
-- Can be executed multiple times safely (IF NOT EXISTS)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- SCHEMA: ref (reference tables from Silver layer)
-- ---------------------------------------------------------------------------

-- Météo France weather stations
CREATE TABLE IF NOT EXISTS ref.stations_meteo (
    id                  VARCHAR(20) PRIMARY KEY,
    nom                 VARCHAR(255),
    lieu_dit            VARCHAR(255),
    bassin              VARCHAR(100),
    date_debut          DATE,
    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    altitude            INTEGER,
    mesure_solaire      BOOLEAN,
    mesure_eolien       BOOLEAN,
    params_solaires     TEXT[],  -- Array of solar parameters
    params_eoliens      TEXT[],  -- Array of wind parameters
    nb_parametres       INTEGER,
    updated_at          TIMESTAMP DEFAULT NOW()
);

-- Index for geographic queries (simple lat/lon, PostGIS later if needed)
CREATE INDEX IF NOT EXISTS idx_stations_meteo_coords
    ON ref.stations_meteo(latitude, longitude);

-- Index for filtering by measurement type
CREATE INDEX IF NOT EXISTS idx_stations_meteo_solaire
    ON ref.stations_meteo(mesure_solaire) WHERE mesure_solaire = TRUE;
CREATE INDEX IF NOT EXISTS idx_stations_meteo_eolien
    ON ref.stations_meteo(mesure_eolien) WHERE mesure_eolien = TRUE;


-- ---------------------------------------------------------------------------
-- SCHEMA: gold (analytical tables from Gold layer)
-- ---------------------------------------------------------------------------

-- Renewable installations linked to nearest weather station
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
    updated_at              TIMESTAMP DEFAULT NOW(),

    -- Foreign key to stations_meteo (optional, station can be NULL)
    CONSTRAINT fk_station_meteo
        FOREIGN KEY (station_meteo_id)
        REFERENCES ref.stations_meteo(id)
        ON DELETE SET NULL
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
