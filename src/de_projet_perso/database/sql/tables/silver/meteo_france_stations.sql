-- =============================================================================
-- Table: silver.dim_stations_meteo
-- Source: Silver meteo_france_stations parquet
-- Description: Weather stations reference table from Météo France
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.dim_stations_meteo (
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
    params_solaires     TEXT[],
    params_eoliens      TEXT[],
    nb_parametres       INTEGER,
    updated_at          TIMESTAMP DEFAULT NOW()
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_dim_stations_meteo_coords 
    ON silver.dim_stations_meteo(latitude, longitude);

CREATE INDEX IF NOT EXISTS idx_dim_stations_meteo_solaire 
    ON silver.dim_stations_meteo(mesure_solaire) WHERE mesure_solaire = TRUE;

CREATE INDEX IF NOT EXISTS idx_dim_stations_meteo_eolien 
    ON silver.dim_stations_meteo(mesure_eolien) WHERE mesure_eolien = TRUE;
