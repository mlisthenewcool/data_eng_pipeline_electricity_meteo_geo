-- =============================================================================
-- Table: silver.fact_eco2mix_cons_def
-- Source: Silver odre_eco2mix_cons_def parquet
-- Description: Consolidated regional electricity production (eco2mix)
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.fact_eco2mix_cons_def (
    -- Composite primary key: region + datetime
    code_insee_region   VARCHAR(2),
    date_heure          TIMESTAMP WITH TIME ZONE,
    
    -- Dimensions
    libelle_region      VARCHAR(100),
    nature              VARCHAR(50),
    date                VARCHAR(10),
    heure               VARCHAR(5),
    
    -- Production facts (MW)
    consommation        INTEGER,
    thermique           INTEGER,
    nucleaire           INTEGER,
    eolien              VARCHAR(50),  -- String in source (can be NULL/empty)
    solaire             INTEGER,
    hydraulique         INTEGER,
    pompage             INTEGER,
    bioenergies         INTEGER,
    ech_physiques       INTEGER,
    stockage_batterie   INTEGER,
    destockage_batterie INTEGER,
    eolien_terrestre    INTEGER,
    eolien_offshore     INTEGER,
    
    -- Coverage rates (%)
    tco_thermique       DOUBLE PRECISION,
    tch_thermique       DOUBLE PRECISION,
    tco_nucleaire       DOUBLE PRECISION,
    tch_nucleaire       DOUBLE PRECISION,
    tco_eolien          DOUBLE PRECISION,
    tch_eolien          DOUBLE PRECISION,
    tco_solaire         DOUBLE PRECISION,
    tch_solaire         DOUBLE PRECISION,
    tco_hydraulique     DOUBLE PRECISION,
    tch_hydraulique     DOUBLE PRECISION,
    tco_bioenergies     DOUBLE PRECISION,
    tch_bioenergies     DOUBLE PRECISION,
    
    updated_at          TIMESTAMP DEFAULT NOW(),
    
    PRIMARY KEY (code_insee_region, date_heure)
);

-- Indexes for time-series queries
CREATE INDEX IF NOT EXISTS idx_fact_eco2mix_cons_def_date 
    ON silver.fact_eco2mix_cons_def(date_heure);

CREATE INDEX IF NOT EXISTS idx_fact_eco2mix_cons_def_region 
    ON silver.fact_eco2mix_cons_def(code_insee_region);
