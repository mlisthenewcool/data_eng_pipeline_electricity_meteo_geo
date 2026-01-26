-- =============================================================================
-- Table: silver.fact_eco2mix_tr
-- Source: Silver odre_eco2mix_tr parquet
-- Description: Real-time regional electricity production (eco2mix)
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.fact_eco2mix_tr (
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
    eolien              INTEGER,
    solaire             INTEGER,
    hydraulique         INTEGER,
    pompage             VARCHAR(50),  -- Can be NULL/string in source
    bioenergies         INTEGER,
    ech_physiques       INTEGER,
    stockage_batterie   VARCHAR(50),
    destockage_batterie VARCHAR(50),
    
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
CREATE INDEX IF NOT EXISTS idx_fact_eco2mix_tr_date 
    ON silver.fact_eco2mix_tr(date_heure);

CREATE INDEX IF NOT EXISTS idx_fact_eco2mix_tr_region 
    ON silver.fact_eco2mix_tr(code_insee_region);
