-- =============================================================================
-- Table: silver.dim_installations
-- Source: Silver odre_installations parquet
-- Description: Electricity production installations registry
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.dim_installations (
    id_peps                     VARCHAR(50) PRIMARY KEY,
    nom_installation            VARCHAR(500),
    code_iris                   VARCHAR(9),
    code_insee                  VARCHAR(5),
    commune                     VARCHAR(255),
    code_departement            VARCHAR(3),
    departement                 VARCHAR(100),
    code_region                 VARCHAR(2),
    region                      VARCHAR(100),
    code_filiere                VARCHAR(10),
    filiere                     VARCHAR(100),
    code_technologie            VARCHAR(10),
    technologie                 VARCHAR(100),
    puissance_max_installee     DOUBLE PRECISION,
    puissance_max_raccordement  DOUBLE PRECISION,
    nb_groupes                  INTEGER,
    date_mise_en_service        DATE,
    date_raccordement           DATE,
    date_deraccordement         DATE,
    regime                      VARCHAR(50),
    gestionnaire                VARCHAR(100),
    code_epci                   VARCHAR(20),
    epci                        VARCHAR(255),
    est_renouvelable            BOOLEAN,
    type_energie                VARCHAR(20),
    est_actif                   BOOLEAN,
    updated_at                  TIMESTAMP DEFAULT NOW()
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_dim_installations_iris 
    ON silver.dim_installations(code_iris);

CREATE INDEX IF NOT EXISTS idx_dim_installations_type_energie 
    ON silver.dim_installations(type_energie);

CREATE INDEX IF NOT EXISTS idx_dim_installations_filiere 
    ON silver.dim_installations(code_filiere);

CREATE INDEX IF NOT EXISTS idx_dim_installations_region 
    ON silver.dim_installations(code_region);

CREATE INDEX IF NOT EXISTS idx_dim_installations_renouvelable 
    ON silver.dim_installations(est_renouvelable) WHERE est_renouvelable = TRUE;
