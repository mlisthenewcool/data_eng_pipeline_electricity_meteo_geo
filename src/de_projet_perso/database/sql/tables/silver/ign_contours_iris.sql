-- =============================================================================
-- Table: silver.dim_contours_iris
-- Source: Silver ign_contours_iris parquet
-- Description: IRIS geographic contours from IGN
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.dim_contours_iris (
    code_iris           VARCHAR(9) PRIMARY KEY,
    nom_iris            VARCHAR(255),
    code_insee          VARCHAR(5),
    nom_commune         VARCHAR(255),
    type_iris           VARCHAR(10),
    geom_wkb            BYTEA,  -- WKB geometry (Lambert 93)
    centroid_lat        DOUBLE PRECISION,
    centroid_lon        DOUBLE PRECISION,
    updated_at          TIMESTAMP DEFAULT NOW()
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_dim_contours_iris_commune 
    ON silver.dim_contours_iris(code_insee);

CREATE INDEX IF NOT EXISTS idx_dim_contours_iris_coords 
    ON silver.dim_contours_iris(centroid_lat, centroid_lon);
