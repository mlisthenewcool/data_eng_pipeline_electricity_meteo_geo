-- =============================================================================
-- UPSERT for silver.dim_contours_iris
-- Source: Silver ign_contours_iris parquet
-- Strategy: COPY to staging + UPSERT to target
-- =============================================================================
-- @target_table: silver.dim_contours_iris
-- @primary_key: code_iris

INSERT INTO silver.dim_contours_iris (
    code_iris, nom_iris, code_insee, nom_commune, type_iris,
    geom_wkb, centroid_lat, centroid_lon,
    updated_at
)
SELECT
    code_iris, nom_iris, code_insee, nom_commune, type_iris,
    geom_wkb, centroid_lat, centroid_lon,
    NOW()
FROM staging_ign_contours_iris
ON CONFLICT (code_iris) DO UPDATE SET
    nom_iris = EXCLUDED.nom_iris,
    code_insee = EXCLUDED.code_insee,
    nom_commune = EXCLUDED.nom_commune,
    type_iris = EXCLUDED.type_iris,
    geom_wkb = EXCLUDED.geom_wkb,
    centroid_lat = EXCLUDED.centroid_lat,
    centroid_lon = EXCLUDED.centroid_lon,
    updated_at = NOW()
WHERE (
    silver.dim_contours_iris.nom_iris,
    silver.dim_contours_iris.centroid_lat,
    silver.dim_contours_iris.centroid_lon
) IS DISTINCT FROM (
    EXCLUDED.nom_iris,
    EXCLUDED.centroid_lat,
    EXCLUDED.centroid_lon
);
