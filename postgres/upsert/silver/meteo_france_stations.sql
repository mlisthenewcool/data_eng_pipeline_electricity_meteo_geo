-- =============================================================================
-- UPSERT for silver.dim_stations_meteo
-- Source: Silver meteo_france_stations parquet
-- Strategy: COPY to staging + UPSERT to target
-- =============================================================================
-- @target_table: silver.dim_stations_meteo
-- @primary_key: id

INSERT INTO silver.dim_stations_meteo (
    id, nom, lieu_dit, bassin, date_debut,
    latitude, longitude, altitude,
    mesure_solaire, mesure_eolien,
    params_solaires, params_eoliens, nb_parametres,
    updated_at
)
SELECT
    id, nom, lieu_dit, bassin, date_debut,
    latitude, longitude, altitude,
    mesure_solaire, mesure_eolien,
    params_solaires, params_eoliens, nb_parametres,
    NOW()
FROM staging_meteo_france_stations
ON CONFLICT (id) DO UPDATE SET
    nom = EXCLUDED.nom,
    lieu_dit = EXCLUDED.lieu_dit,
    bassin = EXCLUDED.bassin,
    date_debut = EXCLUDED.date_debut,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    altitude = EXCLUDED.altitude,
    mesure_solaire = EXCLUDED.mesure_solaire,
    mesure_eolien = EXCLUDED.mesure_eolien,
    params_solaires = EXCLUDED.params_solaires,
    params_eoliens = EXCLUDED.params_eoliens,
    nb_parametres = EXCLUDED.nb_parametres,
    updated_at = NOW()
WHERE (
    silver.dim_stations_meteo.nom,
    silver.dim_stations_meteo.latitude,
    silver.dim_stations_meteo.longitude
) IS DISTINCT FROM (
    EXCLUDED.nom,
    EXCLUDED.latitude,
    EXCLUDED.longitude
);
