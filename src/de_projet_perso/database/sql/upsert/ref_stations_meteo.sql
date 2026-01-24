-- =============================================================================
-- UPSERT for ref.stations_meteo
-- Source: Silver meteo_france_stations
-- Strategy: Insert new rows, update existing if data changed
-- =============================================================================

INSERT INTO ref.stations_meteo (
    id,
    nom,
    lieu_dit,
    bassin,
    date_debut,
    latitude,
    longitude,
    altitude,
    mesure_solaire,
    mesure_eolien,
    params_solaires,
    params_eoliens,
    nb_parametres,
    updated_at
)
VALUES (
    %(id)s,
    %(nom)s,
    %(lieu_dit)s,
    %(bassin)s,
    %(date_debut)s,
    %(latitude)s,
    %(longitude)s,
    %(altitude)s,
    %(mesure_solaire)s,
    %(mesure_eolien)s,
    %(params_solaires)s,
    %(params_eoliens)s,
    %(nb_parametres)s,
    NOW()
)
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
    ref.stations_meteo.nom,
    ref.stations_meteo.lieu_dit,
    ref.stations_meteo.bassin,
    ref.stations_meteo.date_debut,
    ref.stations_meteo.latitude,
    ref.stations_meteo.longitude,
    ref.stations_meteo.altitude,
    ref.stations_meteo.mesure_solaire,
    ref.stations_meteo.mesure_eolien,
    ref.stations_meteo.params_solaires,
    ref.stations_meteo.params_eoliens,
    ref.stations_meteo.nb_parametres
) IS DISTINCT FROM (
    EXCLUDED.nom,
    EXCLUDED.lieu_dit,
    EXCLUDED.bassin,
    EXCLUDED.date_debut,
    EXCLUDED.latitude,
    EXCLUDED.longitude,
    EXCLUDED.altitude,
    EXCLUDED.mesure_solaire,
    EXCLUDED.mesure_eolien,
    EXCLUDED.params_solaires,
    EXCLUDED.params_eoliens,
    EXCLUDED.nb_parametres
);
