-- =============================================================================
-- UPSERT for silver.dim_installations
-- Source: Silver odre_installations parquet
-- Strategy: COPY to staging + UPSERT to target
-- =============================================================================
-- @target_table: silver.dim_installations
-- @primary_key: id_peps

INSERT INTO silver.dim_installations (
    id_peps, nom_installation, code_iris, code_insee, commune,
    code_departement, departement, code_region, region,
    code_filiere, filiere, code_technologie, technologie,
    puissance_max_installee, puissance_max_raccordement, nb_groupes,
    date_mise_en_service, date_raccordement, date_deraccordement,
    regime, gestionnaire, code_epci, epci,
    est_renouvelable, type_energie, est_actif,
    updated_at
)
SELECT
    id_peps, nom_installation, code_iris, code_insee, commune,
    code_departement, departement, code_region, region,
    code_filiere, filiere, code_technologie, technologie,
    puissance_max_installee, puissance_max_raccordement, nb_groupes,
    date_mise_en_service, date_raccordement, date_deraccordement,
    regime, gestionnaire, code_epci, epci,
    est_renouvelable, type_energie, est_actif,
    NOW()
FROM staging_odre_installations
ON CONFLICT (id_peps) DO UPDATE SET
    nom_installation = EXCLUDED.nom_installation,
    code_iris = EXCLUDED.code_iris,
    puissance_max_installee = EXCLUDED.puissance_max_installee,
    date_deraccordement = EXCLUDED.date_deraccordement,
    est_actif = EXCLUDED.est_actif,
    updated_at = NOW()
WHERE (
    silver.dim_installations.nom_installation,
    silver.dim_installations.puissance_max_installee,
    silver.dim_installations.est_actif
) IS DISTINCT FROM (
    EXCLUDED.nom_installation,
    EXCLUDED.puissance_max_installee,
    EXCLUDED.est_actif
);
