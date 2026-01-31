-- =============================================================================
-- UPSERT for silver.fact_eco2mix_cons_def
-- Source: Silver odre_eco2mix_cons_def parquet
-- Strategy: COPY to staging + UPSERT to target
--
-- Note: For large fact tables like this one (~2.5M rows), a TRUNCATE + COPY
-- strategy could be significantly faster if full refresh is acceptable.
-- This would skip the staging table and conflict checking entirely.
-- =============================================================================
-- @target_table: silver.fact_eco2mix_cons_def
-- @primary_key: code_insee_region, date_heure

INSERT INTO silver.fact_eco2mix_cons_def (
    code_insee_region, date_heure, libelle_region, nature, date, heure,
    consommation, thermique, nucleaire, eolien, solaire, hydraulique,
    pompage, bioenergies, ech_physiques, stockage_batterie, destockage_batterie,
    eolien_terrestre, eolien_offshore,
    tco_thermique, tch_thermique, tco_nucleaire, tch_nucleaire,
    tco_eolien, tch_eolien, tco_solaire, tch_solaire,
    tco_hydraulique, tch_hydraulique, tco_bioenergies, tch_bioenergies,
    updated_at
)
SELECT
    code_insee_region, date_heure, libelle_region, nature, date, heure,
    consommation, thermique, nucleaire, eolien, solaire, hydraulique,
    pompage, bioenergies, ech_physiques, stockage_batterie, destockage_batterie,
    eolien_terrestre, eolien_offshore,
    tco_thermique, tch_thermique, tco_nucleaire, tch_nucleaire,
    tco_eolien, tch_eolien, tco_solaire, tch_solaire,
    tco_hydraulique, tch_hydraulique, tco_bioenergies, tch_bioenergies,
    NOW()
FROM staging_odre_eco2mix_cons_def
ON CONFLICT (code_insee_region, date_heure) DO UPDATE SET
    consommation = EXCLUDED.consommation,
    thermique = EXCLUDED.thermique,
    nucleaire = EXCLUDED.nucleaire,
    eolien = EXCLUDED.eolien,
    solaire = EXCLUDED.solaire,
    hydraulique = EXCLUDED.hydraulique,
    bioenergies = EXCLUDED.bioenergies,
    eolien_terrestre = EXCLUDED.eolien_terrestre,
    eolien_offshore = EXCLUDED.eolien_offshore,
    updated_at = NOW()
WHERE (
    silver.fact_eco2mix_cons_def.consommation,
    silver.fact_eco2mix_cons_def.solaire
) IS DISTINCT FROM (
    EXCLUDED.consommation,
    EXCLUDED.solaire
);
