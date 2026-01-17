import marimo

__generated_with = "0.18.4"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import polars as pl

    from de_electricity_meteo.config.paths import DATA_BRONZE

    return DATA_BRONZE, mo, pl


@app.cell
def _(DATA_BRONZE, pl):
    obs_path = DATA_BRONZE / "paquetobs-stations-horaire_2026-01-05T10_00_00Z.geojson"
    df = pl.read_json(obs_path)
    return (df,)


@app.cell
def _(df):
    df
    return


@app.cell
def _(DATA_BRONZE, pl):
    obs_path_csv = DATA_BRONZE / "paquetobs-stations-horaire_2026-01-05T10_00_00Z.csv"
    df_csv = pl.read_csv(
        obs_path_csv, has_header=True, separator=";", schema_overrides={"geo_id_insee": pl.String}
    )
    return (df_csv,)


@app.cell
def _(df_csv):
    print(df_csv.shape)
    print(df_csv.columns)
    return


@app.cell
def _(df_csv):
    df_csv
    return


@app.cell
def _():
    # contrôlé
    catalogue_observations = {
        "geo_id_insee": "ID du point défini par le numéro Insee",
        "lat": "latitude du poste en degrés",
        "lon": "longitude du poste en degrés",
        "reference_time": "date et heure de la production des données",
        "insert_time": "date et heure d'insertion des données dans la base de données",
        "validity_time": "date et heure de validité des données",
        "t": "température sous abri en degrés kelvins",
        "td": "point de rosée à 2 mètres au-dessus du sol en degrés kelvins",
        "tx": "température maximale de l'air à 2 mètres au-dessus du sol en degrés kelvins",
        "tn": "température minimale de l'air à 2 mètres au-dessus du sol en degrés kelvins",
        "u": "humidité relative en %",
        "ux": "humidité relative maximale dans l'heure en %",
        "un": "humidité relative minimale dans l'heure en %",
        "dd": "direction de ff en degrés",
        "ff": "force du vent moyen à 10 mètres au-dessus du sol en m/s",
        "dxy": "direction de fxy en degrés",
        "fxy": "force maximale de FF dans l'heure à 10 mètres au-dessus du sol en m/s",
        "dxi": "direction de fxi en degrés",
        "fxi": "force maximale du vent instantané dans l'heure à 10 mètres au-dessus du sol en m/s",
        "rr1": "hauteur de précipitations dans l'heure en mm",
        "t_10": "température à 10 centimètres de profondeur sous le sol en degrés kelvins",
        "t_20": "température à 20 centimètres de profondeur sous le sol en degrés kelvins",
        "t_50": "température à 50 centimètres de profondeur sous le sol en degrés kelvins",
        "t_100": "température à 100 centimètres de profondeur (1 m) sous le sol en degrés kelvins",
        "vv": "visibilité horizontale en mètres",
        "etat_sol": "code de l'état du sol",
        "sss": "hauteur totale de la couverture neigeuse en mètres",
        "n": "nébulosité totale en octas",
        "insolh": "durée d'insolation au cours de la période en minutes",
        "ray_glo01": "rayonnement global sur la période en J/m²",
        "pres": "pression au niveau de la station en pascals",
        "pmer": "pression au niveau de la mer en pascals",
        # uniquement sur les données à la fréquence 6 min
        # "dxi10": "direction de fxi10 en degrés",
        # "fxi10": "force maximale du vent instantané sur 10 mn à 10 mètres au-dessus du sol en m/s",
        # "rr_per": "quantité de précipitations tombées sur 6 mn en mm",
    }

    selection_projets = {
        "solaire": ["ray_glo01", "insolh", "n"],
        "eolien": ["ff", "dd", "fxi", "dxi"],
        "thermique": ["t", "tx", "tn", "td"],
        "sol_agri": ["t_10", "t_20", "t_50", "t_100", "rr1", "u"],
        "metadonnees": ["geo_id_insee", "lat", "lon", "validity_time"],
    }
    return (catalogue_observations,)


@app.cell
def _(DATA_BRONZE, pl):
    liste_stations_observations_24h = DATA_BRONZE / "liste-stations-obs-metropole-om.csv"
    df_liste_stations_observations_24h = pl.read_csv(
        liste_stations_observations_24h,
        separator=";",
        has_header=True,
        schema_overrides={"Id_station": pl.String},
    )

    print(df_liste_stations_observations_24h.shape)
    print(df_liste_stations_observations_24h.columns)
    df_liste_stations_observations_24h
    return (df_liste_stations_observations_24h,)


@app.cell
def _(DATA_BRONZE, pl):
    liste_stations_paquet_observations_24h = (
        DATA_BRONZE / "liste-stations-paquet-obs-metropole-om.csv"
    )
    df_liste_stations_paquet_observations_24h = pl.read_csv(
        liste_stations_paquet_observations_24h,
        separator=";",
        has_header=True,
        schema_overrides={"Id_station": pl.String},
    )

    print(df_liste_stations_paquet_observations_24h.shape)
    print(df_liste_stations_paquet_observations_24h.columns)
    df_liste_stations_paquet_observations_24h
    return (df_liste_stations_paquet_observations_24h,)


@app.cell
def _(
    df_liste_stations_observations_24h,
    df_liste_stations_paquet_observations_24h,
):
    df_liste_stations_observations_24h.equals(other=df_liste_stations_paquet_observations_24h)
    return


@app.cell
def _():
    # todo: comparer cette liste avec celle créée à partir de toutes les stations climatologies
    return


@app.cell
def _(mo):
    mo.md(r"""
    API observations /station/horaire -> 1 station (1h)
    """)
    return


@app.cell
def _(DATA_BRONZE, pl):
    observations_path = DATA_BRONZE / "obs-horaire_01014002_2026-01-06T15_32_01Z.csv"
    df_observations = pl.read_csv(
        observations_path,
        separator=";",
        has_header=True,
        schema_overrides={"geo_id_insee": pl.String},
    )

    print(df_observations.shape)
    print(df_observations.columns)
    df_observations
    return (df_observations,)


@app.cell
def _(mo):
    mo.md(r"""
    API paquet-obs /paquet/horaire -> toutes stations par département (24h)
    """)
    return


@app.cell
def _(DATA_BRONZE, pl):
    paquet_observations_path = DATA_BRONZE / "paquetobs-horaire_13_2026-01-06T16_03_03Z.csv"
    df_paquet_observations = pl.read_csv(
        paquet_observations_path,
        separator=";",
        has_header=True,
        schema_overrides={"geo_id_insee": pl.String},
    )

    print(df_paquet_observations.shape)
    print(df_paquet_observations.columns)
    df_paquet_observations
    return (df_paquet_observations,)


@app.cell
def _(catalogue_observations, df_observations, df_paquet_observations):
    # conclusions:
    # catalogue observations complet
    # observations & paquet observations renvoient les mêmes colonnes
    print(df_observations.columns == df_paquet_observations.columns)
    print(sorted(df_observations.columns) == sorted(list(catalogue_observations.keys())))
    return


@app.cell
def _(mo):
    mo.md(r"""
    est-ce que observations/paquet_observations renvoient les mêmes données que climatologie ? probablement faux
    """)
    return


@app.cell
def _(df_liste_stations_observations_24h, pl):
    # pour 2/3 stations
    # vérifier si pour la même période on obtient des données similaires ?

    df_liste_stations_observations_24h.filter(pl.col("Id_station") == "01089001")
    return


@app.cell
def _(s):
    s
    return


if __name__ == "__main__":
    app.run()
