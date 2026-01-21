import json
import os
from calendar import isleap

import marimo
import polars as pl
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

__generated_with = "0.18.4"
app = marimo.App(width="full")


@app.cell
def _():
    pl.Config.set_tbl_rows(250)
    pl.Config.set_tbl_cols(10)
    pl.Config.set_tbl_width_chars(500)
    return (pl,)


@app.cell
def lecture_du_df(pl):
    df = pl.read_parquet("data/bronze/eco2mix-national-tr_2025.parquet").sort(
        by="date_heure", descending=False
    )
    return (df,)


@app.cell
def _(df):
    print(df.shape)
    print(df.columns)


@app.cell
def _(df):
    print(df.head(20))


@app.cell
def _(df, pl):
    print(df.filter(pl.col("perimetre") != "France"))

    print(df.select(df.null_count() > 0))


@app.cell
def _(df):
    df_null_cols = df.select([col for col in df.columns if df[col].null_count() > 0])
    return (df_null_cols,)


@app.cell
def _(df_null_cols):
    print(df_null_cols.shape)


@app.cell
def _(df_null_cols):
    print(df_null_cols)


@app.cell
def _(pl):
    df_historique = pl.read_parquet("eco2mix-regional-cons-def.parquet").sort(
        by="date_heure", descending=False
    )
    return (df_historique,)


@app.cell
def _(df_historique):
    print(df_historique.shape)
    print(df_historique.columns)


@app.cell
def _(df_historique, pl):
    print(df_historique.filter(pl.col("column_30").is_not_null()).select(pl.col("column_30")))


@app.cell
def _(df_historique, pl):
    print(
        df_historique.select(
            pl.col([col for col in df_historique.columns if "tco" in col])
        ).null_count()
    )


@app.cell
def _(df_historique, pl):
    print(df_historique.select(pl.col("date_heure")).min())
    print(df_historique.select(pl.col("date_heure")).max())


@app.cell
def _(df_historique, pl):
    print(df_historique.filter(pl.col("nature") == "Données consolidées").count())
    print(df_historique.filter(pl.col("nature") == "Données définitives").count())


@app.cell
def _(df_historique, pl):
    df_donnees_consolidees = df_historique.filter(pl.col("nature") == "Données consolidées")
    print(df_donnees_consolidees.shape)
    print(df_donnees_consolidees.select(pl.col("date_heure")).min())
    print(df_donnees_consolidees.select(pl.col("date_heure")).max())
    return (df_donnees_consolidees,)


@app.cell
def _(df_historique, pl):
    df_donnees_definitives = df_historique.filter(pl.col("nature") == "Données définitives")
    print(df_donnees_definitives.shape)
    print(df_donnees_definitives.select(pl.col("date_heure")).min())
    print(df_donnees_definitives.select(pl.col("date_heure")).max())
    return (df_donnees_definitives,)


@app.cell
def _(df_donnees_consolidees, df_donnees_definitives, df_historique):
    assert len(df_historique) == len(df_donnees_consolidees) + len(df_donnees_definitives), (
        "probleme dans les donnees"
    )
    # donc données définitives
    #   => depuis 2013-01-01 00:00:00 -> 2023-12-31 23:30:00
    # donc données consolidées
    #   => depuis 2024-01-01 00:00:00 -> 2024-12-31 23:30:00
    # donc données temps réel
    #   => depuis 2025-01-01 00:00:00 -> aujourd'hui (max fin d'année 2025)


@app.cell
def _(df_historique, pl):
    print(df_historique.select(pl.col("libelle_region").unique().count()))


@app.cell
def _(df_donnees_definitives):
    nombre_demie_heures_dans_1_jour = 48
    nombre_regions = 12  # pas la Corse ni les DOM/TOM
    nombre_total_jours_entre_2013_et_2024 = sum(
        [365 if isleap(annee) else 366 for annee in range(2013, 2024)]
    )

    n = nombre_demie_heures_dans_1_jour * nombre_regions * nombre_total_jours_entre_2013_et_2024
    print(len(df_donnees_definitives) - n)  # = -4032

    # todo: vérifier ce qui cloche
    # on doit vérifier tous les totaux


@app.cell
def _():
    # Configuration
    URL_FICHES_JSON = "https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/BASE/METADONNEES_STATION/fiches.json"
    OUTPUT_PATH = "data/bronze/fiches_stations.json"

    def download_file(url, destination):
        """
        Télécharge un fichier de manière robuste avec gestion des retries
        et création de dossiers parents.
        """
        # 1. Création du dossier de destination si inexistant
        os.makedirs(os.path.dirname(destination), exist_ok=True)

        # 2. Configuration des retries (Exponential backoff)
        # Re-essaie jusqu'à 5 fois avec des délais croissants (1s, 2s, 4s, 8s, 16s)
        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
        )
        session.mount("https://", HTTPAdapter(max_retries=retries))

        print(f"Début du téléchargement depuis : {url}")

        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()  # Lève une exception si erreur HTTP(4xx ou 5xx)

            # 3. Vérification du contenu JSON
            data = response.json()

            # 4. Sauvegarde locale
            with open(destination, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

            print(f"Succès ! Fichier sauvegardé sous : {destination}")
            print(f"Nombre d'enregistrements trouvés : {len(data)}")
            return data

        except requests.exceptions.HTTPError as errh:
            print(f"Erreur HTTP : {errh}")
        except requests.exceptions.ConnectionError as errc:
            print(f"Erreur de Connexion : {errc}")
        except requests.exceptions.Timeout as errt:
            print(f"Erreur de Timeout : {errt}")
        except requests.exceptions.RequestException as err:
            print(f"Erreur imprévue : {err}")
        except json.JSONDecodeError:
            print("Erreur : Le fichier reçu n'est pas un JSON valide.")

        return None

    if __name__ == "__main__":
        download_file(URL_FICHES_JSON, OUTPUT_PATH)


@app.cell
def _(pl):
    df_stations = pl.read_json("data/bronze/fiches_stations.json")
    return (df_stations,)


@app.cell
def _(df_stations):
    df_stations.head(10)


@app.cell
def _(df_stations, pl):
    df_stations_en_service = df_stations.filter(pl.col("dateFin") == "")
    return (df_stations_en_service,)


@app.cell
def _(df_stations_en_service):
    df_stations_en_service.head(10)


@app.cell
def _(df_stations, df_stations_en_service):
    print(df_stations.shape)
    print(df_stations_en_service.shape)


@app.cell
def _(df_stations_en_service):
    df_stations_en_service


@app.cell
def _(pl):
    df_donnees_meteo_13_2020_2025 = pl.read_csv("data/bronze/clim-base_hor-13-2020-2025.csv")
    return (df_donnees_meteo_13_2020_2025,)


@app.cell
def _(df_donnees_meteo_13_2020_2025):
    print(df_donnees_meteo_13_2020_2025.shape)
    print(df_donnees_meteo_13_2020_2025.head(10))


@app.cell
def _():
    # bec_de_l_aigle = df_stations_en_service.filter(pl.col("id") == "13028001")
    return


@app.cell
def _(df_donnees_meteo_13_2020_2025):
    print(df_donnees_meteo_13_2020_2025)


@app.cell
def _(df_donnees_meteo_13_2020_2025, pl):
    print(df_donnees_meteo_13_2020_2025.select(pl.col("aaaammjjhh").max()))
    print(df_donnees_meteo_13_2020_2025.select(pl.col("aaaammjjhh").min()))


@app.cell
def _(df_stations_en_service, pl):
    df_stations_en_service.filter(pl.col("nom").is_duplicated())


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
