import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import polars as pl
    from utils.downloader import save_file
    from utils.logger import logger

    return logger, pl, save_file


@app.cell
def _():
    DOWNLOAD = False
    return (DOWNLOAD,)


@app.cell
async def _(DOWNLOAD, logger, save_file):
    if DOWNLOAD:
        ODRE_URL = (
            "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
            "registre-national-installation-production-stockage-electricite-agrege/records"
            "?limit=-1"
        )

        ODRE_PARQUET_URL = (
            "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
            "registre-national-installation-production-stockage-electricite-agrege/"
            "exports/parquet?lang=fr&timezone=Europe%2FBerlin"
        )
        try:
            await save_file(ODRE_URL, dest_path="../data/bronze/installations.json")
            await save_file(ODRE_PARQUET_URL, dest_path="../data/bronze/installations.parquet")
        except Exception as e:
            logger.error(e.with_traceback)
    else:
        logger.info("not downloading data, be sure to already have it")


@app.cell
def _(pl):
    df_from_json = pl.read_json("data/bronze/installations.json")  # , infer_schema_length=130_000)

    print(df_from_json.columns)
    print(df_from_json.shape)

    # df_from_json_final = df_from_json.select(pl.col("results")).explode("results").unnest("results")
    return (df_from_json,)


@app.cell
def _(df_from_json):
    df_from_json


@app.cell
def _(pl):
    df_parquet = pl.read_parquet("data/bronze/installations.parquet")
    print(df_parquet.columns)
    print(df_parquet.shape)

    df_parquet
    return (df_parquet,)


@app.cell
def _(df_parquet, pl):
    df_parquet.filter(pl.col("codeeicresourceobject").is_duplicated()).select(
        pl.col("puismaxinstallee").sum()
    )


@app.cell
def _(df_parquet, pl):
    df_parquet_aggregations = df_parquet.filter(pl.col("codeeicresourceobject").is_null())
    df_parquet_aggregations
    return (df_parquet_aggregations,)


@app.cell
def _(df_parquet_aggregations, pl):
    df_parquet_aggregations.filter(
        (pl.col("puismaxinstallee") / pl.col("nbinstallations")) > 45
    ).select(pl.col("puismaxinstallee").sum())


@app.cell
def _(df_parquet):
    df_parquet.null_count()


@app.cell
def _(df_parquet, pl):
    df_parquet.group_by("filiere").agg(pl.col("nbinstallations").sum())


@app.cell
def _():
    columns_puissance = [
        "puismaxinstallee",
        "puismaxraccharge",
        "puismaxcharge",
        "puismaxrac",
        "puismaxinstalleedischarge",
        "energiestockable",
        "capacitereservoir",
        "hauteurchute",
        "productible",
        "debitmaximal",
        "energieannuelleglissanteinjectee",
        "energieannuelleglissanteproduite",
        "energieannuelleglissantesoutiree",
        "energieannuelleglissantestockee",
        "maxpuis",
    ]

    return (columns_puissance,)


@app.cell
def _(columns_puissance, df_parquet, pl):
    df_parquet.select([pl.col(column).sum() for column in columns_puissance])


@app.cell
def _(columns_puissance, df_from_json, pl):
    df_from_json.select([pl.col(column).sum() for column in columns_puissance])


@app.cell
def _():
    # https://www.rte-france.com/donnees-publications/eco2mix-donnees-temps-reel/chiffres-cles-electricite#parc-France
    repartitions_site_internet_rte = {
        "charbon": 1810,
        "bioenergies": 2277,
        "fioul": 2623,
        "gaz": 12462,
        "eolien": 25512,
        "hydraulique": 25524,
        "solaire": 28761,
        "nucleaire": 62990,
    }

    print(
        "puissance max installee selon le site de RTE",
        sum(repartitions_site_internet_rte.values()),
    )


@app.cell
def _():
    regions_hors_scope_site_internet_rte = [
        "Martinique",
        "Guadeloupe",
        "Guyane",
        "La Réunion",
    ]
    return (regions_hors_scope_site_internet_rte,)


@app.cell
def _():
    return


@app.cell
def _(df_parquet, pl, regions_hors_scope_site_internet_rte):
    df_parquet_sans_dom_tom = df_parquet.filter(
        ~pl.col("region").is_in(regions_hors_scope_site_internet_rte)
    )
    df_parquet_sans_dom_tom_2025 = df_parquet_sans_dom_tom.filter(
        pl.col("datemiseenservice").str.ends_with("2025")
    )

    puismaxinstallee_totale_sans_dom_tom_2025 = df_parquet_sans_dom_tom_2025.select(
        pl.col("puismaxinstallee").sum()
    ).item()

    puismaxinstallee_totale_sans_dom_tom = df_parquet_sans_dom_tom.select(
        pl.col("puismaxinstallee").sum()
    ).item()
    puismaxcharge_totale_sans_dom_tom = df_parquet_sans_dom_tom.select(
        pl.col("puismaxcharge").sum()
    ).item()
    puismaxraccharge_totale_sans_dom_tom = df_parquet_sans_dom_tom.select(
        pl.col("puismaxraccharge").sum()
    ).item()

    print("puismaxinstallee =>", puismaxinstallee_totale_sans_dom_tom)
    print(
        "puismaxinstallee - puismaxcharge =>",
        puismaxinstallee_totale_sans_dom_tom - puismaxcharge_totale_sans_dom_tom,
    )
    print(
        "puismaxinstallee - puismaxcharge - puismaxinstallee(en retrait provisoire) =>",
        puismaxinstallee_totale_sans_dom_tom - puismaxcharge_totale_sans_dom_tom - 1_101_500,
    )
    print(
        "puismaxinstallee - aggrégations =>",
        puismaxinstallee_totale_sans_dom_tom - 5_648_319.464,
    )
    print(
        "puismaxinstallee - mise en service 2025 =>",
        puismaxinstallee_totale_sans_dom_tom - puismaxinstallee_totale_sans_dom_tom_2025,
    )
    print(
        "puismaxinstallee - puismaxcharge - puismaxraccharge =>",
        puismaxinstallee_totale_sans_dom_tom
        - puismaxcharge_totale_sans_dom_tom
        - puismaxraccharge_totale_sans_dom_tom,
    )
    return (df_parquet_sans_dom_tom,)


@app.cell
def _():
    return


@app.cell
def _(df_parquet, pl):
    puismaxinstallee_totale = df_parquet.select(pl.col("puismaxinstallee").sum()).item()
    puismaxcharge_totale = df_parquet.select(pl.col("puismaxcharge").sum()).item()
    # puismaxinstallee_totale = df_parquet.select(pl.col("puismaxinstallee").sum()).item()

    print("puismaxinstallee =>", puismaxinstallee_totale)
    print(
        "puismaxinstallee - puismaxcharge =>",
        puismaxinstallee_totale - puismaxcharge_totale,
    )
    print(
        "puismaxinstallee - puismaxcharge - puismaxinstallee(en retrait provisoire) =>",
        puismaxinstallee_totale - puismaxcharge_totale - 1_101_500,
    )


@app.cell
def _(df_parquet, pl):
    print(df_parquet.select(pl.col("regime").value_counts()))

    df_parquet.filter(pl.col("regime") == "En retrait provisoire").select(
        pl.col("puismaxinstallee").sum()
    )


@app.cell
def _(df_parquet_sans_dom_tom, pl):
    df_parquet_sans_dom_tom.filter(pl.col("technologie") == "Photovoltaïque").select(
        [
            pl.col("nbinstallations").sum(),
            pl.col("puismaxinstallee").sum(),
        ]
    )


@app.cell
def _(df_parquet_sans_dom_tom, pl):
    df_parquet_sans_dom_tom_ni_aggregations = df_parquet_sans_dom_tom.filter(
        pl.col("codeeicresourceobject").is_not_null()
    )

    puismaxinstallee_totale_sans_dom_tom_filter_aggregations = (
        df_parquet_sans_dom_tom_ni_aggregations.select(pl.col("puismaxinstallee").sum()).item()
    )

    df_parquet_sans_dom_tom_ni_aggregations.group_by("filiere").agg(
        pl.col("puismaxinstallee").sum(),
        (
            pl.col("puismaxinstallee").sum()
            / puismaxinstallee_totale_sans_dom_tom_filter_aggregations
            * 100
        )
        .round(2)
        .alias("puismaxinstalle_pourcentage"),
    ).sort(by="puismaxinstallee", descending=True)
    return (df_parquet_sans_dom_tom_ni_aggregations,)


@app.cell
def _(df_parquet, pl):
    df_parquet_sans_aggregations = df_parquet.filter(pl.col("codeeicresourceobject") != "")

    puismaxinstallee_totale_sans_aggregations = df_parquet_sans_aggregations.select(
        pl.col("puismaxinstallee").sum()
    ).item()

    df_parquet_sans_aggregations.group_by("filiere").agg(
        pl.col("puismaxinstallee").sum(),
        (pl.col("puismaxinstallee").sum() / puismaxinstallee_totale_sans_aggregations * 100)
        .round(2)
        .alias("puismaxinstalle_pourcentage"),
    ).sort(by="puismaxinstallee", descending=True)


@app.cell
def _(df_parquet_sans_dom_tom, pl):
    df_parquet_sans_dom_tom.filter(pl.col("codeeicresourceobject").is_null()).group_by(
        "technologie"
    ).agg(
        pl.col("puismaxinstallee").sum(),
        pl.col("puismaxcharge").sum(),
    ).sort(by="puismaxinstallee", descending=True)


@app.cell
def _(df_parquet_sans_dom_tom, pl):
    df_parquet_sans_dom_tom.group_by("region").agg(
        pl.col("puismaxinstallee").sum(), pl.col("puismaxcharge").sum()
    ).sort(by="puismaxinstallee", descending=True)


@app.cell
def _(df_parquet_sans_dom_tom_ni_aggregations, pl):
    df_parquet_sans_dom_tom_ni_aggregations.group_by("region").agg(
        pl.col("puismaxinstallee").sum(), pl.col("puismaxcharge").sum()
    ).sort(by="puismaxinstallee", descending=True)


@app.cell
def _(df_parquet_sans_dom_tom, pl):
    df_parquet_sans_dom_tom_filter_en_retrait_prov = (
        df_parquet_sans_dom_tom.filter(pl.col("regime") != "En retrait provisoire")
        .group_by("region")
        .agg(
            pl.col("puismaxinstallee").sum(),
            pl.col("puismaxcharge").sum(),
        )
        .sort(by="puismaxinstallee", descending=True)
    )
    df_parquet_sans_dom_tom_filter_en_retrait_prov


@app.cell
def _(df_parquet, pl):
    df_aggregations = df_parquet.filter(pl.col("nbinstallations") > 1)
    return (df_aggregations,)


@app.cell
def _(df_aggregations, pl):
    df_aggregations.select(pl.col("codeiris")).null_count()


@app.cell
def _():
    return


@app.cell
def _(df_parquet, pl):
    df_parquet.select(pl.col("codeiris")).null_count()


@app.cell
def _(df_parquet, pl):
    df_parquet.filter(pl.col("codeiris").is_null())


@app.cell
def _(df_parquet, pl):
    df_parquet.filter(pl.col("codeiris").is_null()).select(pl.col("puismaxinstallee").sum())


@app.cell
def _(df_parquet, pl):
    df_parquet.filter(pl.col("codeiris").is_null()).group_by("technologie").agg(
        pl.col("puismaxinstallee").sum()
    ).sort(by="puismaxinstallee", descending=True)


@app.cell
def _(df_parquet, pl):
    df_parquet.filter(pl.col("codeiris").is_null() & pl.col("codeinseecommune").is_null())


@app.cell
def _(df_parquet, pl):
    df_parquet.filter(pl.col("codeinseecommune").is_null())


@app.cell
def _():
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
