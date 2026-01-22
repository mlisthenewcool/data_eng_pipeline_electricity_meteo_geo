import marimo

__generated_with = "0.19.4"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import polars as pl
    from de_projet_perso.core.data_catalog import DataCatalog, Dataset
    from de_projet_perso.core.settings import settings

    return DataCatalog, pl, settings


@app.cell
def _(DataCatalog, settings):
    catalog = DataCatalog.load(settings.data_catalog_file_path)
    dataset = catalog.get_dataset("odre_eco2mix_cons_def")
    return (dataset,)


@app.cell
def _(dataset, pl):
    df = pl.read_parquet(dataset.bronze_path())
    return (df,)


@app.cell
def _(df):
    print(df.columns)
    print(df.height)
    return


@app.cell
def _(df):
    df_ = df.drop(["column_30"])
    return (df_,)


@app.cell
def _(df_):
    print(df_.columns)
    return


@app.cell
def _(pl):
    df_opendatasoft = pl.read_csv("data/landing/eco2mix-regional-cons-def.csv", separator=";").drop(
        ["Column 30"]
    )
    df_datagouv = pl.read_csv(
        "data/landing/from_datagouv_eco2mix-regional-cons-def.csv", separator=";"
    ).drop(["Column 30"])

    df_opendatasoft_sans_dateheure = df_opendatasoft.drop(["Date - Heure"])
    df_datagouv_sans_dateheure = df_datagouv.drop(["Date - Heure"])

    df_opendatasoft_sans_dateheure.equals(other=df_datagouv_sans_dateheure, null_equal=True)
    return df_datagouv, df_opendatasoft


@app.cell
def _(df_datagouv, df_opendatasoft, pl):
    df_opendatasoft_def = df_opendatasoft.filter(pl.col("Nature") == "Données définitives")
    df_datagouv_def = df_datagouv.filter(pl.col("Nature") == "Données définitives")
    return df_datagouv_def, df_opendatasoft_def


@app.cell
def _(df_datagouv_def):
    df_datagouv_def
    return


@app.cell
def _(df_opendatasoft_def):
    df_opendatasoft_def
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
