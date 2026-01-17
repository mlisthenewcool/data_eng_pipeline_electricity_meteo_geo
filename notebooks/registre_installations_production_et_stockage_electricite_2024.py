import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import polars as pl

    return mo, pl


@app.cell
def _(pl):
    df = pl.read_parquet(
        "data/bronze/registre-national-installation-production-stockage-electricite-agrege-311224.parquet"
    )
    print(df.columns)
    print(df.shape)

    df
    return (df,)


@app.cell
def _(mo):
    mo.md(r"""
    * vérifier les puissances par rapport aux données publiées sur internet
    """)


@app.cell
def _(df, pl):
    df.select(
        [
            pl.col("puismaxinstallee").sum().round(decimals=2),
            pl.col("puismaxrac").sum().round(decimals=2),
        ]
    )


@app.cell
def _(df, pl):
    regions_hors_scope = ["Martinique", "Guadeloupe", "Guyane", "La Réunion"]
    df_sans_regions_hors_scope = df.filter(~pl.col("region").is_in(regions_hors_scope))

    df_sans_regions_hors_scope.select(
        [
            pl.col("puismaxinstallee").sum().round(decimals=2),
            pl.col("puismaxinstalleedischarge").sum().round(decimals=2),
            pl.col("puismaxrac").sum().round(decimals=2),
        ]
    )
    return (df_sans_regions_hors_scope,)


@app.cell
def _(df_sans_regions_hors_scope, pl):
    df_sans_regions_hors_scope.group_by(pl.col("filiere")).agg(
        pl.col("puismaxinstallee").sum()
    ).sort(by="puismaxinstallee", descending=True)


@app.cell
def _(df, pl):
    df.filter(pl.col("nominstallation") == "CYCOFOS TV2")


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
