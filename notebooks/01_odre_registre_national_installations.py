import marimo

__generated_with = "0.18.4"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import polars as pl

    return mo, pl


@app.cell
def _():
    from de_electricity_meteo.config.paths import (
        ODRE_REGISTRE_NATIONAL_INSTALLATIONS_BRONZE,
    )

    return (ODRE_REGISTRE_NATIONAL_INSTALLATIONS_BRONZE,)


@app.cell
def _(ODRE_REGISTRE_NATIONAL_INSTALLATIONS_BRONZE, pl):
    df = pl.scan_parquet(source=ODRE_REGISTRE_NATIONAL_INSTALLATIONS_BRONZE).collect()
    return (df,)


@app.cell
def _(df, mo):
    mo.ui.dataframe(df)
    return


@app.cell
def _(df, pl):
    pk_cols = ["idpeps", "codeeicresourceobject"]

    def check_duplicated_on_pk(cols: list[str]):
        for col in cols:
            print(
                f"duplicated rows on '{col}':",
                df.filter(pl.col(col).is_not_null() & pl.col(col).is_duplicated()).shape[0],
            )

    check_duplicated_on_pk(cols=pk_cols)
    return


@app.cell
def _():
    return


@app.cell
def _(df, pl):
    # check datederaccordement
    df.filter(pl.col("datederaccordement").is_not_null())

    # datemiseenservice      2010-02-26
    # datedebutversion       2022-09-21
    # datemiseenservice_date 2010-02-26
    return


@app.cell
def _(df, pl):
    # check datemiseenservice <> datedebutversion
    df.filter(
        pl.col("datedebutversion").is_not_null()
        & pl.col("datemiseenservice").ne_missing(other=pl.col("datedebutversion"))
    )
    return


@app.cell
def _(df, pl):
    # todo: datemiseenservice_date == 1897-03-21 ???

    df.filter(pl.col("datemiseenservice_date") < pl.date(1905, 1, 1))
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Geography
    """)
    return


@app.cell
def _(df, pl):
    geography_cols = ["codeiris", "codeinseecommune", "codeepci", "codedepartement", "coderegion"]

    print(df.select(pl.col(geography_cols).null_count()))

    # inexploitable
    df.filter(pl.all_horizontal(pl.col(geography_cols).is_null()))
    return


@app.cell
def _(df, mo, pl):
    # probably inexploitable too
    mo.ui.dataframe(
        df.filter(pl.all_horizontal(pl.col(["codeiris", "codeinseecommune", "codeepci"]).is_null()))
    )

    # seulement des installations agrégées ? OUI
    df.filter(
        pl.all_horizontal(pl.col(["codeiris", "codeinseecommune", "codeepci"]).is_null())
    ).select(pl.col("nominstallation").unique())
    return


@app.cell
def _(df, pl):
    # probably inexploitable too, 1306 rows
    df.filter(pl.all_horizontal(pl.col(["codeiris", "codeinseecommune"]).is_null()))
    return


@app.cell
def _(df, pl):
    _df = df.group_by(pl.col(["codeiris", "codeinseecommune"])).agg(
        pl.col("epci").n_unique().alias("n_unique_epci")
    )
    _df.filter(pl.col("n_unique_epci") > 1)
    return


@app.cell
def _(df, pl):
    print("df (shape)   => ", df.shape)
    print(f"df (maxpuis) => {df.select(pl.col('maxpuis').sum().round(2)).item():_}")
    print()

    _df_with_codeiris_at_best = df.filter(pl.col("codeiris").is_not_null())
    print("_df_with_codeiris_at_best (shape)   => ", _df_with_codeiris_at_best.shape)
    print(
        f"_df_with_codeiris_at_best (maxpuis) => {_df_with_codeiris_at_best.select(pl.col('maxpuis').sum().round(2)).item():_}"
    )
    print()

    _df_with_codeinseecommune_at_best = df.filter(
        pl.col("codeiris").is_null() & pl.col("codeinseecommune").is_not_null()
    )
    print(
        "_df_with_codeinseecommune_at_best (shape)   => ", _df_with_codeinseecommune_at_best.shape
    )
    print(
        f"_df_with_codeinseecommune_at_best (maxpuis) => {_df_with_codeinseecommune_at_best.select(pl.col('maxpuis').sum().round(2)).item():_}"
    )
    print()

    _df_with_codeepci_at_best = df.filter(
        pl.col("codeiris").is_null()
        & pl.col("codeinseecommune").is_null()
        & pl.col("codeepci").is_not_null()
    )
    print("_df_with_codeepci_at_best (shape)   => ", _df_with_codeepci_at_best.shape)
    print(
        f"_df_with_codeepci_at_best (maxpuis) => {_df_with_codeepci_at_best.select(pl.col('maxpuis').sum().round(2)).item():_}"
    )
    print()

    _df_with_codedepartement_at_best = df.filter(
        pl.all_horizontal(pl.col(["codeiris", "codeinseecommune", "codeepci"]).is_null())
        & pl.col("codedepartement").is_not_null()
    )
    print("_df_with_codedepartement_at_best (shape)   => ", _df_with_codedepartement_at_best.shape)
    print(
        f"_df_with_codedepartement_at_best (maxpuis) => {_df_with_codedepartement_at_best.select(pl.col('maxpuis').sum().round(2)).item():_}"
    )
    print()

    _df_with_coderegion_at_best = df.filter(
        pl.all_horizontal(
            pl.col(["codeiris", "codeinseecommune", "codeepci", "codedepartement"]).is_null()
        )
        & pl.col("coderegion").is_not_null()
    )
    print("_df_with_coderegion_at_best (shape)   => ", _df_with_coderegion_at_best.shape)
    print(
        f"_df_with_coderegion_at_best (maxpuis) => {_df_with_coderegion_at_best.select(pl.col('maxpuis').sum().round(2)).item():_}"
    )
    print()

    _df_with_no_geographical_data = df.filter(
        pl.all_horizontal(
            pl.col(
                ["codeiris", "codeinseecommune", "codeepci", "codedepartement", "coderegion"]
            ).is_null()
        )
    )
    print("_df_with_no_geographical_data (shape)   => ", _df_with_no_geographical_data.shape)
    print(
        f"_df_with_no_geographical_data (maxpuis) => {_df_with_no_geographical_data.select(pl.col('maxpuis').sum().round(2)).item():_}"
    )
    print()
    return


@app.cell
def _():
    return


@app.cell
def _(df, pl):
    df.filter(pl.col("region") == "Corse")
    return


@app.cell
def _(df, pl):
    # should be 0, coherent
    print(df.filter(pl.col("codeiris").is_not_null() & pl.col("codeinseecommune").is_null()).shape)
    return


@app.cell
def _(df, pl):
    df.filter(pl.col("codeepci").is_null())
    return


@app.cell
def _(df, pl):
    df.filter(pl.col("codeinseecommune").is_null() & pl.col("codeepci").is_not_null())
    return


@app.cell
def _(mo):
    mo.md(r"""
    -> check power values to select the best one(s)s
    """)
    return


@app.cell
def _(df, pl):
    df.select(pl.col("puismaxinstallee")).to_series().equals(
        other=df.select(pl.col("maxpuis")).to_series()
    )
    return


@app.cell
def _(df, pl):
    df.filter(pl.col("puismaxinstallee").ne_missing(other=pl.col("maxpuis")))
    return


@app.cell
def _(df, pl):
    for col in ["puismaxinstallee", "maxpuis"]:
        print(col, df.select(pl.col(col).sum()).item())
    return


@app.cell
def _(df, pl):
    df.select(pl.selectors.contains("puis").sum().round(decimals=3))
    return


@app.cell
def _(df, pl):
    df.select(pl.selectors.contains("energie").sum().round(decimals=3))
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
