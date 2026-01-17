import marimo

__generated_with = "0.18.4"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    import polars as pl

    from de_electricity_meteo.config.paths import DATA_BRONZE

    return DATA_BRONZE, pl


@app.cell
def _(DATA_BRONZE, pl):
    station_ids = ["01071001"]  # "01089001", "01014002"

    paths = {
        id: DATA_BRONZE / f"/meteo_france_climatologie/_station_{id}_*.csv" for id in station_ids
    }

    dfs = {
        id: pl.read_csv(
            paths[id],
            has_header=True,
            separator=";",
            schema_overrides={"DATE": pl.String, "POSTE": pl.String},
        )
        for id in station_ids
    }
    return dfs, station_ids


@app.cell
def _(dfs, station_ids):
    print(len(dfs[station_ids[0]].columns))
    return


@app.cell
def _(dfs, pl, station_ids):
    dfs_date_cleaned = {
        id: (
            dfs[id].with_columns(
                pl.col("DATE")
                .str.strip_chars()
                .add("00")
                .str.strptime(pl.Datetime, format="%Y%m%d%H%M")
            )
        )
        for id in station_ids
    }
    return (dfs_date_cleaned,)


@app.cell
def _(dfs_date_cleaned, station_ids):
    dfs_date_cleaned[station_ids[0]]
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
