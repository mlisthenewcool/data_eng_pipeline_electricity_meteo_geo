import marimo

__generated_with = "0.18.4"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo

    import duckdb
    import polars as pl

    return duckdb, pl


@app.cell
def _(contours_iris_path, duckdb, pl):
    con = duckdb.connect()
    con.install_extension("spatial")
    con.load_extension("spatial")

    # Liste les couches disponibles
    layers = (
        con.execute(
            query="SELECT layers FROM st_read_meta(?)", parameters=[str(contours_iris_path)]
        )
        .pl()
        .explode("layers")
        .unnest("layers")
    )
    pl.Config.set_tbl_width_chars(width=300)
    pl.Config.set_tbl_rows(n=30)
    print(layers)

    #########################

    query = """
        SELECT *, ST_AsGeoJSON(geometrie)
        FROM st_read(?, layer='contours_iris')
    """
    iris = con.execute(query=query, parameters=[str(contours_iris_path)]).pl()
    print(iris.shape)
    print(iris.columns)
    print(iris.sort(by="code_iris"))

    print(iris.filter(pl.col("code_insee") == "01014"))
    return


if __name__ == "__main__":
    app.run()
