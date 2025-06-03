import polars as pl

def create_polars_df():
    pl.Config.set_streaming_chunk_size(4_000_000)
    return (
        pl.scan_csv(
            "data/measurements.txt",
            separator=";",
            has_header=False,
            schema={"station": pl.String, "measure": pl.Float64}
        )
        .group_by("station")
        .agg(
            max=pl.col("measure").max(),
            min=pl.col("measure").min(),
            mean=pl.col("measure").mean()
        )
        .sort("station")
        .collect()  # sem argumento
    )

if __name__ == "__main__":
    import time

    start_time = time.time()
    df = create_polars_df()
    print(df)
    print(f"Polars Took: {time.time() - start_time:.2f} sec")
