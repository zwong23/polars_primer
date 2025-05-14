import polars as pl
import time

# Repeat N times and average
N = 100000

eager_total = 0
lazy_total = 0

for _ in range(N):
    df = pl.DataFrame({
        "a": list(range(500)),
        "b": list(range(500))
    })

    start = time.perf_counter()
    _ = (
        df
        .filter(pl.col("a") > 500)
        # .with_columns((pl.col("b")).alias("b_double"))
        # .group_by("a")
        # .agg(pl.col("b").mean())
    )
    eager_total += time.perf_counter() - start

    start = time.perf_counter()
    _ = (
        df.lazy()
        .filter(pl.col("a") > 500)
        # .with_columns((pl.col("b")).alias("b_double"))
        # .group_by("a")
        # .agg(pl.col("b").mean())
        .collect()
    )
    lazy_total += time.perf_counter() - start

print(f"Average eager time: {eager_total / N:.6f} seconds")
print(f"Average lazy time:  {lazy_total / N:.6f} seconds")
