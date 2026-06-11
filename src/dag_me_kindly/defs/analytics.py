import os

import dagster as dg
import duckdb
import pandas as pd


def _connect() -> duckdb.DuckDBPyConnection:
    token = os.environ["motherduck_token"]
    return duckdb.connect(f"md:local_dev?motherduck_token={token}")


@dg.asset(
    deps=["orders"],
    group_name="analytics",
    kinds={"duckdb", "python"},
)
def orders_imputed(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Orders with NULL payment amounts filled using the per-status group average."""
    conn = _connect()

    df: pd.DataFrame = conn.execute("SELECT * FROM orders").df()

    nulls_before = int(df["amount"].isna().sum())

    df["amount"] = df.groupby("status")["amount"].transform(
        lambda x: x.fillna(x.mean())
    )

    conn.execute(
        "CREATE OR REPLACE TABLE orders_imputed AS SELECT * FROM df"
    )
    conn.close()

    context.log.info(f"Filled {nulls_before} NULL amounts across {len(df)} orders")

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(len(df)),
            "nulls_filled": dg.MetadataValue.int(nulls_before),
        }
    )


@dg.asset(
    deps=["customers"],
    group_name="analytics",
    kinds={"duckdb", "python"},
)
def customers_imputed(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Customers with NULL lifetime values filled using the overall average."""
    conn = _connect()

    df: pd.DataFrame = conn.execute("SELECT * FROM customers").df()

    nulls_before = int(df["customer_lifetime_value"].isna().sum())
    avg_clv = df["customer_lifetime_value"].mean()

    df["customer_lifetime_value"] = df["customer_lifetime_value"].fillna(avg_clv)

    conn.execute(
        "CREATE OR REPLACE TABLE customers_imputed AS SELECT * FROM df"
    )
    conn.close()

    context.log.info(
        f"Filled {nulls_before} NULL CLV values with mean={avg_clv:.2f}"
    )

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(len(df)),
            "nulls_filled": dg.MetadataValue.int(nulls_before),
            "imputed_mean": dg.MetadataValue.float(round(avg_clv, 2)),
        }
    )
