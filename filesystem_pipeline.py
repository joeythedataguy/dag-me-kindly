"""dlt pipeline: load CSVs from URLs listed in import_csv.txt into MotherDuck raw_csv schema."""

import os
from pathlib import Path

import dlt
import duckdb
import pandas as pd


URLS_FILE = Path(__file__).parent / "import_csv.txt"


def _table_name_from_url(url: str) -> str:
    """Derive table name from URL filename: raw_customers.csv -> raw_customers_dlt."""
    stem = url.rstrip("/").split("/")[-1].removesuffix(".csv")
    return f"{stem}_dlt"


@dlt.source
def csv_from_urls(urls_file: Path = URLS_FILE):
    """Yields one dlt resource per URL in urls_file, each loading into its own table."""
    urls = [u.strip() for u in urls_file.read_text().splitlines() if u.strip()]

    for url in urls:
        table = _table_name_from_url(url)

        @dlt.resource(name=table, write_disposition="replace")
        def _load(u=url):
            yield pd.read_csv(u).to_dict("records")

        yield _load


def load_csvs() -> None:
    token = os.environ["motherduck_token"]
    conn = duckdb.connect(f"md:local_dev?motherduck_token={token}")

    pipeline = dlt.pipeline(
        pipeline_name="import_csv_pipeline",
        destination=dlt.destinations.duckdb(credentials=conn),
        dataset_name="raw_csv",
        dev_mode=False,
    )

    load_info = pipeline.run(csv_from_urls())
    print(load_info)
    print(pipeline.last_trace.last_normalize_info)


if __name__ == "__main__":
    load_csvs()
