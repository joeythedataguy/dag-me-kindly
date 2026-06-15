import os
from pathlib import Path

import dlt
import duckdb
import pandas as pd
from dagster import AssetExecutionContext
from dagster_dlt import DagsterDltResource, dlt_assets

_URLS_FILE = Path(__file__).parent / "import_csv.txt"


def _table_name_from_url(url: str) -> str:
    stem = url.rstrip("/").split("/")[-1].removesuffix(".csv")
    return f"{stem}_dlt"


@dlt.source
def csv_from_urls(urls_file: Path = _URLS_FILE):
    """Yields one dlt resource per URL in urls_file."""
    urls = [u.strip() for u in urls_file.read_text().splitlines() if u.strip()]
    for url in urls:
        table = _table_name_from_url(url)

        @dlt.resource(name=table, write_disposition="replace")
        def _load(u=url):
            yield pd.read_csv(u).to_dict("records")

        yield _load


# Local pipeline used only for asset key/schema generation at definition load time.
# Never executed — all actual runs use _motherduck_pipeline() to get a live connection.
_schema_pipeline = dlt.pipeline(
    pipeline_name="import_csv_pipeline",
    destination=dlt.destinations.duckdb(
        credentials="/tmp/dag_me_kindly_import_csv_schema.duckdb"
    ),
    dataset_name="raw_csv",
)


def _motherduck_pipeline() -> dlt.Pipeline:
    """Creates a fresh pipeline with a live MotherDuck connection.

    Passing a real DuckDBPyConnection bypasses dlt's make_location() which would
    otherwise treat 'md:local_dev' as a relative filesystem path.
    """
    token = os.environ["motherduck_token"]
    conn = duckdb.connect(f"md:local_dev?motherduck_token={token}")
    return dlt.pipeline(
        pipeline_name="import_csv_pipeline",
        destination=dlt.destinations.duckdb(credentials=conn),
        dataset_name="raw_csv",
        dev_mode=False,
    )


@dlt_assets(
    dlt_source=csv_from_urls(),
    dlt_pipeline=_schema_pipeline,
    group_name="raw_csv",
)
def import_csv_assets(context: AssetExecutionContext):
    """Load CSVs listed in import_csv.txt into MotherDuck raw_csv schema."""
    yield from DagsterDltResource().run(
        context=context,
        dlt_pipeline=_motherduck_pipeline(),
    )
