import csv
import io
import os
import re
import zipfile
from datetime import datetime
from functools import lru_cache

import dlt
import duckdb
import requests
from dagster import AssetExecutionContext
from dagster_dlt import DagsterDltResource, dlt_assets

CATALOG_URL = "https://opendata.sukl.cz/?q=katalog/databaze-lecivych-pripravku-dlp"
ZIP_PATTERN = re.compile(r"https://opendata\.sukl\.cz/soubory/SOD\d+/DLP\d+\.zip")
CHECK_TABLE = "dim_dlp_lecivepripravky"

# Filenames found inside the DLP zip as of 2026-05-27. New files that show up in a
# future release are skipped with a warning rather than silently ignored.
KNOWN_FILES = [
    "dlp_atc.csv", "dlp_cesty.csv", "dlp_doping.csv", "dlp_dopinglp.csv",
    "dlp_formy.csv", "dlp_indikacniskupiny.csv", "dlp_jednotky.csv", "dlp_latky.csv",
    "dlp_lecivelatky.csv", "dlp_lecivepripravky.csv", "dlp_narvla.csv",
    "dlp_nazvydokumentu.csv", "dlp_obaly.csv", "dlp_organizace.csv",
    "dlp_pravnizakladreg.csv", "dlp_regproc.csv", "dlp_slozeni.csv",
    "dlp_slozenipriznak.csv", "dlp_soli.csv", "dlp_splp.csv", "dlp_stavyreg.csv",
    "dlp_synonyma.csv", "dlp_typlp.csv", "dlp_vpois.csv", "dlp_vydej.csv",
    "dlp_zavislost.csv", "dlp_zdroje.csv", "dlp_zeme.csv", "dlp_zruseneregistrace.csv",
    "dlp_platnost.csv",
]


def get_latest_zip_url() -> tuple[str, str]:
    response = requests.get(CATALOG_URL)
    response.raise_for_status()
    matches = ZIP_PATTERN.findall(response.text)
    if not matches:
        raise ValueError("Nenalezen ZIP soubor na strance DLP.")
    url = matches[0]
    filename = url.split("/")[-1]
    return url, filename


def already_loaded(zip_filename: str) -> bool:
    token = os.environ["motherduck_token"]
    try:
        with duckdb.connect(f"md:local_dev?motherduck_token={token}") as conn:
            result = conn.execute(
                f"SELECT COUNT(*) FROM raw_dim_dlp.{CHECK_TABLE} WHERE zdrojovy_soubor = ?",
                [zip_filename],
            ).fetchone()
        return result[0] > 0
    except Exception:
        return False


def datum_z_zipu(zip_filename: str) -> str | None:
    match = re.search(r"DLP(\d{8})\.zip", zip_filename, re.IGNORECASE)
    if match:
        return datetime.strptime(match.group(1), "%Y%m%d").date().isoformat()
    return None


def parse_csv(text: str, zip_filename: str, datum_aktualizace: str | None) -> list[dict]:
    reader = csv.DictReader(io.StringIO(text), delimiter=";")
    return [{"zdrojovy_soubor": zip_filename, "datum_aktualizace": datum_aktualizace, **row} for row in reader]


@lru_cache(maxsize=1)
def _fetch_zip_if_new() -> tuple[dict[str, bytes], str, str | None]:
    """Downloads the latest DLP zip once per pipeline run, skipping if already loaded.

    Cached so the 30 per-file resources below share a single download instead of
    each fetching the zip independently.
    """
    url, zip_filename = get_latest_zip_url()

    if already_loaded(zip_filename):
        print(f"Data ze souboru '{zip_filename}' uz jsou v databazi. Preskakuji.")
        return {}, zip_filename, None

    print(f"Stahuji ZIP: {url}")
    response = requests.get(url)
    response.raise_for_status()
    datum = datum_z_zipu(zip_filename)

    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        names = {n for n in zf.namelist() if n.lower().endswith(".csv")}
        unexpected = names - set(KNOWN_FILES)
        if unexpected:
            print(f"  [NOVE SOUBORY] nenalezeny v KNOWN_FILES: {sorted(unexpected)}")
        contents = {name: zf.read(name) for name in names}

    return contents, zip_filename, datum


def _make_dlp_resource(filename: str):
    table_name = "dim_" + filename.removesuffix(".csv")

    @dlt.resource(name=table_name, write_disposition="append")
    def _resource():
        contents, zip_filename, datum = _fetch_zip_if_new()
        data = contents.get(filename)
        if data is None:
            return
        text = data.decode("cp1250", errors="replace")
        yield from parse_csv(text, zip_filename, datum)

    return _resource


@dlt.source
def dlp_source():
    """Yields one resource per known CSV file in the latest SUKL DLP zip."""
    for filename in KNOWN_FILES:
        yield _make_dlp_resource(filename)


# Local pipeline used only for asset key/schema generation at definition load time.
# Never executed — all actual runs use _motherduck_pipeline() to get a live connection.
_schema_pipeline = dlt.pipeline(
    pipeline_name="dim_dlp_pipeline",
    destination=dlt.destinations.duckdb(
        credentials="/tmp/dag_me_kindly_dim_dlp_schema.duckdb"
    ),
    dataset_name="raw_dim_dlp",
)


def _motherduck_pipeline() -> dlt.Pipeline:
    """Creates a fresh pipeline with a live MotherDuck connection.

    Passing a real DuckDBPyConnection bypasses dlt's make_location() which would
    otherwise treat 'md:local_dev' as a relative filesystem path.
    """
    token = os.environ["motherduck_token"]
    conn = duckdb.connect(f"md:local_dev?motherduck_token={token}")
    return dlt.pipeline(
        pipeline_name="dim_dlp_pipeline",
        destination=dlt.destinations.duckdb(credentials=conn),
        dataset_name="raw_dim_dlp",
        dev_mode=False,
    )


@dlt_assets(
    dlt_source=dlp_source(),
    dlt_pipeline=_schema_pipeline,
    group_name="raw_dim_dlp",
)
def dim_dlp_assets(context: AssetExecutionContext):
    """Load the latest SUKL DLP catalog ZIP into MotherDuck raw_dim_dlp schema."""
    yield from DagsterDltResource().run(
        context=context,
        dlt_pipeline=_motherduck_pipeline(),
    )
