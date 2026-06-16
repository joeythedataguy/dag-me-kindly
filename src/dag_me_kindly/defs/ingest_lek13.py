import csv
import io
import os
import re
import unicodedata
import zipfile

import dlt
import duckdb
import requests
from dagster import AssetExecutionContext
from dagster_dlt import DagsterDltResource, dlt_assets

CATALOG_URL = "https://opendata.sukl.cz/?q=katalog/lek-13"
CSV_PATTERN = re.compile(r"https://opendata\.sukl\.cz/soubory/LEK13/LEK13_\d{4}/LEK13_\d+v\d+\.csv")
ZIP_PATTERN = re.compile(r"https://opendata\.sukl\.cz/soubory/LEK13/LEK13_\d{4}/LEK13_\d{4}\.zip")

COLUMN_ALIASES = {
    "drzitel_registracniho_rozhodnuti": "drzitel_registrace",
    "pocet_definovanych_dennich_davek_baleni": "pocet_ddd_baleni",
}

KNOWN_COLUMNS = {
    "zdrojovy_soubor", "obdobi", "typ_hlaseni", "atc7", "kod_sukl", "nazev_pripravku",
    "doplnek_nazvu", "drzitel_registrace", "zeme", "pocet_baleni",
    "nakupni_cena_bez_dph", "konecna_prodejni_cena_s_dph", "pocet_ddd_baleni",
    "zpusob_vydeje", "hrazeno",
}


def normalize_col(name: str) -> str:
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_name = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", "_", ascii_name.lower()).strip("_")


def map_col(normalized: str) -> str:
    return COLUMN_ALIASES.get(normalized, normalized)


def get_all_links():
    response = requests.get(CATALOG_URL)
    response.raise_for_status()
    csv_urls = list(dict.fromkeys(CSV_PATTERN.findall(response.text)))
    zip_urls = list(dict.fromkeys(ZIP_PATTERN.findall(response.text)))
    return csv_urls, zip_urls


def parse_csv(text: str, source_file: str, new_columns: set) -> list[dict]:
    reader = csv.DictReader(io.StringIO(text), delimiter=";")
    rows = []
    for row in reader:
        mapped = {"zdrojovy_soubor": source_file}
        for k, v in row.items():
            col = map_col(normalize_col(k))
            if col not in KNOWN_COLUMNS and col not in new_columns:
                new_columns.add(col)
                print(f"  [NOVY SLOUPEC] '{k}' -> '{col}'")
            mapped[col] = v
        rows.append(mapped)
    return rows


@dlt.source
def sukl_source():
    """Scrapes the SUKL LEK-13 catalog page and yields one resource over all CSV/ZIP files."""

    @dlt.resource(name="src_lek13", write_disposition="replace")
    def lek13_resource():
        csv_urls, zip_urls = get_all_links()
        new_columns: set = set()

        for url in csv_urls:
            print(f"Stahuji CSV: {url}")
            response = requests.get(url)
            response.encoding = "cp1250"
            yield from parse_csv(response.text, url.split("/")[-1], new_columns)

        for url in zip_urls:
            print(f"Stahuji ZIP: {url}")
            response = requests.get(url)
            with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                for name in zf.namelist():
                    if name.lower().endswith(".csv"):
                        with zf.open(name) as f:
                            text = f.read().decode("cp1250")
                            yield from parse_csv(text, name, new_columns)

        if new_columns:
            print("\n" + "=" * 60)
            print("POZOR: Nalezeny nove sloupce, ktere nejsou v KNOWN_COLUMNS:")
            for col in sorted(new_columns):
                print(f"  - {col}")
            print("Upravte lek13.py a spustte pipeline znovu.")
            print("=" * 60 + "\n")

    yield lek13_resource


# Local pipeline used only for asset key/schema generation at definition load time.
# Never executed — all actual runs use _motherduck_pipeline() to get a live connection.
_schema_pipeline = dlt.pipeline(
    pipeline_name="lek13_pipeline",
    destination=dlt.destinations.duckdb(
        credentials="/tmp/dag_me_kindly_lek13_schema.duckdb"
    ),
    dataset_name="raw_lek13",
)


def _motherduck_pipeline() -> dlt.Pipeline:
    """Creates a fresh pipeline with a live MotherDuck connection.

    Passing a real DuckDBPyConnection bypasses dlt's make_location() which would
    otherwise treat 'md:local_dev' as a relative filesystem path.
    """
    token = os.environ["motherduck_token"]
    conn = duckdb.connect(f"md:local_dev?motherduck_token={token}")
    return dlt.pipeline(
        pipeline_name="lek13_pipeline",
        destination=dlt.destinations.duckdb(credentials=conn),
        dataset_name="raw_lek13",
        dev_mode=False,
    )


@dlt_assets(
    dlt_source=sukl_source(),
    dlt_pipeline=_schema_pipeline,
    group_name="raw_lek13",
)
def lek13_assets(context: AssetExecutionContext):
    """Load SUKL LEK-13 catalog CSV/ZIP files into MotherDuck raw_lek13 schema."""
    yield from DagsterDltResource().run(
        context=context,
        dlt_pipeline=_motherduck_pipeline(),
    )
