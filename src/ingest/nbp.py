"""Fetch NBP table A mid exchange rates and save them as Parquet.

Calls the NBP Web API in chunks of at most 93 days, flattens the response to
one row per date x currency, and writes:

  $NBP_RATES_FILE (default data/raw/nbp_rates_a.parquet)
  columns: rate_date, currency, mid

Run as part of the ingest package:
  uv run python -m src.ingest --start 2025-01-01 --end 2025-12-31
"""

import time
from collections.abc import Iterator
from datetime import date, timedelta
from decimal import Decimal
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from src.config import Settings

# --- Configuration ----------------------------------------------------------

FIRST_AVAILABLE_DATE = date(2002, 1, 2)   # earliest data NBP publishes
DEFAULT_START_DATE = date(2025, 1, 1)
MAX_DAYS_PER_REQUEST = 93                 # API limit per query

TIMEOUT_SECONDS = 30
MAX_RETRIES = 5
BACKOFF_SECONDS = 1                       # 1, 2, 4, 8 ... seconds between attempts
RETRY_STATUSES = {429, 500, 502, 503, 504}

SCHEMA = pa.schema([
    ("rate_date", pa.date32()),
    ("currency", pa.string()),
    ("mid", pa.decimal128(18, 6)),
])

MID_PRECISION = Decimal("0.000001")


# --- API access -------------------------------------------------------------------

def date_chunks(start: date, end: date, max_days: int = MAX_DAYS_PER_REQUEST) -> Iterator[tuple[date, date]]:
    """Split [start, end] into consecutive ranges of at most `max_days` days (inclusive)."""
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=max_days - 1), end)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


def fetch_chunk(session: requests.Session, base_url: str, start: date, end: date) -> list[dict]:
    """Return the list of tables for [start, end], or [] if the API has no data (404).

    Retries with exponential backoff on network errors and transient HTTP errors.
    Any other HTTP error (e.g. 400 Bad Request) is raised immediately.
    """
    url = f"{base_url}/{start}/{end}/"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, params={"format": "json"}, timeout=TIMEOUT_SECONDS)
        except (requests.ConnectionError, requests.Timeout) as exc:
            error = f"{type(exc).__name__}: {exc}"
        else:
            if response.status_code == 404:
                return []                      # no tables published in this range
            if response.status_code not in RETRY_STATUSES:
                response.raise_for_status()
                return response.json(parse_float=Decimal)   # keep rates exact
            error = f"HTTP {response.status_code}"

        if attempt == MAX_RETRIES:
            raise RuntimeError(f"Giving up on {url} after {MAX_RETRIES} attempts ({error})")
        wait = BACKOFF_SECONDS * 2 ** (attempt - 1)
        print(f"    attempt {attempt} failed ({error}), retrying in {wait}s")
        time.sleep(wait)

    raise AssertionError("unreachable")


def flatten(tables: list[dict]) -> list[dict]:
    """One row per (effective date, currency)."""
    return [
        {
            "rate_date": date.fromisoformat(table["effectiveDate"]),
            "currency": rate["code"],
            "mid": rate["mid"].quantize(MID_PRECISION),
        }
        for table in tables
        for rate in table["rates"]
    ]


# --- Main ----------------------------------------------------------------------

def run(start_date: date = DEFAULT_START_DATE, end_date: date | None = None,
        settings: Settings | None = None) -> None:
    settings = settings or Settings.from_env()
    out_file = settings.nbp_rates_file
    if end_date is None:
        end_date = date.today() - timedelta(days=1)
    if start_date < FIRST_AVAILABLE_DATE:
        raise ValueError(f"start_date cannot be earlier than {FIRST_AVAILABLE_DATE}")
    if end_date > date.today():
        print(f"Note: end date {end_date} is in the future, using today instead.")
        end_date = date.today()
    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date")

    print(f"Fetching NBP table A from {start_date} to {end_date}")

    rows: list[dict] = []
    with requests.Session() as session:
        session.headers["Accept"] = "application/json"
        for start, end in date_chunks(start_date, end_date):
            tables = fetch_chunk(session, settings.nbp_base_url, start, end)
            if not tables:
                print(f"  {start} -> {end}: no data (404)")
                continue
            chunk_rows = flatten(tables)
            rows.extend(chunk_rows)
            print(f"  {start} -> {end}: {len(tables)} tables, {len(chunk_rows):,} rows")

    rows.sort(key=lambda r: (r["rate_date"], r["currency"]))

    out_file.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), out_file)

    n_dates = len({r["rate_date"] for r in rows})
    n_currencies = len({r["currency"] for r in rows})
    print(f"Saved {len(rows):,} rows ({n_dates} dates x up to {n_currencies} currencies) -> {out_file}")
