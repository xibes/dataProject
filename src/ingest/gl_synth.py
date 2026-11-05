"""Generate synthetic GL journal lines with deliberately injected data-quality issues.

Output:
  $GL_JOURNALS_FILE (default data/raw/journal_lines.parquet)
  ~100k journal lines, plus injected duplicates

Run as part of the ingest package:
  uv run python -m src.ingest --start 2026-01-01 --end 2026-06-30
"""

import random
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker

from src.config import Settings

# --- Configuration ----------------------------------------------------------

SEED = 42
TARGET_LINES = 100_000
DEFAULT_START_DATE = date(2025, 1, 1)
DEFAULT_END_DATE = date(2025, 12, 31)

DUPLICATE_RATE = 0.01      # ~1% of lines get duplicated
N_LATE_JOURNALS = 12       # journals entered long after their period closed
N_INVALID_CURRENCY = 8     # lines with a bad currency code

VALID_CURRENCIES = ["PLN", "EUR", "USD", "GBP", "CHF"]
INVALID_CURRENCIES = ["EURO", "US$", "ZZZ", "pln", "PL", ""]

ACCOUNTS = [
    "1000",  # Cash
    "1100",  # Accounts receivable
    "1200",  # Inventory
    "1500",  # Fixed assets
    "2000",  # Accounts payable
    "2100",  # Accrued liabilities
    "2200",  # VAT payable
    "3000",  # Equity
    "4000",  # Revenue
    "4100",  # Other income
    "5000",  # Cost of goods sold
    "6000",  # Salaries
    "6100",  # Rent
    "6200",  # Utilities
    "6300",  # Travel
    "7000",  # Depreciation
]

SCHEMA = pa.schema([
    ("journal_id", pa.string()),
    ("line_no", pa.int32()),
    ("company", pa.string()),
    ("account", pa.string()),
    ("currency", pa.string()),
    ("debit", pa.decimal128(18, 2)),
    ("credit", pa.decimal128(18, 2)),
    ("posting_date", pa.date32()),
    ("created_at", pa.timestamp("s")),
])

ZERO = Decimal("0.00")


# --- Helpers -----------------------------------------------------------------

def cents_to_decimal(cents: int) -> Decimal:
    """12345 -> Decimal('123.45'). Working in integer cents keeps sums exact."""
    return Decimal(cents).scaleb(-2)


def split_amount(total: int, parts: int, rng: random.Random) -> list[int]:
    """Split `total` cents into `parts` positive integers that sum exactly to it."""
    cuts = sorted(rng.sample(range(1, total), parts - 1))
    bounds = [0, *cuts, total]
    return [b - a for a, b in zip(bounds, bounds[1:])]


def lines_by_journal(rows: list[dict]) -> dict[str, list[int]]:
    groups = defaultdict(list)
    for i, row in enumerate(rows):
        groups[row["journal_id"]].append(i)
    return groups


def unbalanced_journals(rows: list[dict]) -> list[str]:
    totals = defaultdict(Decimal)
    for row in rows:
        totals[row["journal_id"]] += row["debit"] - row["credit"]
    return [j for j, diff in totals.items() if diff != 0]


def flag_duplicate_lines(rows: list[dict]) -> list[bool]:
    """Flag each line whose (journal_id, line_no) already appeared earlier.

    The first occurrence is kept as the original (False); every repeat is True.
    """
    seen: set[tuple[str, int]] = set()
    flags = []
    for row in rows:
        key = (row["journal_id"], row["line_no"])
        flags.append(key in seen)
        seen.add(key)
    return flags


def write_parquet(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    pq.write_table(table, path)


# --- Clean data generation ----------------------------------------------------

def make_journal(journal_no: int, fake: Faker, rng: random.Random,
                 companies: list[tuple[str, str]],
                 start_date: date, end_date: date) -> list[dict]:
    company, home_ccy = rng.choice(companies)
    if rng.random() < 0.85:
        currency = home_ccy
    else:
        currency = rng.choice([c for c in VALID_CURRENCIES if c != home_ccy])

    posting_date = fake.date_between(start_date=start_date, end_date=end_date)
    # Normally entered on the posting date or up to 2 days later, in office hours
    created_at = datetime.combine(posting_date, time(8)) + timedelta(
        days=rng.randint(0, 2), seconds=rng.randint(0, 10 * 3600)
    )

    n_lines = rng.randint(2, 8)
    n_debit = rng.randint(1, n_lines - 1)
    debits = [max(100, int(rng.lognormvariate(11, 1.2))) for _ in range(n_debit)]
    credits = split_amount(sum(debits), n_lines - n_debit, rng)

    sides = [("D", c) for c in debits] + [("C", c) for c in credits]
    rng.shuffle(sides)
    accounts = rng.sample(ACCOUNTS, n_lines)

    journal_id = f"JE{journal_no:07d}"
    rows = []
    for line_no, ((side, cents), account) in enumerate(zip(sides, accounts), start=1):
        amount = cents_to_decimal(cents)
        rows.append({
            "journal_id": journal_id,
            "line_no": line_no,
            "company": company,
            "account": account,
            "currency": currency,
            "debit": amount if side == "D" else ZERO,
            "credit": amount if side == "C" else ZERO,
            "posting_date": posting_date,
            "created_at": created_at,
        })
    return rows


# --- Problem injection -------------------------------------------------------

def inject_late_postings(rows: list[dict], rng: random.Random) -> int:
    """Re-stamp whole journals as entered 35-120 days after their period closed.
    The journals still balance; only the timing is wrong."""
    groups = lines_by_journal(rows)
    for journal_id in rng.sample(sorted(groups), N_LATE_JOURNALS):
        idxs = groups[journal_id]
        posting_date = rows[idxs[0]]["posting_date"]
        period_close = (posting_date.replace(day=28) + timedelta(days=4)).replace(day=1)
        late_at = datetime.combine(period_close, time(9)) + timedelta(
            days=rng.randint(35, 120), seconds=rng.randint(0, 8 * 3600)
        )
        for i in idxs:
            rows[i]["created_at"] = late_at
    return N_LATE_JOURNALS


def inject_invalid_currency(rows: list[dict], rng: random.Random) -> int:
    for i in rng.sample(range(len(rows)), N_INVALID_CURRENCY):
        rows[i]["currency"] = rng.choice(INVALID_CURRENCIES)
    return N_INVALID_CURRENCY


def inject_duplicates(rows: list[dict], rng: random.Random) -> tuple[list[dict], int]:
    """Copy ~1% of lines verbatim, as if part of a batch had been loaded twice."""
    n = round(len(rows) * DUPLICATE_RATE)
    dupes = [dict(rows[i]) for i in rng.sample(range(len(rows)), n)]
    combined = rows + dupes
    combined.sort(key=lambda r: (r["journal_id"], r["line_no"]))
    return combined, n


# --- Main ----------------------------------------------------------------------

def run(start_date: date = DEFAULT_START_DATE, end_date: date = DEFAULT_END_DATE,
        settings: Settings | None = None) -> None:
    settings = settings or Settings.from_env()
    out_file = settings.gl_journals_file
    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date")

    Faker.seed(SEED)
    fake = Faker()
    rng = random.Random(SEED)

    companies = [(fake.company(), ccy) for ccy in ["PLN", "PLN", "EUR", "EUR", "USD", "GBP"]]

    rows: list[dict] = []
    journal_no = 0
    while len(rows) < TARGET_LINES:
        journal_no += 1
        rows.extend(make_journal(journal_no, fake, rng, companies,
                                 start_date, end_date))

    unbalanced = unbalanced_journals(rows)
    assert not unbalanced, f"Generator bug, unbalanced journals: {unbalanced[:5]}"

    n_late = inject_late_postings(rows, rng)
    n_invalid = inject_invalid_currency(rows, rng)
    rows, n_dupes = inject_duplicates(rows, rng)  # last, so copies match final rows
    n_flagged = sum(flag_duplicate_lines(rows))
    assert n_flagged == n_dupes, f"Expected {n_dupes} duplicates, flagged {n_flagged}"

    write_parquet(out_file, rows)

    print(f"{journal_no:,} journals, {len(rows):,} lines, "
          f"{start_date} to {end_date} -> {out_file}")
    print(f"  late_posting journals: {n_late}")
    print(f"  invalid_currency lines: {n_invalid}")
    print(f"  duplicate_line lines: {n_dupes}")
    print(f"  journals now unbalanced (due to duplicates): {len(unbalanced_journals(rows))}")
