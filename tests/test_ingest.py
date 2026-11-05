import random
from datetime import date, datetime
from decimal import Decimal

import pyarrow as pa

from src.ingest.gl_synth import ZERO, flag_duplicate_lines, inject_duplicates
from src.ingest.nbp import SCHEMA, flatten


# --- NBP rate parsing ---------------------------------------------------------

# Shape of the NBP API response, as returned by response.json(parse_float=Decimal)
SAMPLE_TABLES = [
    {
        "table": "A",
        "no": "001/A/NBP/2026",
        "effectiveDate": "2026-01-02",
        "rates": [
            {"currency": "euro", "code": "EUR", "mid": Decimal("4.2615")},
            {"currency": "dolar amerykański", "code": "USD", "mid": Decimal("3.6012")},
        ],
    },
    {
        "table": "A",
        "no": "002/A/NBP/2026",
        "effectiveDate": "2026-01-05",
        "rates": [
            {"currency": "euro", "code": "EUR", "mid": Decimal("4.2587")},
        ],
    },
]


def test_rate_parsing_returns_expected_columns():
    rows = flatten(SAMPLE_TABLES)

    assert len(rows) == 3
    for row in rows:
        assert list(row) == ["rate_date", "currency", "mid"]
        assert isinstance(row["rate_date"], date)
        assert isinstance(row["currency"], str)
        assert isinstance(row["mid"], Decimal)

    assert rows[0] == {
        "rate_date": date(2026, 1, 2),
        "currency": "EUR",
        "mid": Decimal("4.261500"),
    }

    # The rows must fit the Parquet schema that gets written to disk
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    assert table.column_names == ["rate_date", "currency", "mid"]


# --- GL duplicate lines -------------------------------------------------------

def make_line(journal_id: str, line_no: int) -> dict:
    return {
        "journal_id": journal_id,
        "line_no": line_no,
        "company": "Acme",
        "account": "1000",
        "currency": "PLN",
        "debit": Decimal("10.00") if line_no == 1 else ZERO,
        "credit": ZERO if line_no == 1 else Decimal("10.00"),
        "posting_date": date(2026, 1, 15),
        "created_at": datetime(2026, 1, 15, 9),
    }


def test_duplicate_lines_are_flagged():
    original = make_line("JE0000001", 1)
    rows = [
        original,
        make_line("JE0000001", 2),
        dict(original),             # repeat of JE0000001 / line 1
        make_line("JE0000002", 1),  # same line_no, different journal: not a duplicate
        dict(original),             # second repeat
    ]

    # First occurrence stays unflagged, every repeat is flagged
    assert flag_duplicate_lines(rows) == [False, False, True, False, True]


def test_injected_duplicates_are_all_flagged():
    rows = [make_line(f"JE{j:07d}", n) for j in range(1, 101) for n in (1, 2)]
    assert not any(flag_duplicate_lines(rows))

    combined, n_dupes = inject_duplicates(rows, random.Random(0))

    assert n_dupes == 2  # 1% of 200 lines
    assert sum(flag_duplicate_lines(combined)) == n_dupes
