from datetime import date
from decimal import Decimal

import pyarrow as pa

from src.ingest.nbp import SCHEMA, flatten

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
