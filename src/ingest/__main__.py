"""Run the full ingest: NBP exchange rates + synthetic GL journal lines.

Run with:
  uv run python -m src.ingest --start 2026-01-01 --end 2026-10-31
  uv run --env-file .env python -m src.ingest --start 2026-01-01 --end 2026-10-31

Output locations are configured via environment variables (see src/config.py).
"""

import argparse
from datetime import date

from src.config import Settings

from . import gl_synth, nbp


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m src.ingest",
        description="Fetch NBP table A rates and generate synthetic GL journal lines.",
    )
    parser.add_argument("--start", type=date.fromisoformat, required=True,
                        help="first date, YYYY-MM-DD")
    parser.add_argument("--end", type=date.fromisoformat, required=True,
                        help="last date, YYYY-MM-DD")
    args = parser.parse_args()

    if args.end < args.start:
        parser.error("--end must be on or after --start")
    if args.start < nbp.FIRST_AVAILABLE_DATE:
        parser.error(f"--start cannot be earlier than {nbp.FIRST_AVAILABLE_DATE}")
    return args


def main() -> None:
    args = parse_args()
    settings = Settings.from_env()

    print("=== NBP exchange rates ===")
    nbp.run(args.start, args.end, settings)

    print("\n=== Synthetic GL journal lines ===")
    gl_synth.run(args.start, args.end, settings)


if __name__ == "__main__":
    main()
