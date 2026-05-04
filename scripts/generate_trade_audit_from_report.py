from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.research.trade_audit import (  # noqa: E402
    load_backtest_report_payload,
    write_trade_audit_csv,
    write_trade_audit_report,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate a trade-level audit pack from a saved backtest report.")
    parser.add_argument("input_report", help="Path to a saved backtest report (.json or .json.gz).")
    parser.add_argument("--out-json", default=str(ROOT / "tests" / "golden_samples" / "trade_audit_report.json"))
    parser.add_argument("--out-csv", default=str(ROOT / "tests" / "golden_samples" / "trade_audit_report.csv"))
    parser.add_argument("--no-snapshots", action="store_true", help="Exclude full entry/exit snapshots from the JSON output.")
    args = parser.parse_args()

    payload = load_backtest_report_payload(args.input_report)
    out_json = write_trade_audit_report(args.out_json, payload, include_snapshots=(not args.no_snapshots))
    out_csv = write_trade_audit_csv(args.out_csv, payload)
    print(f"Wrote trade audit JSON: {out_json}")
    print(f"Wrote trade audit CSV: {out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
