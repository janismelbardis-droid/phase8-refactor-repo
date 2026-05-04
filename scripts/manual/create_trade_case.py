from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path


SUBDIRS = ("inputs", "charts", "exports", "notes", "presets")
TOKEN_PATTERN = re.compile(r"\{\{([A-Z_]+)\}\}")


def slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip().lower())
    cleaned = cleaned.strip("_")
    if not cleaned:
        raise ValueError("slug cannot be empty after normalization")
    return cleaned


def default_casebook_root() -> Path:
    script_path = Path(__file__).resolve()
    workspace_root = script_path.parents[4]
    return workspace_root / "research_exports" / "trade_entry_casebook"


def next_case_id(cases_dir: Path) -> str:
    max_id = 0
    for path in cases_dir.iterdir():
        if not path.is_dir():
            continue
        match = re.match(r"^(\d{4})_", path.name)
        if match:
            max_id = max(max_id, int(match.group(1)))
    return f"{max_id + 1:04d}"


def render_template(template_text: str, values: dict[str, str]) -> str:
    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        return values.get(key, match.group(0))

    return TOKEN_PATTERN.sub(replace, template_text)


def append_case_index(case_index_path: Path, row: dict[str, str]) -> None:
    fieldnames = [
        "case_id",
        "slug",
        "status",
        "direction",
        "symbol",
        "timeframe",
        "anchor_type",
        "anchor_time_utc",
        "source",
        "folder",
    ]

    needs_header = not case_index_path.exists() or case_index_path.stat().st_size == 0
    with case_index_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if needs_header:
            writer.writeheader()
        writer.writerow(row)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create the next numbered trade-entry research case."
    )
    parser.add_argument("slug", help="Short case slug, for example vidya_flip_anchor_long")
    parser.add_argument("--direction", default="unknown")
    parser.add_argument("--symbol", default="unknown")
    parser.add_argument("--timeframe", default="unknown")
    parser.add_argument("--status", default="collecting")
    parser.add_argument("--anchor-type", default="vidya_regime_flip")
    parser.add_argument("--source", default="manual case creation")
    parser.add_argument("--anchor-time-utc", default="")
    parser.add_argument("--casebook-root", type=Path, default=default_casebook_root())
    return parser


def main() -> int:
    args = build_parser().parse_args()

    casebook_root = args.casebook_root.resolve()
    cases_dir = casebook_root / "cases"
    template_path = casebook_root / "_TEMPLATE" / "case.md"
    case_index_path = casebook_root / "CASE_INDEX.csv"

    cases_dir.mkdir(parents=True, exist_ok=True)
    for subdir in SUBDIRS:
        (casebook_root / "_TEMPLATE" / subdir).mkdir(parents=True, exist_ok=True)

    slug = slugify(args.slug)
    case_id = next_case_id(cases_dir)
    case_folder_name = f"{case_id}_{slug}"
    case_folder = cases_dir / case_folder_name
    case_folder.mkdir(parents=True, exist_ok=False)

    for subdir in SUBDIRS:
        (case_folder / subdir).mkdir(parents=True, exist_ok=True)

    if template_path.exists():
        template_text = template_path.read_text(encoding="utf-8")
    else:
        template_text = "# Case {{CASE_ID}} - {{CASE_SLUG}}\n"

    rendered = render_template(
        template_text,
        {
            "CASE_ID": case_id,
            "CASE_SLUG": slug,
            "STATUS": args.status,
            "DIRECTION": args.direction,
            "SYMBOL": args.symbol,
            "TIMEFRAME": args.timeframe,
            "ANCHOR_TYPE": args.anchor_type,
            "SOURCE": args.source,
        },
    )
    (case_folder / "case.md").write_text(rendered, encoding="utf-8")

    append_case_index(
        case_index_path,
        {
            "case_id": case_id,
            "slug": slug,
            "status": args.status,
            "direction": args.direction,
            "symbol": args.symbol,
            "timeframe": args.timeframe,
            "anchor_type": args.anchor_type,
            "anchor_time_utc": args.anchor_time_utc,
            "source": args.source,
            "folder": f"cases/{case_folder_name}",
        },
    )

    print(f"Created case {case_id}: {case_folder}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
