from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.data_binance import _exact_1m_window_paths, _parse_kline_cache_name


@dataclass
class MoveAction:
    source: str
    target: str
    action: str


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit and organize local candle/indicator cache libraries.")
    parser.add_argument("--cache-dir", type=Path, default=REPO_ROOT / "data_cache")
    parser.add_argument("--apply", action="store_true", help="Apply migrations and safe cleanup actions.")
    return parser


def _utc_now() -> str:
    return pd.Timestamp.now(tz="UTC").isoformat()


def _safe_size(path: Path) -> int:
    try:
        return int(path.stat().st_size)
    except OSError:
        return 0


def _walk_files(path: Path) -> Iterable[Path]:
    if not path.exists():
        return []
    return (p for p in path.rglob("*") if p.is_file())


def _walk_stats(path: Path) -> Dict[str, Any]:
    file_count = 0
    total_bytes = 0
    for fp in _walk_files(path):
        file_count += 1
        total_bytes += _safe_size(fp)
    return {"files": file_count, "bytes": total_bytes}


def _bytes_to_gb(value: int) -> float:
    return round(float(value) / (1024.0 ** 3), 3)


def _load_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8") as fh:
            payload = json.load(fh)
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def _day_from_name(name: str, suffix: str) -> Optional[str]:
    stem = name[: -len(suffix)]
    if "_" not in stem:
        return None
    return stem.rsplit("_", 1)[-1]


def _scan_root_window_klines(cache_dir: Path) -> List[Path]:
    out: List[Path] = []
    for path in cache_dir.iterdir():
        if not path.is_file():
            continue
        if _parse_kline_cache_name(str(path)) is None:
            continue
        out.append(path)
    return sorted(out, key=lambda item: item.name)


def _migrate_root_window_klines(cache_dir: Path, *, apply: bool) -> Dict[str, Any]:
    conflict_dir = cache_dir / "_quarantine" / "legacy_root_window_klines"
    actions: List[MoveAction] = []
    moved = 0
    duplicates_removed = 0
    conflicts = 0

    for src in _scan_root_window_klines(cache_dir):
        meta = _parse_kline_cache_name(str(src))
        if meta is None:
            continue
        pq_dst, csv_dst = _exact_1m_window_paths(
            str(cache_dir),
            str(meta["symbol"]),
            pd.to_datetime(meta["start_utc"], utc=True),
            pd.to_datetime(meta["end_utc"], utc=True),
            str(meta["price_source"]),
        )
        dst = Path(pq_dst if str(meta.get("ext")) == "parquet" else csv_dst)
        if dst.exists():
            if _safe_size(dst) == _safe_size(src):
                if apply:
                    src.unlink()
                duplicates_removed += 1
                actions.append(MoveAction(str(src), str(dst), "duplicate_removed"))
                continue
            conflict_target = conflict_dir / src.name
            if apply:
                conflict_target.parent.mkdir(parents=True, exist_ok=True)
                os.replace(str(src), str(conflict_target))
            conflicts += 1
            actions.append(MoveAction(str(src), str(conflict_target), "conflict_quarantined"))
            continue
        if apply:
            dst.parent.mkdir(parents=True, exist_ok=True)
            os.replace(str(src), str(dst))
        moved += 1
        actions.append(MoveAction(str(src), str(dst), "migrated"))

    return {
        "moved": moved,
        "duplicates_removed": duplicates_removed,
        "conflicts": conflicts,
        "actions": [action.__dict__ for action in actions],
    }


def _prune_empty_indicator_profiles(cache_dir: Path, *, apply: bool) -> Dict[str, Any]:
    root = cache_dir / "indicator_store"
    removed: List[str] = []
    kept: List[str] = []
    if not root.exists():
        return {"removed": removed, "kept": kept}
    for symbol_dir in sorted(root.iterdir(), key=lambda item: item.name):
        if not symbol_dir.is_dir():
            continue
        for profile_dir in sorted(symbol_dir.iterdir(), key=lambda item: item.name):
            if not profile_dir.is_dir():
                continue
            has_entries = any(profile_dir.iterdir())
            if has_entries:
                kept.append(str(profile_dir))
                continue
            if apply:
                profile_dir.rmdir()
            removed.append(str(profile_dir))
    return {"removed": removed, "kept_count": len(kept)}


def _ohlcv_store_summary(cache_dir: Path) -> Dict[str, Any]:
    root = cache_dir / "ohlcv_store"
    stores: List[Dict[str, Any]] = []
    for symbol_dir in sorted(root.iterdir(), key=lambda item: item.name) if root.exists() else []:
        if not symbol_dir.is_dir():
            continue
        for price_dir in sorted(symbol_dir.iterdir(), key=lambda item: item.name):
            if not price_dir.is_dir():
                continue
            for tf_dir in sorted(price_dir.iterdir(), key=lambda item: item.name):
                if not tf_dir.is_dir():
                    continue
                files = sorted(tf_dir.glob("*.parquet"))
                first_day = files[0].stem.rsplit("_", 1)[-1] if files else None
                last_day = files[-1].stem.rsplit("_", 1)[-1] if files else None
                stores.append(
                    {
                        "symbol": symbol_dir.name,
                        "price_source": price_dir.name,
                        "timeframe": tf_dir.name,
                        "days": len(files),
                        "first_day": first_day,
                        "last_day": last_day,
                        "bytes": sum(_safe_size(path) for path in files),
                    }
                )
    return {"stores": stores, "totals": _walk_stats(root)}


def _indicator_profile_summary(cache_dir: Path, ohlcv_start_day: Optional[str]) -> Dict[str, Any]:
    root = cache_dir / "indicator_store"
    profiles: List[Dict[str, Any]] = []
    for symbol_dir in sorted(root.iterdir(), key=lambda item: item.name) if root.exists() else []:
        if not symbol_dir.is_dir():
            continue
        for profile_dir in sorted(symbol_dir.iterdir(), key=lambda item: item.name):
            if not profile_dir.is_dir():
                continue
            metas = sorted(profile_dir.glob("*.meta.json"))
            if not metas:
                profiles.append(
                    {
                        "symbol": symbol_dir.name,
                        "profile_hash": profile_dir.name,
                        "days": 0,
                        "first_day": None,
                        "last_day": None,
                        "timeframes": [],
                        "required_families": [],
                        "bytes": 0,
                        "legacy_extends_before_ohlcv": False,
                    }
                )
                continue
            first_meta = _load_json(metas[0]) or {}
            first_day = _day_from_name(metas[0].name, ".meta.json")
            last_day = _day_from_name(metas[-1].name, ".meta.json")
            profiles.append(
                {
                    "symbol": symbol_dir.name,
                    "profile_hash": profile_dir.name,
                    "days": len(metas),
                    "first_day": first_day,
                    "last_day": last_day,
                    "timeframes": list(first_meta.get("timeframes") or []),
                    "required_families": list(first_meta.get("required_families") or []),
                    "bytes": sum(_safe_size(path) for path in _walk_files(profile_dir)),
                    "legacy_extends_before_ohlcv": bool(ohlcv_start_day and first_day and first_day < ohlcv_start_day),
                }
            )
    profiles.sort(key=lambda row: (-int(row["days"]), str(row["profile_hash"])))
    return {
        "profiles": profiles,
        "totals": _walk_stats(root),
        "profile_count": len(profiles),
        "legacy_extending_profile_count": sum(1 for row in profiles if row["legacy_extends_before_ohlcv"]),
    }


def _indicator_stream_summary(cache_dir: Path) -> Dict[str, Any]:
    root = cache_dir / "indicator_streams"
    per_symbol: List[Dict[str, Any]] = []
    for symbol_dir in sorted(root.iterdir(), key=lambda item: item.name) if root.exists() else []:
        if not symbol_dir.is_dir():
            continue
        metas = sorted(symbol_dir.glob("*.meta.json"))
        per_symbol.append(
            {
                "symbol": symbol_dir.name,
                "window_pairs": len(metas),
                "bytes": sum(_safe_size(path) for path in _walk_files(symbol_dir)),
            }
        )
    return {"symbols": per_symbol, "totals": _walk_stats(root)}


def _prepared_dataset_summary(cache_dir: Path) -> Dict[str, Any]:
    root = cache_dir / "prepared_datasets"
    per_symbol: List[Dict[str, Any]] = []
    for symbol_dir in sorted(root.iterdir(), key=lambda item: item.name) if root.exists() else []:
        if not symbol_dir.is_dir():
            continue
        metas = sorted(symbol_dir.glob("*.meta.json"))
        start_values: List[str] = []
        end_values: List[str] = []
        for meta_path in metas:
            meta = _load_json(meta_path) or {}
            if isinstance(meta.get("start_utc"), str):
                start_values.append(str(meta["start_utc"]))
            if isinstance(meta.get("end_utc"), str):
                end_values.append(str(meta["end_utc"]))
        per_symbol.append(
            {
                "symbol": symbol_dir.name,
                "prepared_entries": len(metas),
                "earliest_start_utc": min(start_values) if start_values else None,
                "latest_end_utc": max(end_values) if end_values else None,
                "bytes": sum(_safe_size(path) for path in _walk_files(symbol_dir)),
            }
        )
    return {"symbols": per_symbol, "totals": _walk_stats(root)}


def _root_file_summary(cache_dir: Path) -> Dict[str, Any]:
    files = sorted(path for path in cache_dir.iterdir() if path.is_file())
    loose_klines = _scan_root_window_klines(cache_dir)
    other_files = [path for path in files if path not in loose_klines]
    return {
        "root_file_count": len(files),
        "root_file_bytes": sum(_safe_size(path) for path in files),
        "loose_window_klines_count": len(loose_klines),
        "loose_window_klines_bytes": sum(_safe_size(path) for path in loose_klines),
        "other_root_files": [path.name for path in other_files],
    }


def _report_markdown(report: Dict[str, Any]) -> str:
    before = dict(report.get("before") or {})
    after = dict(report.get("after") or {})
    actions = dict(report.get("actions") or {})
    ohlcv = dict(after.get("ohlcv_store") or {})
    indicator = dict(after.get("indicator_store") or {})
    indicator_streams = dict(after.get("indicator_streams") or {})
    prepared = dict(after.get("prepared_datasets") or {})
    root_after = dict(after.get("root_files") or {})
    stores = list(ohlcv.get("stores") or [])
    top_profiles = list(indicator.get("profiles") or [])[:12]

    lines: List[str] = []
    lines.append("# Cache Library Audit")
    lines.append("")
    lines.append(f"- Generated at (UTC): `{report.get('generated_at_utc')}`")
    lines.append(f"- Apply mode: `{report.get('apply_mode')}`")
    lines.append(f"- Cache dir: `{report.get('cache_dir')}`")
    lines.append("")
    lines.append("## Actions")
    lines.append("")
    lines.append(f"- Root exact 1m kline caches migrated: `{actions.get('migrated_root_window_klines', {}).get('moved', 0)}`")
    lines.append(f"- Root exact 1m duplicates removed: `{actions.get('migrated_root_window_klines', {}).get('duplicates_removed', 0)}`")
    lines.append(f"- Root exact 1m conflicts quarantined: `{actions.get('migrated_root_window_klines', {}).get('conflicts', 0)}`")
    lines.append(f"- Empty indicator profile dirs removed: `{len(actions.get('pruned_empty_indicator_profiles', {}).get('removed', []))}`")
    lines.append("")
    lines.append("## Root Cache Hygiene")
    lines.append("")
    lines.append(f"- Loose root window kline files before: `{before.get('root_files', {}).get('loose_window_klines_count', 0)}`")
    lines.append(f"- Loose root window kline files after: `{root_after.get('loose_window_klines_count', 0)}`")
    lines.append(f"- Other root files after: `{', '.join(root_after.get('other_root_files', [])) or '-'}`")
    lines.append("")
    lines.append("## Canonical Stores")
    lines.append("")
    for store in stores:
        lines.append(
            f"- `{store['symbol']} / {store['price_source']} / {store['timeframe']}`: "
            f"`{store['days']}` days, `{store['first_day']} -> {store['last_day']}`, `{_bytes_to_gb(int(store['bytes']))} GB`"
        )
    lines.append("")
    lines.append("## Indicator Library")
    lines.append("")
    lines.append(f"- Indicator day-store profiles: `{indicator.get('profile_count', 0)}`")
    lines.append(f"- Profiles extending before current OHLCV canon start: `{indicator.get('legacy_extending_profile_count', 0)}`")
    lines.append(f"- Exact indicator windows: `{sum(int(item.get('window_pairs', 0)) for item in indicator_streams.get('symbols', []))}`")
    lines.append(f"- Prepared datasets: `{sum(int(item.get('prepared_entries', 0)) for item in prepared.get('symbols', []))}`")
    lines.append("")
    lines.append("Top indicator profiles by day coverage:")
    lines.append("")
    for row in top_profiles:
        fams = ",".join(row.get("required_families") or [])
        tfs = ",".join(row.get("timeframes") or [])
        legacy_note = " legacy-before-ohlcv" if row.get("legacy_extends_before_ohlcv") else ""
        lines.append(
            f"- `{row['profile_hash']}`: `{row['days']}` days, `{row['first_day']} -> {row['last_day']}`, "
            f"`{tfs}`, `{fams}`{legacy_note}"
        )
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _write_report(cache_dir: Path, report: Dict[str, Any]) -> Dict[str, str]:
    inventory_dir = cache_dir / "_inventory"
    inventory_dir.mkdir(parents=True, exist_ok=True)
    json_path = inventory_dir / "cache_library_audit.json"
    md_path = inventory_dir / "cache_library_audit.md"
    with json_path.open("w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, indent=2)
        fh.write("\n")
    md_path.write_text(_report_markdown(report), encoding="utf-8")
    return {"json": str(json_path), "md": str(md_path)}


def build_report(cache_dir: Path, *, apply: bool) -> Dict[str, Any]:
    before_root = _root_file_summary(cache_dir)
    before_ohlcv = _ohlcv_store_summary(cache_dir)
    ohlcv_stores = list(before_ohlcv.get("stores") or [])
    canonical_1m = next((row for row in ohlcv_stores if row.get("timeframe") == "1m"), None)
    ohlcv_start_day = canonical_1m.get("first_day") if isinstance(canonical_1m, dict) else None

    migrated = _migrate_root_window_klines(cache_dir, apply=apply)
    pruned = _prune_empty_indicator_profiles(cache_dir, apply=apply)

    after = {
        "root_files": _root_file_summary(cache_dir),
        "ohlcv_store": _ohlcv_store_summary(cache_dir),
        "indicator_store": _indicator_profile_summary(cache_dir, ohlcv_start_day),
        "indicator_streams": _indicator_stream_summary(cache_dir),
        "prepared_datasets": _prepared_dataset_summary(cache_dir),
    }
    return {
        "generated_at_utc": _utc_now(),
        "apply_mode": apply,
        "cache_dir": str(cache_dir),
        "before": {
            "root_files": before_root,
            "ohlcv_store": before_ohlcv,
        },
        "actions": {
            "migrated_root_window_klines": migrated,
            "pruned_empty_indicator_profiles": pruned,
        },
        "after": after,
    }


def main() -> int:
    args = build_parser().parse_args()
    cache_dir = args.cache_dir.resolve()
    report = build_report(cache_dir, apply=bool(args.apply))
    outputs = _write_report(cache_dir, report)
    compact = {
        "cache_dir": str(cache_dir),
        "apply_mode": bool(args.apply),
        "report_json": outputs["json"],
        "report_md": outputs["md"],
        "root_loose_klines_before": report["before"]["root_files"]["loose_window_klines_count"],
        "root_loose_klines_after": report["after"]["root_files"]["loose_window_klines_count"],
        "migrated_root_window_klines": report["actions"]["migrated_root_window_klines"]["moved"],
        "removed_empty_indicator_profiles": len(report["actions"]["pruned_empty_indicator_profiles"]["removed"]),
    }
    print(json.dumps(compact, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
