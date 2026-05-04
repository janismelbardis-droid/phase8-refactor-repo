from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prune derived backtest cache layers so only the useful canonical stores remain."
    )
    parser.add_argument("--cache-dir", type=Path, default=REPO_ROOT / "data_cache")
    parser.add_argument(
        "--drop-indicator-streams",
        action="store_true",
        help="Remove exact-window indicator_streams cache files.",
    )
    parser.add_argument(
        "--drop-prepared-datasets",
        action="store_true",
        help="Remove prepared_datasets cache files.",
    )
    parser.add_argument(
        "--drop-market-aug-profiles",
        action="store_true",
        help="Remove indicator_store profiles whose metadata requires market_aug.",
    )
    parser.add_argument("--apply", action="store_true", help="Apply removals. Without this flag the script is dry-run only.")
    parser.add_argument("--output-json", type=Path, default=None)
    return parser


def _safe_size(path: Path) -> int:
    try:
        return int(path.stat().st_size)
    except OSError:
        return 0


def _walk_files(path: Path) -> Iterable[Path]:
    if not path.exists():
        return []
    return (item for item in path.rglob("*") if item.is_file())


def _path_stats(path: Path) -> Dict[str, Any]:
    file_count = 0
    total_bytes = 0
    for fp in _walk_files(path):
        file_count += 1
        total_bytes += _safe_size(fp)
    return {
        "path": str(path),
        "exists": path.exists(),
        "files": file_count,
        "bytes": total_bytes,
        "gigabytes": round(float(total_bytes) / (1024.0 ** 3), 3),
    }


def _load_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _market_aug_candidates(cache_dir: Path) -> List[Dict[str, Any]]:
    root = cache_dir / "indicator_store"
    candidates: List[Dict[str, Any]] = []
    if not root.exists():
        return candidates
    for symbol_dir in sorted(root.iterdir(), key=lambda item: item.name):
        if not symbol_dir.is_dir():
            continue
        for profile_dir in sorted(symbol_dir.iterdir(), key=lambda item: item.name):
            if not profile_dir.is_dir():
                continue
            meta_payload: Optional[Dict[str, Any]] = None
            for meta_path in sorted(profile_dir.glob("*.meta.json")):
                meta_payload = _load_json(meta_path)
                if meta_payload:
                    break
            families = list((meta_payload or {}).get("required_families") or [])
            if "market_aug" not in families:
                continue
            stats = _path_stats(profile_dir)
            candidates.append(
                {
                    "symbol": symbol_dir.name,
                    "profile_hash": profile_dir.name,
                    "required_families": families,
                    **stats,
                }
            )
    return candidates


def _delete_tree(path: Path) -> bool:
    if not path.exists():
        return False
    shutil.rmtree(path)
    return True


def main() -> int:
    args = build_parser().parse_args()
    cache_dir = args.cache_dir.resolve()

    drop_indicator_streams = bool(args.drop_indicator_streams)
    drop_prepared_datasets = bool(args.drop_prepared_datasets)
    drop_market_aug_profiles = bool(args.drop_market_aug_profiles)
    if not any([drop_indicator_streams, drop_prepared_datasets, drop_market_aug_profiles]):
        drop_indicator_streams = True
        drop_prepared_datasets = True
        drop_market_aug_profiles = True

    actions: List[Dict[str, Any]] = []
    deleted_bytes = 0
    deleted_files = 0

    if drop_indicator_streams:
        target = cache_dir / "indicator_streams"
        stats = _path_stats(target)
        applied = bool(args.apply and _delete_tree(target))
        actions.append(
            {
                "kind": "indicator_streams",
                "apply_requested": bool(args.apply),
                "applied": applied,
                **stats,
            }
        )
        if applied:
            deleted_bytes += int(stats["bytes"])
            deleted_files += int(stats["files"])

    if drop_prepared_datasets:
        target = cache_dir / "prepared_datasets"
        stats = _path_stats(target)
        applied = bool(args.apply and _delete_tree(target))
        actions.append(
            {
                "kind": "prepared_datasets",
                "apply_requested": bool(args.apply),
                "applied": applied,
                **stats,
            }
        )
        if applied:
            deleted_bytes += int(stats["bytes"])
            deleted_files += int(stats["files"])

    market_aug_candidates = _market_aug_candidates(cache_dir) if drop_market_aug_profiles else []
    for candidate in market_aug_candidates:
        target = Path(candidate["path"])
        applied = bool(args.apply and _delete_tree(target))
        action = dict(candidate)
        action.update(
            {
                "kind": "market_aug_profile",
                "apply_requested": bool(args.apply),
                "applied": applied,
            }
        )
        actions.append(action)
        if applied:
            deleted_bytes += int(candidate["bytes"])
            deleted_files += int(candidate["files"])

    payload = {
        "cache_dir": str(cache_dir),
        "apply": bool(args.apply),
        "drop_indicator_streams": drop_indicator_streams,
        "drop_prepared_datasets": drop_prepared_datasets,
        "drop_market_aug_profiles": drop_market_aug_profiles,
        "actions": actions,
        "market_aug_profile_count": len(market_aug_candidates),
        "deleted_files": deleted_files,
        "deleted_bytes": deleted_bytes,
        "deleted_gigabytes": round(float(deleted_bytes) / (1024.0 ** 3), 3),
    }

    text = json.dumps(payload, indent=2, ensure_ascii=False)
    if args.output_json is not None:
        out_path = args.output_json.resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text, encoding="utf-8")
    print(text, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
