from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd


WORKSPACE_ROOT = Path(__file__).resolve().parents[4]
CASEBOOK_ROOT = WORKSPACE_ROOT / "research_exports" / "trade_entry_casebook"
CASES_ROOT = CASEBOOK_ROOT / "cases"
DEFAULT_OUTPUT_ROOT = CASEBOOK_ROOT / "presentation" / "exact_reference_engine_0001_0005"


@dataclass(frozen=True)
class EngineParams:
    max_pullback_bars: int = 18
    reclaim_window_bars: int = 4
    max_vidya_gap_pct: float = 0.25
    require_t0_vidya_buy: bool = True
    require_t0_rf_buy: bool = True
    require_vidya_buy_through_signal: bool = True
    simple_reclaim_phases: tuple[str, ...] = ("BUY_WEAK", "NEUTRAL")
    delayed_rf_buy_phases: tuple[str, ...] = ("SELL_WEAK", "SELL_STRONG")
    stop_abs: float = 160.0
    take_abs: float = 320.0


@dataclass(frozen=True)
class CaseSpec:
    case_id: str
    case_dir: str
    window_csv: str
    t0_close_ts: str
    expected_signal_close_ts: str
    expected_fill_open_ts: str


CASE_SPECS: tuple[CaseSpec, ...] = (
    CaseSpec(
        case_id="0001",
        case_dir="0001_vidya_flip_anchor_long",
        window_csv="prep_05287a6b9ecebfd6_event_678918_window.csv",
        t0_close_ts="2026-04-17 11:18:59.999000+00:00",
        expected_signal_close_ts="2026-04-17 11:21:59.999000+00:00",
        expected_fill_open_ts="2026-04-17 11:22:00+00:00",
    ),
    CaseSpec(
        case_id="0002",
        case_dir="0002_vidya_flip_anchor_long_neutral_drawdown",
        window_csv="prep_05287a6b9ecebfd6_event_678606_window.csv",
        t0_close_ts="2026-04-17 06:06:59.999000+00:00",
        expected_signal_close_ts="2026-04-17 06:10:59.999000+00:00",
        expected_fill_open_ts="2026-04-17 06:11:00+00:00",
    ),
    CaseSpec(
        case_id="0003",
        case_dir="0003_vidya_flip_anchor_long_apr16_1443",
        window_csv="prep_05287a6b9ecebfd6_event_677683_window.csv",
        t0_close_ts="2026-04-16 14:43:59.999000+00:00",
        expected_signal_close_ts="2026-04-16 15:07:59.999000+00:00",
        expected_fill_open_ts="2026-04-16 15:08:00+00:00",
    ),
    CaseSpec(
        case_id="0004",
        case_dir="0004_vidya_flip_anchor_long_apr16_1137",
        window_csv="prep_05287a6b9ecebfd6_event_677497_window.csv",
        t0_close_ts="2026-04-16 11:37:59.999000+00:00",
        expected_signal_close_ts="2026-04-16 11:43:59.999000+00:00",
        expected_fill_open_ts="2026-04-16 11:44:00+00:00",
    ),
    CaseSpec(
        case_id="0005",
        case_dir="0005_vidya_flip_anchor_long_apr15_2143",
        window_csv="prep_05287a6b9ecebfd6_event_676663_window.csv",
        t0_close_ts="2026-04-15 21:43:59.999000+00:00",
        expected_signal_close_ts="2026-04-15 22:00:59.999000+00:00",
        expected_fill_open_ts="2026-04-15 22:01:00+00:00",
    ),
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Verify the unified research reference engine against casebook anchors 0001-0005."
    )
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--strict", action="store_true", help="Exit non-zero if any case does not match exactly.")
    return parser


def _ts(value: str) -> pd.Timestamp:
    return pd.Timestamp(value, tz="UTC")


def _next_open_from_signal_close(ts: pd.Timestamp) -> pd.Timestamp:
    return ts.floor("min") + pd.Timedelta(minutes=1)


def _load_window(spec: CaseSpec) -> pd.DataFrame:
    path = CASES_ROOT / spec.case_dir / "exports" / spec.window_csv
    df = pd.read_csv(path)
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    return df


def _safe_float(value: Any) -> float | None:
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        return float(value)
    except Exception:
        return None


def _first_simple_reclaim_index(df: pd.DataFrame, pullback_idx: int, t0_close: float, window: int) -> int | None:
    start = pullback_idx + 1
    stop = min(len(df), pullback_idx + 1 + int(window))
    for idx in range(start, stop):
        close = float(df.at[idx, "close"])
        open_ = float(df.at[idx, "open"])
        prev_high = float(df.at[idx - 1, "high"]) if idx - 1 >= 0 else close
        vidya_line = _safe_float(df.at[idx, "vidya_line"])
        if close >= prev_high and close >= open_ and (vidya_line is None or close >= vidya_line):
            return idx
        if close >= t0_close and close >= open_:
            return idx
    return None


def _first_rf_buy_after_pullback(df: pd.DataFrame, pullback_idx: int) -> int | None:
    for idx in range(pullback_idx + 1, len(df)):
        if str(df.at[idx, "range_filter_state"] or "").upper() == "BUY":
            return idx
    return None


def _simulate_window_exit(df: pd.DataFrame, entry_idx: int, params: EngineParams) -> dict[str, Any]:
    entry_price = float(df.at[entry_idx, "open"])
    stop_price = entry_price - float(params.stop_abs)
    take_price = entry_price + float(params.take_abs)
    exit_idx = len(df) - 1
    exit_price = float(df.iloc[exit_idx]["close"])
    exit_reason = "Force Close (End)"
    for idx in range(entry_idx, len(df)):
        low = float(df.at[idx, "low"])
        high = float(df.at[idx, "high"])
        if low <= stop_price and high >= take_price:
            exit_idx = idx
            exit_price = stop_price
            exit_reason = "Stop Loss"
            break
        if low <= stop_price:
            exit_idx = idx
            exit_price = stop_price
            exit_reason = "Stop Loss"
            break
        if high >= take_price:
            exit_idx = idx
            exit_price = take_price
            exit_reason = "Take Profit"
            break
    return {
        "entry_price": entry_price,
        "stop_price": stop_price,
        "take_price": take_price,
        "window_exit_close_ts": pd.Timestamp(df.at[exit_idx, "ts"]),
        "window_exit_price": exit_price,
        "window_exit_reason": exit_reason,
    }


def _evaluate_case(spec: CaseSpec, params: EngineParams) -> dict[str, Any]:
    df = _load_window(spec)
    t0_close_ts = _ts(spec.t0_close_ts)
    expected_signal = _ts(spec.expected_signal_close_ts)
    expected_fill = _ts(spec.expected_fill_open_ts)
    anchor_matches = df.index[df["ts"] == t0_close_ts].tolist()
    if not anchor_matches:
        raise ValueError(f"{spec.case_id}: anchor {t0_close_ts} not found in export window")
    anchor_idx = int(anchor_matches[0])
    t0 = df.iloc[anchor_idx]
    if params.require_t0_vidya_buy and str(t0.get("vidya_state") or "").upper() != "BUY":
        raise ValueError(f"{spec.case_id}: t0 vidya_state is not BUY")
    if params.require_t0_rf_buy and str(t0.get("range_filter_state") or "").upper() != "BUY":
        raise ValueError(f"{spec.case_id}: t0 range_filter_state is not BUY")

    start = anchor_idx + 1
    stop = min(len(df), anchor_idx + 1 + int(params.max_pullback_bars))
    if start >= stop:
        raise ValueError(f"{spec.case_id}: no room to search pullback")
    pullback_idx = int(df.iloc[start:stop]["low"].astype(float).idxmin())
    pullback = df.iloc[pullback_idx]
    phase = str(pullback.get("range_filter_phase") or "").upper()
    t0_body_bottom = min(float(t0["open"]), float(t0["close"]))
    pullback_low = float(pullback["low"])
    vidya_line = _safe_float(pullback.get("vidya_line"))
    vidya_gap_pct = ((pullback_low - vidya_line) / vidya_line) * 100.0 if vidya_line else None

    if pullback_low >= t0_body_bottom:
        raise ValueError(f"{spec.case_id}: pullback low did not pierce below t0 body")
    if vidya_line is None or pullback_low < vidya_line:
        raise ValueError(f"{spec.case_id}: pullback low fell below vidya_line")
    if vidya_gap_pct is not None and vidya_gap_pct > float(params.max_vidya_gap_pct):
        raise ValueError(f"{spec.case_id}: pullback low exceeded vidya gap tolerance")
    if params.require_vidya_buy_through_signal:
        vidya_slice = df.iloc[anchor_idx : pullback_idx + 1]["vidya_state"].astype(str).str.upper()
        if not vidya_slice.eq("BUY").all():
            raise ValueError(f"{spec.case_id}: vidya_state lost BUY before pullback completed")

    if phase in set(params.delayed_rf_buy_phases):
        signal_idx = _first_rf_buy_after_pullback(df, pullback_idx)
        signal_mode = "delayed_rf_buy_reclaim"
    else:
        signal_idx = _first_simple_reclaim_index(df, pullback_idx, float(t0["close"]), int(params.reclaim_window_bars))
        signal_mode = "simple_reclaim"
    if signal_idx is None:
        raise ValueError(f"{spec.case_id}: no signal found after pullback")

    if params.require_vidya_buy_through_signal:
        vidya_slice = df.iloc[anchor_idx : signal_idx + 1]["vidya_state"].astype(str).str.upper()
        if not vidya_slice.eq("BUY").all():
            raise ValueError(f"{spec.case_id}: vidya_state lost BUY before signal")

    signal_close_ts = pd.Timestamp(df.at[signal_idx, "ts"])
    executable_fill_ts = _next_open_from_signal_close(signal_close_ts)
    entry_idx = signal_idx + 1
    exit_preview = _simulate_window_exit(df, entry_idx, params)

    return {
        "case_id": spec.case_id,
        "signal_mode": signal_mode,
        "anchor_close_ts": t0_close_ts.isoformat(),
        "pullback_close_ts": pd.Timestamp(pullback["ts"]).isoformat(),
        "pullback_phase": phase,
        "pullback_low_minus_t0_body_abs": round(pullback_low - t0_body_bottom, 6),
        "pullback_low_to_vidya_pct": None if vidya_gap_pct is None else round(float(vidya_gap_pct), 6),
        "signal_close_ts": signal_close_ts.isoformat(),
        "executable_fill_open_ts": executable_fill_ts.isoformat(),
        "expected_signal_close_ts": expected_signal.isoformat(),
        "expected_fill_open_ts": expected_fill.isoformat(),
        "exact_signal_match": signal_close_ts == expected_signal,
        "exact_fill_match": executable_fill_ts == expected_fill,
        "entry_open_price": round(float(df.at[entry_idx, "open"]), 6),
        "window_exit_close_ts": exit_preview["window_exit_close_ts"].isoformat(),
        "window_exit_reason": exit_preview["window_exit_reason"],
        "window_exit_price": round(float(exit_preview["window_exit_price"]), 6),
        "stop_price": round(float(exit_preview["stop_price"]), 6),
        "take_price": round(float(exit_preview["take_price"]), 6),
        "window_csv": str(CASES_ROOT / spec.case_dir / "exports" / spec.window_csv),
    }


def _build_markdown(results: list[dict[str, Any]], params: EngineParams) -> str:
    exact_count = sum(1 for row in results if row["exact_fill_match"])
    lines = [
        "# Exact Reference Engine Verification",
        "",
        "Unified research engine rebuilt from case export windows.",
        "",
        f"- Exact fill matches: `{exact_count}/{len(results)}`",
        f"- Engine params: `{json.dumps(asdict(params), ensure_ascii=False)}`",
        "",
        "| Case | Mode | Pullback phase | Signal close UTC | Expected signal close UTC | Fill UTC | Expected fill UTC | Exact fill | Window exit preview |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for row in results:
        lines.append(
            "| {case_id} | {signal_mode} | {pullback_phase} | {signal_close_ts} | {expected_signal_close_ts} | {executable_fill_open_ts} | {expected_fill_open_ts} | {exact_fill_match} | {window_exit_reason} @ {window_exit_close_ts} |".format(
                **row
            )
        )
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    args = build_parser().parse_args()
    params = EngineParams()
    results = [_evaluate_case(spec, params) for spec in CASE_SPECS]
    payload = {
        "engine_name": "casebook_reference_pullback_engine_v1",
        "params": asdict(params),
        "exact_fill_matches": sum(1 for row in results if row["exact_fill_match"]),
        "results": results,
    }

    output_root = Path(args.output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / "exact_reference_engine_0001_0005.json"
    md_path = output_root / "exact_reference_engine_0001_0005.md"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    md_path.write_text(_build_markdown(results, params), encoding="utf-8")

    print(json.dumps({"output_json": str(json_path), "output_md": str(md_path), "exact_fill_matches": payload["exact_fill_matches"]}, ensure_ascii=False, indent=2))
    if args.strict and payload["exact_fill_matches"] != len(results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
