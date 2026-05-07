from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Sequence

import numpy as np
import pandas as pd


WORKSPACE_ROOT = Path(__file__).resolve().parents[4]
CASEBOOK_ROOT = WORKSPACE_ROOT / "research_exports" / "trade_entry_casebook"
CASES_ROOT = CASEBOOK_ROOT / "cases"
DEFAULT_OUTPUT_ROOT = CASEBOOK_ROOT / "presentation" / "range_filter_oracle_verification"
OHLCV_STORE_ROOT = WORKSPACE_ROOT / "phase8_ui_phase7_backtest_results_lazy_load-main" / "phase8_refactor_repo" / "data_cache" / "ohlcv_store" / "BTCUSDT" / "LAST" / "1m"

RANGE_FILTER_PER = 100
RANGE_FILTER_MULT = 3.0
FLOAT_TOL = 1e-9
DEFAULT_WARMUP_MINUTES = 10080

STATE_FIELDS: Sequence[str] = (
    "range_filter_state",
    "range_filter_phase",
)
BOOL_FIELDS: Sequence[str] = (
    "range_filter_buy",
    "range_filter_sell",
)
FLOAT_FIELDS: Sequence[str] = (
    "range_filter_line",
    "range_filter_upper",
    "range_filter_lower",
    "range_filter_smooth_range",
    "range_filter_cond_ini",
    "range_filter_upward_count",
    "range_filter_downward_count",
)
ALL_FIELDS: Sequence[str] = (*STATE_FIELDS, *BOOL_FIELDS, *FLOAT_FIELDS)


@dataclass(frozen=True)
class WindowSpec:
    case_id: str
    case_dir: str
    window_csv: str


WINDOW_SPECS: Sequence[WindowSpec] = (
    WindowSpec("0001", "0001_vidya_flip_anchor_long", "prep_05287a6b9ecebfd6_event_678918_window.csv"),
    WindowSpec("0002", "0002_vidya_flip_anchor_long_neutral_drawdown", "prep_05287a6b9ecebfd6_event_678606_window.csv"),
    WindowSpec("0003", "0003_vidya_flip_anchor_long_apr16_1443", "prep_05287a6b9ecebfd6_event_677683_window.csv"),
    WindowSpec("0004", "0004_vidya_flip_anchor_long_apr16_1137", "prep_05287a6b9ecebfd6_event_677497_window.csv"),
    WindowSpec("0005", "0005_vidya_flip_anchor_long_apr15_2143", "prep_05287a6b9ecebfd6_event_676663_window.csv"),
)


class OracleEMA:
    __slots__ = ("length", "alpha", "_buf", "value", "seeded")

    def __init__(self, length: int):
        self.length = max(1, int(length))
        self.alpha = 2.0 / (self.length + 1.0)
        self._buf: List[float] = []
        self.value = float("nan")
        self.seeded = False

    def commit(self, x: float) -> float:
        if not np.isfinite(x):
            return float("nan")
        if not self.seeded:
            self._buf.append(float(x))
            if len(self._buf) < self.length:
                return float("nan")
            if len(self._buf) == self.length:
                self.value = float(np.mean(self._buf))
                self.seeded = True
                self._buf = []
                return float(self.value)
        self.value = self.alpha * float(x) + (1.0 - self.alpha) * float(self.value)
        return float(self.value)


class OracleRangeFilter:
    __slots__ = (
        "per",
        "mult",
        "wper",
        "avrng_ema",
        "smooth_ema",
        "prev_src",
        "prev_filt",
        "cond_ini",
        "upward",
        "downward",
        "filt",
        "smrng",
        "hband",
        "lband",
        "phase",
        "long_cond",
        "short_cond",
        "buy_event",
        "sell_event",
    )

    def __init__(self, per: int = RANGE_FILTER_PER, mult: float = RANGE_FILTER_MULT):
        self.per = max(1, int(per))
        self.mult = max(0.1, float(mult))
        self.wper = max(1, int(self.per * 2 - 1))
        self.avrng_ema = OracleEMA(self.per)
        self.smooth_ema = OracleEMA(self.wper)
        self.prev_src = float("nan")
        self.prev_filt = float("nan")
        self.cond_ini = 0
        self.upward = 0.0
        self.downward = 0.0
        self.filt = float("nan")
        self.smrng = float("nan")
        self.hband = float("nan")
        self.lband = float("nan")
        self.phase = "NEUTRAL"
        self.long_cond = False
        self.short_cond = False
        self.buy_event = False
        self.sell_event = False

    def update(self, source: float) -> Dict[str, Any]:
        x = float(source) if np.isfinite(float(source)) else float("nan")

        abs_delta = float("nan")
        if np.isfinite(x) and np.isfinite(self.prev_src):
            abs_delta = abs(float(x) - float(self.prev_src))
        avrng = self.avrng_ema.commit(abs_delta)
        smooth_base = self.smooth_ema.commit(avrng)
        smrng = float(smooth_base * self.mult) if np.isfinite(float(smooth_base)) else float("nan")

        prev_filt_nz = float(self.prev_filt) if np.isfinite(float(self.prev_filt)) else 0.0
        if np.isfinite(x) and np.isfinite(smrng):
            if x > prev_filt_nz:
                filt = prev_filt_nz if (x - smrng) < prev_filt_nz else float(x - smrng)
            else:
                filt = prev_filt_nz if (x + smrng) > prev_filt_nz else float(x + smrng)
        else:
            filt = float("nan")

        prev_filt = float(self.prev_filt)
        upward_prev = float(self.upward) if np.isfinite(float(self.upward)) else 0.0
        downward_prev = float(self.downward) if np.isfinite(float(self.downward)) else 0.0
        if np.isfinite(filt) and np.isfinite(prev_filt):
            if filt > prev_filt:
                upward = upward_prev + 1.0
            elif filt < prev_filt:
                upward = 0.0
            else:
                upward = upward_prev
            if filt < prev_filt:
                downward = downward_prev + 1.0
            elif filt > prev_filt:
                downward = 0.0
            else:
                downward = downward_prev
        else:
            upward = upward_prev
            downward = downward_prev

        prev_src = float(self.prev_src)
        buy_strong = bool(np.isfinite(x) and np.isfinite(filt) and (x > filt) and (upward > 0.0) and np.isfinite(prev_src) and (x > prev_src))
        buy_weak = bool(np.isfinite(x) and np.isfinite(filt) and (x > filt) and (upward > 0.0) and np.isfinite(prev_src) and (x < prev_src))
        sell_strong = bool(np.isfinite(x) and np.isfinite(filt) and (x < filt) and (downward > 0.0) and np.isfinite(prev_src) and (x < prev_src))
        sell_weak = bool(np.isfinite(x) and np.isfinite(filt) and (x < filt) and (downward > 0.0) and np.isfinite(prev_src) and (x > prev_src))
        long_cond = bool(buy_strong or buy_weak)
        short_cond = bool(sell_strong or sell_weak)
        phase = "BUY_STRONG" if buy_strong else ("BUY_WEAK" if buy_weak else ("SELL_STRONG" if sell_strong else ("SELL_WEAK" if sell_weak else "NEUTRAL")))

        prev_cond_ini = int(self.cond_ini)
        cond_ini = 1 if long_cond else (-1 if short_cond else prev_cond_ini)
        buy_event = bool(long_cond and prev_cond_ini == -1)
        sell_event = bool(short_cond and prev_cond_ini == 1)

        hband = float(filt + smrng) if np.isfinite(filt) and np.isfinite(smrng) else float("nan")
        lband = float(filt - smrng) if np.isfinite(filt) and np.isfinite(smrng) else float("nan")

        self.prev_src = x
        self.prev_filt = filt
        self.cond_ini = int(cond_ini)
        self.upward = float(upward)
        self.downward = float(downward)
        self.filt = filt
        self.smrng = smrng
        self.hband = hband
        self.lband = lband
        self.phase = str(phase)
        self.long_cond = bool(long_cond)
        self.short_cond = bool(short_cond)
        self.buy_event = bool(buy_event)
        self.sell_event = bool(sell_event)
        return self.snapshot()

    def snapshot(self) -> Dict[str, Any]:
        state = "BUY" if self.long_cond else ("SELL" if self.short_cond else "NEUTRAL")
        return {
            "range_filter_line": float(self.filt) if np.isfinite(float(self.filt)) else np.nan,
            "range_filter_upper": float(self.hband) if np.isfinite(float(self.hband)) else np.nan,
            "range_filter_lower": float(self.lband) if np.isfinite(float(self.lband)) else np.nan,
            "range_filter_smooth_range": float(self.smrng) if np.isfinite(float(self.smrng)) else np.nan,
            "range_filter_state": state,
            "range_filter_phase": self.phase if self.phase in ("BUY_STRONG", "BUY_WEAK", "SELL_STRONG", "SELL_WEAK", "NEUTRAL") else "NEUTRAL",
            "range_filter_buy": bool(self.buy_event),
            "range_filter_sell": bool(self.sell_event),
            "range_filter_long_cond": bool(self.long_cond),
            "range_filter_short_cond": bool(self.short_cond),
            "range_filter_cond_ini": float(self.cond_ini),
            "range_filter_upward_count": float(self.upward),
            "range_filter_downward_count": float(self.downward),
        }


def _boolize(value: Any) -> bool:
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    text = str(value or "").strip().upper()
    return text in {"1", "TRUE", "YES", "Y", "ON"}


def _str_norm(value: Any) -> str:
    return str(value or "").strip().upper()


def _float_or_nan(value: Any) -> float:
    try:
        numeric = float(value)
    except Exception:
        return float("nan")
    return float(numeric)


def _float_equal(left: Any, right: Any, tol: float = FLOAT_TOL) -> bool:
    a = _float_or_nan(left)
    b = _float_or_nan(right)
    if np.isnan(a) and np.isnan(b):
        return True
    if np.isnan(a) or np.isnan(b):
        return False
    return abs(a - b) <= float(tol)


def _window_path(spec: WindowSpec) -> Path:
    return CASES_ROOT / spec.case_dir / "exports" / spec.window_csv


def _load_history(*, start_ts: pd.Timestamp, end_ts: pd.Timestamp, warmup_minutes: int) -> pd.DataFrame:
    history_start = start_ts - pd.Timedelta(minutes=int(warmup_minutes))
    day_start = history_start.floor("D")
    day_end = end_ts.floor("D")
    frames: List[pd.DataFrame] = []
    for path in sorted(OHLCV_STORE_ROOT.glob("*.parquet")):
        try:
            day_text = path.stem.rsplit("_", 1)[-1]
            day_ts = pd.Timestamp(day_text, tz="UTC")
        except Exception:
            continue
        if day_ts < day_start or day_ts > day_end:
            continue
        day_frame = pd.read_parquet(path, columns=["close_time", "close"])
        day_frame["ts"] = pd.to_datetime(day_frame["close_time"], utc=True)
        frames.append(day_frame[["ts", "close"]])
    if not frames:
        raise ValueError(f"No candle history found for {history_start}..{end_ts} in {OHLCV_STORE_ROOT}")
    history = pd.concat(frames, ignore_index=True)
    history = history[(history["ts"] >= history_start) & (history["ts"] <= end_ts)].copy()
    history.sort_values("ts", inplace=True)
    history.reset_index(drop=True, inplace=True)
    if history.empty:
        raise ValueError(f"Filtered candle history is empty for {history_start}..{end_ts}")
    return history


def _compute_oracle_frame(close: np.ndarray) -> pd.DataFrame:
    state = OracleRangeFilter()
    rows: List[Dict[str, Any]] = []
    for value in np.asarray(close, dtype=float):
        rows.append(state.update(float(value)))
    return pd.DataFrame(rows)


def _compare_window(spec: WindowSpec, *, warmup_minutes: int) -> Dict[str, Any]:
    path = _window_path(spec)
    frame = pd.read_csv(path)
    if "close" not in frame.columns:
        raise ValueError(f"{path} missing close column")
    frame["ts"] = pd.to_datetime(frame["ts"], utc=True)
    compare_fields = [field for field in ALL_FIELDS if field in frame.columns]
    for field in compare_fields:
        if field not in frame.columns:
            raise ValueError(f"{path} missing {field}")

    history = _load_history(
        start_ts=pd.Timestamp(frame["ts"].iloc[0]),
        end_ts=pd.Timestamp(frame["ts"].iloc[-1]),
        warmup_minutes=warmup_minutes,
    )
    oracle_history = _compute_oracle_frame(pd.to_numeric(history["close"], errors="coerce").to_numpy(dtype=float))
    oracle_history["ts"] = history["ts"].to_numpy()
    oracle = oracle_history.set_index("ts").reindex(frame["ts"])
    if oracle.index.isna().any():
        raise ValueError(f"{path} oracle alignment failed")
    mismatches: Dict[str, List[Dict[str, Any]]] = {field: [] for field in compare_fields}

    for idx, row in frame.iterrows():
        oracle_row = oracle.iloc[idx]
        ts = str(row.get("ts") or "")
        for field in [field for field in STATE_FIELDS if field in compare_fields]:
            actual = _str_norm(row.get(field))
            expected = _str_norm(oracle_row.get(field))
            if actual != expected:
                mismatches[field].append({"row": int(idx), "ts": ts, "expected": expected, "actual": actual})
        for field in [field for field in BOOL_FIELDS if field in compare_fields]:
            actual = _boolize(row.get(field))
            expected = _boolize(oracle_row.get(field))
            if actual != expected:
                mismatches[field].append({"row": int(idx), "ts": ts, "expected": expected, "actual": actual})
        for field in [field for field in FLOAT_FIELDS if field in compare_fields]:
            actual = row.get(field)
            expected = oracle_row.get(field)
            if not _float_equal(actual, expected):
                mismatches[field].append(
                    {
                        "row": int(idx),
                        "ts": ts,
                        "expected": None if np.isnan(_float_or_nan(expected)) else round(float(expected), 12),
                        "actual": None if np.isnan(_float_or_nan(actual)) else round(float(actual), 12),
                    }
                )

    mismatch_counts = {field: len(rows) for field, rows in mismatches.items()}
    total_mismatches = sum(mismatch_counts.values())
    exact = total_mismatches == 0
    return {
        "case_id": spec.case_id,
        "window_csv": str(path),
        "rows": int(len(frame)),
        "exact_match": bool(exact),
        "total_mismatches": int(total_mismatches),
        "mismatch_counts": mismatch_counts,
        "first_mismatches": {field: rows[0] for field, rows in mismatches.items() if rows},
        "compared_fields": compare_fields,
        "warmup_minutes": int(warmup_minutes),
    }


def _build_markdown(results: List[Dict[str, Any]]) -> str:
    exact_count = sum(1 for row in results if row["exact_match"])
    lines = [
        "# Range Filter Oracle Verification",
        "",
        "Independent `Range Filter` oracle written outside the old engine.",
        "",
        f"- Exact windows: `{exact_count}/{len(results)}`",
        f"- Params: `per={RANGE_FILTER_PER}, mult={RANGE_FILTER_MULT}, warmup_minutes={results[0]['warmup_minutes'] if results else DEFAULT_WARMUP_MINUTES}`",
        "",
        "| Case | Rows | Exact | Total mismatches | First state mismatch | First buy mismatch | First sell mismatch |",
        "|---|---:|---|---:|---|---|---|",
    ]
    for row in results:
        first_state = row["first_mismatches"].get("range_filter_state", {})
        first_buy = row["first_mismatches"].get("range_filter_buy", {})
        first_sell = row["first_mismatches"].get("range_filter_sell", {})
        lines.append(
            f"| {row['case_id']} | {row['rows']} | {row['exact_match']} | {row['total_mismatches']} | "
            f"{first_state.get('ts', '')} | {first_buy.get('ts', '')} | {first_sell.get('ts', '')} |"
        )
    lines.append("")
    lines.append("## Field mismatch counts")
    lines.append("")
    all_used_fields: List[str] = []
    for row in results:
        for field in list(row.get("compared_fields") or []):
            if field not in all_used_fields:
                all_used_fields.append(field)
    lines.append("| Case | " + " | ".join(all_used_fields) + " |")
    lines.append("|---|" + "|".join("---:" for _ in all_used_fields) + "|")
    for row in results:
        lines.append("| " + " | ".join([row["case_id"], *[str(row["mismatch_counts"].get(field, 0)) for field in all_used_fields]]) + " |")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify Range Filter states/triggers with an independent oracle implementation.")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--warmup-minutes", type=int, default=DEFAULT_WARMUP_MINUTES)
    parser.add_argument("--strict", action="store_true", help="Exit non-zero if any mismatch is found.")
    args = parser.parse_args()

    results = [_compare_window(spec, warmup_minutes=int(args.warmup_minutes)) for spec in WINDOW_SPECS]
    payload = {
        "oracle_name": "independent_range_filter_oracle_v1",
        "params": {
            "per": RANGE_FILTER_PER,
            "mult": RANGE_FILTER_MULT,
            "float_tolerance": FLOAT_TOL,
            "warmup_minutes": int(args.warmup_minutes),
        },
        "exact_windows": sum(1 for row in results if row["exact_match"]),
        "results": results,
    }

    output_root = Path(args.output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / "range_filter_oracle_verification.json"
    md_path = output_root / "range_filter_oracle_verification.md"
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    md_path.write_text(_build_markdown(results), encoding="utf-8")

    print(json.dumps({"output_json": str(json_path), "output_md": str(md_path), "exact_windows": payload["exact_windows"]}, indent=2, ensure_ascii=False))
    if args.strict and payload["exact_windows"] != len(results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
