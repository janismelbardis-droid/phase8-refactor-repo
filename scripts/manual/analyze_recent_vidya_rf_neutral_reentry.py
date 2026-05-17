from __future__ import annotations

import argparse
import json
import math
import shutil
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
LATEST_MIRROR = REPO_ROOT / "runs" / "perf" / "vidya_rf_neutral_reentry_latest"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "runs" / "perf"
LOCAL_DAY_STORE = REPO_ROOT / "data_cache" / "ohlcv_store" / "BTCUSDT" / "LAST" / "1m"

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.indicators_streaming import compute_range_filter_series, compute_vidya_series
from scripts.manual.probe_ms_trend_matrix_strategy import _compute_ms_trend_matrix_frame


def _load_local_1m(symbol: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    if str(symbol).upper() != "BTCUSDT":
        raise ValueError("This recent-analysis harness currently supports BTCUSDT only.")
    if not LOCAL_DAY_STORE.exists():
        raise FileNotFoundError(LOCAL_DAY_STORE)
    start = pd.to_datetime(start, utc=True)
    end = pd.to_datetime(end, utc=True)
    frames: list[pd.DataFrame] = []
    for path in sorted(LOCAL_DAY_STORE.glob("BTCUSDT_LAST_*.parquet")):
        try:
            day = pd.read_parquet(path, columns=["open_time", "open", "high", "low", "close", "volume"])
        except Exception:
            continue
        day["open_time"] = pd.to_datetime(day["open_time"], utc=True)
        day = day[(day["open_time"] >= start) & (day["open_time"] <= end)]
        if not day.empty:
            frames.append(day)
    if not frames:
        return pd.DataFrame(columns=["open_time", "open", "high", "low", "close", "volume"])
    out = pd.concat(frames, ignore_index=True)
    out = out.sort_values("open_time").drop_duplicates("open_time").reset_index(drop=True)
    return out


def _atr_rma(frame: pd.DataFrame, length: int = 14) -> pd.Series:
    high = frame["high"]
    low = frame["low"]
    close = frame["close"]
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1.0 / float(length), adjust=False).mean()


def _prepare_frame(raw: pd.DataFrame) -> pd.DataFrame:
    frame = raw.copy().reset_index(drop=True)
    vid = compute_vidya_series(
        frame["open"].to_numpy(dtype=float),
        frame["high"].to_numpy(dtype=float),
        frame["low"].to_numpy(dtype=float),
        frame["close"].to_numpy(dtype=float),
        frame["volume"].to_numpy(dtype=float),
    )
    rf = compute_range_filter_series(frame["close"].to_numpy(dtype=float), per=100, mult=3.0)
    ms = _compute_ms_trend_matrix_frame(
        frame[["open_time", "open", "high", "low", "close", "volume"]].copy(),
        ms_len=10,
        atr_length=14,
        atr_mult=4.0,
        target_step_mult=2.0,
    )

    frame["ts"] = pd.to_datetime(frame["open_time"], utc=True)
    frame["atr14"] = _atr_rma(frame, length=14)

    for key in ("vidya_state", "vidya_line", "vidya_delta_pct"):
        frame[key] = vid[key]
    for key in ("range_filter_state", "range_filter_phase", "range_filter_buy", "range_filter_sell", "range_filter_line"):
        frame[key] = rf[key]
    for key in ("ms_direction_bull", "ms_choch_up", "ms_choch_down", "ms_atr_ts", "ms_current_target"):
        frame[key] = ms[key]

    ny = frame["ts"].dt.tz_convert("America/New_York")
    frame["ny_day"] = ny.dt.floor("D")
    daily = frame.groupby("ny_day").agg(day_high=("high", "max"), day_low=("low", "min")).sort_index()
    daily["prev_day_high"] = daily["day_high"].shift(1)
    daily["prev_day_low"] = daily["day_low"].shift(1)
    frame = frame.merge(daily[["prev_day_high", "prev_day_low"]], left_on="ny_day", right_index=True, how="left")
    frame["prev_day_mid"] = (frame["prev_day_high"] + frame["prev_day_low"]) / 2.0
    return frame


def _candidate_rows(frame: pd.DataFrame, analysis_start: pd.Timestamp) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    signal_mask = frame["ts"] >= pd.to_datetime(analysis_start, utc=True)
    signal_indices = frame.index[signal_mask].tolist()
    for gi in signal_indices:
        row = frame.iloc[gi]
        side = "LONG" if bool(row["range_filter_buy"]) else ("SHORT" if bool(row["range_filter_sell"]) else None)
        if side is None:
            continue
        vidya_state = str(row.get("vidya_state") or "")
        if side == "LONG" and vidya_state != "BUY":
            continue
        if side == "SHORT" and vidya_state != "SELL":
            continue

        j = gi - 1
        neutral = 0
        while j >= 0 and str(frame.at[j, "range_filter_state"]) == "NEUTRAL":
            neutral += 1
            j -= 1
        if neutral < 1:
            continue
        pre_state = str(frame.at[j, "range_filter_state"]) if j >= 0 else ""
        entry_i = gi + 1
        if entry_i >= len(frame):
            continue

        reset_slice = frame.iloc[j + 1 : gi + 1]
        atr = float(frame.at[gi, "atr14"])
        if not math.isfinite(atr):
            continue
        entry_price = float(frame.at[entry_i, "open"])
        if side == "LONG":
            base_stop = min(
                float(reset_slice["low"].min()),
                float(reset_slice["vidya_line"].dropna().min()) if reset_slice["vidya_line"].notna().any() else float(reset_slice["low"].min()),
            )
            stop_price = base_stop - 0.10 * atr
            risk_abs = entry_price - stop_price
            ms_align = bool(row["ms_direction_bull"])
        else:
            base_stop = max(
                float(reset_slice["high"].max()),
                float(reset_slice["vidya_line"].dropna().max()) if reset_slice["vidya_line"].notna().any() else float(reset_slice["high"].max()),
            )
            stop_price = base_stop + 0.10 * atr
            risk_abs = stop_price - entry_price
            ms_align = not bool(row["ms_direction_bull"])
        if risk_abs <= 0:
            continue

        end_i = min(len(frame) - 1, entry_i + 180)
        chunk = frame.iloc[entry_i : end_i + 1]
        if chunk.empty:
            continue
        if side == "LONG":
            mfe_r = (float(chunk["high"].max()) - entry_price) / risk_abs
            mae_r = (float(chunk["low"].min()) - entry_price) / risk_abs
        else:
            mfe_r = (entry_price - float(chunk["low"].min())) / risk_abs
            mae_r = (entry_price - float(chunk["high"].max())) / risk_abs

        prev_high = row.get("prev_day_high")
        prev_low = row.get("prev_day_low")
        prev_mid = row.get("prev_day_mid")
        rows.append(
            {
                "signal_time": pd.Timestamp(row["ts"]),
                "entry_time": pd.Timestamp(frame.at[entry_i, "ts"]),
                "side": side,
                "vidya_state": vidya_state,
                "range_phase": row.get("range_filter_phase"),
                "neutral_bars": int(neutral),
                "pre_neutral_state": pre_state,
                "entry_price": float(entry_price),
                "stop_price": float(stop_price),
                "risk_abs": float(risk_abs),
                "ms_align": bool(ms_align),
                "prev_day_high": float(prev_high) if pd.notna(prev_high) else np.nan,
                "prev_day_low": float(prev_low) if pd.notna(prev_low) else np.nan,
                "prev_day_mid": float(prev_mid) if pd.notna(prev_mid) else np.nan,
                "entry_vs_prev_high_pct": ((entry_price - float(prev_high)) / float(prev_high) * 100.0) if pd.notna(prev_high) and float(prev_high) != 0.0 else np.nan,
                "entry_vs_prev_low_pct": ((entry_price - float(prev_low)) / float(prev_low) * 100.0) if pd.notna(prev_low) and float(prev_low) != 0.0 else np.nan,
                "entry_vs_prev_mid_pct": ((entry_price - float(prev_mid)) / float(prev_mid) * 100.0) if pd.notna(prev_mid) and float(prev_mid) != 0.0 else np.nan,
                "mfe_180_R": float(mfe_r),
                "mae_180_R": float(mae_r),
                "vidya_delta_pct": float(row.get("vidya_delta_pct")) if pd.notna(row.get("vidya_delta_pct")) else np.nan,
            }
        )
    return pd.DataFrame(rows)


def _summary_block(name: str, df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {"name": name, "count": 0}
    return {
        "name": name,
        "count": int(len(df)),
        "mfe_ge_1R": int((df["mfe_180_R"] >= 1.0).sum()),
        "mfe_ge_2R": int((df["mfe_180_R"] >= 2.0).sum()),
        "avg_mfe_R": float(df["mfe_180_R"].mean()),
        "avg_mae_R": float(df["mae_180_R"].mean()),
        "median_mfe_R": float(df["mfe_180_R"].median()),
        "median_mae_R": float(df["mae_180_R"].median()),
    }


def _build_payload(candidates: pd.DataFrame, *, symbol: str, warmup_start: pd.Timestamp, analysis_start: pd.Timestamp, analysis_end: pd.Timestamp) -> dict[str, Any]:
    payload = {
        "symbol": str(symbol).upper(),
        "warmup_start_utc": str(pd.Timestamp(warmup_start)),
        "analysis_start_utc": str(pd.Timestamp(analysis_start)),
        "analysis_end_utc": str(pd.Timestamp(analysis_end)),
        "candidate_count": int(len(candidates)),
        "summaries": [],
    }
    summaries = [
        _summary_block("all_candidates", candidates),
        _summary_block("ms_align", candidates[candidates["ms_align"]]),
        _summary_block(
            "long_support_reclaim_ms_align",
            candidates[
                (candidates["side"] == "LONG")
                & (candidates["ms_align"])
                & (candidates["entry_vs_prev_low_pct"] > 0.0)
                & (candidates["entry_vs_prev_mid_pct"] < 0.0)
            ],
        ),
        _summary_block(
            "short_below_prev_low_ms_align",
            candidates[(candidates["side"] == "SHORT") & (candidates["ms_align"]) & (candidates["entry_vs_prev_low_pct"] < 0.0)],
        ),
        _summary_block(
            "short_below_prev_mid_ms_align",
            candidates[(candidates["side"] == "SHORT") & (candidates["ms_align"]) & (candidates["entry_vs_prev_mid_pct"] < 0.0)],
        ),
        _summary_block(
            "long_above_prev_mid_ms_align",
            candidates[(candidates["side"] == "LONG") & (candidates["ms_align"]) & (candidates["entry_vs_prev_mid_pct"] > 0.0)],
        ),
    ]
    payload["summaries"] = summaries
    if not candidates.empty:
        preview_cols = [
            "signal_time",
            "entry_time",
            "side",
            "neutral_bars",
            "ms_align",
            "entry_vs_prev_high_pct",
            "entry_vs_prev_low_pct",
            "entry_vs_prev_mid_pct",
            "mfe_180_R",
            "mae_180_R",
        ]
        payload["candidate_preview"] = [
            {k: (v.isoformat() if isinstance(v, pd.Timestamp) else (None if pd.isna(v) else v)) for k, v in row.items()}
            for row in candidates[preview_cols].to_dict(orient="records")
        ]
    else:
        payload["candidate_preview"] = []
    return payload


def _render_report(payload: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Recent VIDYA + RF Neutral Re-entry Audit")
    lines.append("")
    lines.append(f"- symbol: `{payload['symbol']}`")
    lines.append(f"- warmup_start_utc: `{payload['warmup_start_utc']}`")
    lines.append(f"- analysis_start_utc: `{payload['analysis_start_utc']}`")
    lines.append(f"- analysis_end_utc: `{payload['analysis_end_utc']}`")
    lines.append(f"- candidate_count: `{payload['candidate_count']}`")
    lines.append("")
    lines.append("## Summaries")
    lines.append("")
    lines.append("| Slice | Count | MFE >= 1R | MFE >= 2R | Avg MFE R | Avg MAE R | Median MFE R | Median MAE R |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for row in payload.get("summaries") or []:
        if int(row.get("count") or 0) <= 0:
            lines.append(f"| `{row['name']}` | `0` | `0` | `0` | `nan` | `nan` | `nan` | `nan` |")
            continue
        lines.append(
            f"| `{row['name']}` | `{row['count']}` | `{row['mfe_ge_1R']}` | `{row['mfe_ge_2R']}` | "
            f"`{row['avg_mfe_R']:.3f}` | `{row['avg_mae_R']:.3f}` | `{row['median_mfe_R']:.3f}` | `{row['median_mae_R']:.3f}` |"
        )
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- Candidates are `Range Filter BUY/SELL` events that occur only after an immediate `NEUTRAL` reset and only when `VIDYA` agrees with the direction.")
    lines.append("- `ms_align` means the BigBeluga-style market structure direction does not veto the trade.")
    lines.append("- `prev_day_*` levels are previous New York day levels.")
    lines.append("- `long_support_reclaim_ms_align` means the long candidate reclaimed from above previous NY low but was still below previous NY midpoint on entry.")
    lines.append("- `mfe_180_R` / `mae_180_R` are forward 180-minute excursion metrics in units of the candidate stop-risk.")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze recent VIDYA + Range Filter neutral-reset re-entry candidates with NY levels and BigBeluga veto.")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--analysis-hours", type=int, default=36)
    parser.add_argument("--warmup-hours", type=int, default=48)
    parser.add_argument("--output-dir", default=None)
    args = parser.parse_args()

    latest_file = sorted(LOCAL_DAY_STORE.glob("BTCUSDT_LAST_*.parquet"))[-1]
    latest_df = pd.read_parquet(latest_file, columns=["open_time"])
    latest_ts = pd.to_datetime(latest_df["open_time"], utc=True).max()
    analysis_end = pd.Timestamp(latest_ts)
    analysis_start = analysis_end - pd.Timedelta(hours=int(args.analysis_hours))
    warmup_start = analysis_start - pd.Timedelta(hours=int(args.warmup_hours))

    raw = _load_local_1m(str(args.symbol).upper(), warmup_start.floor("D"), analysis_end)
    frame = _prepare_frame(raw)
    candidates = _candidate_rows(frame, analysis_start)

    stamp = pd.Timestamp.now(tz="UTC").strftime("%Y%m%d_%H%M%S")
    output_dir = Path(args.output_dir) if args.output_dir else DEFAULT_OUTPUT_ROOT / f"vidya_rf_neutral_reentry_{stamp}"
    output_dir.mkdir(parents=True, exist_ok=True)

    candidates.to_csv(output_dir / "candidates.csv", index=False)
    payload = _build_payload(
        candidates,
        symbol=str(args.symbol).upper(),
        warmup_start=warmup_start,
        analysis_start=analysis_start,
        analysis_end=analysis_end,
    )
    (output_dir / "summary.json").write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    (output_dir / "report.md").write_text(_render_report(payload), encoding="utf-8")

    if LATEST_MIRROR.exists() or LATEST_MIRROR.is_symlink():
        if LATEST_MIRROR.is_symlink() or LATEST_MIRROR.is_file():
            LATEST_MIRROR.unlink()
        else:
            shutil.rmtree(LATEST_MIRROR)
    shutil.copytree(output_dir, LATEST_MIRROR)
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
