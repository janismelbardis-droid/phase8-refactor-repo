from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests
import numpy as np


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKSPACE_ROOT = REPO_ROOT.parents[1]
DEFAULT_PACKAGE_ZIP = Path.home() / "Downloads" / "btc_rf_body_pullback_research_package_2026-05-03.zip"
DEFAULT_DOSSIER_ZIP = Path.home() / "Downloads" / "btc_rf_body_pullback_master_dossier_2026_05_04.zip"
DEFAULT_RUN_ROOT = Path(r"C:\mnt\data")
DEFAULT_RESULTS_ROOT = REPO_ROOT / "runs" / "perf" / "rf_body_pullback_dossier_reproduction_latest"
BINANCE_MONTHLY_ROOT = "https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/1m"
LOCAL_DAY_STORE = REPO_ROOT / "data_cache" / "ohlcv_store" / "BTCUSDT" / "LAST" / "1m"


@dataclass(frozen=True)
class ModuleSpec:
    name: str
    source: str


MODULE_SPECS = (
    ModuleSpec("rf_body_pullback_search.py", "04_scripts_selected/rf_body_pullback_search.py"),
    ModuleSpec("rf_body_pullback_1m_management.py", "04_scripts_selected/rf_body_pullback_1m_management.py"),
)

NUMBA_FALLBACK = """try:
    from numba import njit
except ModuleNotFoundError:
    def njit(*args, **kwargs):
        def decorator(fn):
            return fn
        return decorator
"""


def _extract_package_file(package_zip: Path, member_name: str) -> str:
    with zipfile.ZipFile(package_zip) as zf:
        prefix = "btc_rf_body_pullback_research_package/"
        target = prefix + member_name.replace("\\", "/")
        with zf.open(target) as handle:
            return handle.read().decode("utf-8")


def _extract_nested_member(package_zip: Path, nested_zip_member: str, member_name: str) -> str:
    with zipfile.ZipFile(package_zip) as outer:
        prefix = "btc_rf_body_pullback_research_package/"
        with outer.open(prefix + nested_zip_member.replace("\\", "/")) as nested_handle:
            data = nested_handle.read()
    nested_zip_path = Path(__file__).with_name("_tmp_nested_rf_event_search.zip")
    nested_zip_path.write_bytes(data)
    try:
        with zipfile.ZipFile(nested_zip_path) as nested:
            with nested.open(member_name) as handle:
                return handle.read().decode("utf-8")
    finally:
        nested_zip_path.unlink(missing_ok=True)


def _ensure_original_modules(package_zip: Path, run_root: Path) -> None:
    run_root.mkdir(parents=True, exist_ok=True)
    for spec in MODULE_SPECS:
        text = _extract_package_file(package_zip, spec.source)
        text = text.replace("from numba import njit", NUMBA_FALLBACK.rstrip())
        (run_root / spec.name).write_text(text, encoding="utf-8")
    event_text = _extract_nested_member(
        package_zip,
        "03_results_zips/rf_body_pullback_tight_results.zip",
        "rf_body_pullback_event_search.py",
    )
    event_text = event_text.replace("from numba import njit", NUMBA_FALLBACK.rstrip())
    (run_root / "rf_body_pullback_event_search.py").write_text(event_text, encoding="utf-8")


def _month_iter(start_month: str, end_month: str) -> Iterable[pd.Timestamp]:
    cursor = pd.Timestamp(f"{start_month}-01", tz="UTC")
    end = pd.Timestamp(f"{end_month}-01", tz="UTC")
    while cursor <= end:
        yield cursor
        cursor += pd.offsets.MonthBegin(1)


def _download_month_zip(month: pd.Timestamp, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    file_name = f"BTCUSDT-1m-{month:%Y-%m}.zip"
    out_path = out_dir / file_name
    if out_path.exists():
        return out_path
    url = f"{BINANCE_MONTHLY_ROOT}/{file_name}"
    with requests.get(url, timeout=60) as response:
        response.raise_for_status()
        out_path.write_bytes(response.content)
    return out_path


def _read_month_zip(zip_path: Path) -> pd.DataFrame:
    columns = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_volume",
        "trade_count",
        "taker_buy_volume",
        "taker_buy_quote_volume",
        "ignore",
    ]
    with zipfile.ZipFile(zip_path) as zf:
        csv_name = zip_path.stem + ".csv"
        with zf.open(csv_name) as handle:
            df = pd.read_csv(handle, header=None, names=columns)
    df["open_time"] = pd.to_numeric(df["open_time"], errors="coerce")
    df = df[df["open_time"].notna()].copy()
    df = df[["open_time", "open", "high", "low", "close", "volume"]].copy()
    df["dt"] = pd.to_datetime(df["open_time"].astype("int64") * 1_000_000, utc=True)
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)
    return df[["dt", "open", "high", "low", "close", "volume"]]


def _build_1m_dataset(run_root: Path, start_month: str, end_month: str) -> Path:
    cache_dir = run_root / "btc_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    out_path = cache_dir / "BTCUSDT_1m_2022_2026.pkl"
    if out_path.exists() and out_path.stat().st_size > 0:
        try:
            cached = pd.read_pickle(out_path)
            if str(cached["dt"].dtype) == "datetime64[ns, UTC]":
                return out_path
        except Exception:
            pass
    out_path.unlink(missing_ok=True)
    zip_dir = run_root / "monthly_zips"
    frames = []
    for month in _month_iter(start_month, end_month):
        zip_path = _download_month_zip(month, zip_dir)
        frames.append(_read_month_zip(zip_path))
    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(subset=["dt"]).sort_values("dt").reset_index(drop=True)
    df.to_pickle(out_path)
    return out_path


def _resample_from_1m(df_1m: pd.DataFrame, rule: str) -> pd.DataFrame:
    agg = (
        df_1m.set_index("dt")
        .resample(rule, label="left", closed="left")
        .agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }
        )
        .dropna()
        .reset_index()
    )
    return agg


def _ensure_resampled_pickles(run_root: Path) -> None:
    cache_dir = run_root / "btc_cache"
    df_1m = pd.read_pickle(cache_dir / "BTCUSDT_1m_2022_2026.pkl")
    tf_map = {
        "5m": "5min",
        "15m": "15min",
        "1h": "1h",
        "4h": "4h",
    }
    for tf_name, rule in tf_map.items():
        out_path = cache_dir / f"BTCUSDT_{tf_name}_2022_2026.pkl"
        if out_path.exists():
            try:
                cached = pd.read_pickle(out_path)
                if str(cached["dt"].dtype) == "datetime64[ns, UTC]":
                    continue
            except Exception:
                pass
            out_path.unlink(missing_ok=True)
        _resample_from_1m(df_1m, rule).to_pickle(out_path)


def _append_local_tail(run_root: Path) -> bool:
    cache_dir = run_root / "btc_cache"
    out_path = cache_dir / "BTCUSDT_1m_2022_2026.pkl"
    df_1m = pd.read_pickle(out_path).copy()
    last_dt = pd.to_datetime(df_1m["dt"].iloc[-1], utc=True)
    if not LOCAL_DAY_STORE.exists():
        return False
    frames = []
    for parquet_path in sorted(LOCAL_DAY_STORE.glob("BTCUSDT_LAST_2026*.parquet")):
        day_df = pd.read_parquet(parquet_path, columns=["open_time", "open", "high", "low", "close", "volume"])
        day_df = day_df.rename(columns={"open_time": "dt"})
        day_df["dt"] = pd.to_datetime(day_df["dt"], utc=True)
        day_df = day_df[day_df["dt"] > last_dt]
        if not day_df.empty:
            frames.append(day_df[["dt", "open", "high", "low", "close", "volume"]])
    if not frames:
        return False
    df_tail = pd.concat(frames, ignore_index=True)
    merged = pd.concat([df_1m, df_tail], ignore_index=True)
    merged = merged.drop_duplicates(subset=["dt"]).sort_values("dt").reset_index(drop=True)
    merged.to_pickle(out_path)
    for tf_name in ("5m", "15m", "1h", "4h"):
        (cache_dir / f"BTCUSDT_{tf_name}_2022_2026.pkl").unlink(missing_ok=True)
    return True


def _load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module {module_name} from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _official_summary_from_dossier(dossier_zip: Path) -> dict:
    with zipfile.ZipFile(dossier_zip) as zf:
        member = "btc_rf_body_pullback_master_dossier_2026_05_04/selected_csv/targeted_summary.csv"
        with zf.open(member) as handle:
            df = pd.read_csv(handle)
    row = df[df["name"] == "main_1m_emergency"].iloc[0]
    return {k: row[k] for k in df.columns}


def _run_reproduction(run_root: Path, cutoff_dt: pd.Timestamp | None = None) -> tuple[dict, pd.DataFrame, pd.DataFrame]:
    if str(run_root) not in sys.path:
        sys.path.insert(0, str(run_root))
    search = _load_module("rf_body_pullback_search", run_root / "rf_body_pullback_search.py")
    event = _load_module("rf_body_pullback_event_search", run_root / "rf_body_pullback_event_search.py")
    mgmt = _load_module("rf_body_pullback_1m_management", run_root / "rf_body_pullback_1m_management.py")

    df5, arr5 = event.prepare("5m")
    if cutoff_dt is not None:
        cutoff_dt = pd.to_datetime(cutoff_dt, utc=True)
        mask5 = df5["dt"] <= cutoff_dt
        df5 = df5.loc[mask5].reset_index(drop=True)
        arr5 = tuple(x[mask5.to_numpy()] for x in arr5)
    base_5m_arrays = arr5[:16]
    (
        _o5,
        _h5,
        _l5,
        c5,
        body5,
        atr5,
        adx5,
        ema20_5,
        ema50_5,
        ema200_5,
        ema50s5,
        h1_close,
        h1_ema50,
        h1_ema200,
        h1_slope,
        h1_adx,
    ) = base_5m_arrays
    t5 = df5["dt"].astype("int64").to_numpy()
    rf5_state, sig_idx, sig_side = event.make_signals(c5, 80, 3.0)

    df1 = search.add_features(search.load_tf("1m"))
    if cutoff_dt is not None:
        mask1 = df1["dt"] <= cutoff_dt
        df1 = df1.loc[mask1].reset_index(drop=True)
    t1 = df1["dt"].astype("int64").to_numpy()
    o1 = df1.open.to_numpy(dtype=float)
    h1a = df1.high.to_numpy(dtype=float)
    l1a = df1.low.to_numpy(dtype=float)
    c1 = df1.close.to_numpy(dtype=float)
    ema20_1 = df1.ema20.to_numpy(dtype=float)
    ema50_1 = df1.ema50.to_numpy(dtype=float)
    ema200_1 = df1.ema200.to_numpy(dtype=float)
    ema50s1 = df1.ema50_slope5.to_numpy(dtype=float)
    adx1 = df1.adx14.to_numpy(dtype=float)
    rf1_state = search.range_filter_state(c1, 80, 3.0)[0].astype("int8")

    params = dict(
        body_mult=0.65,
        min_body_atr=0.25,
        max_body_atr=2.0,
        expiry_bars=4,
        sl_pct=0.015,
        rr=3.0,
        max_hold_bars=192,
        entry_adx_min=25,
        h1_adx_min=28,
        fee=search.FEE,
        early_mode=1,
        adverse_r=0.65,
        micro_adx_min=25.0,
        micro_confirm_bars=1,
    )

    ret, dd, trades, wins, pf, fills, expired, canceled, micro_exits = mgmt.simulate_5m_with_1m_exit(
        t5,
        *_to_float_arrays(base_5m_arrays),
        rf5_state,
        sig_idx,
        sig_side,
        t1,
        o1,
        h1a,
        l1a,
        c1,
        ema20_1,
        ema50_1,
        ema200_1,
        ema50s1,
        adx1,
        rf1_state,
        params["body_mult"],
        params["min_body_atr"],
        params["max_body_atr"],
        params["expiry_bars"],
        params["sl_pct"],
        params["rr"],
        params["max_hold_bars"],
        params["entry_adx_min"],
        params["h1_adx_min"],
        params["fee"],
        params["early_mode"],
        params["adverse_r"],
        params["micro_adx_min"],
        params["micro_confirm_bars"],
        100000,
    )
    rec = mgmt.record_5m_with_1m_exit(
        t5,
        *_to_float_arrays(base_5m_arrays),
        rf5_state,
        sig_idx,
        sig_side,
        t1,
        o1,
        h1a,
        l1a,
        c1,
        ema20_1,
        ema50_1,
        ema200_1,
        ema50s1,
        adx1,
        rf1_state,
        params["body_mult"],
        params["min_body_atr"],
        params["max_body_atr"],
        params["expiry_bars"],
        params["sl_pct"],
        params["rr"],
        params["max_hold_bars"],
        params["entry_adx_min"],
        params["h1_adx_min"],
        params["fee"],
        params["early_mode"],
        params["adverse_r"],
        params["micro_adx_min"],
        params["micro_confirm_bars"],
        100000,
    )
    trades_df = mgmt.build_df_from_rec(rec, df5, df1)
    if trades_df.empty:
        raise RuntimeError(
            "Reproduction produced zero trades. "
            f"simulate_5m_with_1m_exit -> trades={trades}, fills={fills}, expired={expired}, canceled={canceled}."
        )
    stats, curve, yearly = mgmt.equity_stats(trades_df)
    stats.update(
        {
            "fills": int(fills),
            "expired": int(expired),
            "canceled": int(canceled),
            "micro_exits": int(micro_exits),
            "sim_return_pct": float(ret * 100.0),
            "sim_max_dd_pct": float(dd * 100.0),
            "sim_trades": int(trades),
            "sim_winrate_pct": float((wins / trades) * 100.0 if trades else 0.0),
            "sim_pf": float(pf),
        }
    )
    return stats, trades_df, yearly


def _to_float_arrays(arrays: tuple) -> tuple:
    return tuple(x if getattr(x, "dtype", None) == float else x.astype(float) for x in arrays)


def _write_outputs(results_root: Path, official: dict, reproduced: dict, trades_df: pd.DataFrame, yearly: pd.DataFrame) -> None:
    results_root.mkdir(parents=True, exist_ok=True)
    (results_root / "official_summary.json").write_text(json.dumps(_json_ready(official), indent=2), encoding="utf-8")
    (results_root / "reproduced_summary.json").write_text(json.dumps(_json_ready(reproduced), indent=2), encoding="utf-8")
    trades_df.to_csv(results_root / "reproduced_trades.csv", index=False)
    yearly.to_csv(results_root / "reproduced_yearly.csv", index=False)

    compare = {
        "return_pct_gap": float(reproduced["return_pct"] - float(official["return_pct"])),
        "max_dd_pct_gap": float(reproduced["max_dd_pct"] - float(official["max_dd_pct"])),
        "trades_gap": int(reproduced["trades"] - int(official["trades"])),
        "winrate_pct_gap": float(reproduced["winrate_pct"] - float(official["winrate_pct"])),
        "pf_gap": float(reproduced["pf"] - float(official["pf"])),
        "micro_exits_gap": int(reproduced["micro_exits"] - int(official["micro_exits"])),
        "expired_gap": int(reproduced["expired"] - int(official["expired"])),
        "canceled_gap": int(reproduced["canceled"] - int(official["canceled"])),
    }
    (results_root / "compare.json").write_text(json.dumps(_json_ready(compare), indent=2), encoding="utf-8")

    lines = [
        "# RF Body-Pullback Dossier Reproduction",
        "",
        "## Official",
        "",
        f"- return_pct: `{official['return_pct']}`",
        f"- max_dd_pct: `{official['max_dd_pct']}`",
        f"- trades: `{official['trades']}`",
        f"- winrate_pct: `{official['winrate_pct']}`",
        f"- pf: `{official['pf']}`",
        f"- fills/expired/canceled/micro_exits: `{official['fills']}/{official['expired']}/{official['canceled']}/{official['micro_exits']}`",
        "",
        "## Reproduced",
        "",
        f"- return_pct: `{reproduced['return_pct']}`",
        f"- max_dd_pct: `{reproduced['max_dd_pct']}`",
        f"- trades: `{reproduced['trades']}`",
        f"- winrate_pct: `{reproduced['winrate_pct']}`",
        f"- pf: `{reproduced['pf']}`",
        f"- fills/expired/canceled/micro_exits: `{reproduced['fills']}/{reproduced['expired']}/{reproduced['canceled']}/{reproduced['micro_exits']}`",
        "",
        "## Gaps",
        "",
        f"- return_pct_gap: `{compare['return_pct_gap']}`",
        f"- max_dd_pct_gap: `{compare['max_dd_pct_gap']}`",
        f"- trades_gap: `{compare['trades_gap']}`",
        f"- winrate_pct_gap: `{compare['winrate_pct_gap']}`",
        f"- pf_gap: `{compare['pf_gap']}`",
        f"- micro_exits_gap: `{compare['micro_exits_gap']}`",
        f"- expired_gap: `{compare['expired_gap']}`",
        f"- canceled_gap: `{compare['canceled_gap']}`",
    ]
    (results_root / "report.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Reproduce the archived RF body-pullback dossier baseline.")
    parser.add_argument("--package-zip", default=str(DEFAULT_PACKAGE_ZIP))
    parser.add_argument("--dossier-zip", default=str(DEFAULT_DOSSIER_ZIP))
    parser.add_argument("--run-root", default=str(DEFAULT_RUN_ROOT))
    parser.add_argument("--results-root", default=str(DEFAULT_RESULTS_ROOT))
    parser.add_argument("--start-month", default="2022-01")
    parser.add_argument("--end-month", default="2026-04")
    parser.add_argument("--append-local-tail", action="store_true")
    parser.add_argument("--cutoff-dt", default=None)
    args = parser.parse_args()

    package_zip = Path(args.package_zip).resolve()
    dossier_zip = Path(args.dossier_zip).resolve()
    run_root = Path(args.run_root)
    results_root = Path(args.results_root).resolve()

    _ensure_original_modules(package_zip, run_root)
    _build_1m_dataset(run_root, args.start_month, args.end_month)
    if args.append_local_tail:
        _append_local_tail(run_root)
    _ensure_resampled_pickles(run_root)
    official = _official_summary_from_dossier(dossier_zip)
    cutoff_dt = pd.to_datetime(args.cutoff_dt, utc=True) if args.cutoff_dt else None
    reproduced, trades_df, yearly = _run_reproduction(run_root, cutoff_dt=cutoff_dt)
    _write_outputs(results_root, official, reproduced, trades_df, yearly)
    print(json.dumps(_json_ready(reproduced), indent=2))
    return 0


def _json_ready(value):
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_ready(v) for v in value]
    if isinstance(value, tuple):
        return [_json_ready(v) for v in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return float(value)
    return value


if __name__ == "__main__":
    raise SystemExit(main())
