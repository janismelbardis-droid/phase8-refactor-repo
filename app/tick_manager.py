# tick_manager.py
# Standalone Tick Manager (Binance USD‑M Futures aggTrades daily cache)
#
# Why this exists
#  - Tick (aggTrades) backtests can require millions of records for liquid symbols.
#  - Downloading those inside the main GUI repeatedly is slow and fragile.
#  - This tool downloads once and caches per UTC day, so the main app can reuse the data.
#
# Cache layout (shared with main backtest script):
#   {cache_dir}/aggtrades_futures_daily/{SYMBOL}/{YYYY-MM-DD}.jsonl.gz
#   {cache_dir}/aggtrades_futures_daily/{SYMBOL}/{YYYY-MM-DD}.meta.json
#
# Notes
#  - "Today" is always considered OPEN (in-progress). You can update it to append new ticks.
#  - Past days should become FINAL once the day is fully downloaded.
#
# Requirements:
#   pip install pandas requests

from __future__ import annotations

import os
import sys
import argparse
import json
import gzip
import time
import random
import hashlib
import shutil
import collections
import queue
import threading
import traceback
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple

try:
    import tkinter as tk
    from tkinter import ttk, messagebox, filedialog
    from tkinter import scrolledtext
except Exception:
    print("Tkinter is missing or broken on this Python installation.")
    raise

try:
    import pandas as pd
except Exception as e:
    raise SystemExit("pandas is required for tick_manager.py (pip install pandas).") from e

try:
    import requests
except Exception as e:
    raise SystemExit("requests is required for tick_manager.py (pip install requests).") from e

BINANCE_FAPI_BASE = "https://fapi.binance.com"
AGGTRADES_DAILY_SUBDIR = "aggtrades_futures_daily"
SCHEMA_VERSION = 2

# Throttling defaults (Binance public REST can rate-limit / temporarily ban on aggressive paging)
SUCCESS_SLEEP_S = 0.00  # pacing is controlled by the RateLimiter (req/min); keep 0 here
DAY_PAUSE_S = 1.5        # pause between days when downloading many



def _project_root_dir() -> str:
    """Return the project root directory (parent of the 'app' package).

    This file lives in .../app/tick_manager.py, so project root is one level up.
    """
    here = os.path.abspath(os.path.dirname(__file__))
    return os.path.abspath(os.path.join(here, os.pardir))


def _default_cache_dir() -> str:
    """Default cache directory shared with the main app."""
    d = os.path.join(_project_root_dir(), "data_cache")
    try:
        os.makedirs(d, exist_ok=True)
    except Exception:
        pass
    return d


def _coerce_cache_dir(user_value: str) -> str:
    """Normalize cache dir and ensure it exists."""
    v = (user_value or "").strip()
    if not v:
        v = _default_cache_dir()
    v = os.path.abspath(v)
    try:
        os.makedirs(v, exist_ok=True)
    except Exception:
        pass
    return v
def _sleep_cancel(total_s: float, stop_evt: Optional[threading.Event] = None):
    """Sleep in small increments so the Stop button can interrupt long backoffs."""
    try:
        total_s = float(total_s)
    except Exception:
        total_s = 0.0
    if total_s <= 0:
        return
    end = time.monotonic() + total_s
    while True:
        if stop_evt is not None and stop_evt.is_set():
            break
        now = time.monotonic()
        if now >= end:
            break
        time.sleep(min(0.25, end - now))


class RateLimiter:
    """Simple, conservative client-side pacing to avoid Binance 429/418.

    Binance aggTrades pagination for liquid symbols can require thousands of requests per day.
    This limiter enforces BOTH:
      - a minimum interval between requests, and
      - a maximum number of requests in any rolling 60s window.

    It can also (optionally) react to Binance weight headers if present.
    """

    def __init__(self, req_per_min: float = 20.0, jitter_s: float = 0.05):
        self.jitter_s = float(jitter_s) if jitter_s is not None else 0.0
        self.recent = collections.deque()
        self.last_req = 0.0
        self.used_weight_1m: Optional[int] = None
        self.set_rate(req_per_min)

    def set_rate(self, req_per_min: float):
        try:
            rpm = float(req_per_min)
        except Exception:
            rpm = 20.0
        rpm = max(5.0, min(300.0, rpm))
        self.req_per_min = rpm
        self.max_per_60 = max(1, int(round(rpm)))
        self.min_interval = 60.0 / rpm

    def wait(self, stop_evt: Optional[threading.Event] = None):
        # Minimum interval pacing
        target = self.last_req + self.min_interval + (random.uniform(0.0, self.jitter_s) if self.jitter_s > 0 else 0.0)
        now = time.monotonic()
        if target > now:
            _sleep_cancel(target - now, stop_evt)

        # Rolling 60s window pacing (avoid bursts)
        now = time.monotonic()
        while self.recent and (now - self.recent[0]) > 60.0:
            self.recent.popleft()
        if len(self.recent) >= self.max_per_60:
            extra = 60.0 - (now - self.recent[0]) + 0.15
            _sleep_cancel(max(0.15, extra), stop_evt)
            now = time.monotonic()
            while self.recent and (now - self.recent[0]) > 60.0:
                self.recent.popleft()

        # Record this request time
        t = time.monotonic()
        self.recent.append(t)
        self.last_req = t

    def on_rate_limit(self, status_code: int):
        # If Binance rate-limits us, slow down proactively so we stop triggering 429/418.
        try:
            status_code = int(status_code)
        except Exception:
            status_code = 429
        factor = 2.4 if status_code == 418 else 1.7
        self.min_interval = min(12.0, self.min_interval * factor)
        self.max_per_60 = max(1, int(60.0 / self.min_interval))

    def observe_headers(self, headers: Dict[str, Any], stop_evt: Optional[threading.Event] = None, log_cb=None):
        """If Binance sends weight headers, cool down near the limit.

        Futures responses sometimes include X-MBX-USED-WEIGHT-1M-like headers.
        If present and high, we pause until the next minute boundary.
        """
        try:
            used = None
            for k, v in (headers or {}).items():
                lk = str(k).lower()
                if "used-weight" in lk and "1m" in lk:
                    try:
                        used = int(v)
                        break
                    except Exception:
                        continue
            if used is None:
                return
            self.used_weight_1m = used

            # These thresholds are conservative; the goal is to avoid hitting 429 at all.
            if used >= 2200:
                # Sleep roughly until the next minute boundary
                sec = time.time()
                to_next = 60.0 - (sec % 60.0) + 0.25
                if log_cb:
                    try:
                        log_cb(f"Weight 1m={used} — cooling down {to_next:.1f}s to stay under limits…")
                    except Exception:
                        pass
                _sleep_cancel(to_next, stop_evt)
                # Also slow baseline pacing a bit
                self.min_interval = max(self.min_interval, 2.0)
                self.max_per_60 = max(1, int(60.0 / self.min_interval))
        except Exception:
            return


BAN_418_MIN_SLEEP_S = 120.0  # 418 often means temporary IP ban; wait longer
EDGE_TOL_MS = 15 * 60 * 1000  # tolerance when deciding if a day looks complete

# -----------------------------
# Time helpers (robust across pandas versions)
# -----------------------------
def _utc_now() -> "pd.Timestamp":
    """
    pandas behaviour changed across versions:
      - some versions return tz-naive Timestamp.utcnow()
      - some return tz-aware
    This helper always returns tz-aware UTC safely.
    """
    try:
        return pd.Timestamp.now(tz="UTC")
    except Exception:
        ts = pd.Timestamp.utcnow()
        try:
            if ts.tzinfo is None:
                return ts.tz_localize("UTC")
            return ts.tz_convert("UTC")
        except Exception:
            # very defensive fallback
            return pd.to_datetime(time.time(), unit="s", utc=True)

def parse_utc(s: str) -> "pd.Timestamp":
    s = (s or "").strip()
    if not s:
        raise ValueError("Empty datetime")
    ts = pd.to_datetime(s, utc=True, errors="raise")
    if getattr(ts, "tzinfo", None) is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts

def _day_floor_utc(ts: "pd.Timestamp") -> "pd.Timestamp":
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts.floor("D")

def _iter_days_utc(start_utc: "pd.Timestamp", end_utc: "pd.Timestamp") -> List["pd.Timestamp"]:
    # end is exclusive
    start = _day_floor_utc(start_utc)
    end = end_utc.tz_convert("UTC") if end_utc.tzinfo else end_utc.tz_localize("UTC")
    if end <= start:
        return [start]
    last_day = (end - pd.Timedelta(microseconds=1)).floor("D")
    out = []
    cur = start
    while cur <= last_day:
        out.append(cur)
        cur = cur + pd.Timedelta(days=1)
    return out

def _day_bounds_ms(day_utc: "pd.Timestamp") -> Tuple[int, int, str]:
    d0 = _day_floor_utc(day_utc)
    d1 = d0 + pd.Timedelta(days=1)
    return int(d0.value // 1_000_000), int(d1.value // 1_000_000), d0.strftime("%Y-%m-%d")

# -----------------------------
# Cache paths + meta
# -----------------------------
def _ensure_dir(path: str):
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass

def _sym_folder(cache_dir: str, symbol: str) -> str:
    symbol = (symbol or "").upper().strip()
    folder = os.path.join(cache_dir, AGGTRADES_DAILY_SUBDIR, symbol)
    _ensure_dir(folder)
    return folder

def day_path(cache_dir: str, symbol: str, day_utc: "pd.Timestamp") -> str:
    _, _, day_s = _day_bounds_ms(day_utc)
    return os.path.join(_sym_folder(cache_dir, symbol), f"{day_s}.jsonl.gz")

def meta_path(day_file_path: str) -> str:
    if day_file_path.endswith(".jsonl.gz"):
        base = day_file_path[:-len(".jsonl.gz")]
    else:
        base = day_file_path
    return base + ".meta.json"

def _read_meta(mp: str) -> Optional[Dict[str, Any]]:
    try:
        with open(mp, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def _write_meta_atomic(mp: str, meta: Dict[str, Any]):
    tmp = mp + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, sort_keys=True)
    try:
        os.replace(tmp, mp)
    except Exception:
        try:
            if os.path.exists(mp):
                os.remove(mp)
            os.rename(tmp, mp)
        except Exception:
            pass

def _format_ts_ms(ms: Optional[int]) -> str:
    if ms is None:
        return ""
    try:
        return pd.Timestamp(ms, unit="ms", tz="UTC").strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""

def _file_size_mb(p: str) -> str:
    try:
        return f"{(os.path.getsize(p) / (1024*1024)):.2f}"
    except Exception:
        return ""


def _fsync_file_handle(f) -> None:
    try:
        f.flush()
    except Exception:
        pass
    try:
        os.fsync(f.fileno())
    except Exception:
        pass


def _fsync_dir(path: str) -> None:
    if not path:
        return
    try:
        fd = os.open(path, os.O_RDONLY)
    except Exception:
        return
    try:
        os.fsync(fd)
    except Exception:
        pass
    finally:
        try:
            os.close(fd)
        except Exception:
            pass


def _atomic_replace_strict(src: str, dst: str) -> None:
    if not os.path.exists(src):
        raise FileNotFoundError(f"Source file missing for atomic replace: {src}")
    os.replace(src, dst)
    if not os.path.exists(dst):
        raise OSError(f"Atomic replace failed, destination missing: {dst}")
    _fsync_dir(os.path.dirname(dst) or os.getcwd())


def _write_meta_atomic(mp: str, meta: Dict[str, Any]):
    tmp = mp + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, sort_keys=True)
        _fsync_file_handle(f)
    _atomic_replace_strict(tmp, mp)


def _sha256_file(path: str, stop_evt: Optional[threading.Event] = None) -> Optional[str]:
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            while True:
                if stop_evt is not None and stop_evt.is_set():
                    return None
                chunk = f.read(1024 * 1024)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None


def _meta_from_verification(
    symbol: str,
    day_utc: "pd.Timestamp",
    report: Dict[str, Any],
    *,
    status_override: Optional[str] = None,
    note_override: Optional[str] = None,
) -> Dict[str, Any]:
    day_start_ms, day_end_ms, day_s = _day_bounds_ms(day_utc)
    verify_status = str(report.get("status") or "UNKNOWN").upper().strip()
    status = verify_status
    if status_override and verify_status in ("FINAL", "PARTIAL", "OPEN"):
        status = str(status_override).upper().strip()
    note = note_override if note_override is not None else report.get("note")
    now = _utc_now()
    return {
        "schema": SCHEMA_VERSION,
        "symbol": (symbol or "").upper().strip(),
        "day": day_s,
        "status": status,
        "rows": int(report.get("rows") or 0),
        "first_id": int(report["first_id"]) if report.get("first_id") is not None else None,
        "last_id": int(report["last_id"]) if report.get("last_id") is not None else None,
        "first_ts_ms": int(report["first_ts_ms"]) if report.get("first_ts_ms") is not None else None,
        "last_ts_ms": int(report["last_ts_ms"]) if report.get("last_ts_ms") is not None else None,
        "day_start_ms": int(day_start_ms),
        "day_end_ms": int(day_end_ms),
        "created_utc": now.isoformat(),
        "updated_utc": now.isoformat(),
        "verified_utc": now.isoformat(),
        "verify_status": verify_status,
        "verify_note": note or "",
        "bad_lines": int(report.get("bad_lines") or 0),
        "out_of_bounds": int(report.get("out_of_bounds") or 0),
        "non_monotonic_id_count": int(report.get("non_monotonic_id_count") or 0),
        "non_monotonic_ts_count": int(report.get("non_monotonic_ts_count") or 0),
        "complete_start": bool(report.get("complete_start", False)),
        "complete_end": bool(report.get("complete_end", False)),
        "file_size_bytes": int(report.get("file_size_bytes") or 0),
        "sha256": report.get("sha256"),
    }


def _verify_day_file(
    file_path: str,
    symbol: str,
    day_utc: "pd.Timestamp",
    *,
    compute_hash: bool = False,
    stop_evt: Optional[threading.Event] = None,
    log_cb=None,
) -> Dict[str, Any]:
    day_start_ms, day_end_ms, day_s = _day_bounds_ms(day_utc)
    out: Dict[str, Any] = {
        "path": file_path,
        "symbol": (symbol or "").upper().strip(),
        "day": day_s,
        "status": "MISSING",
        "note": "file_missing",
        "rows": 0,
        "first_id": None,
        "last_id": None,
        "first_ts_ms": None,
        "last_ts_ms": None,
        "bad_lines": 0,
        "out_of_bounds": 0,
        "non_monotonic_id_count": 0,
        "non_monotonic_ts_count": 0,
        "complete_start": False,
        "complete_end": False,
        "file_size_bytes": 0,
        "sha256": None,
    }

    def _log(msg: str) -> None:
        if log_cb:
            try:
                log_cb(msg)
            except Exception:
                pass

    if not file_path or not os.path.isfile(file_path):
        return out

    try:
        out["file_size_bytes"] = int(os.path.getsize(file_path))
    except Exception:
        out["file_size_bytes"] = 0
    if int(out["file_size_bytes"] or 0) <= 0:
        out["status"] = "EMPTY"
        out["note"] = "empty_file"
        return out

    prev_id = None
    prev_ts = None
    try:
        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                if stop_evt is not None and stop_evt.is_set():
                    out["status"] = "CANCELLED"
                    out["note"] = "verification_cancelled"
                    return out
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                    a = int(d.get("a"))
                    t = int(d.get("T"))
                except Exception:
                    out["bad_lines"] = int(out.get("bad_lines") or 0) + 1
                    continue

                if t < day_start_ms or t >= day_end_ms:
                    out["out_of_bounds"] = int(out.get("out_of_bounds") or 0) + 1
                    continue

                if out["first_id"] is None:
                    out["first_id"] = a
                    out["first_ts_ms"] = t
                out["last_id"] = a
                out["last_ts_ms"] = t
                out["rows"] = int(out.get("rows") or 0) + 1

                if prev_id is not None and a <= prev_id:
                    out["non_monotonic_id_count"] = int(out.get("non_monotonic_id_count") or 0) + 1
                if prev_ts is not None and t < prev_ts:
                    out["non_monotonic_ts_count"] = int(out.get("non_monotonic_ts_count") or 0) + 1
                prev_id = a
                prev_ts = t
    except EOFError as e:
        out["status"] = "CORRUPT_GZIP"
        out["note"] = f"truncated_gzip:{e}"
        return out
    except OSError as e:
        out["status"] = "CORRUPT_GZIP"
        out["note"] = f"gzip_error:{e}"
        return out
    except Exception as e:
        out["status"] = "CORRUPT_READ"
        out["note"] = str(e)
        return out

    if compute_hash:
        out["sha256"] = _sha256_file(file_path, stop_evt=stop_evt)

    rows = int(out.get("rows") or 0)
    if rows <= 0:
        out["status"] = "EMPTY"
        out["note"] = "no_in_bounds_rows"
        return out
    if int(out.get("bad_lines") or 0) > 0:
        out["status"] = "CORRUPT_BAD_LINES"
        out["note"] = f"bad_lines={int(out.get('bad_lines') or 0)}"
        return out
    if int(out.get("out_of_bounds") or 0) > 0:
        out["status"] = "CORRUPT_SPILLOVER"
        out["note"] = f"out_of_bounds={int(out.get('out_of_bounds') or 0)}"
        return out
    if int(out.get("non_monotonic_id_count") or 0) > 0 or int(out.get("non_monotonic_ts_count") or 0) > 0:
        out["status"] = "CORRUPT_ORDER"
        out["note"] = (
            f"non_monotonic_id={int(out.get('non_monotonic_id_count') or 0)},"
            f"non_monotonic_ts={int(out.get('non_monotonic_ts_count') or 0)}"
        )
        return out

    try:
        out["complete_start"] = int(out["first_ts_ms"]) <= (day_start_ms + EDGE_TOL_MS)
        out["complete_end"] = int(out["last_ts_ms"]) >= (day_end_ms - EDGE_TOL_MS)
    except Exception:
        out["complete_start"] = False
        out["complete_end"] = False

    today_s = _utc_now().strftime("%Y-%m-%d")
    if day_s == today_s:
        out["status"] = "OPEN"
        out["note"] = "today_open_day_verified"
    elif out["complete_start"] and out["complete_end"]:
        out["status"] = "FINAL"
        out["note"] = "verified_complete"
    else:
        out["status"] = "PARTIAL"
        missing = []
        if not out["complete_start"]:
            missing.append("start_edge")
        if not out["complete_end"]:
            missing.append("end_edge")
        out["note"] = "missing_" + "_and_".join(missing) if missing else "partial"

    _log(f"Verified {day_s}: status={out['status']}, rows={rows:,}, first={_format_ts_ms(out['first_ts_ms'])}, last={_format_ts_ms(out['last_ts_ms'])}")
    return out


def _finalize_verified_cache_file(
    source_path: str,
    final_path: str,
    meta_file_path: str,
    symbol: str,
    day_utc: "pd.Timestamp",
    *,
    status_override: Optional[str] = None,
    note_override: Optional[str] = None,
    compute_hash: bool = True,
    stop_evt: Optional[threading.Event] = None,
    log_cb=None,
) -> Tuple[bool, Dict[str, Any]]:
    report = _verify_day_file(source_path, symbol, day_utc, compute_hash=compute_hash, stop_evt=stop_evt, log_cb=log_cb)
    verify_status = str(report.get("status") or "").upper().strip()
    if int(report.get("rows") or 0) <= 0:
        return False, report
    if verify_status.startswith("CORRUPT") or verify_status in ("EMPTY", "MISSING", "CANCELLED"):
        return False, report
    meta = _meta_from_verification(symbol, day_utc, report, status_override=status_override, note_override=note_override)
    if source_path != final_path:
        _atomic_replace_strict(source_path, final_path)
    _write_meta_atomic(meta_file_path, meta)
    return True, meta


def _recover_readable_gzip_prefix(
    source_path: str,
    recovered_path: str,
    *,
    stop_evt: Optional[threading.Event] = None,
    log_cb=None,
) -> Tuple[int, bool]:
    """Rewrite the readable prefix of a possibly truncated gzip file into a clean gzip file.

    This is a salvage path for interrupted `.tmp/.part` tick caches. If the source gzip footer
    is missing but most JSONL rows are intact, we preserve the fully readable newline-terminated
    prefix so later repair logic can continue from the last cached aggTradeId instead of
    re-downloading the entire day from scratch.
    """

    def _log(message: str) -> None:
        if log_cb:
            try:
                log_cb(message)
            except Exception:
                pass

    wrote = 0
    truncated = False
    try:
        if os.path.exists(recovered_path):
            os.remove(recovered_path)
    except Exception:
        pass

    try:
        with gzip.open(source_path, "rt", encoding="utf-8") as src, gzip.open(recovered_path, "wt", encoding="utf-8") as dst:
            while True:
                if stop_evt is not None and stop_evt.is_set():
                    raise InterruptedError("recovery_cancelled")
                try:
                    line = src.readline()
                except EOFError:
                    truncated = True
                    break
                except OSError:
                    truncated = True
                    break
                if line == "":
                    break
                if not line.endswith("\n"):
                    truncated = True
                    break
                dst.write(line.rstrip("\r\n"))
                dst.write("\n")
                wrote += 1
            try:
                dst.flush()
                _fsync_file_handle(dst)
            except Exception:
                pass
    except InterruptedError:
        try:
            if os.path.exists(recovered_path):
                os.remove(recovered_path)
        except Exception:
            pass
        return 0, False
    except Exception as e:
        _log(f"Readable-prefix recovery failed for {os.path.basename(source_path)}: {e}")
        try:
            if os.path.exists(recovered_path):
                os.remove(recovered_path)
        except Exception:
            pass
        return 0, False

    if wrote <= 0:
        try:
            if os.path.exists(recovered_path):
                os.remove(recovered_path)
        except Exception:
            pass
        return 0, truncated

    _log(
        f"Recovered {wrote:,} readable rows from {os.path.basename(source_path)}"
        f"{' (truncated gzip tail detected)' if truncated else ''}."
    )
    return wrote, truncated


def _recover_variant_into_final(
    existing_fp: str,
    final_fp: str,
    meta_fp: str,
    symbol: str,
    day_utc: "pd.Timestamp",
    *,
    log_cb=None,
    stop_evt: Optional[threading.Event] = None,
    compute_hash: bool = False,
) -> Optional[Dict[str, Any]]:
    recovered_tmp = final_fp + ".recover.tmp"
    wrote, truncated = _recover_readable_gzip_prefix(existing_fp, recovered_tmp, stop_evt=stop_evt, log_cb=log_cb)
    if wrote <= 0:
        return None

    try:
        ok, meta = _finalize_verified_cache_file(
            recovered_tmp,
            final_fp,
            meta_fp,
            symbol,
            day_utc,
            note_override="recovered_truncated_variant" if truncated else "recovered_variant",
            compute_hash=compute_hash,
            stop_evt=stop_evt,
            log_cb=log_cb,
        )
        if ok:
            if existing_fp != final_fp:
                try:
                    if os.path.exists(existing_fp):
                        os.remove(existing_fp)
                except Exception:
                    pass
            return meta
    except Exception as e:
        if log_cb:
            try:
                log_cb(f"Recovered promotion failed for {os.path.basename(existing_fp)}: {e}")
            except Exception:
                pass
    finally:
        try:
            if os.path.exists(recovered_tmp):
                os.remove(recovered_tmp)
        except Exception:
            pass
    return None


def _salvage_variant_into_final(
    existing_fp: str,
    final_fp: str,
    meta_fp: str,
    symbol: str,
    day_utc: "pd.Timestamp",
    *,
    log_cb=None,
    stop_evt: Optional[threading.Event] = None,
    compute_hash: bool = False,
) -> Optional[Dict[str, Any]]:
    try:
        ok, meta = _finalize_verified_cache_file(
            existing_fp, final_fp, meta_fp, symbol, day_utc,
            compute_hash=compute_hash, stop_evt=stop_evt, log_cb=log_cb,
        )
        if ok:
            return meta
    except Exception as e:
        if log_cb:
            try:
                log_cb(f"Salvage failed for {os.path.basename(existing_fp)}: {e}")
            except Exception:
                pass
    recovered = _recover_variant_into_final(
        existing_fp,
        final_fp,
        meta_fp,
        symbol,
        day_utc,
        log_cb=log_cb,
        stop_evt=stop_evt,
        compute_hash=compute_hash,
    )
    if recovered is not None:
        return recovered
    return None


def _build_meta_from_existing_file(
    dfp: str,
    mp: str,
    symbol: str,
    day_utc: "pd.Timestamp",
    log_cb=None,
    stop_evt: Optional[threading.Event] = None,
) -> Optional[Dict[str, Any]]:
    """Create or refresh meta for an existing cache file by fully verifying it."""
    day_start_ms, day_end_ms, day_s = _day_bounds_ms(day_utc)

    def _log(m: str):
        if log_cb:
            try:
                log_cb(m)
            except Exception:
                pass

    try:
        if (not os.path.isfile(dfp)) or os.path.getsize(dfp) <= 0:
            return None
    except Exception:
        return None

    _log(f"{day_s}: verifying existing cache file and rebuilding meta…")
    report = _verify_day_file(dfp, symbol, day_utc, compute_hash=False, stop_evt=stop_evt, log_cb=log_cb)
    verify_status = str(report.get("status") or "").upper().strip()
    if verify_status in ("CORRUPT_GZIP", "CORRUPT_READ") and int(report.get("rows") or 0) > 0:
        _log(f"{day_s}: readable gzip prefix detected despite {verify_status}; attempting clean recovery before rebuilding meta…")
        recovered = _recover_variant_into_final(
            dfp,
            dfp,
            mp,
            symbol,
            day_utc,
            log_cb=log_cb,
            stop_evt=stop_evt,
            compute_hash=False,
        )
        if recovered is not None:
            return recovered
    meta = _meta_from_verification(symbol, day_utc, report)
    try:
        _write_meta_atomic(mp, meta)
    except Exception as e:
        _log(f"{day_s}: ERROR writing meta: {e}")
        return None
    _log(f"{day_s}: meta rebuilt — status={meta.get('status')} rows={int(meta.get('rows') or 0):,}")
    return meta

    # Determine completeness (best-effort):
    # For very liquid symbols, a complete day should usually have data near both edges.
    today_s = now.strftime("%Y-%m-%d")
    if day_s == today_s:
        status = "OPEN"
    else:
        complete_start = first_ts <= (day_start_ms + EDGE_TOL_MS)
        complete_end = last_ts >= (day_end_ms - EDGE_TOL_MS)
        status = "FINAL" if (complete_start and complete_end) else "PARTIAL"

    meta = {
        "schema": SCHEMA_VERSION,
        "symbol": symbol,
        "day": day_s,
        "status": status,
        "rows": int(rows),
        "first_id": int(first_id),
        "last_id": int(last_id),
        "first_ts_ms": int(first_ts),
        "last_ts_ms": int(last_ts),
        "day_start_ms": int(day_start_ms),
        "day_end_ms": int(day_end_ms),
        "created_utc": now.isoformat(),
        "note": "built_from_existing_file",
    }
    _write_meta_atomic(mp, meta)
    _log(f"{day_s}: meta built — rows={rows:,} status={status}")
    return meta


def _find_day_file_variants(fp: str) -> Tuple[Optional[str], Optional[str]]:
    """Return (existing_path, kind) where kind is one of: 'FINAL','TINY','TMP','PART'.
    This helps detect interrupted downloads where only a temp/partial file exists."""
    try:
        if os.path.isfile(fp):
            sz = os.path.getsize(fp)
            if sz > 80:
                return fp, "FINAL"
            if sz > 0:
                return fp, "TINY"
    except Exception:
        pass

    # common temp/partial variants
    candidates = [
        fp + ".tmp",
        fp + ".part",
        fp + ".partial",
        fp + ".incomplete",
    ]
    for c in candidates:
        try:
            if os.path.isfile(c) and os.path.getsize(c) > 0:
                if c.endswith(".tmp"):
                    return c, "TMP"
                return c, "PART"
        except Exception:
            continue
    return None, None



def _list_day_file_variants(fp: str) -> List[Tuple[str, str]]:
    """Return all known cache variants for a day as (path, kind)."""
    out: List[Tuple[str, str]] = []
    candidates = [
        (fp, 'FINAL'),
        (fp + '.tmp', 'TMP'),
        (fp + '.part', 'PART'),
        (fp + '.partial', 'PART'),
        (fp + '.incomplete', 'PART'),
        (fp + '.append.tmp', 'APPEND_TMP'),
        (fp + '.merge.tmp', 'MERGE_TMP'),
    ]
    seen = set()
    for path, kind in candidates:
        if path in seen:
            continue
        seen.add(path)
        try:
            if os.path.isfile(path):
                out.append((path, kind))
        except Exception:
            continue
    return out


def _integrity_badge(day_s: str, meta: Optional[Dict[str, Any]], kind: Optional[str], now_utc: "pd.Timestamp") -> str:
    today_s = now_utc.strftime('%Y-%m-%d')
    if kind == 'TINY':
        return 'TINY'
    if kind == 'TMP':
        return 'TEMP'
    if kind == 'PART':
        return 'PART FILE'
    if meta is None:
        return 'MISSING' if kind is None else 'NO META'

    verify_status = str(meta.get('verify_status') or '').upper().strip()
    status = str(meta.get('status') or meta.get('state') or '').upper().strip()
    complete_start = bool(meta.get('complete_start', False))
    complete_end = bool(meta.get('complete_end', False))

    if verify_status.startswith('CORRUPT') or status.startswith('CORRUPT'):
        return 'CORRUPT'
    if verify_status == 'EMPTY' or status == 'EMPTY':
        return 'EMPTY'
    if day_s == today_s and (status in ('OPEN', 'FINAL') or verify_status in ('OPEN', 'FINAL')):
        return 'LIVE OPEN'
    if status == 'FINAL' or verify_status == 'FINAL':
        return 'OK FINAL'
    if status in ('OPEN', 'STALE_OPEN') and day_s != today_s:
        if complete_start and not complete_end:
            return 'RESUME TAIL'
        return 'RELOAD'
    if verify_status == 'PARTIAL' or status == 'PARTIAL':
        if complete_start and not complete_end:
            return 'RESUME TAIL'
        if (not complete_start) and complete_end:
            return 'RELOAD HEAD'
        if (not complete_start) and (not complete_end):
            return 'RELOAD BOTH'
        return 'PARTIAL'
    if status in ('NO_META', 'OPEN_NO_META'):
        return 'NO META'
    return 'UNVERIFIED'


def _integrity_tag(badge: str) -> str:
    badge = str(badge or '').upper().strip()
    if badge in ('OK FINAL', 'LIVE OPEN'):
        return 'ok'
    if badge == 'RESUME TAIL':
        return 'resume'
    if badge in ('RELOAD', 'RELOAD HEAD', 'RELOAD BOTH', 'PARTIAL', 'PART FILE', 'NO META', 'UNVERIFIED', 'TINY'):
        return 'warn'
    if badge in ('CORRUPT', 'EMPTY'):
        return 'bad'
    if badge == 'TEMP':
        return 'temp'
    if badge == 'MISSING':
        return 'missing'
    return ''


def _can_resume_tail_from_meta(meta: Optional[Dict[str, Any]]) -> bool:
    if not isinstance(meta, dict):
        return False
    verify_status = str(meta.get('verify_status') or '').upper().strip()
    status = str(meta.get('status') or meta.get('state') or '').upper().strip()
    if verify_status.startswith('CORRUPT') or status.startswith('CORRUPT'):
        return False
    if verify_status == 'EMPTY' or status == 'EMPTY':
        return False
    if meta.get('last_id') is None:
        return False
    return bool(meta.get('complete_start', False)) and (not bool(meta.get('complete_end', False)))


def _format_verification_report_text(
    symbol: str,
    day_s: str,
    expected_path: str,
    existing_path: Optional[str],
    kind: Optional[str],
    meta: Optional[Dict[str, Any]],
    report: Optional[Dict[str, Any]],
    variants: List[Tuple[str, str]],
    now_utc: "pd.Timestamp",
) -> str:
    badge = _integrity_badge(day_s, meta, kind, now_utc)
    lines: List[str] = []
    lines.append(f'Symbol: {symbol}')
    lines.append(f'Day (UTC): {day_s}')
    lines.append(f'Integrity badge: {badge}')
    lines.append(f'Expected cache file: {expected_path}')
    lines.append(f'Current file used: {existing_path or "<missing>"}')
    lines.append(f'Variant kind: {kind or "NONE"}')
    lines.append('')

    lines.append('Files present:')
    if variants:
        for vp, vk in variants:
            try:
                size_txt = f'{os.path.getsize(vp):,} bytes'
            except Exception:
                size_txt = 'size=?'
            lines.append(f'  - {vk:<10} {vp}  [{size_txt}]')
    else:
        lines.append('  - none')
    lines.append('')

    if report is None:
        lines.append('Verification: no file available to verify.')
    else:
        lines.append('Verification summary:')
        lines.append(f"  status: {report.get('status')}")
        lines.append(f"  note: {report.get('note')}")
        lines.append(f"  rows: {int(report.get('rows') or 0):,}")
        lines.append(f"  first aggTradeId: {report.get('first_id')}")
        lines.append(f"  last aggTradeId: {report.get('last_id')}")
        lines.append(f"  first trade UTC: {_format_ts_ms(report.get('first_ts_ms'))}")
        lines.append(f"  last trade UTC: {_format_ts_ms(report.get('last_ts_ms'))}")
        lines.append(f"  complete_start: {bool(report.get('complete_start', False))}")
        lines.append(f"  complete_end: {bool(report.get('complete_end', False))}")
        lines.append(f"  bad_lines: {int(report.get('bad_lines') or 0):,}")
        lines.append(f"  out_of_bounds: {int(report.get('out_of_bounds') or 0):,}")
        lines.append(f"  non_monotonic_id_count: {int(report.get('non_monotonic_id_count') or 0):,}")
        lines.append(f"  non_monotonic_ts_count: {int(report.get('non_monotonic_ts_count') or 0):,}")
        lines.append(f"  file_size_bytes: {int(report.get('file_size_bytes') or 0):,}")
        lines.append(f"  sha256: {report.get('sha256') or '<not computed>'}")
    lines.append('')

    lines.append('Stored meta:')
    if meta is None:
        lines.append('  <missing>')
    else:
        try:
            lines.append(json.dumps(meta, indent=2, sort_keys=True))
        except Exception:
            lines.append(str(meta))
    lines.append('')

    if report is not None:
        lines.append('Raw verification report:')
        try:
            lines.append(json.dumps(report, indent=2, sort_keys=True))
        except Exception:
            lines.append(str(report))

    return '\n'.join(lines)

# -----------------------------
# Downloader core
# -----------------------------
@dataclass
class DayStatus:
    day: str
    status: str
    integrity: str = ""
    rows: Optional[int] = None
    first_ts: Optional[int] = None
    last_ts: Optional[int] = None
    first_id: Optional[int] = None
    last_id: Optional[int] = None
    size_mb: str = ""
    path: str = ""
    meta_path: str = ""

def _status_for_day(day_s: str, mp: Optional[Dict[str, Any]], now_utc: "pd.Timestamp") -> str:
    today_s = now_utc.strftime("%Y-%m-%d")
    if mp is None:
        return "OPEN_NO_META" if day_s == today_s else "NO_META"

    verify_status = str(mp.get("verify_status") or "").upper().strip()
    status = str(mp.get("status") or mp.get("state") or "").upper().strip()

    if verify_status.startswith("CORRUPT"):
        return verify_status
    if verify_status == "EMPTY":
        return "EMPTY"
    if day_s == today_s:
        return "OPEN"
    if status == "FINAL" or verify_status == "FINAL":
        return "FINAL"
    if status == "OPEN":
        return "STALE_OPEN"
    if verify_status == "PARTIAL":
        return "PARTIAL"
    if bool(mp.get("final", False)) or bool(mp.get("finished", False)):
        return "FINAL"
    return "PARTIAL"

def _download_day_full(
    cache_dir: str,
    symbol: str,
    day_utc: "pd.Timestamp",
    log_cb=None,
    progress_cb=None,
    stop_evt: Optional[threading.Event] = None,
    rate_limiter: Optional[RateLimiter] = None,
) -> bool:
    """Download one UTC day, verify it, and commit it atomically.

    Professional behaviour goals:
      - never silently claim success if the final file was not saved,
      - always verify the completed cache before promotion,
      - preserve a partial-but-valid download if the user stops or the session dies after writing rows.
    """
    symbol = (symbol or "").upper().strip()
    day_start_ms, day_end_ms, day_s = _day_bounds_ms(day_utc)
    dfp = day_path(cache_dir, symbol, day_utc)
    mp = meta_path(dfp)
    _ensure_dir(os.path.dirname(dfp))

    url = f"{BINANCE_FAPI_BASE}/fapi/v1/aggTrades"
    headers = {"User-Agent": "tick-manager/2.0"}
    tmp = dfp + ".tmp"

    wrote = 0
    approx_bytes = 0
    first_id = last_id = None
    first_ts = last_ts = None
    next_from_id = None
    cur_start_ms = int(day_start_ms)

    sess = requests.Session()
    retries = 0
    backoff = 0.8
    rate_wait = 1.0
    req_count = 0
    t_start = time.time()
    t_last_ui = t_start
    t_last_log = t_start
    last_seen_ts_any = None
    terminated_reason: Optional[str] = None
    terminal_error: Optional[str] = None

    def _log(m: str):
        if log_cb:
            try:
                log_cb(m)
            except Exception:
                pass

    def _progress(frac: float, text: str):
        if progress_cb is None:
            return
        try:
            progress_cb(frac, text)
        except Exception:
            pass

    def _fmt_ts(ms: Optional[int]) -> str:
        if ms is None:
            return "n/a"
        try:
            return pd.to_datetime(int(ms), unit="ms", utc=True).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return str(ms)

    def _emit_ui(force: bool = False):
        nonlocal t_last_ui
        if progress_cb is None:
            return
        now_s = time.time()
        if (not force) and (now_s - t_last_ui) < 1.0:
            return

        ts_prog = last_ts if last_ts is not None else last_seen_ts_any
        try:
            now_ms = int(_utc_now().value // 10**6)
        except Exception:
            now_ms = int(time.time() * 1000)
        target_end_ms = min(now_ms, int(day_end_ms))
        denom = max(1, int(target_end_ms - int(day_start_ms)))
        if ts_prog is None:
            frac = 0.0
        else:
            frac = float(int(ts_prog) - int(day_start_ms)) / float(denom)
            frac = max(0.0, min(1.0, frac))

        elapsed = max(0.001, now_s - t_start)
        trades_per_s = wrote / elapsed
        mb_per_s = (approx_bytes / 1_000_000.0) / elapsed
        req_per_min = req_count / (elapsed / 60.0)
        w1m = getattr(rate_limiter, "used_weight_1m", None) if rate_limiter is not None else None
        parts = [
            f"{day_s} {frac*100:.1f}%",
            f"{wrote:,} lines",
            f"{trades_per_s:.0f}/s",
            f"{mb_per_s:.2f} MB/s",
            f"req {req_count} ({req_per_min:.1f}/min)",
            f"last {_fmt_ts(ts_prog)} UTC",
        ]
        if w1m is not None:
            parts.append(f"w1m={w1m}")
        _progress(frac, " — ".join(parts))
        t_last_ui = now_s

    _log(f"Downloading {symbol} {day_s} …")
    _emit_ui(force=True)

    try:
        with gzip.open(tmp, "wt", encoding="utf-8") as out:
            while True:
                if stop_evt and stop_evt.is_set():
                    terminated_reason = "cancelled"
                    _log("Download cancelled by user — preserving partial cache if it is valid.")
                    break

                params = {"symbol": symbol, "limit": 1000}
                if next_from_id is not None:
                    params["fromId"] = int(next_from_id)
                else:
                    params["startTime"] = int(cur_start_ms)

                try:
                    if rate_limiter is not None:
                        rate_limiter.wait(stop_evt)
                    r = sess.get(url, params=params, headers=headers, timeout=20)
                    if r.status_code in (429, 418):
                        ra = r.headers.get("Retry-After")
                        try:
                            wait_s = float(ra) if ra else float(rate_wait)
                        except Exception:
                            wait_s = float(rate_wait)
                        if r.status_code == 418:
                            wait_s = max(float(wait_s), float(BAN_418_MIN_SLEEP_S))
                        _log(f"Rate limited by Binance ({r.status_code}) — backing off for {wait_s:.1f}s…")
                        _sleep_cancel(max(1.0, wait_s), stop_evt)
                        if rate_limiter is not None:
                            rate_limiter.on_rate_limit(r.status_code)
                        rate_wait = min(600.0 if r.status_code == 418 else 180.0, max(1.0, rate_wait * (2.2 if r.status_code == 418 else 1.8)))
                        _emit_ui(force=True)
                        continue

                    r.raise_for_status()
                    data = r.json() or []
                    req_count += 1
                    rate_wait = 1.0
                    retries = 0
                    backoff = 0.8
                    if rate_limiter is not None:
                        rate_limiter.observe_headers(r.headers, stop_evt=stop_evt, log_cb=log_cb)
                    _sleep_cancel(max(0.05, SUCCESS_SLEEP_S + random.uniform(0.0, 0.05)), stop_evt)
                except Exception as e:
                    retries += 1
                    terminal_error = str(e)
                    if retries > 12:
                        terminated_reason = "download_error"
                        _log(f"ERROR: download failed too many times: {e}")
                        break
                    _log(f"ERROR: {e} — retrying…")
                    _sleep_cancel(backoff, stop_evt)
                    backoff = min(6.0, backoff * 1.35)
                    _emit_ui(force=True)
                    continue

                if not data:
                    terminated_reason = terminated_reason or "complete"
                    break

                try:
                    data.sort(key=lambda x: int(x.get("a", 0)))
                except Exception:
                    pass

                stop_on_day_end = False
                for d in data:
                    try:
                        t = int(d.get("T", 0))
                        a = int(d.get("a", 0))
                        last_seen_ts_any = t
                    except Exception:
                        continue

                    if t < day_start_ms:
                        continue
                    if t >= day_end_ms:
                        stop_on_day_end = True
                        terminated_reason = terminated_reason or "complete"
                        break

                    if first_id is None:
                        first_id = a
                    last_id = a
                    if first_ts is None:
                        first_ts = t
                    last_ts = t

                    line = json.dumps({"a": a, "T": t, "p": d.get("p"), "q": d.get("q"), "m": d.get("m")}, separators=(",", ":"))
                    out.write(line + "\n")
                    wrote += 1
                    approx_bytes += (len(line) + 1)

                try:
                    out.flush()
                except Exception:
                    pass

                if stop_on_day_end:
                    break

                try:
                    last = data[-1]
                    last_a = int(last.get("a"))
                    next_from_id = last_a + 1
                except Exception:
                    try:
                        last_t = int(data[-1].get("T", cur_start_ms))
                        cur_start_ms = last_t + 1
                        next_from_id = None
                    except Exception:
                        terminated_reason = "advance_error"
                        terminal_error = "could_not_advance_pagination"
                        break

                _emit_ui(force=False)

                now_s = time.time()
                if (now_s - t_last_log) >= 12.0:
                    elapsed = max(0.001, now_s - t_start)
                    trades_per_s = wrote / elapsed
                    req_per_min = req_count / (elapsed / 60.0)
                    mb_per_s = (approx_bytes / 1_000_000.0) / elapsed
                    ts_prog = last_ts if last_ts is not None else last_seen_ts_any
                    w1m = getattr(rate_limiter, "used_weight_1m", None) if rate_limiter is not None else None
                    extra = f", w1m={w1m}" if w1m is not None else ""
                    _log(f"Progress {day_s}: rows={wrote:,}, {trades_per_s:.0f}/s, {mb_per_s:.2f} MB/s, req={req_count} (~{req_per_min:.1f}/min), last={_fmt_ts(ts_prog)} UTC{extra}")
                    t_last_log = now_s

                if wrote and wrote % 250000 == 0:
                    _log(f"  … {wrote:,} trades written")
    except Exception as e:
        terminated_reason = terminated_reason or "fatal_write_error"
        terminal_error = str(e)
        _log(f"FATAL: exception while writing day file: {e}")

    if wrote <= 0:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass
        _log(f"No aggTrades returned for {symbol} {day_s}.")
        _emit_ui(force=True)
        return False

    today_s = _utc_now().strftime("%Y-%m-%d")
    status_override = None
    note_override = None
    if terminated_reason and terminated_reason != "complete":
        status_override = "OPEN" if day_s == today_s else "PARTIAL"
        detail = f" ({terminal_error})" if terminal_error else ""
        note_override = f"{terminated_reason}{detail}"

    try:
        ok, meta = _finalize_verified_cache_file(
            tmp,
            dfp,
            mp,
            symbol,
            day_utc,
            status_override=status_override,
            note_override=note_override,
            compute_hash=True,
            stop_evt=stop_evt,
            log_cb=log_cb,
        )
    except Exception as e:
        _log(f"ERROR: failed to finalize cache atomically: {e}")
        return False

    if not ok:
        bad_target = dfp + ".corrupt"
        try:
            if os.path.exists(tmp):
                _atomic_replace_strict(tmp, bad_target)
                _log(f"Verification failed — preserved diagnostic file at {bad_target}")
        except Exception:
            pass
        _emit_ui(force=True)
        return False

    _log(
        f"Cached {symbol} {day_s}: {int(meta.get('rows') or 0):,} trades "
        f"[{meta.get('status')}] sha256={str(meta.get('sha256') or '')[:12]}…"
    )
    _emit_ui(force=True)
    return True


def _append_update_open_day(
    cache_dir: str,
    symbol: str,
    day_utc: "pd.Timestamp",
    log_cb=None,
    progress_cb=None,
    stop_evt: Optional[threading.Event] = None,
    rate_limiter: Optional[RateLimiter] = None,
) -> bool:
    """Safely update today's OPEN day.

    Instead of appending directly into the live cache file, this downloads the delta into a
    temporary gzip member, verifies it, merges it with the existing cache into a new temp file,
    verifies the merged result, and only then atomically replaces the live cache.
    """
    symbol = (symbol or "").upper().strip()
    dfp = day_path(cache_dir, symbol, day_utc)
    mp = meta_path(dfp)
    day_start_ms, day_end_ms, day_s = _day_bounds_ms(day_utc)
    append_tmp = dfp + ".append.tmp"
    merge_tmp = dfp + ".merge.tmp"

    def _log(m: str):
        if log_cb:
            try:
                log_cb(m)
            except Exception:
                pass

    def _progress(frac=None, text: str = ""):
        if progress_cb:
            try:
                progress_cb(frac, text)
            except Exception:
                pass

    if not os.path.isfile(dfp):
        return _download_day_full(cache_dir, symbol, day_utc, log_cb=log_cb, progress_cb=progress_cb, stop_evt=stop_evt, rate_limiter=rate_limiter)

    meta = _read_meta(mp)
    if not meta or meta.get("last_id") is None:
        _log(f"{day_s}: missing meta/last_id → rebuilding meta from file.")
        meta = _build_meta_from_existing_file(dfp, mp, symbol, day_utc, log_cb=log_cb, stop_evt=stop_evt)
    if not meta or meta.get("last_id") is None:
        _log(f"{day_s}: still no valid meta → re-downloading open day from scratch.")
        return _download_day_full(cache_dir, symbol, day_utc, log_cb=log_cb, progress_cb=progress_cb, stop_evt=stop_evt, rate_limiter=rate_limiter)

    current_status = _status_for_day(day_s, meta, _utc_now())
    if current_status.startswith("CORRUPT") or current_status == "EMPTY":
        _log(f"{day_s}: existing OPEN cache is {current_status} → re-downloading safely.")
        return _download_day_full(cache_dir, symbol, day_utc, log_cb=log_cb, progress_cb=progress_cb, stop_evt=stop_evt, rate_limiter=rate_limiter)

    try:
        last_id = int(meta.get("last_id"))
    except Exception:
        last_id = None
    if last_id is None:
        _log(f"{day_s}: invalid last_id → re-downloading open day.")
        return _download_day_full(cache_dir, symbol, day_utc, log_cb=log_cb, progress_cb=progress_cb, stop_evt=stop_evt, rate_limiter=rate_limiter)

    url = f"{BINANCE_FAPI_BASE}/fapi/v1/aggTrades"
    headers = {"User-Agent": "tick-manager/2.0"}
    sess = requests.Session()

    wrote = 0
    req_count = 0
    first_new_id = None
    last_new_id = None
    first_new_ts = None
    last_new_ts = None
    next_from_id = last_id + 1
    retries = 0
    backoff = 0.8
    rate_wait = 1.0
    t_start = time.time()
    t_last_log = t_start
    last_seen_ts_any = None

    _log(f"Updating OPEN day {symbol} {day_s} fromId={last_id+1} …")
    _progress(0.0, f"{day_s} 0.0% — starting fromId={last_id+1}")

    try:
        with gzip.open(append_tmp, "wt", encoding="utf-8") as out:
            while True:
                if stop_evt and stop_evt.is_set():
                    _log("Update cancelled before commit — existing cache left untouched.")
                    break

                params = {"symbol": symbol, "limit": 1000, "fromId": int(next_from_id)}
                try:
                    if rate_limiter is not None:
                        rate_limiter.wait(stop_evt)
                    r = sess.get(url, params=params, headers=headers, timeout=20)
                    if r.status_code in (429, 418):
                        ra = r.headers.get("Retry-After")
                        try:
                            wait_s = float(ra) if ra else float(rate_wait)
                        except Exception:
                            wait_s = float(rate_wait)
                        if r.status_code == 418:
                            wait_s = max(float(wait_s), float(BAN_418_MIN_SLEEP_S))
                        _log(f"Rate limited by Binance ({r.status_code}) — backing off for {wait_s:.1f}s…")
                        _sleep_cancel(max(1.0, wait_s), stop_evt)
                        if rate_limiter is not None:
                            rate_limiter.on_rate_limit(r.status_code)
                        rate_wait = min(600.0 if r.status_code == 418 else 180.0, max(1.0, rate_wait * (2.2 if r.status_code == 418 else 1.8)))
                        continue
                    r.raise_for_status()
                    data = r.json() or []
                    req_count += 1
                    rate_wait = 1.0
                    retries = 0
                    if rate_limiter is not None:
                        rate_limiter.observe_headers(r.headers, stop_evt=stop_evt, log_cb=log_cb)
                    _sleep_cancel(max(0.05, SUCCESS_SLEEP_S + random.uniform(0.0, 0.05)), stop_evt)
                except Exception as e:
                    retries += 1
                    if retries > 10:
                        _log(f"ERROR: update failed too many times: {e}")
                        break
                    _log(f"ERROR: {e} — retrying…")
                    _sleep_cancel(backoff, stop_evt)
                    backoff = min(6.0, backoff * 1.35)
                    continue

                if not data:
                    _log("No new aggTrades returned (empty response) — likely up to date.")
                    break
                try:
                    data.sort(key=lambda x: int(x.get("a", 0)))
                except Exception:
                    pass

                stop = False
                for d in data:
                    try:
                        t = int(d.get("T", 0))
                        a = int(d.get("a", 0))
                        last_seen_ts_any = t
                    except Exception:
                        continue
                    if t < day_start_ms:
                        continue
                    if t >= day_end_ms:
                        stop = True
                        break
                    if first_new_id is None:
                        first_new_id = a
                        first_new_ts = t
                    last_new_id = a
                    last_new_ts = t
                    out.write(json.dumps({"a": a, "T": t, "p": d.get("p"), "q": d.get("q"), "m": d.get("m")}, separators=(",", ":")) + "\n")
                    wrote += 1

                try:
                    out.flush()
                except Exception:
                    pass

                if stop:
                    break

                try:
                    next_from_id = int(data[-1].get("a")) + 1
                except Exception:
                    break

                now_s = time.time()
                if (now_s - t_last_log) >= 15.0:
                    ts_prog = last_new_ts if last_new_ts is not None else last_seen_ts_any
                    try:
                        now_ms = int(_utc_now().value // 10**6)
                    except Exception:
                        now_ms = int(time.time() * 1000)
                    target_end_ms = min(now_ms, day_end_ms)
                    denom = max(1, int(target_end_ms - day_start_ms))
                    if ts_prog is None:
                        frac = 0.0
                        ts_txt = 'n/a'
                    else:
                        frac = float(ts_prog - day_start_ms) / float(denom)
                        frac = max(0.0, min(1.0, frac))
                        try:
                            ts_txt = pd.to_datetime(int(ts_prog), unit='ms', utc=True).strftime('%Y-%m-%d %H:%M:%S')
                        except Exception:
                            ts_txt = str(ts_prog)
                    elapsed = max(0.001, now_s - t_start)
                    req_per_min = req_count / (elapsed / 60.0)
                    trades_per_s = wrote / elapsed
                    _log(f"Progress: req={req_count} (~{req_per_min:.1f}/min), +{wrote:,} trades ({trades_per_s:.0f}/s), last={ts_txt} UTC")
                    _progress(frac, f"{day_s} {frac*100:.1f}% — +{wrote:,} trades, last {ts_txt} UTC")
                    t_last_log = now_s
    except Exception as e:
        _log(f"FATAL: update exception: {e}")

    if stop_evt and stop_evt.is_set():
        try:
            if os.path.exists(append_tmp):
                os.remove(append_tmp)
        except Exception:
            pass
        return False

    if wrote <= 0:
        try:
            if os.path.exists(append_tmp):
                os.remove(append_tmp)
        except Exception:
            pass
        _progress(1.0, f"{day_s} already up to date (no new trades)")
        return False

    delta_report = _verify_day_file(append_tmp, symbol, day_utc, compute_hash=False, stop_evt=stop_evt, log_cb=log_cb)
    delta_status = str(delta_report.get("status") or "").upper().strip()
    if delta_status.startswith("CORRUPT") or int(delta_report.get("rows") or 0) <= 0:
        _log(f"{day_s}: delta verification failed ({delta_status}) — keeping original file unchanged.")
        try:
            if os.path.exists(append_tmp):
                os.remove(append_tmp)
        except Exception:
            pass
        return False
    try:
        if int(delta_report.get("first_id")) <= int(last_id):
            _log(f"{day_s}: delta starts at invalid aggTradeId {delta_report.get('first_id')} (last existing {last_id}) — aborting update.")
            try:
                if os.path.exists(append_tmp):
                    os.remove(append_tmp)
            except Exception:
                pass
            return False
    except Exception:
        pass

    try:
        with open(merge_tmp, "wb") as out_f, open(dfp, "rb") as base_f, open(append_tmp, "rb") as delta_f:
            shutil.copyfileobj(base_f, out_f, length=1024 * 1024)
            shutil.copyfileobj(delta_f, out_f, length=1024 * 1024)
            _fsync_file_handle(out_f)
    except Exception as e:
        _log(f"{day_s}: failed to merge updated cache atomically: {e}")
        try:
            if os.path.exists(append_tmp):
                os.remove(append_tmp)
        except Exception:
            pass
        return False

    merged_report = _verify_day_file(merge_tmp, symbol, day_utc, compute_hash=True, stop_evt=stop_evt, log_cb=log_cb)
    merged_status = str(merged_report.get("status") or "").upper().strip()
    if merged_status.startswith("CORRUPT") or int(merged_report.get("rows") or 0) < int(meta.get("rows") or 0):
        _log(f"{day_s}: merged cache verification failed ({merged_status}) — original file preserved.")
        try:
            if os.path.exists(merge_tmp):
                os.remove(merge_tmp)
        except Exception:
            pass
        try:
            if os.path.exists(append_tmp):
                os.remove(append_tmp)
        except Exception:
            pass
        return False

    try:
        _atomic_replace_strict(merge_tmp, dfp)
        merged_meta = _meta_from_verification(symbol, day_utc, merged_report, status_override="OPEN", note_override="verified_open_update")
        _write_meta_atomic(mp, merged_meta)
    except Exception as e:
        _log(f"{day_s}: failed to commit updated OPEN file: {e}")
        return False
    finally:
        for p in (append_tmp, merge_tmp):
            try:
                if os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass

    _log(f"{day_s}: appended {wrote:,} trades safely — cache now has {int(merged_meta.get('rows') or 0):,} rows")
    _progress(1.0, f"{day_s} updated — +{wrote:,} trades")
    return True



def _resume_partial_tail_day(
    cache_dir: str,
    symbol: str,
    day_utc: "pd.Timestamp",
    log_cb=None,
    progress_cb=None,
    stop_evt: Optional[threading.Event] = None,
    rate_limiter: Optional[RateLimiter] = None,
) -> bool:
    """Resume a verified historical day that has a missing tail.

    This is for PARTIAL/STALE days where the cache starts correctly at the beginning of the UTC day
    but does not reach the end yet. We download only the missing suffix, verify it, merge it into a
    fresh gzip file, verify the merged file again, then atomically promote it.
    """
    symbol = (symbol or '').upper().strip()
    dfp = day_path(cache_dir, symbol, day_utc)
    mp = meta_path(dfp)
    day_start_ms, day_end_ms, day_s = _day_bounds_ms(day_utc)
    append_tmp = dfp + '.append.tmp'
    merge_tmp = dfp + '.merge.tmp'

    def _log(m: str):
        if log_cb:
            try:
                log_cb(m)
            except Exception:
                pass

    def _progress(frac=None, text: str = ''):
        if progress_cb:
            try:
                progress_cb(frac, text)
            except Exception:
                pass

    existing_fp, kind = _find_day_file_variants(dfp)
    if existing_fp is None:
        _log(f'{day_s}: no existing cache variant to resume — downloading full day.')
        return _download_day_full(cache_dir, symbol, day_utc, log_cb=log_cb, progress_cb=progress_cb, stop_evt=stop_evt, rate_limiter=rate_limiter)

    if existing_fp != dfp:
        _log(f'{day_s}: promoting {os.path.basename(existing_fp)} before resume…')
        salvaged = _salvage_variant_into_final(existing_fp, dfp, mp, symbol, day_utc, log_cb=log_cb, stop_evt=stop_evt, compute_hash=False)
        if salvaged is None:
            _log(f'{day_s}: could not salvage temp/partial variant — downloading full day instead.')
            return _download_day_full(cache_dir, symbol, day_utc, log_cb=log_cb, progress_cb=progress_cb, stop_evt=stop_evt, rate_limiter=rate_limiter)

    if not os.path.isfile(dfp):
        _log(f'{day_s}: final cache file still missing after salvage — downloading full day.')
        return _download_day_full(cache_dir, symbol, day_utc, log_cb=log_cb, progress_cb=progress_cb, stop_evt=stop_evt, rate_limiter=rate_limiter)

    meta = _read_meta(mp)
    if (not meta) or (meta.get('last_id') is None):
        _log(f'{day_s}: missing meta/last_id — rebuilding from existing cache file.')
        meta = _build_meta_from_existing_file(dfp, mp, symbol, day_utc, log_cb=log_cb, stop_evt=stop_evt)
    if not meta or meta.get('last_id') is None:
        _log(f'{day_s}: unable to recover valid meta — downloading full day instead.')
        return _download_day_full(cache_dir, symbol, day_utc, log_cb=log_cb, progress_cb=progress_cb, stop_evt=stop_evt, rate_limiter=rate_limiter)

    current_status = _status_for_day(day_s, meta, _utc_now())
    if current_status.startswith('CORRUPT') or current_status == 'EMPTY':
        _log(f'{day_s}: cache is {current_status} — downloading full day instead of resume.')
        return _download_day_full(cache_dir, symbol, day_utc, log_cb=log_cb, progress_cb=progress_cb, stop_evt=stop_evt, rate_limiter=rate_limiter)

    if not _can_resume_tail_from_meta(meta):
        _log(f"{day_s}: cache is not a safe tail-resume candidate (complete_start={bool(meta.get('complete_start', False))}, complete_end={bool(meta.get('complete_end', False))}) — downloading full day.")
        return _download_day_full(cache_dir, symbol, day_utc, log_cb=log_cb, progress_cb=progress_cb, stop_evt=stop_evt, rate_limiter=rate_limiter)

    try:
        last_id = int(meta.get('last_id'))
    except Exception:
        _log(f'{day_s}: invalid last_id in meta — downloading full day instead.')
        return _download_day_full(cache_dir, symbol, day_utc, log_cb=log_cb, progress_cb=progress_cb, stop_evt=stop_evt, rate_limiter=rate_limiter)

    url = f'{BINANCE_FAPI_BASE}/fapi/v1/aggTrades'
    headers = {'User-Agent': 'tick-manager/2.1'}
    sess = requests.Session()

    wrote = 0
    req_count = 0
    next_from_id = last_id + 1
    retries = 0
    backoff = 0.8
    rate_wait = 1.0
    t_start = time.time()
    t_last_log = t_start
    last_seen_ts_any = None

    _log(f'Resuming {symbol} {day_s} fromId={next_from_id} …')
    _progress(0.0, f'{day_s} tail resume — starting fromId={next_from_id}')

    try:
        with gzip.open(append_tmp, 'wt', encoding='utf-8') as out:
            while True:
                if stop_evt and stop_evt.is_set():
                    _log('Resume cancelled before commit — existing cache left untouched.')
                    break

                params = {'symbol': symbol, 'limit': 1000, 'fromId': int(next_from_id)}
                try:
                    if rate_limiter is not None:
                        rate_limiter.wait(stop_evt)
                    r = sess.get(url, params=params, headers=headers, timeout=20)
                    if r.status_code in (429, 418):
                        ra = r.headers.get('Retry-After')
                        try:
                            wait_s = float(ra) if ra else float(rate_wait)
                        except Exception:
                            wait_s = float(rate_wait)
                        if r.status_code == 418:
                            wait_s = max(float(wait_s), float(BAN_418_MIN_SLEEP_S))
                        _log(f'Rate limited by Binance ({r.status_code}) — backing off for {wait_s:.1f}s…')
                        _sleep_cancel(max(1.0, wait_s), stop_evt)
                        if rate_limiter is not None:
                            rate_limiter.on_rate_limit(r.status_code)
                        rate_wait = min(600.0 if r.status_code == 418 else 180.0, max(1.0, rate_wait * (2.2 if r.status_code == 418 else 1.8)))
                        continue
                    r.raise_for_status()
                    data = r.json() or []
                    req_count += 1
                    rate_wait = 1.0
                    retries = 0
                    if rate_limiter is not None:
                        rate_limiter.observe_headers(r.headers, stop_evt=stop_evt, log_cb=log_cb)
                    _sleep_cancel(max(0.05, SUCCESS_SLEEP_S + random.uniform(0.0, 0.05)), stop_evt)
                except Exception as e:
                    retries += 1
                    if retries > 10:
                        _log(f'ERROR: resume failed too many times: {e}')
                        break
                    _log(f'ERROR: {e} — retrying tail resume…')
                    _sleep_cancel(backoff, stop_evt)
                    backoff = min(6.0, backoff * 1.35)
                    continue

                if not data:
                    _log('No further aggTrades returned during tail resume.')
                    break
                try:
                    data.sort(key=lambda x: int(x.get('a', 0)))
                except Exception:
                    pass

                stop = False
                for d in data:
                    try:
                        t = int(d.get('T', 0))
                        a = int(d.get('a', 0))
                        last_seen_ts_any = t
                    except Exception:
                        continue
                    if t < day_start_ms:
                        continue
                    if t >= day_end_ms:
                        stop = True
                        break
                    out.write(json.dumps({'a': a, 'T': t, 'p': d.get('p'), 'q': d.get('q'), 'm': d.get('m')}, separators=(',', ':')) + '\n')
                    wrote += 1

                try:
                    out.flush()
                except Exception:
                    pass

                if stop:
                    break

                try:
                    next_from_id = int(data[-1].get('a')) + 1
                except Exception:
                    break

                now_s = time.time()
                if (now_s - t_last_log) >= 15.0:
                    ts_prog = last_seen_ts_any
                    denom = max(1, int(day_end_ms - day_start_ms))
                    if ts_prog is None:
                        frac = 0.0
                        ts_txt = 'n/a'
                    else:
                        frac = max(0.0, min(1.0, float(ts_prog - day_start_ms) / float(denom)))
                        try:
                            ts_txt = pd.to_datetime(int(ts_prog), unit='ms', utc=True).strftime('%Y-%m-%d %H:%M:%S')
                        except Exception:
                            ts_txt = str(ts_prog)
                    elapsed = max(0.001, now_s - t_start)
                    req_per_min = req_count / (elapsed / 60.0)
                    trades_per_s = wrote / elapsed
                    _log(f'Progress {day_s}: req={req_count} (~{req_per_min:.1f}/min), +{wrote:,} trades ({trades_per_s:.0f}/s), last={ts_txt} UTC')
                    _progress(frac, f'{day_s} {frac*100:.1f}% — +{wrote:,} trades, last {ts_txt} UTC')
                    t_last_log = now_s
    except Exception as e:
        _log(f'FATAL: tail resume exception: {e}')

    if stop_evt and stop_evt.is_set():
        for p in (append_tmp, merge_tmp):
            try:
                if os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass
        return False

    if wrote <= 0:
        try:
            if os.path.exists(append_tmp):
                os.remove(append_tmp)
        except Exception:
            pass
        _progress(1.0, f'{day_s} resume: no new trades appended')
        return False

    delta_report = _verify_day_file(append_tmp, symbol, day_utc, compute_hash=False, stop_evt=stop_evt, log_cb=log_cb)
    delta_status = str(delta_report.get('status') or '').upper().strip()
    if delta_status.startswith('CORRUPT') or int(delta_report.get('rows') or 0) <= 0:
        _log(f'{day_s}: resume delta verification failed ({delta_status}) — keeping original file unchanged.')
        try:
            if os.path.exists(append_tmp):
                os.remove(append_tmp)
        except Exception:
            pass
        return False
    try:
        if int(delta_report.get('first_id')) <= int(last_id):
            _log(f"{day_s}: resume delta starts at invalid aggTradeId {delta_report.get('first_id')} (last existing {last_id}) — aborting merge.")
            try:
                if os.path.exists(append_tmp):
                    os.remove(append_tmp)
            except Exception:
                pass
            return False
    except Exception:
        pass

    try:
        with open(merge_tmp, 'wb') as out_f, open(dfp, 'rb') as base_f, open(append_tmp, 'rb') as delta_f:
            shutil.copyfileobj(base_f, out_f, length=1024 * 1024)
            shutil.copyfileobj(delta_f, out_f, length=1024 * 1024)
            _fsync_file_handle(out_f)
    except Exception as e:
        _log(f'{day_s}: failed to merge resumed cache atomically: {e}')
        try:
            if os.path.exists(append_tmp):
                os.remove(append_tmp)
        except Exception:
            pass
        return False

    merged_report = _verify_day_file(merge_tmp, symbol, day_utc, compute_hash=True, stop_evt=stop_evt, log_cb=log_cb)
    merged_status = str(merged_report.get('status') or '').upper().strip()
    if merged_status.startswith('CORRUPT') or int(merged_report.get('rows') or 0) < int(meta.get('rows') or 0):
        _log(f'{day_s}: merged resumed cache verification failed ({merged_status}) — original file preserved.')
        for p in (merge_tmp, append_tmp):
            try:
                if os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass
        return False

    try:
        _atomic_replace_strict(merge_tmp, dfp)
        merged_meta = _meta_from_verification(symbol, day_utc, merged_report, note_override='verified_tail_resume')
        _write_meta_atomic(mp, merged_meta)
    except Exception as e:
        _log(f'{day_s}: failed to commit resumed file: {e}')
        return False
    finally:
        for p in (append_tmp, merge_tmp):
            try:
                if os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass

    _log(f"{day_s}: resumed +{wrote:,} trades safely — cache now {int(merged_meta.get('rows') or 0):,} rows ({merged_meta.get('status')})")
    _progress(1.0, f'{day_s} resumed — +{wrote:,} trades')
    return True

# -----------------------------
# GUI App
# -----------------------------
class TickManagerApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Tick Manager — Binance Futures aggTrades daily cache")
        self.geometry("1080x680")

        self.stop_evt = threading.Event()
        self.worker: Optional[threading.Thread] = None

        self.var_cache = tk.StringVar(value=_default_cache_dir())
        self.var_symbol = tk.StringVar(value="BTCUSDT")
        self.var_start = tk.StringVar(value="")
        self.var_end = tk.StringVar(value="")
        self.var_include_no_meta = tk.BooleanVar(value=False)
        self.var_req_per_min = tk.StringVar(value="20")

        # thread-safe UI messaging (logs / progress)
        self.msg_q: "queue.Queue" = queue.Queue()
        self.var_progress = tk.StringVar(value="")
        self._log_max_chars = 250_000
        self._last_verify_reports: Dict[str, Dict[str, Any]] = {}
        self._report_windows: Dict[str, tk.Toplevel] = {}

        self._build_ui()
        # start queue pump
        self.after(120, self._process_queue)

    def _build_ui(self):
        top = ttk.Frame(self)
        top.pack(side=tk.TOP, fill=tk.X, padx=10, pady=8)

        ttk.Label(top, text="Cache dir:").grid(row=0, column=0, sticky="w")
        self.ent_cache = ttk.Entry(top, textvariable=self.var_cache, width=62)
        self.ent_cache.grid(row=0, column=1, sticky="we", padx=6)
        ttk.Button(top, text="Browse…", command=self.browse_cache).grid(row=0, column=2, padx=4)

        ttk.Label(top, text="Symbol:").grid(row=0, column=3, sticky="e", padx=(12, 4))
        ttk.Entry(top, textvariable=self.var_symbol, width=14).grid(row=0, column=4, sticky="w")

        ttk.Label(top, text="Start UTC:").grid(row=1, column=0, sticky="w", pady=(6, 0))
        ttk.Entry(top, textvariable=self.var_start, width=28).grid(row=1, column=1, sticky="w", padx=6, pady=(6, 0))
        ttk.Label(top, text="End UTC:").grid(row=1, column=3, sticky="e", padx=(12, 4), pady=(6, 0))
        ttk.Entry(top, textvariable=self.var_end, width=28).grid(row=1, column=4, sticky="w", pady=(6, 0))

        # Conservative pacing control to avoid Binance 429/418 during heavy paging
        frm_rate = ttk.Frame(top)
        frm_rate.grid(row=1, column=2, sticky="e", padx=(6, 2), pady=(6, 0))
        ttk.Label(frm_rate, text="Req/min:").pack(side=tk.LEFT)
        ttk.Entry(frm_rate, textvariable=self.var_req_per_min, width=6).pack(side=tk.LEFT, padx=(4, 0))


        btns = ttk.Frame(top)
        btns.grid(row=2, column=0, columnspan=5, sticky="w", pady=(10, 0))

        ttk.Button(btns, text="Scan Cache", command=self.scan).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(btns, text="Verify Cache", command=self.verify_cache).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(btns, text="Verification Report…", command=self.open_verification_report).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(btns, text="Download Missing (range)", command=self.download_missing).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(btns, text="Update OPEN days (today)", command=self.update_open).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(btns, text="Repair Incomplete (STALE/PARTIAL)", command=self.repair_incomplete).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Checkbutton(btns, text="Include NO_META in Repair", variable=self.var_include_no_meta).pack(side=tk.LEFT, padx=(10, 0))

        ttk.Button(btns, text="Stop", command=self.stop_worker).pack(side=tk.LEFT, padx=(18, 0))

        # status line
        self.lbl = ttk.Label(self, text="Ready.")
        self.lbl.pack(side=tk.TOP, fill=tk.X, padx=10, pady=(6, 2))

        # progress line
        prog = ttk.Frame(self)
        prog.pack(side=tk.TOP, fill=tk.X, padx=10, pady=(0, 4))
        self.pbar = ttk.Progressbar(prog, orient='horizontal', mode='determinate', maximum=1.0)
        self.pbar.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.lbl_prog = ttk.Label(prog, textvariable=self.var_progress, width=86, anchor='w')
        self.lbl_prog.pack(side=tk.LEFT, padx=(8, 0))
        ttk.Button(prog, text='Clear Log', command=self.clear_log).pack(side=tk.LEFT, padx=(8, 0))

        # table
        cols = ("day", "status", "integrity", "rows", "first", "last", "size_mb", "path")
        self.tree = ttk.Treeview(self, columns=cols, show="headings", height=22)
        self.tree.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=10, pady=8)

        self.tree.heading("day", text="Day (UTC)")
        self.tree.heading("status", text="Status")
        self.tree.heading("integrity", text="Integrity")
        self.tree.heading("rows", text="Rows")
        self.tree.heading("first", text="First tick (UTC)")
        self.tree.heading("last", text="Last tick (UTC)")
        self.tree.heading("size_mb", text="MB")
        self.tree.heading("path", text="Path")

        self.tree.column("day", width=110, anchor="w")
        self.tree.column("status", width=110, anchor="w")
        self.tree.column("integrity", width=120, anchor="w")
        self.tree.column("rows", width=90, anchor="e")
        self.tree.column("first", width=160, anchor="w")
        self.tree.column("last", width=160, anchor="w")
        self.tree.column("size_mb", width=60, anchor="e")
        self.tree.column("path", width=360, anchor="w")

        # scrollbar
        ysb = ttk.Scrollbar(self, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscroll=ysb.set)
        ysb.place(in_=self.tree, relx=1.0, rely=0, relheight=1.0, anchor="ne")
        self.tree.tag_configure('ok', foreground='#137333')
        self.tree.tag_configure('resume', foreground='#8a5a00')
        self.tree.tag_configure('warn', foreground='#8a4f00')
        self.tree.tag_configure('bad', foreground='#b3261e')
        self.tree.tag_configure('temp', foreground='#6f42c1')
        self.tree.tag_configure('missing', foreground='#666666')
        self.tree.bind('<Double-1>', lambda _evt: self.open_verification_report())


        # log window
        logfrm = ttk.Labelframe(self, text='Log')
        logfrm.pack(side=tk.BOTTOM, fill=tk.BOTH, expand=False, padx=10, pady=(0, 10))
        self.txt_log = scrolledtext.ScrolledText(logfrm, height=10, wrap='word', state='disabled')
        self.txt_log.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

    def set_status(self, msg: str):
        self.lbl.configure(text=msg)
        self.update_idletasks()

    def _enqueue(self, kind: str, payload):
        try:
            self.msg_q.put((kind, payload))
        except Exception:
            pass

    def _append_log(self, line: str):
        try:
            ts = time.strftime('%H:%M:%S')
            msg = f'[{ts}] {line}\n'
            self.txt_log.configure(state='normal')
            self.txt_log.insert('end', msg)
            try:
                n_chars = int(self.txt_log.count('1.0', 'end', 'chars')[0])
            except Exception:
                n_chars = 0
            if n_chars > self._log_max_chars:
                self.txt_log.delete('1.0', '200.0')
            self.txt_log.see('end')
            self.txt_log.configure(state='disabled')
        except Exception:
            pass

    def clear_log(self):
        try:
            self.txt_log.configure(state='normal')
            self.txt_log.delete('1.0', 'end')
            self.txt_log.configure(state='disabled')
        except Exception:
            pass

    def _set_progress_ui(self, *, mode: str = 'determinate', maximum: float = 1.0, value: float = 0.0, text: str = ''):
        try:
            if mode not in ('determinate', 'indeterminate'):
                mode = 'determinate'
            self.pbar.configure(mode=mode)
            if mode == 'determinate':
                try:
                    self.pbar.stop()
                except Exception:
                    pass
                self.pbar.configure(maximum=float(maximum))
                self.pbar['value'] = float(value)
            else:
                self.pbar.start(12)
            self.var_progress.set(text or '')
        except Exception:
            pass

    def _process_queue(self):
        try:
            while True:
                try:
                    kind, payload = self.msg_q.get_nowait()
                except Exception:
                    break
                if kind == 'log':
                    self._append_log(str(payload))
                elif kind == 'status':
                    try:
                        self.lbl.configure(text=str(payload))
                    except Exception:
                        pass
                elif kind == 'progress':
                    try:
                        d = dict(payload or {})
                        self._set_progress_ui(
                            mode=d.get('mode', 'determinate'),
                            maximum=d.get('maximum', 1.0),
                            value=d.get('value', 0.0),
                            text=d.get('text', '')
                        )
                    except Exception:
                        pass
        finally:
            self.after(150, self._process_queue)

    def browse_cache(self):
        p = filedialog.askdirectory(initialdir=self.var_cache.get() or os.getcwd())
        if p:
            self.var_cache.set(p)

    def _get_range_days(self) -> Optional[List["pd.Timestamp"]]:
        # If user left start/end empty: return None (scan existing files instead)
        s = (self.var_start.get() or "").strip()
        e = (self.var_end.get() or "").strip()
        if not s or not e:
            return None
        try:
            start = parse_utc(s)
            end = parse_utc(e)
            if end <= start:
                raise ValueError("End must be after Start")
            return _iter_days_utc(start, end)
        except Exception as ex:
            messagebox.showerror("Invalid datetime", f"{ex}\n\nUse e.g.:\n  2026-03-01 00:00:00\n  2026-03-03 12:30\n(all UTC)")
            return None

    def _get_req_per_min_value(self) -> float:
        """User-controlled pacing. Lower values reduce 429/418 risk."""
        s = (self.var_req_per_min.get() or "").strip()
        try:
            v = float(s)
        except Exception:
            v = 20.0
        return max(5.0, min(300.0, v))

    def _scan_existing_days(self, cache_dir: str, symbol: str) -> List[str]:
        folder = _sym_folder(cache_dir, symbol)
        if not os.path.isdir(folder):
            return []
        day_set = set()
        for fn in os.listdir(folder):
            day_s = fn[:10]
            if len(day_s) == 10 and day_s[4] == "-" and day_s[7] == "-":
                day_set.add(day_s)
        return sorted(day_set)

    def _clear_tree(self):
        for item in self.tree.get_children():
            self.tree.delete(item)

    def _selected_tree_item(self):
        sel = self.tree.selection()
        if sel:
            return sel[0]
        focus = self.tree.focus()
        return focus or None

    def _selected_day_string(self) -> Optional[str]:
        item = self._selected_tree_item()
        if not item:
            return None
        vals = self.tree.item(item, 'values') or []
        if not vals:
            return None
        day_s = str(vals[0]).strip()
        return day_s or None

    def open_verification_report(self):
        cache_dir = self.var_cache.get().strip() or 'data_cache'
        symbol = self.var_symbol.get().strip().upper()
        day_s = self._selected_day_string()
        if not day_s:
            messagebox.showinfo('Verification Report', 'Select a day in the table first.')
            return

        key = f'{symbol}:{day_s}'
        win = self._report_windows.get(key)
        if win is not None and win.winfo_exists():
            try:
                win.lift()
                win.focus_force()
                return
            except Exception:
                pass

        win = tk.Toplevel(self)
        win.title(f'Verification Report — {symbol} {day_s}')
        win.geometry('940x700')
        self._report_windows[key] = win

        top = ttk.Frame(win)
        top.pack(side=tk.TOP, fill=tk.X, padx=10, pady=8)
        lbl = ttk.Label(top, text=f'{symbol} {day_s}', font=('', 10, 'bold'))
        lbl.pack(side=tk.LEFT)
        ttk.Button(top, text='Refresh', command=lambda: _start_load()).pack(side=tk.RIGHT)

        txt = scrolledtext.ScrolledText(win, wrap='word', state='disabled')
        txt.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))

        def _set_text(body: str):
            try:
                txt.configure(state='normal')
                txt.delete('1.0', 'end')
                txt.insert('1.0', body)
                txt.see('1.0')
                txt.configure(state='disabled')
            except Exception:
                pass

        def _load_report():
            try:
                now_utc = _utc_now()
                dts = pd.Timestamp(day_s, tz='UTC')
                fp_expected = day_path(cache_dir, symbol, dts)
                mp = meta_path(fp_expected)
                variants = _list_day_file_variants(fp_expected)
                existing_fp, kind = _find_day_file_variants(fp_expected)
                meta = _read_meta(mp) if os.path.isfile(mp) else None
                report = None
                if existing_fp and os.path.isfile(existing_fp):
                    report = _verify_day_file(existing_fp, symbol, dts, compute_hash=True, stop_evt=None, log_cb=None)
                body = _format_verification_report_text(symbol, day_s, fp_expected, existing_fp, kind, meta, report, variants, now_utc)
                self._last_verify_reports[key] = {
                    'meta': meta,
                    'report': report,
                    'expected_path': fp_expected,
                    'existing_path': existing_fp,
                    'kind': kind,
                    'variants': variants,
                }
                self.after(0, lambda: _set_text(body))
            except Exception as e:
                err_txt = f"Error building report:\n\n{e}\n\n{traceback.format_exc()}"
                self.after(0, lambda txt=err_txt: _set_text(txt))

        def _start_load():
            _set_text('Building verification report…')
            th = threading.Thread(target=_load_report, daemon=True)
            th.start()

        def _on_close():
            try:
                self._report_windows.pop(key, None)
            finally:
                win.destroy()

        win.protocol('WM_DELETE_WINDOW', _on_close)
        _start_load()

    def verify_cache(self):
        cache_dir = self.var_cache.get().strip() or "data_cache"
        symbol = self.var_symbol.get().strip().upper()

        def _job(log, prog):
            now = _utc_now()
            days_range = self._get_range_days()
            if days_range is None:
                day_strs = self._scan_existing_days(cache_dir, symbol)
                if not day_strs:
                    log("No files found to verify.")
                    return
            else:
                day_strs = [d.strftime("%Y-%m-%d") for d in days_range]

            total = len(day_strs)
            prog(mode='determinate', maximum=1.0, value=0.0, text=f"0/{total} days")
            done = 0
            for day_s in day_strs:
                if self.stop_evt.is_set():
                    break
                dts = pd.Timestamp(day_s, tz="UTC")
                fp_expected = day_path(cache_dir, symbol, dts)
                mp = meta_path(fp_expected)
                existing_fp, kind = _find_day_file_variants(fp_expected)
                if existing_fp is None:
                    log(f"{day_s}: missing file")
                    self._last_verify_reports[f"{symbol}:{day_s}"] = {
                        'meta': _read_meta(mp) if os.path.isfile(mp) else None,
                        'report': None,
                        'expected_path': fp_expected,
                        'existing_path': None,
                        'kind': kind,
                        'variants': _list_day_file_variants(fp_expected),
                    }
                    done += 1
                    prog(mode='determinate', maximum=1.0, value=done / float(max(1, total)), text=f"{done}/{total} — {day_s} missing")
                    continue

                log(f"{day_s}: verifying {os.path.basename(existing_fp)} …")
                report = None
                if existing_fp != fp_expected:
                    meta = _salvage_variant_into_final(existing_fp, fp_expected, mp, symbol, dts, log_cb=log, stop_evt=self.stop_evt, compute_hash=True)
                    if meta is not None and os.path.isfile(fp_expected):
                        report = _verify_day_file(fp_expected, symbol, dts, compute_hash=True, stop_evt=self.stop_evt, log_cb=None)
                        existing_fp = fp_expected
                        kind = 'FINAL'
                else:
                    report = _verify_day_file(existing_fp, symbol, dts, compute_hash=True, stop_evt=self.stop_evt, log_cb=log)
                    meta = None
                    verify_status = str(report.get('status') or '').upper().strip()
                    if int(report.get('rows') or 0) > 0 and not verify_status.startswith('CORRUPT'):
                        meta = _meta_from_verification(symbol, dts, report)
                        try:
                            _write_meta_atomic(mp, meta)
                        except Exception as e:
                            log(f"{day_s}: failed to write meta: {e}")
                    else:
                        log(f"{day_s}: verification failed ({verify_status})")
                self._last_verify_reports[f"{symbol}:{day_s}"] = {
                    'meta': meta if isinstance(meta, dict) else (_read_meta(mp) if os.path.isfile(mp) else None),
                    'report': report,
                    'expected_path': fp_expected,
                    'existing_path': existing_fp,
                    'kind': kind,
                    'variants': _list_day_file_variants(fp_expected),
                }
                done += 1
                st = meta.get('status') if isinstance(meta, dict) else kind or 'UNKNOWN'
                prog(mode='determinate', maximum=1.0, value=done / float(max(1, total)), text=f"{done}/{total} — {day_s} {st}")

        self._run_worker(_job, "Verifying cache files…")

    def scan(self):
        try:
            cache_dir = self.var_cache.get().strip() or "data_cache"
            symbol = self.var_symbol.get().strip().upper()
            now_utc = _utc_now()

            self._clear_tree()

            days_range = self._get_range_days()
            if days_range is None:
                day_strs = self._scan_existing_days(cache_dir, symbol)
                if not day_strs:
                    self.set_status("No files found (and no date range provided).")
                    return
            else:
                day_strs = [d.strftime("%Y-%m-%d") for d in days_range]

            rows_out: List[DayStatus] = []
            for day_s in day_strs:
                dts = pd.Timestamp(day_s, tz="UTC")
                fp = day_path(cache_dir, symbol, dts)
                mp = meta_path(fp)
                meta = _read_meta(mp) if os.path.isfile(mp) else None

                existing_fp, kind = _find_day_file_variants(fp)
                if existing_fp is None:
                    rows_out.append(DayStatus(day=day_s, status='MISSING', integrity='MISSING', size_mb='', path=fp, meta_path=mp))
                    continue

                if kind == 'TINY':
                    st = 'TINY_FILE'
                elif kind == 'TMP':
                    st = 'TEMP_PRESENT'
                elif kind == 'PART':
                    st = 'PARTIAL_FILE'
                else:
                    st = _status_for_day(day_s, meta, now_utc)

                badge = _integrity_badge(day_s, meta, kind, now_utc)
                ds = DayStatus(
                    day=day_s,
                    status=st,
                    integrity=badge,
                    rows=int(meta.get('rows')) if meta and meta.get('rows') is not None else None,
                    first_ts=int(meta.get('first_ts_ms')) if meta and meta.get('first_ts_ms') is not None else None,
                    last_ts=int(meta.get('last_ts_ms')) if meta and meta.get('last_ts_ms') is not None else None,
                    first_id=int(meta.get('first_id')) if meta and meta.get('first_id') is not None else None,
                    last_id=int(meta.get('last_id')) if meta and meta.get('last_id') is not None else None,
                    size_mb=_file_size_mb(existing_fp),
                    path=existing_fp,
                    meta_path=mp,
                )
                rows_out.append(ds)

            for ds in rows_out:
                self.tree.insert(
                    '',
                    'end',
                    values=(
                        ds.day,
                        ds.status,
                        ds.integrity,
                        '' if ds.rows is None else f"{ds.rows:,}",
                        _format_ts_ms(ds.first_ts),
                        _format_ts_ms(ds.last_ts),
                        ds.size_mb,
                        ds.path,
                    ),
                    tags=(_integrity_tag(ds.integrity),),
                )

            self.set_status(f"Scan complete: {len(rows_out)} day(s).  (Today UTC: {now_utc.strftime('%Y-%m-%d')})")
        except Exception as e:
            messagebox.showerror("Scan error", f"{e}\n\n{traceback.format_exc()}")

    def _run_worker(self, fn, title: str):
        if self.worker and self.worker.is_alive():
            messagebox.showinfo("Busy", "A task is already running. Click Stop first.")
            return
        self.stop_evt.clear()

        def _log(m: str):
            self._enqueue('log', m)
            self._enqueue('status', m)

        def _prog(**kwargs):
            self._enqueue('progress', kwargs)

        def _wrap():
            try:
                self._enqueue('log', f"=== {title} ===")
                self._enqueue('status', title)
                self._enqueue('progress', {'mode': 'indeterminate', 'text': title})
                fn(_log, _prog)
                self._enqueue('progress', {'mode': 'determinate', 'maximum': 1.0, 'value': 1.0, 'text': 'Done.'})
                self._enqueue('log', "=== Done ===")
                self._enqueue('status', 'Done.')
                self.after(0, self.scan)
            except Exception as ex:
                self._enqueue('progress', {'mode': 'determinate', 'maximum': 1.0, 'value': 0.0, 'text': 'Error'})
                self._enqueue('status', 'Error.')
                self._enqueue('log', f"ERROR: {ex}")
                self.after(0, lambda: messagebox.showerror("Task error", f"{ex}\n\n{traceback.format_exc()}"))

        self.worker = threading.Thread(target=_wrap, daemon=True)
        self.worker.start()

    def stop_worker(self):
        self.stop_evt.set()
        self._enqueue('status', 'Stopping… (may take a moment)')
        self._enqueue('log', 'Stop requested.')
        try:
            self.pbar.stop()
        except Exception:
            pass

    def download_missing(self):
        cache_dir = self.var_cache.get().strip() or "data_cache"
        symbol = self.var_symbol.get().strip().upper()
        days = self._get_range_days()
        if not days:
            messagebox.showinfo("Need a range", "Enter Start UTC and End UTC to download missing days.")
            return

        def _job(log, prog):
            rl = RateLimiter(self._get_req_per_min_value())
            total = len(days)
            prog(mode='determinate', maximum=1.0, value=0.0, text=f"0/{total} days")
            done = 0
            for d in days:
                if self.stop_evt.is_set():
                    break
                day_s = d.strftime('%Y-%m-%d')

                prog(
                    mode='determinate',
                    maximum=1.0,
                    value=(done / float(max(1, total))),
                    text=f"{done}/{total} — {day_s}"
                )

                fp = day_path(cache_dir, symbol, d)
                mp = meta_path(fp)
                existing_fp, kind = _find_day_file_variants(fp)
                meta = _read_meta(mp) if os.path.isfile(mp) else None
                status = _status_for_day(day_s, meta, _utc_now()) if meta is not None else None
                if existing_fp == fp and kind == "FINAL" and status == "FINAL":
                    log(f"Have verified FINAL cache for {symbol} {day_s} — skip")
                    done += 1
                    continue

                def _pcb(frac, text: str = ""):
                    try:
                        frac = float(frac)
                    except Exception:
                        frac = 0.0
                    frac = max(0.0, min(1.0, frac))
                    overall = (done + frac) / float(max(1, total))
                    prog(mode='determinate', maximum=1.0, value=overall, text=text or f"{day_s}…")

                _download_day_full(
                    cache_dir, symbol, d,
                    log_cb=log,
                    progress_cb=_pcb,
                    stop_evt=self.stop_evt,
                    rate_limiter=rl
                )

                done += 1
                prog(
                    mode='determinate',
                    maximum=1.0,
                    value=(done / float(max(1, total))),
                    text=f"{done}/{total} — done {day_s}"
                )
                _sleep_cancel(max(0.0, DAY_PAUSE_S), self.stop_evt)

        self._run_worker(_job, "Downloading missing days…")
    def update_open(self):
        cache_dir = self.var_cache.get().strip() or "data_cache"
        symbol = self.var_symbol.get().strip().upper()
        now = _utc_now()
        today = now.floor("D")

        def _job(log, prog):
            rl = RateLimiter(self._get_req_per_min_value())
            prog(mode='determinate', maximum=1.0, value=0.0, text=f"Updating OPEN day: {today.strftime('%Y-%m-%d')}…")
            def _pcb(frac=None, text: str = ""):
                if frac is None:
                    prog(mode='indeterminate', text=text or f"Updating OPEN day: {today.strftime('%Y-%m-%d')}…")
                    return
                try:
                    v = float(frac)
                except Exception:
                    v = 0.0
                v = max(0.0, min(1.0, v))
                prog(mode='determinate', maximum=1.0, value=v, text=text)
            _append_update_open_day(cache_dir, symbol, today, log_cb=log, progress_cb=_pcb, stop_evt=self.stop_evt, rate_limiter=rl)
            prog(mode='determinate', maximum=1.0, value=1.0, text="Update complete")

        self._run_worker(_job, "Updating OPEN day (today)…")
    def repair_incomplete(self):
        cache_dir = self.var_cache.get().strip() or "data_cache"
        symbol = self.var_symbol.get().strip().upper()
        include_no_meta = bool(self.var_include_no_meta.get())

        def _job(log, prog):
            rl = RateLimiter(self._get_req_per_min_value())
            now = _utc_now()
            today_s = now.strftime("%Y-%m-%d")

            days_range = self._get_range_days()
            if days_range is not None:
                day_strs = [d.strftime("%Y-%m-%d") for d in days_range]
            else:
                folder = _sym_folder(cache_dir, symbol)
                if not os.path.isdir(folder):
                    log("No symbol folder; nothing to repair.")
                    return
                day_set = set()
                for fn in os.listdir(folder):
                    if len(fn) < 10:
                        continue
                    day_s = fn[:10]
                    if len(day_s) == 10 and day_s[4] == "-" and day_s[7] == "-":
                        day_set.add(day_s)
                day_strs = sorted(day_set)

            eligible = [d for d in day_strs if d != today_s]
            if not eligible:
                log("Nothing to repair (no eligible days; today is handled by Update OPEN).")
                return

            total = len(eligible)
            prog(mode='determinate', maximum=1.0, value=0.0, text=f"0/{total} days")
            done = 0

            for day_s in eligible:
                if self.stop_evt.is_set():
                    break
                prog(mode='determinate', maximum=total, value=done, text=f"{done}/{total} — check {day_s}")

                dts = pd.Timestamp(day_s, tz="UTC")
                fp_expected = day_path(cache_dir, symbol, dts)
                mp = meta_path(fp_expected)
                meta = _read_meta(mp) if os.path.isfile(mp) else None

                existing_fp, kind = _find_day_file_variants(fp_expected)
                if existing_fp is None:
                    done += 1
                    continue

                if existing_fp != fp_expected:
                    log(f"{day_s}: found {os.path.basename(existing_fp)} — trying salvage before repair…")
                    salvaged = _salvage_variant_into_final(existing_fp, fp_expected, mp, symbol, dts, log_cb=log, stop_evt=self.stop_evt, compute_hash=False)
                    if salvaged is not None:
                        meta = salvaged
                        existing_fp = fp_expected
                        kind = 'FINAL'
                    else:
                        log(f"{day_s}: salvage failed; will choose between resume/full redownload.")

                if existing_fp == fp_expected and (not meta or meta.get('last_id') is None) and include_no_meta:
                    log(f"{day_s}: building meta from existing file (NO_META)…")
                    meta = _build_meta_from_existing_file(existing_fp, mp, symbol, dts, log_cb=log, stop_evt=self.stop_evt)

                badge = _integrity_badge(day_s, meta, kind, now)
                status = _status_for_day(day_s, meta, now) if meta is not None and kind == 'FINAL' else badge

                if status == 'FINAL' or badge == 'OK FINAL':
                    done += 1
                    continue

                def _pcb(frac=None, text: str = ''):
                    if frac is None:
                        prog(mode='indeterminate', text=text or f"{day_s}…")
                        return
                    try:
                        frac_f = float(frac)
                    except Exception:
                        frac_f = 0.0
                    frac_f = max(0.0, min(1.0, frac_f))
                    overall = (done + frac_f) / float(max(1, total))
                    prog(mode='determinate', maximum=1.0, value=overall, text=text or f"{day_s}…")

                if _can_resume_tail_from_meta(meta):
                    log(f"{day_s}: resumable tail gap detected — continuing from cached aggTradeId tail instead of full re-download.")
                    ok = _resume_partial_tail_day(cache_dir, symbol, dts, log_cb=log, progress_cb=_pcb, stop_evt=self.stop_evt, rate_limiter=rl)
                    if (not ok) and (not self.stop_evt.is_set()):
                        log(f"{day_s}: tail resume did not complete; falling back to full verified re-download.")
                        _download_day_full(cache_dir, symbol, dts, log_cb=log, progress_cb=_pcb, stop_evt=self.stop_evt, rate_limiter=rl)
                else:
                    log(f"{day_s}: not resumable ({badge}) — doing full verified re-download.")
                    _download_day_full(cache_dir, symbol, dts, log_cb=log, progress_cb=_pcb, stop_evt=self.stop_evt, rate_limiter=rl)

                done += 1
                prog(mode='determinate', maximum=1.0, value=(done / float(max(1, total))), text=f"{done}/{total} — repaired {day_s}")
                _sleep_cancel(max(0.0, DAY_PAUSE_S), self.stop_evt)

        self._run_worker(_job, "Repairing incomplete days…")

# -------------------------# -------------------------
# Entry point + crash guard
# -------------------------

import traceback as _tb


def _pause_console():
    """Keep console open on Windows when double-clicked, so errors are visible."""
    try:
        input("\n[PAUSE] Press ENTER to close...")
    except Exception:
        pass


def _install_thread_excepthook():
    try:
        import threading

        def _hook(args):
            print("\n========== THREAD CRASH ==========")
            _tb.print_exception(args.exc_type, args.exc_value, args.exc_traceback)
            print("========== END THREAD CRASH ======\n")

        threading.excepthook = _hook  # type: ignore[attr-defined]
    except Exception:
        pass


def main(argv: Optional[List[str]] = None):
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("--cache-dir", default=None, help="Cache directory (e.g., data_cache)")
    parser.add_argument("--symbol", default=None, help="Symbol (e.g., BTCUSDT)")
    parser.add_argument("--start", default=None, help="Start UTC (e.g., 2024-03-01 00:00:00)")
    parser.add_argument("--end", default=None, help="End UTC (exclusive) (e.g., 2024-03-03 00:00:00)")
    parser.add_argument("--req-per-min", default=None, help="Request pacing (req/min).")
    parser.add_argument("--pause", action="store_true", help="Pause console on exit/crash (for double-click).")

    args = parser.parse_args(argv)

    app = TickManagerApp()

    # Resolve cache dir relative to project root (parent of this file's directory)
    if args.cache_dir is not None:
        cd = (args.cache_dir or "").strip()
        if cd and not os.path.isabs(cd):
            cd = os.path.join(_project_root_dir(), cd)
        app.var_cache.set(_coerce_cache_dir(cd))

    if args.symbol:
        app.var_symbol.set(args.symbol.strip().upper())

    if args.start:
        app.var_start.set(args.start.strip())

    if args.end:
        app.var_end.set(args.end.strip())

    if args.req_per_min:
        try:
            app.var_req_per_min.set(str(float(args.req_per_min)))
        except Exception:
            pass

    app._pause_always = bool(args.pause)  # type: ignore[attr-defined]
    app.mainloop()


if __name__ == "__main__":
    _install_thread_excepthook()
    pause_always = "--pause" in sys.argv
    try:
        main(sys.argv[1:])
        if pause_always:
            _pause_console()
    except Exception:
        # Print to console
        print("\n========== FATAL CRASH ==========")
        _tb.print_exc()
        print("========== END FATAL CRASH ======\n")

        # Write crash log next to script
        try:
            here = os.path.dirname(os.path.abspath(__file__))
            crash_path = os.path.join(here, "tick_manager_crash.log")
            with open(crash_path, "w", encoding="utf-8") as f:
                f.write(_tb.format_exc())
            print(f"Crash log written to: {crash_path}")
        except Exception:
            pass

        _pause_console()
