from __future__ import annotations

import asyncio
import json
import math
import mimetypes
import threading
import traceback
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import parse_qs, unquote, urlparse

import websockets

from app.services.ui_v2_downloader import DownloaderJobStore, DownloaderRuntimeService
from app.services.ui_v2_live import LiveRuntimeService
from app.services.ui_v2_runtime import BacktestRuntimeService, ExitSweepRequest, PresetRepository, RunOverrides


STATIC_ROOT = Path(__file__).resolve().parent / "static"


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_json_safe(item) for item in value]
    try:
        scalar = value.item()
    except Exception:
        scalar = None
    if scalar is not None and scalar is not value:
        return _json_safe(scalar)
    try:
        numeric = float(value)
    except Exception:
        return value
    return numeric if math.isfinite(numeric) else None


def _json_dumps(payload: Any) -> str:
    return json.dumps(_json_safe(payload), ensure_ascii=False, default=str, allow_nan=False)


@dataclass
class JobRecord:
    job_id: str
    kind: str = "backtest"
    state: str = "queued"
    message: str = "Queued"
    progress_pct: float = 0.0
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")
    updated_at: str = field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")


class JobStore:
    def __init__(self, runtime: BacktestRuntimeService) -> None:
        self.runtime = runtime
        self._lock = threading.Lock()
        self._jobs: Dict[str, JobRecord] = {}

    def create_backtest_job(self, preset_name: str, overrides: Optional[Dict[str, Any]]) -> JobRecord:
        job = JobRecord(job_id=str(uuid.uuid4()), kind="backtest")
        with self._lock:
            self._jobs[job.job_id] = job
        thread = threading.Thread(
            target=self._run_backtest_job,
            args=(job.job_id, preset_name, overrides or {}),
            name=f"ui-v2-job-{job.job_id[:8]}",
            daemon=True,
        )
        thread.start()
        return job

    def create_backtest_batch_job(self, preset_name: str, cases: list[dict[str, Any]]) -> JobRecord:
        job = JobRecord(job_id=str(uuid.uuid4()), kind="backtest_batch")
        with self._lock:
            self._jobs[job.job_id] = job
        thread = threading.Thread(
            target=self._run_backtest_batch_job,
            args=(job.job_id, preset_name, list(cases or [])),
            name=f"ui-v2-batch-{job.job_id[:8]}",
            daemon=True,
        )
        thread.start()
        return job

    def create_exit_sweep_job(self, preset_name: str, run_id: Optional[str], exit_sweep: Dict[str, Any]) -> JobRecord:
        job = JobRecord(job_id=str(uuid.uuid4()), kind="exit_sweep")
        with self._lock:
            self._jobs[job.job_id] = job
        thread = threading.Thread(
            target=self._run_exit_sweep_job,
            args=(job.job_id, preset_name, run_id, exit_sweep or {}),
            name=f"ui-v2-sweep-{job.job_id[:8]}",
            daemon=True,
        )
        thread.start()
        return job

    def _run_backtest_job(self, job_id: str, preset_name: str, overrides: Dict[str, Any]) -> None:
        def progress(message: str, pct: Optional[float] = None) -> None:
            self.update(job_id, state="running", message=message, progress_pct=float(pct or 0.0))

        try:
            self.update(job_id, state="running", message="Starting backtest...", progress_pct=1.0)
            result = self.runtime.run_saved_preset(
                preset_name=preset_name,
                overrides=RunOverrides.from_payload(overrides),
                progress_cb=progress,
            )
            self.update(job_id, state="completed", message="Backtest complete.", progress_pct=100.0, result=result)
        except Exception as exc:
            details = "".join(traceback.format_exception_only(type(exc), exc)).strip()
            self.update(job_id, state="failed", message="Backtest failed.", error=details)

    def _run_exit_sweep_job(self, job_id: str, preset_name: str, run_id: Optional[str], exit_sweep: Dict[str, Any]) -> None:
        def progress(message: str, pct: Optional[float] = None) -> None:
            self.update(job_id, state="running", message=message, progress_pct=float(pct or 0.0))

        try:
            request = ExitSweepRequest.from_payload(exit_sweep)
            if request is None:
                raise ValueError("exit_sweep payload is required.")
            self.update(job_id, state="running", message="Starting exit sweep...", progress_pct=1.0)
            result = self.runtime.run_exit_sweep(
                preset_name=preset_name,
                run_id=run_id,
                request=request,
                progress_cb=progress,
            )
            self.update(job_id, state="completed", message="Exit sweep complete.", progress_pct=100.0, result=result)
        except Exception as exc:
            details = "".join(traceback.format_exception_only(type(exc), exc)).strip()
            self.update(job_id, state="failed", message="Exit sweep failed.", error=details)

    def _run_backtest_batch_job(self, job_id: str, preset_name: str, cases: list[dict[str, Any]]) -> None:
        def progress(message: str, pct: Optional[float] = None) -> None:
            self.update(job_id, state="running", message=message, progress_pct=float(pct or 0.0))

        try:
            self.update(job_id, state="running", message="Starting backtest batch...", progress_pct=1.0)
            result = self.runtime.run_saved_preset_batch(
                preset_name=preset_name,
                cases=list(cases or []),
                progress_cb=progress,
            )
            self.update(job_id, state="completed", message="Backtest batch complete.", progress_pct=100.0, result=result)
        except Exception as exc:
            details = "".join(traceback.format_exception_only(type(exc), exc)).strip()
            self.update(job_id, state="failed", message="Backtest batch failed.", error=details)

    def update(
        self,
        job_id: str,
        *,
        state: Optional[str] = None,
        message: Optional[str] = None,
        progress_pct: Optional[float] = None,
        result: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> None:
        with self._lock:
            job = self._jobs[job_id]
            if state is not None:
                job.state = state
            if message is not None:
                job.message = message
            if progress_pct is not None:
                job.progress_pct = progress_pct
            if result is not None:
                job.result = result
            if error is not None:
                job.error = error
            job.updated_at = datetime.utcnow().isoformat() + "Z"

    def get(self, job_id: str) -> Optional[JobRecord]:
        with self._lock:
            return self._jobs.get(job_id)


@dataclass
class UiV2Context:
    presets: PresetRepository
    runtime: BacktestRuntimeService
    jobs: JobStore
    downloader: DownloaderRuntimeService
    downloader_jobs: DownloaderJobStore
    live: LiveRuntimeService
    live_ws_port: int


class UiV2RequestHandler(BaseHTTPRequestHandler):
    server_version = "TradeInspectorV2/0.2"

    def __init__(self, *args: Any, context: UiV2Context, **kwargs: Any) -> None:
        self.context = context
        super().__init__(*args, **kwargs)

    def log_message(self, format: str, *args: Any) -> None:
        return

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        if path.startswith("/api/"):
            self._handle_api_get(path, parse_qs(parsed.query or ""))
            return
        self._serve_static(path)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        if path.startswith("/api/v2/data/jobs/") and path.endswith("/cancel"):
            self._handle_data_job_cancel_post(path)
            return
        if path == "/api/v2/backtests/exit-sweep":
            self._handle_exit_sweep_post()
            return
        if path == "/api/v2/backtests/batch":
            self._handle_backtest_batch_post()
            return
        if path == "/api/v2/backtests":
            self._handle_backtest_post()
            return
        if path == "/api/v2/presets":
            self._handle_preset_post()
            return
        if path == "/api/v2/presets/preview-snapshot":
            self._handle_preset_preview_snapshot_post()
            return
        if path == "/api/v2/presets/preview-market-audit":
            self._handle_preset_preview_market_audit_post()
            return
        if path == "/api/v2/market-audit":
            self._handle_market_audit_post()
            return
        if path == "/api/v2/live/session":
            self._handle_live_session_post()
            return
        if path == "/api/v2/data/jobs":
            self._handle_data_job_post()
            return
        self._send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def do_DELETE(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        if path == "/api/v2/live/session":
            self._handle_live_session_delete()
            return
        if path.startswith("/api/v2/presets/"):
            self._handle_preset_delete(path)
            return
        self._send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def _handle_api_get(self, path: str, query: Dict[str, list[str]]) -> None:
        if path == "/api/v2/health":
            self._send_json(
                {
                    "status": "ok",
                    "ui": "web-v2",
                    "mode": "local",
                    "static_root": str(STATIC_ROOT),
                    "live_transport": "websocket",
                    "live_ws_url": self._make_live_ws_url(),
                    "downloader": "enabled",
                }
            )
            return
        if path == "/api/v2/presets":
            self._send_json(self.context.presets.list_presets())
            return
        if path == "/api/v2/data/summary":
            self._send_json(self.context.downloader.inspect_cache(self._query_to_payload(query)))
            return
        if path == "/api/v2/data/tick-report":
            self._send_json(self.context.downloader.get_tick_report(self._query_to_payload(query)))
            return
        if path.startswith("/api/v2/data/jobs/"):
            job_id = path.rsplit("/", 1)[-1]
            job = self.context.downloader_jobs.get(job_id)
            if job is None:
                self._send_json({"error": "Job not found"}, status=HTTPStatus.NOT_FOUND)
                return
            self._send_json(
                {
                    "job_id": job.job_id,
                    "action": job.action,
                    "state": job.state,
                    "message": job.message,
                    "progress_pct": job.progress_pct,
                    "created_at": job.created_at,
                    "updated_at": job.updated_at,
                    "result": job.result,
                    "error": job.error,
                    "logs": list(job.logs),
                    "can_cancel": job.can_cancel,
                    "stop_requested": job.stop_requested,
                }
            )
            return
        if path == "/api/v2/live/session":
            self._send_json(self._live_payload(self.context.live.get_state()))
            return
        if path.startswith("/api/v2/backtests/") and path.endswith("/trades"):
            parts = [part for part in path.split("/") if part]
            if len(parts) >= 5:
                run_id = unquote(parts[3])
                preset_name = (query.get("preset_name") or [None])[0]
                try:
                    payload = self.context.runtime.list_run_trades(run_id=run_id, preset_name=preset_name)
                except ValueError as exc:
                    self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
                    return
                self._send_json(payload)
                return
        if "/trades/" in path and path.startswith("/api/v2/backtests/"):
            parts = [part for part in path.split("/") if part]
            if len(parts) >= 6 and parts[4] == "trades":
                run_id = unquote(parts[3])
                preset_name = (query.get("preset_name") or [None])[0]
                try:
                    trade_id = int(parts[5])
                except Exception:
                    self._send_json({"error": "trade_id must be an integer"}, status=HTTPStatus.BAD_REQUEST)
                    return
                padding_bars_raw = (query.get("padding_bars") or [8])[0]
                try:
                    padding_bars = max(0, int(padding_bars_raw))
                except Exception:
                    padding_bars = 8
                try:
                    payload = self.context.runtime.get_trade_detail(
                        run_id=run_id,
                        preset_name=preset_name,
                        trade_id=trade_id,
                        padding_bars=padding_bars,
                    )
                except ValueError as exc:
                    self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
                    return
                self._send_json(payload)
                return
        if path.startswith("/api/v2/presets/") and path.endswith("/snapshot"):
            parts = [part for part in path.split("/") if part]
            if len(parts) >= 5:
                name = unquote(parts[3])
                self._send_json(self.context.runtime.get_preset_snapshot(name))
                return
        if path.startswith("/api/v2/presets/") and path.endswith("/market-audit"):
            parts = [part for part in path.split("/") if part]
            if len(parts) >= 5:
                name = unquote(parts[3])
                timeframe = str((query.get("timeframe") or ["5m"])[0] or "5m")
                bars_raw = (query.get("bars") or [300])[0]
                end_utc = (query.get("end") or query.get("end_utc") or [None])[0]
                try:
                    bars = int(bars_raw)
                except Exception:
                    bars = 300
                try:
                    payload = self.context.runtime.get_preset_market_audit(
                        name,
                        timeframe=timeframe,
                        bars=bars,
                        end_utc=end_utc,
                    )
                except ValueError as exc:
                    self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
                    return
                self._send_json(payload)
                return
        if path.startswith("/api/v2/presets/"):
            name = unquote(path.rsplit("/", 1)[-1])
            self._send_json(self.context.presets.get_preset(name))
            return
        if path.startswith("/api/v2/jobs/"):
            job_id = path.rsplit("/", 1)[-1]
            job = self.context.jobs.get(job_id)
            if job is None:
                self._send_json({"error": "Job not found"}, status=HTTPStatus.NOT_FOUND)
                return
            self._send_json(
                {
                    "job_id": job.job_id,
                    "kind": job.kind,
                    "state": job.state,
                    "message": job.message,
                    "progress_pct": job.progress_pct,
                    "created_at": job.created_at,
                    "updated_at": job.updated_at,
                    "result": job.result,
                    "error": job.error,
                }
            )
            return
        self._send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def _handle_backtest_post(self) -> None:
        payload = self._read_json_body()
        preset_name = str(payload.get("preset_name") or "").strip()
        if not preset_name:
            self._send_json({"error": "preset_name is required"}, status=HTTPStatus.BAD_REQUEST)
            return
        overrides = payload.get("overrides", {})
        if not isinstance(overrides, dict):
            self._send_json({"error": "overrides must be a JSON object"}, status=HTTPStatus.BAD_REQUEST)
            return
        job = self.context.jobs.create_backtest_job(preset_name, overrides)
        self._send_json(
            {
                "job_id": job.job_id,
                "kind": job.kind,
                "state": job.state,
                "message": job.message,
                "progress_pct": job.progress_pct,
            },
            status=HTTPStatus.ACCEPTED,
        )

    def _handle_backtest_batch_post(self) -> None:
        payload = self._read_json_body()
        preset_name = str(payload.get("preset_name") or "").strip()
        if not preset_name:
            self._send_json({"error": "preset_name is required"}, status=HTTPStatus.BAD_REQUEST)
            return
        cases = payload.get("cases")
        if not isinstance(cases, list) or not cases:
            self._send_json({"error": "cases must be a non-empty JSON array"}, status=HTTPStatus.BAD_REQUEST)
            return
        if not all(isinstance(item, dict) for item in cases):
            self._send_json({"error": "each batch case must be a JSON object"}, status=HTTPStatus.BAD_REQUEST)
            return
        job = self.context.jobs.create_backtest_batch_job(preset_name, cases)
        self._send_json(
            {
                "job_id": job.job_id,
                "kind": job.kind,
                "state": job.state,
                "message": job.message,
                "progress_pct": job.progress_pct,
            },
            status=HTTPStatus.ACCEPTED,
        )

    def _handle_exit_sweep_post(self) -> None:
        payload = self._read_json_body()
        preset_name = str(payload.get("preset_name") or "").strip()
        if not preset_name:
            self._send_json({"error": "preset_name is required"}, status=HTTPStatus.BAD_REQUEST)
            return
        run_id_raw = payload.get("run_id")
        run_id = str(run_id_raw).strip() if run_id_raw is not None and str(run_id_raw).strip() else None
        exit_sweep = payload.get("exit_sweep")
        if not isinstance(exit_sweep, dict):
            self._send_json({"error": "exit_sweep must be a JSON object"}, status=HTTPStatus.BAD_REQUEST)
            return
        job = self.context.jobs.create_exit_sweep_job(preset_name, run_id, exit_sweep)
        self._send_json(
            {
                "job_id": job.job_id,
                "kind": job.kind,
                "state": job.state,
                "message": job.message,
                "progress_pct": job.progress_pct,
            },
            status=HTTPStatus.ACCEPTED,
        )

    def _handle_preset_post(self) -> None:
        payload = self._read_json_body()
        preset_name = str(payload.get("name") or "").strip()
        preset = payload.get("preset")
        set_last = bool(payload.get("set_last", True))
        if not preset_name:
            self._send_json({"error": "Preset name is required"}, status=HTTPStatus.BAD_REQUEST)
            return
        if not isinstance(preset, dict):
            self._send_json({"error": "Preset must be a JSON object"}, status=HTTPStatus.BAD_REQUEST)
            return
        saved = self.context.presets.save_preset(preset_name, preset, set_last=set_last)
        self._send_json(saved, status=HTTPStatus.OK)

    def _handle_preset_preview_snapshot_post(self) -> None:
        payload = self._read_json_body()
        preset_name = str(payload.get("name") or "builder_preview").strip() or "builder_preview"
        preset = payload.get("preset")
        if not isinstance(preset, dict):
            self._send_json({"error": "preset must be a JSON object"}, status=HTTPStatus.BAD_REQUEST)
            return
        try:
            result = self.context.runtime.get_preset_snapshot_preview(preset_name, preset)
        except ValueError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        self._send_json(result, status=HTTPStatus.OK)

    def _handle_preset_preview_market_audit_post(self) -> None:
        payload = self._read_json_body()
        preset_name = str(payload.get("name") or "builder_preview").strip() or "builder_preview"
        preset = payload.get("preset")
        if not isinstance(preset, dict):
            self._send_json({"error": "preset must be a JSON object"}, status=HTTPStatus.BAD_REQUEST)
            return
        timeframe = str(payload.get("timeframe") or "5m").strip() or "5m"
        try:
            bars = int(payload.get("bars") or 300)
        except Exception:
            bars = 300
        end_utc_raw = payload.get("end")
        end_utc = str(end_utc_raw).strip() if end_utc_raw is not None and str(end_utc_raw).strip() else None
        try:
            result = self.context.runtime.get_preset_market_audit_preview(
                preset_name,
                preset,
                timeframe=timeframe,
                bars=bars,
                end_utc=end_utc,
            )
        except ValueError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        self._send_json(result, status=HTTPStatus.OK)

    def _handle_market_audit_post(self) -> None:
        payload = self._read_json_body()
        source = str(payload.get("source") or "live").strip().lower() or "live"
        timeframe = str(payload.get("timeframe") or "5m").strip() or "5m"
        try:
            bars = int(payload.get("bars") or 300)
        except Exception:
            bars = 300
        end_utc_raw = payload.get("end")
        end_utc = str(end_utc_raw).strip() if end_utc_raw is not None and str(end_utc_raw).strip() else None
        try:
            if source == "live":
                settings = payload.get("settings")
                session_name = str(payload.get("session_name") or payload.get("name") or "live_market").strip() or "live_market"
                if isinstance(settings, dict):
                    self.context.live.ensure_session_from_settings(session_name, settings)
                elif not self.context.live.has_active_session(session_name):
                    self._send_json({"error": "live session is unavailable"}, status=HTTPStatus.BAD_REQUEST)
                    return
                result = self.context.live.get_market_audit_from_cache(
                    session_name=session_name,
                    timeframe=timeframe,
                    bars=bars,
                    end_utc=end_utc,
                )
            elif source == "preview":
                preset_name = str(payload.get("name") or "builder_preview").strip() or "builder_preview"
                preset = payload.get("preset")
                if not isinstance(preset, dict):
                    self._send_json({"error": "preset must be a JSON object"}, status=HTTPStatus.BAD_REQUEST)
                    return
                result = self.context.runtime.get_preset_market_audit_preview(
                    preset_name,
                    preset,
                    timeframe=timeframe,
                    bars=bars,
                    end_utc=end_utc,
                )
            elif source == "saved":
                preset_name = str(payload.get("preset_name") or payload.get("name") or "").strip()
                if not preset_name:
                    self._send_json({"error": "preset_name is required"}, status=HTTPStatus.BAD_REQUEST)
                    return
                result = self.context.runtime.get_preset_market_audit(
                    preset_name,
                    timeframe=timeframe,
                    bars=bars,
                    end_utc=end_utc,
                )
            else:
                self._send_json({"error": "Unsupported market-audit source"}, status=HTTPStatus.BAD_REQUEST)
                return
        except ValueError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        self._send_json(result, status=HTTPStatus.OK)

    def _handle_preset_delete(self, path: str) -> None:
        name = unquote(path.rsplit("/", 1)[-1])
        if not str(name or "").strip():
            self._send_json({"error": "Preset name is required"}, status=HTTPStatus.BAD_REQUEST)
            return
        try:
            deleted = self.context.presets.delete_preset(name)
        except ValueError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        self._send_json(deleted, status=HTTPStatus.OK)

    def _handle_live_session_post(self) -> None:
        payload = self._read_json_body()
        preset_name = str(payload.get("preset_name") or "").strip()
        preset = payload.get("preset")
        settings = payload.get("settings")
        session_name = str(payload.get("session_name") or payload.get("name") or "live_market").strip() or "live_market"
        if not preset_name and isinstance(payload.get("name"), str):
            preset_name = str(payload.get("name") or "").strip()
        if not preset_name and not isinstance(preset, dict) and not isinstance(settings, dict):
            self._send_json({"error": "preset_name is required"}, status=HTTPStatus.BAD_REQUEST)
            return
        try:
            if isinstance(settings, dict):
                result = self.context.live.ensure_session_from_settings(session_name, settings)
            elif isinstance(preset, dict):
                result = self.context.live.ensure_session_from_preset(preset_name or "builder_preview", preset)
            else:
                result = self.context.live.ensure_session(preset_name)
        except ValueError as exc:
            self.context.live.set_unavailable(str(exc))
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        self._send_json(self._live_payload(result), status=HTTPStatus.OK)

    def _handle_live_session_delete(self) -> None:
        self.context.live.stop()
        self._send_json(self._live_payload(self.context.live.get_state()), status=HTTPStatus.OK)

    def _handle_data_job_post(self) -> None:
        payload = self._read_json_body()
        action = str(payload.get("action") or "").strip()
        params = payload.get("params", {})
        if not action:
            self._send_json({"error": "action is required"}, status=HTTPStatus.BAD_REQUEST)
            return
        if not isinstance(params, dict):
            self._send_json({"error": "params must be a JSON object"}, status=HTTPStatus.BAD_REQUEST)
            return
        job = self.context.downloader_jobs.create_job(action, params)
        self._send_json(
            {
                "job_id": job.job_id,
                "action": job.action,
                "state": job.state,
                "message": job.message,
                "progress_pct": job.progress_pct,
                "can_cancel": job.can_cancel,
                "stop_requested": job.stop_requested,
            },
            status=HTTPStatus.ACCEPTED,
        )

    def _handle_data_job_cancel_post(self, path: str) -> None:
        parts = [part for part in path.split("/") if part]
        if len(parts) < 6:
            self._send_json({"error": "Invalid job cancel path"}, status=HTTPStatus.BAD_REQUEST)
            return
        job_id = parts[4]
        try:
            job = self.context.downloader_jobs.cancel_job(job_id)
        except ValueError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        if job is None:
            self._send_json({"error": "Job not found"}, status=HTTPStatus.NOT_FOUND)
            return
        self._send_json(
            {
                "job_id": job.job_id,
                "action": job.action,
                "state": job.state,
                "message": job.message,
                "progress_pct": job.progress_pct,
                "created_at": job.created_at,
                "updated_at": job.updated_at,
                "result": job.result,
                "error": job.error,
                "logs": list(job.logs),
                "can_cancel": job.can_cancel,
                "stop_requested": job.stop_requested,
            },
            status=HTTPStatus.OK,
        )

    def _read_json_body(self) -> Dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0") or "0")
        except Exception:
            length = 0
        raw = self.rfile.read(length) if length > 0 else b"{}"
        try:
            decoded = json.loads(raw.decode("utf-8"))
        except Exception:
            decoded = {}
        return decoded if isinstance(decoded, dict) else {}

    def _query_to_payload(self, query: Dict[str, list[str]]) -> Dict[str, Any]:
        payload: Dict[str, Any] = {}
        for key, values in (query or {}).items():
            if not values:
                continue
            if key in {"timeframes", "indicator_families", "indicator_fields"}:
                combined = ",".join([str(value or "").strip() for value in values if str(value or "").strip()])
                payload[key] = [part for part in combined.split(",") if part]
            else:
                payload[key] = values[-1]
        return payload

    def _serve_static(self, path: str) -> None:
        normalized = path or "/"
        if normalized in {"/", ""}:
            normalized = "/index.html"
        rel = normalized.lstrip("/")
        target = (STATIC_ROOT / rel).resolve()
        if not str(target).startswith(str(STATIC_ROOT.resolve())) or not target.exists() or not target.is_file():
            self._send_plain("Not found", status=HTTPStatus.NOT_FOUND)
            return
        mime, _ = mimetypes.guess_type(str(target))
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", mime or "application/octet-stream")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(target.read_bytes())

    def _send_json(self, payload: Dict[str, Any], status: HTTPStatus = HTTPStatus.OK) -> None:
        body = _json_dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_plain(self, text: str, status: HTTPStatus = HTTPStatus.OK) -> None:
        body = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _make_live_ws_url(self) -> str:
        host_header = str(self.headers.get("Host") or "")
        host = host_header.split(":", 1)[0].strip() or "127.0.0.1"
        return f"ws://{host}:{int(self.context.live_ws_port)}/ws/live"

    def _live_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        data = dict(payload)
        data["live_ws_url"] = self._make_live_ws_url()
        return data


class LiveSocketHub:
    def __init__(self, live: LiveRuntimeService, host: str, port: int) -> None:
        self.live = live
        self.host = host
        self.port = int(port)
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._server: Any = None
        self._ready = threading.Event()
        self._clients: set[Any] = set()
        self._listener_id: Optional[int] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name="ui-v2-live-ws", daemon=True)
        self._thread.start()
        self._ready.wait(timeout=5.0)
        self._listener_id = self.live.add_listener(self.publish)

    def stop(self) -> None:
        listener_id = self._listener_id
        self._listener_id = None
        if listener_id is not None:
            self.live.remove_listener(listener_id)
        loop = self._loop
        if loop is not None:
            loop.call_soon_threadsafe(self._shutdown_loop)
        thread = self._thread
        if thread is not None:
            thread.join(timeout=2.0)

    def publish(self, payload: Dict[str, Any]) -> None:
        loop = self._loop
        if loop is None:
            return
        try:
            message = _json_dumps(payload)
        except Exception:
            return
        loop.call_soon_threadsafe(self._schedule_broadcast, message)

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        self._loop = loop
        asyncio.set_event_loop(loop)
        loop.set_exception_handler(self._handle_loop_exception)
        try:
            loop.run_until_complete(self._start_server())
            loop.run_forever()
        finally:
            try:
                if self._server is not None:
                    self._server.close()
                    loop.run_until_complete(self._server.wait_closed())
            except Exception:
                pass
            try:
                pending = asyncio.all_tasks(loop)
                for task in pending:
                    task.cancel()
                if pending:
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except Exception:
                pass
            loop.close()
            self._loop = None

    async def _start_server(self) -> None:
        self._server = await websockets.serve(self._handle_client, self.host, self.port, ping_interval=20, ping_timeout=20)
        self._ready.set()

    async def _handle_client(self, websocket: Any, path: Optional[str] = None) -> None:
        raw_path = path or getattr(websocket, "path", "/ws/live")
        parsed = urlparse(raw_path)
        if parsed.path not in {"/ws/live", "/live"}:
            await websocket.close(code=1008, reason="Unsupported path")
            return
        query = parse_qs(parsed.query or "")
        preset_name = (query.get("preset") or [None])[0]
        try:
            if preset_name:
                state = self.live.ensure_session(str(preset_name))
            else:
                state = self.live.get_state()
        except ValueError as exc:
            state = self.live.set_unavailable(str(exc))
        self._clients.add(websocket)
        try:
            await websocket.send(_json_dumps(state))
            await websocket.wait_closed()
        except (ConnectionResetError, BrokenPipeError, OSError, websockets.exceptions.ConnectionClosed):
            pass
        finally:
            self._clients.discard(websocket)

    async def _broadcast(self, message: str) -> None:
        if not self._clients:
            return
        dead: list[Any] = []
        for websocket in list(self._clients):
            try:
                await websocket.send(message)
            except Exception:
                dead.append(websocket)
        for websocket in dead:
            self._clients.discard(websocket)

    def _schedule_broadcast(self, message: str) -> None:
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        loop.create_task(self._broadcast(message))

    def _shutdown_loop(self) -> None:
        loop = self._loop
        if loop is None:
            return
        try:
            if self._server is not None:
                self._server.close()
            for websocket in list(self._clients):
                loop.create_task(websocket.close())
        finally:
            loop.stop()

    def _handle_loop_exception(self, loop: asyncio.AbstractEventLoop, context: Dict[str, Any]) -> None:
        exc = context.get("exception")
        if isinstance(exc, websockets.exceptions.ConnectionClosed):
            return
        if isinstance(exc, (ConnectionResetError, BrokenPipeError)):
            return
        if isinstance(exc, OSError) and getattr(exc, "winerror", None) in {10053, 10054, 10058}:
            return
        loop.default_exception_handler(context)


class ReusableThreadingHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True


def create_server(host: str = "127.0.0.1", port: int = 8765) -> ReusableThreadingHTTPServer:
    presets = PresetRepository()
    runtime = BacktestRuntimeService()
    jobs = JobStore(runtime)
    downloader = DownloaderRuntimeService()
    downloader_jobs = DownloaderJobStore(downloader)
    live = LiveRuntimeService()
    live_ws_port = int(port) + 1
    context = UiV2Context(
        presets=presets,
        runtime=runtime,
        jobs=jobs,
        downloader=downloader,
        downloader_jobs=downloader_jobs,
        live=live,
        live_ws_port=live_ws_port,
    )
    handler = partial(UiV2RequestHandler, context=context)
    server = ReusableThreadingHTTPServer((host, port), handler)
    server.context = context  # type: ignore[attr-defined]
    server.live_socket = LiveSocketHub(live=live, host=host, port=live_ws_port)  # type: ignore[attr-defined]
    server.live_socket.start()  # type: ignore[attr-defined]
    return server


def serve(host: str = "127.0.0.1", port: int = 8765) -> None:
    server = create_server(host=host, port=port)
    print(f"Trade Inspector UI v2 listening on http://{host}:{port}", flush=True)
    print("Open that address in your browser. Press Ctrl+C here to stop the server.", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        try:
            server.live_socket.stop()  # type: ignore[attr-defined]
        except Exception:
            pass
        try:
            server.context.live.stop()  # type: ignore[attr-defined]
        except Exception:
            pass
        server.server_close()
