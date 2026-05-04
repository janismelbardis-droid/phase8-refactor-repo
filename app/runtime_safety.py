from __future__ import annotations

"""Shared safety helpers for degraded-mode execution."""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Mapping, MutableMapping, Optional, Sequence, Tuple, TypeVar
import logging

T = TypeVar("T")

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExecutionReport:
    status: str = "ok"
    module: str = ""
    function: str = ""
    degraded_feature: str = ""
    reason: str = ""
    error_type: str = ""
    context: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_ok(self) -> bool:
        return self.status == "ok"

    def as_dict(self, prefix: str) -> Dict[str, Any]:
        return {
            f"{prefix}_status": self.status,
            f"{prefix}_status_reason": self.reason,
            f"{prefix}_error_type": self.error_type,
            f"{prefix}_degraded_feature": self.degraded_feature,
        }


OK_EXECUTION_REPORT = ExecutionReport()


def _compact_reason(exc: BaseException, degraded_feature: str = "") -> str:
    feature = f"{degraded_feature}: " if degraded_feature else ""
    text = str(exc).strip()
    if not text:
        text = exc.__class__.__name__
    return f"{feature}{text}"[:280]


def safe_execute(
    func: Callable[[], T],
    *,
    default: T,
    module: str,
    function: str,
    degraded_feature: str,
    context: Optional[Mapping[str, Any]] = None,
    log_exceptions: bool = True,
) -> Tuple[T, ExecutionReport]:
    try:
        return func(), OK_EXECUTION_REPORT
    except Exception as exc:
        report = ExecutionReport(
            status="degraded",
            module=module,
            function=function,
            degraded_feature=degraded_feature,
            reason=_compact_reason(exc, degraded_feature),
            error_type=exc.__class__.__name__,
            context=dict(context or {}),
        )
        if log_exceptions:
            logger.warning(
                "%s.%s degraded at %s: %s",
                module,
                function,
                degraded_feature or "runtime",
                report.reason,
                exc_info=True,
            )
        return default, report


def merge_reports(prefix: str, *reports: ExecutionReport) -> Dict[str, Any]:
    active = [r for r in reports if isinstance(r, ExecutionReport) and not r.is_ok]
    if not active:
        return {
            f"{prefix}_status": "ok",
            f"{prefix}_status_reason": "",
            f"{prefix}_error_type": "",
            f"{prefix}_degraded_feature": "",
            f"{prefix}_error_count": 0,
            f"{prefix}_degraded_features": [],
        }
    reasons = [r.reason for r in active if r.reason]
    features = [r.degraded_feature for r in active if r.degraded_feature]
    error_types = [r.error_type for r in active if r.error_type]
    status = "failed" if len(active) >= 2 else "partial"
    return {
        f"{prefix}_status": status,
        f"{prefix}_status_reason": " | ".join(reasons)[:500],
        f"{prefix}_error_type": ",".join(dict.fromkeys(error_types)),
        f"{prefix}_degraded_feature": features[0] if features else "",
        f"{prefix}_error_count": len(active),
        f"{prefix}_degraded_features": list(dict.fromkeys(features)),
    }


def annotate_payload(payload: MutableMapping[str, Any], prefix: str, *reports: ExecutionReport) -> MutableMapping[str, Any]:
    payload.update(merge_reports(prefix, *reports))
    return payload
