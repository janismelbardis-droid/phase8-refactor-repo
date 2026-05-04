from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "dist" / "remote_jobs"


def _python_executable() -> str:
    return str(Path(sys.executable).resolve())


def _env_path(name: str, fallback: Path) -> Path:
    raw = str(os.environ.get(name, "") or "").strip()
    if not raw:
        return fallback.resolve()
    return Path(raw).expanduser().resolve()


def _default_cache_dir() -> Path:
    return _env_path("TRADE_INSPECTOR_CACHE_DIR", REPO_ROOT / "data_cache")


def _default_output_root() -> Path:
    return _env_path("TRADE_INSPECTOR_OUTPUT_DIR", DEFAULT_OUTPUT_ROOT)


def _run(label: str, command: Sequence[str]) -> int:
    print(f"[remote_job] {label}", flush=True)
    print(f"[remote_job] exec: {' '.join(command)}", flush=True)
    return subprocess.call(list(command), cwd=str(REPO_ROOT))


def _smoke(args: argparse.Namespace) -> int:
    commands: list[tuple[str, list[str]]] = [
        (
            "phase0_phase1_smoke",
            [_python_executable(), "scripts/phase0_phase1_smoke_check.py"],
        )
    ]
    if not bool(args.skip_hardening):
        commands.append(
            (
                "phase_final_hardening_smoke",
                [_python_executable(), "-m", "scripts.phase_final_hardening_smoke_check"],
            )
        )
    for label, command in commands:
        code = _run(label, command)
        if code != 0:
            return code
    return 0


def _saved_preset(args: argparse.Namespace) -> int:
    command = [
        _python_executable(),
        "tools/run_saved_preset.py",
        "--cache-dir",
        str(Path(args.cache_dir).expanduser().resolve()),
    ]
    if args.preset:
        command += ["--preset", str(args.preset)]
    if args.presets_file:
        command += ["--presets-file", str(Path(args.presets_file).expanduser().resolve())]
    if args.start:
        command += ["--start", str(args.start)]
    if args.end:
        command += ["--end", str(args.end)]
    if args.output_json:
        command += ["--output-json", str(Path(args.output_json).expanduser().resolve())]
    if args.initial_balance is not None:
        command += ["--initial-balance", str(args.initial_balance)]
    if args.order_notional is not None:
        command += ["--order-notional", str(args.order_notional)]
    if args.fee_pct is not None:
        command += ["--fee-pct", str(args.fee_pct)]
    if args.slippage_bps is not None:
        command += ["--slippage-bps", str(args.slippage_bps)]
    if args.capture_trade_details:
        command.append("--capture-trade-details")
    return _run("saved_preset", command)


def _build_pack(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    command = [
        _python_executable(),
        "tools/build_ai_research_pack.py",
        "--output-dir",
        str(output_dir),
        "--symbol",
        str(args.symbol),
        f"--{'include-recent' if args.include_recent else 'no-include-recent'}",
        "--recent-days",
        str(args.recent_days),
        "--recent-warmup-days",
        str(args.recent_warmup_days),
    ]
    return _run("build_ai_research_pack", command)


def _submit_pack(args: argparse.Namespace) -> int:
    command = [
        _python_executable(),
        "tools/submit_ai_research_pack.py",
        "--zip",
        str(Path(args.zip).expanduser().resolve()),
        "--model",
        str(args.model),
        "--memory-limit",
        str(args.memory_limit),
        "--prompt-file",
        str(args.prompt_file),
        "--output-dir",
        str(Path(args.output_dir).expanduser().resolve()),
    ]
    if args.extra_prompt:
        command += ["--extra-prompt", str(args.extra_prompt)]
    return _run("submit_ai_research_pack", command)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stable headless launcher for local, CI, and cloud repo tasks.")
    sub = parser.add_subparsers(dest="command", required=True)

    smoke = sub.add_parser("smoke", help="Run the hardened smoke path.")
    smoke.add_argument("--skip-hardening", action="store_true")
    smoke.set_defaults(handler=_smoke)

    saved = sub.add_parser("saved-preset", help="Run a saved preset headlessly.")
    saved.add_argument("--preset", default="")
    saved.add_argument("--presets-file", default="")
    saved.add_argument("--cache-dir", default=str(_default_cache_dir()))
    saved.add_argument("--start", default="")
    saved.add_argument("--end", default="")
    saved.add_argument("--output-json", default="")
    saved.add_argument("--initial-balance", type=float, default=None)
    saved.add_argument("--order-notional", type=float, default=None)
    saved.add_argument("--fee-pct", type=float, default=None)
    saved.add_argument("--slippage-bps", type=float, default=None)
    saved.add_argument("--capture-trade-details", action="store_true")
    saved.set_defaults(handler=_saved_preset)

    pack = sub.add_parser("build-pack", help="Build a compact AI research pack.")
    pack.add_argument("--output-dir", default=str(_default_output_root() / "ai_research_pack"))
    pack.add_argument("--symbol", default="BTCUSDT")
    pack.add_argument("--include-recent", action=argparse.BooleanOptionalAction, default=True)
    pack.add_argument("--recent-days", type=int, default=45)
    pack.add_argument("--recent-warmup-days", type=int, default=7)
    pack.set_defaults(handler=_build_pack)

    submit = sub.add_parser("submit-pack", help="Submit a compact AI research pack to OpenAI Code Interpreter.")
    submit.add_argument("--zip", required=True)
    submit.add_argument("--model", default=str(os.environ.get("OPENAI_MODEL", "gpt-5.5") or "gpt-5.5"))
    submit.add_argument(
        "--memory-limit",
        default=str(os.environ.get("OPENAI_MEMORY_LIMIT", "4g") or "4g"),
        choices=["1g", "4g", "16g", "64g"],
    )
    submit.add_argument("--prompt-file", default="PROMPT_FOR_CHATGPT.md")
    submit.add_argument("--extra-prompt", default="")
    submit.add_argument("--output-dir", default=str(_default_output_root() / "openai_submission"))
    submit.set_defaults(handler=_submit_pack)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.handler(args))


if __name__ == "__main__":
    raise SystemExit(main())
