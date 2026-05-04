from __future__ import annotations

"""Entry point for the local web UI v2.

This launcher is intentionally defensive for double-click usage:
- keeps the console open on fatal crashes
- shows a friendly message if the wrong Python interpreter is used
- prints the exact local URL before serving
"""

import argparse
import os
import sys
import threading
import traceback


PY312_HINT = r"C:\Users\Lenovo\AppData\Local\Programs\Python\Python312\python.exe"


def _pause_console() -> None:
    try:
        input("\n\n[PAUSE] Press ENTER to close...")
    except Exception:
        pass


def _install_excepthooks() -> None:
    def _hook(exc_type, exc, tb) -> None:
        print("\n\n========== FATAL CRASH ==========")
        traceback.print_exception(exc_type, exc, tb)
        print("========== END FATAL CRASH ======\n")
        _pause_console()

    sys.excepthook = _hook
    try:
        def _thread_hook(args) -> None:
            _hook(args.exc_type, args.exc_value, args.exc_traceback)
        threading.excepthook = _thread_hook  # type: ignore[attr-defined]
    except Exception:
        pass


def _friendly_import_server():
    try:
        from app.ui_v2.server import serve
        return serve
    except ModuleNotFoundError as exc:
        missing = getattr(exc, "name", None) or "unknown module"
        lines = [
            f"Missing required Python package: {missing}",
            "",
            f"Current interpreter: {sys.executable}",
        ]
        if os.path.exists(PY312_HINT):
            lines.extend(
                [
                    "",
                    "This project was previously run with Python 3.12.",
                    "Try launching v2 with:",
                    f'  "{PY312_HINT}" "{os.path.abspath(__file__)}"',
                    "",
                    "If you want double-click launch to work again, use the new run_app_v2.cmd launcher.",
                ]
            )
        msg = "\n".join(lines)
        print(msg)
        _pause_console()
        raise SystemExit(1) from exc


def main() -> int:
    _install_excepthooks()

    root_dir = os.path.dirname(os.path.abspath(__file__))
    if root_dir not in sys.path:
        sys.path.insert(0, root_dir)

    if not os.path.isdir(os.path.join(root_dir, "app")):
        print(
            "Couldn't find the 'app' folder next to run_app_v2.py.\n\n"
            "Extract the ZIP first, then run the launcher from the extracted folder."
        )
        _pause_console()
        return 1

    parser = argparse.ArgumentParser(description="Run the local web UI v2 for Trade Inspector.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()

    serve = _friendly_import_server()

    print(f"Trade Inspector UI v2 starting on http://{args.host}:{args.port}")
    print("Open that address in your browser. Press Ctrl+C here to stop the server.")
    serve(host=str(args.host), port=int(args.port))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
