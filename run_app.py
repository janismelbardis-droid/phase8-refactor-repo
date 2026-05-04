from __future__ import annotations

"""Entry point for the refactored Trade Inspector.

This keeps the original behavior:
- Installs a crash hook that keeps the console open on fatal errors
- Shows friendly "missing packages" messages before exiting
- Uses TkAgg matplotlib backend and a few performance tweaks

Run:
  python run_app.py
"""

import sys
import traceback
import threading
import os
import logging

# ---------- Crash handler: keep console open on fatal crash ----------
def _pause_console():
    try:
        input("\n\n[PAUSE] Press ENTER to close...")
    except Exception:
        pass


def _install_excepthooks():
    def _hook(exc_type, exc, tb):
        print("\n\n========== FATAL CRASH ==========")
        traceback.print_exception(exc_type, exc, tb)
        print("========== END FATAL CRASH ======\n")
        _pause_console()

    sys.excepthook = _hook
    try:
        def _thread_hook(args):
            _hook(args.exc_type, args.exc_value, args.exc_traceback)
        threading.excepthook = _thread_hook  # type: ignore[attr-defined]
    except Exception:
        pass


_install_excepthooks()

# ---------- Imports with friendly errors ----------
try:
    import tkinter as tk
    from tkinter import messagebox
except Exception:
    print("Tkinter is missing or broken on this Python installation.")
    _pause_console()
    raise

missing = []
try:
    import numpy as np  # noqa: F401
except Exception:
    missing.append("numpy")
try:
    import pandas as pd  # noqa: F401
except Exception:
    missing.append("pandas")
try:
    import requests  # noqa: F401
except Exception:
    missing.append("requests")
try:
    import matplotlib
    matplotlib.use("TkAgg")
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg  # noqa: F401
    import matplotlib.dates as mdates  # noqa: F401
    import matplotlib.pyplot as plt  # noqa: F401
    import warnings

    warnings.filterwarnings(
        "ignore",
        message=r"AutoDateLocator was unable to pick an appropriate interval.*",
        category=UserWarning,
    )
    try:
        matplotlib.rcParams["path.simplify"] = True
        matplotlib.rcParams["path.simplify_threshold"] = 0.55
        matplotlib.rcParams["agg.path.chunksize"] = 10000
    except Exception:
        pass
except Exception:
    missing.append("matplotlib")
try:
    import websocket  # noqa: F401
except Exception:
    missing.append("websocket-client")

if missing:
    msg = (
        "Missing required packages:\n"
        + "\n".join(f" - {m}" for m in missing)
        + "\n\nInstall with:\n"
        + "pip install numpy pandas requests matplotlib websocket-client\n\n"
        + "Note: TA-Lib is optional. If it isn't installed, the app will automatically\n"
        + "fall back to TradingView-style EMA/SMA math for indicators."
    )
    print(msg)
    try:
        messagebox.showerror("Missing packages", msg)
    except Exception:
        pass
    _pause_console()
    raise SystemExit(1)


def main():
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.WARNING,
            format="%(levelname)s %(name)s: %(message)s",
        )

    # Ensure the folder containing this script is on sys.path.
    # This makes `from app...` imports work regardless of the current working directory.
    root_dir = os.path.dirname(os.path.abspath(__file__))
    if root_dir not in sys.path:
        sys.path.insert(0, root_dir)

    # If the user runs this directly from inside a ZIP viewer (without extracting),
    # Windows often extracts only run_app.py into a temp folder and *not* the app/
    # package, which would cause `ModuleNotFoundError: No module named 'app'`.
    if not os.path.isdir(os.path.join(root_dir, "app")):
        msg = (
            "Couldn't find the 'app' folder next to run_app.py.\n\n"
            "This usually happens if you ran run_app.py directly from inside the ZIP.\n\n"
            "Fix:\n"
            "  1) Right-click the ZIP -> Extract All...\n"
            "  2) Open the extracted folder\n"
            "  3) Run: python run_app.py\n"
        )
        print(msg)
        try:
            messagebox.showerror("Extract ZIP first", msg)
        except Exception:
            pass
        _pause_console()
        raise SystemExit(1)

    from app.ui_app import VisualApp  # import after backend is set
    app = VisualApp()
    app.mainloop()


if __name__ == "__main__":
    main()
