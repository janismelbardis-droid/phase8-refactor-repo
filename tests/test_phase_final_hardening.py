from __future__ import annotations

import tempfile
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

from app.release_manifest import should_exclude
from app.utils_time import datetime_like_to_epoch_ms
from scripts.generate_manifest import write_manifest


def _legacy_epoch_ms(values) -> np.ndarray:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", FutureWarning)
            return (values.view("int64") // 1_000_000).astype(np.int64, copy=False)
    except Exception:
        return np.array([int(pd.Timestamp(x).value // 1_000_000) for x in values], dtype=np.int64)


def test_datetime_like_to_epoch_ms_matches_legacy_conversion() -> None:
    idx = pd.date_range("2026-01-01T00:00:00Z", periods=5, freq="1min", tz="UTC")
    idx = idx.insert(2, pd.NaT)
    series = pd.Series(idx)

    np.testing.assert_array_equal(datetime_like_to_epoch_ms(idx), _legacy_epoch_ms(idx))
    np.testing.assert_array_equal(datetime_like_to_epoch_ms(series), _legacy_epoch_ms(series))


def test_generate_manifest_lists_real_release_files_and_skips_junk() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        (root / "app").mkdir()
        (root / "scripts").mkdir()
        (root / "tests").mkdir()
        (root / "docs").mkdir()
        (root / "archive").mkdir()
        (root / "dist").mkdir()
        (root / "README.md").write_text("readme", encoding="utf-8")
        (root / "run_app.py").write_text("print('ok')", encoding="utf-8")
        (root / ".gitignore").write_text("__pycache__/", encoding="utf-8")
        (root / "app" / "logic.py").write_text("VALUE = 1", encoding="utf-8")
        (root / "tests" / "test_ok.py").write_text("def test_ok():\n    assert True\n", encoding="utf-8")
        (root / "archive" / "old.py.bak").write_text("old", encoding="utf-8")
        (root / "dist" / "bundle.zip").write_text("zip", encoding="utf-8")
        (root / "app" / "__pycache__").mkdir()
        (root / "app" / "__pycache__" / "logic.cpython-313.pyc").write_bytes(b"pyc")

        manifest_path = write_manifest(root)
        text = manifest_path.read_text(encoding="utf-8")

    assert "app/logic.py" in text
    assert "tests/test_ok.py" in text
    assert "MANIFEST.txt" in text
    assert "archive/old.py.bak" not in text
    assert "dist/bundle.zip" not in text
    assert "__pycache__" not in text
    assert not should_exclude("MANIFEST.txt")
