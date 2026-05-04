from __future__ import annotations

"""Breakout-context helpers for the staged market-state pipeline."""

from typing import Any, Dict, Mapping

import numpy as np


def apply_breakout_value_bonus(breakout_ctx: Mapping[str, Any], value_overlay: Mapping[str, Any]) -> Dict[str, Any]:
    out = dict(breakout_ctx)
    bonus = float(value_overlay.get('market_value_breakout_bonus') or 0.0)
    if bonus != 0.0:
        try:
            out['market_breakout_score'] = int(np.clip(round(float(out.get('market_breakout_score') or 0.0) + bonus * 100.0), 0, 100))
        except Exception:
            pass
    return out
