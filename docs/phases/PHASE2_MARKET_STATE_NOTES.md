# Phase 2 — market-state UI layer

This phase keeps the existing functionality intact and adds the next necessary layer on top of Phase 1:

- Live **Market State Board** with one card per timeframe
- Card shows:
  - market state label
  - bias
  - phase
  - confidence
  - state age
  - VIDYA angle
  - VIDYA delta volume
  - recommended playbook text
- **Double-click** a card to open a detailed market-state explanation popup
- **Right-click** a card to add market-state rules directly to the active rule group
- Live updates refresh the state board without changing existing rule/backtest logic

No core indicator formulas were changed in this phase.
VIDYA / FRAMA / Range Filter logic remains intact; this phase is a synthesis + UI layer.
