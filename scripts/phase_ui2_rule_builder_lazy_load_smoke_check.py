from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.ui_app import VisualApp, RULE_TABS


class _Placeholder:
    def __init__(self) -> None:
        self.destroyed = False

    def destroy(self) -> None:
        self.destroyed = True


class _Var:
    def __init__(self, value=None) -> None:
        self.value = value

    def set(self, value) -> None:
        self.value = value

    def get(self):
        return self.value


app = VisualApp.__new__(VisualApp)
app._rule_tabs_built = {name: False for name in RULE_TABS}
app._rule_tab_placeholders = {name: None for name in RULE_TABS}
app.rules_tab_frames = {name: object() for name in RULE_TABS}
app.tab_group_join_mode = {name: 'AND' for name in RULE_TABS}
app.tab_group_join_var = {RULE_TABS[1]: _Var('OR')}
app._rule_tab_placeholders[RULE_TABS[1]] = _Placeholder()
seen = []
app._build_rule_tab = lambda parent, tab_name: seen.append((parent, tab_name))  # type: ignore[method-assign]
app._ensure_at_least_one_group = lambda tab_name: seen.append(('ensure', tab_name))  # type: ignore[method-assign]
app._refresh_entry_filter_badges = lambda tab_name=None: seen.append(('badge', tab_name))  # type: ignore[method-assign]

app._ensure_rule_tab_built(RULE_TABS[1])
assert seen[0][1] == RULE_TABS[1]
assert ('ensure', RULE_TABS[1]) in seen
assert ('badge', RULE_TABS[1]) in seen
assert app._rule_tabs_built[RULE_TABS[1]] is True
print('phase_ui2_rule_builder_lazy_load_smoke_check: ok')
