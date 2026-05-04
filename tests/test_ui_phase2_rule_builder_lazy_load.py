from __future__ import annotations

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


class _FakeNotebook:
    def __init__(self, index: int) -> None:
        self._index = index

    def select(self):
        return 'rule-tab-id'

    def index(self, _selected):
        return self._index


def test_ensure_rule_tab_built_builds_once_and_removes_placeholder() -> None:
    tab = RULE_TABS[1]
    app = VisualApp.__new__(VisualApp)
    app._rule_tabs_built = {name: False for name in RULE_TABS}
    app._rule_tab_placeholders = {name: None for name in RULE_TABS}
    app._rule_tab_placeholders[tab] = _Placeholder()
    app.rules_tab_frames = {name: object() for name in RULE_TABS}
    app.tab_group_join_mode = {name: 'AND' for name in RULE_TABS}
    app.tab_group_join_var = {tab: _Var('OR')}
    build_calls = []
    ensure_calls = []
    badge_calls = []

    def _build_rule_tab(parent, tab_name: str) -> None:
        build_calls.append((parent, tab_name))

    app._build_rule_tab = _build_rule_tab  # type: ignore[method-assign]
    app._ensure_at_least_one_group = lambda tab_name: ensure_calls.append(tab_name)  # type: ignore[method-assign]
    app._refresh_entry_filter_badges = lambda tab_name=None: badge_calls.append(tab_name)  # type: ignore[method-assign]

    app._ensure_rule_tab_built(tab)
    app._ensure_rule_tab_built(tab)

    assert len(build_calls) == 1
    assert build_calls[0][1] == tab
    assert ensure_calls == [tab]
    assert badge_calls == [tab]
    assert app._rule_tabs_built[tab] is True
    assert app._rule_tab_placeholders[tab] is None
    assert app.tab_group_join_var[tab].get() == 'AND'



def test_rules_tab_change_builds_selected_tab_before_setting_active_group() -> None:
    tab = RULE_TABS[min(2, len(RULE_TABS) - 1)]
    app = VisualApp.__new__(VisualApp)
    app.rules_nb = _FakeNotebook(RULE_TABS.index(tab))
    app.rules = {name: [[]] for name in RULE_TABS}
    app.active_group_index = {name: 0 for name in RULE_TABS}
    calls = []

    app._ensure_rule_tab_built = lambda tab_name: calls.append(('build', tab_name))  # type: ignore[method-assign]
    app._ensure_at_least_one_group = lambda tab_name: calls.append(('ensure', tab_name))  # type: ignore[method-assign]
    app._set_active_group = lambda tab_name, idx: calls.append(('active', tab_name, idx))  # type: ignore[method-assign]

    app._on_rules_tab_change()

    assert app.active_tab == tab
    assert calls == [
        ('build', tab),
        ('ensure', tab),
        ('active', tab, 0),
    ]



def test_rebuild_groups_ui_skips_unbuilt_tabs() -> None:
    tab = RULE_TABS[0]
    app = VisualApp.__new__(VisualApp)
    app._rule_tabs_built = {name: False for name in RULE_TABS}
    app.rules_tab_frames = {tab: object()}
    app._rule_eval_ctx = type('Ctx', (), {'reset_all_sequences': lambda self: None})()
    app.group_ui = {name: [] for name in RULE_TABS}
    app.rules = {name: [[]] for name in RULE_TABS}
    app.tab_group_join_mode = {name: 'AND' for name in RULE_TABS}
    app.group_rule_join_mode = {name: ['OR'] for name in RULE_TABS}
    app.active_group_index = {name: 0 for name in RULE_TABS}

    app._rebuild_groups_ui(tab)

    assert app.group_ui[tab] == []
