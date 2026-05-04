from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
APP_PATCH = REPO_ROOT / "app" / "ui_v2" / "static" / "app_patch.js"


def test_builder_live_refresh_syncs_builder_settings_into_live_context() -> None:
    source = APP_PATCH.read_text(encoding="utf-8")

    assert "function syncLiveContextFromBuilderSettings(options = {})" in source
    assert "const originalRequestBuilderLiveRefresh = typeof requestBuilderLiveRefresh === \"function\" ? requestBuilderLiveRefresh : null;" in source
    assert "syncLiveContextFromBuilderSettings({ refresh: true });" in source


def test_verify_ema_rsi_popovers_lock_live_rerender_while_editing() -> None:
    source = APP_PATCH.read_text(encoding="utf-8")

    assert "function verifyEditorRenderLocked()" in source
    assert "if (!force && verifyEditorRenderLocked()) {" in source
    assert "renderVerifyHost({ force: true });" in source
    assert "if (typeof renderBuilder === \"function\") renderBuilder();" not in source


def test_verify_regime_cards_show_trigger_age_and_display_state() -> None:
    source = APP_PATCH.read_text(encoding="utf-8")

    assert 'const framaAge = row ? values.frama_display_age : null;' in source
    assert 'const rangeState = row ? (values.range_filter_display_state ?? values.range_filter_state) : null;' in source
    assert 'const rangeAge = row ? values.range_filter_display_age : null;' in source
    assert 'renderPassiveBadge("Trigger Age", framaAge, { digits: 0 })' in source
    assert 'renderPassiveBadge("Trigger Age", rangeAge, { digits: 0 })' in source
    assert 'renderPassiveBadge("Last Trigger"' not in source


def test_indicator_mutation_preserves_trigger_age_display_until_live_rows_recover() -> None:
    source = APP_PATCH.read_text(encoding="utf-8")

    assert "function beginIndicatorRefreshCarry()" in source
    assert "function mergeIndicatorRefreshCarryRow(row)" in source
    assert "beginIndicatorRefreshCarry();" in source
    assert 'currentLivePhase() !== "live"' in source


def test_frama_builder_menu_only_offers_directional_triggers() -> None:
    source = (REPO_ROOT / "app" / "ui_v2" / "static" / "app.js").read_text(encoding="utf-8")

    assert 'Add ★TRIGGER FRAMA BREAK UP' in source
    assert 'Add ★TRIGGER FRAMA BREAK DOWN' in source
    assert 'Add ★TRIGGER FRAMA MID CROSS' not in source


def test_apply_live_payload_no_longer_rerenders_snapshot_header_every_tick() -> None:
    source = APP_PATCH.read_text(encoding="utf-8")

    assert "function applyLivePayload(payload, options = {}) {" in source
    apply_payload_body = source.split("function applyLivePayload(payload, options = {}) {", 1)[1].split("function desiredLiveSocketUrl", 1)[0]
    assert "renderSnapshotCompactPanel();" not in apply_payload_body


def test_taker_bias_live_menu_uses_snapshot_rule_family_items() -> None:
    source = (REPO_ROOT / "app" / "ui_v2" / "static" / "app.js").read_text(encoding="utf-8")

    assert 'if (["taker_bias_state", "taker_trade_count", "taker_delta_pct", "taker_buy_share_pct", "taker_buy_volume", "taker_sell_volume", "taker_delta_volume"].includes(columnKey)) {' in source
    assert 'label: "View Aggressive Pressure Details"' in source
    assert 'Add STATE ${fieldLabel("taker_bias_state", timeframe)} = BUY' in source
    assert 'Add STATE ${fieldLabel("taker_delta_pct", timeframe)} (custom)...' in source
    assert 'Add STATE ${fieldLabel("taker_trade_count", timeframe)} (custom)...' in source


def test_taker_bias_card_prioritizes_pressure_numbers_over_bias_wording() -> None:
    source = APP_PATCH.read_text(encoding="utf-8")

    assert 'renderMetricBlock("Aggressive Pressure", takerRows, "Candle taker-buy versus taker-sell pressure")' in source
    assert 'renderMetricButton("taker_buy_volume", "Buy Vol"' in source
    assert 'renderMetricBadge("taker_sell_volume", "Sell Vol"' in source
    assert 'renderMetricButton("taker_buy_share_pct", "Buy %"' in source
    assert 'renderMetricBadge("taker_buy_share_pct", "Sell %"' in source
    assert 'renderMetricButton("taker_delta_volume", "Delta Vol"' in source
    assert 'renderMetricBadge("taker_delta_pct", "Delta %"' in source


def test_taker_bias_builder_supports_window_bars_and_popup_shows_requested_vs_actual() -> None:
    app_source = (REPO_ROOT / "app" / "ui_v2" / "static" / "app.js").read_text(encoding="utf-8")
    patch_source = APP_PATCH.read_text(encoding="utf-8")

    assert "window_bars: 1" in app_source
    assert 'data-prop="window_bars"' in app_source
    assert '"Closed Window"' in app_source
    assert '"Taker Bias Window"' not in app_source
    assert "TAKER_BIAS_WINDOW_FIELDS" in app_source
    assert "requested_bars" in patch_source
    assert "of requested" in patch_source


def test_live_market_audit_uses_active_session_without_reposting_settings_when_live_rows_exist() -> None:
    source = APP_PATCH.read_text(encoding="utf-8")

    assert "if (!hasAnySnapshotValues()) {" in source
    audit_body = source.split("function marketAuditRequestSpec(options = {}) {", 1)[1].split("function marketAuditSessionLabel()", 1)[0]
    assert "requestBody.settings = cloneJsonSafe(liveContextState?.settings, liveContextSeedSettings());" in audit_body
