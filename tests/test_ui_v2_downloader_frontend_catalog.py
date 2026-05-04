from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
APP_PATCH = REPO_ROOT / "app" / "ui_v2" / "static" / "app_patch.js"


def test_downloader_frontend_catalog_includes_ema_suite_and_payload() -> None:
    source = APP_PATCH.read_text(encoding="utf-8")

    assert '{ key: "ema", label: "EMA" }' in source
    assert 'ema: ["ohlcv"]' in source
    assert 'key: "ema_suite"' in source
    assert 'label: "EMA Pair"' in source
    assert 'ema_settings: downloaderEmaSettings()' in source
    assert '<strong>EMA Profile</strong>' in source
    assert '{ key: "rsi", label: "RSI" }' in source
    assert 'rsi: ["ohlcv"]' in source
    assert 'key: "rsi_suite"' in source
    assert 'fields: ["rsi", "rsi_smooth", "rsi_state"]' in source
    assert 'rsi_settings: downloaderRsiSettings()' in source
    assert '<strong>RSI Profile</strong>' in source
    assert '{ key: "stoch_rsi", label: "Stoch RSI" }' in source
    assert 'key: "stoch_rsi_suite"' in source
    assert 'fields: ["stoch_rsi_k", "stoch_rsi_d", "stoch_rsi_kd"]' in source
    assert '{ key: "taker_bias", label: "Taker Bias" }' in source
    assert 'taker_bias: ["ohlcv"]' in source
    assert 'key: "taker_bias_suite"' in source
    assert 'fields: ["taker_bias_state", "taker_delta_pct", "taker_buy_share_pct", "taker_trade_count", "taker_buy_volume", "taker_sell_volume", "taker_delta_volume"]' in source
