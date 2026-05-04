const TAB_NAMES = ["Long Entry", "Long Exit", "Short Entry", "Short Exit"];
const TIMEFRAME_OPTIONS = ["1m", "5m", "15m", "30m", "1h"];
const BACKTEST_MODE_OPTIONS = [
  "Bar Close (1m candles)",
  "Bar Close (1m, HTF forming)",
  "Bar Close (1m, HTF closed only)",
  "Tick (aggTrades)",
];
const GROUP_JOIN_OPTIONS = ["AND", "OR"];
const RULE_JOIN_OPTIONS = ["OR", "AND", "SEQUENCE"];
const STATE_OP_OPTIONS = ["=", "!=", "<", "<=", ">", ">="];
const EVENT_OP_OPTIONS = ["EVENT", "TURNED", "FLIPPED", "CROSS"];
const FIELD_OPTIONS = [
  "ema_1",
  "ema_2",
  "ema_stack",
  "ema_cross_up",
  "ema_cross_down",
  "ema_1_angle_state",
  "ema_2_angle_state",
  "ema_1_slope",
  "ema_2_slope",
  "ema_spread",
  "ema_spread_pct",
  "ema_spread_delta",
  "ema_spread_pct_delta",
  "ema_spread_trend",
  "ema_stack_pressure",
  "ms",
  "angle",
  "sig_angle",
  "ppo",
  "flip_age",
  "ms_cross",
  "macd0_cross",
  "adx_angle",
  "atr_angle",
  "range_filter_buy",
  "range_filter_sell",
  "range_filter_long_cond",
  "range_filter_short_cond",
  "range_filter_cond_ini",
  "range_filter_state",
  "range_filter_phase",
  "range_filter_line",
  "range_filter_upper",
  "range_filter_lower",
  "range_filter_smooth_range",
  "range_filter_upward_count",
  "range_filter_downward_count",
  "vidya_state",
  "vidya_line",
  "vidya_raw",
  "vidya_upper",
  "vidya_lower",
  "vidya_trend_up",
  "vidya_trend_down",
  "vidya_up_volume",
  "vidya_down_volume",
  "vidya_delta_pct",
  "vidya_angle_state",
  "vidya_angle_accel",
  "vidya_slope",
  "vidya_angle_deg",
  "vidya_angle_deg_norm",
  "vidya_active_liq_low_count",
  "vidya_active_liq_high_count",
  "vidya_nearest_liq_low",
  "vidya_nearest_liq_high",
  "vidya_dist_to_liq_low_pct",
  "vidya_dist_to_liq_high_pct",
  "vidya_new_liq_low",
  "vidya_new_liq_high",
  "vidya_liq_low_taken",
  "vidya_liq_high_taken",
  "market_state",
  "market_bias",
  "market_phase",
  "market_confidence",
  "market_state_age",
  "market_direction_score",
  "market_energy_score",
  "market_alignment_bonus",
  "market_macd_gap_norm",
  "frama_state",
  "frama_break_up",
  "frama_break_down",
  "frama_mid_cross",
  "macd",
  "signal",
  "rsi",
  "rsi_smooth",
  "rsi_state",
  "stoch_rsi_k",
  "stoch_rsi_d",
  "stoch_rsi_kd",
  "adx",
  "atr",
  "di_plus",
  "di_minus",
  "di_spread",
  "di_cross",
  "taker_buy_volume",
  "taker_sell_volume",
  "taker_delta_volume",
  "taker_delta_pct",
  "taker_buy_share_pct",
  "taker_trade_count",
  "taker_bias_state",
];

const SNAPSHOT_COLUMNS = [
  { key: "timestamp_utc", label: "As Of", kind: "meta" },
  { key: "ema_1", label: "Fast EMA", kind: "numeric", digits: 2 },
  { key: "ema_2", label: "Slow EMA", kind: "numeric", digits: 2 },
  { key: "ema_stack", label: "Stack", kind: "state" },
  { key: "price", label: "Price", kind: "numeric", digits: 2 },
  { key: "macd", label: "MACD", kind: "numeric", digits: 2 },
  { key: "signal", label: "Signal", kind: "numeric", digits: 2 },
  { key: "rsi", label: "RSI", kind: "numeric", digits: 2 },
  { key: "rsi_smooth", label: "RSI Smooth", kind: "numeric", digits: 2 },
  { key: "rsi_state", label: "RSI State", kind: "dot" },
  { key: "stoch_rsi_k", label: "Stoch K", kind: "numeric", digits: 2 },
  { key: "stoch_rsi_d", label: "Stoch D", kind: "numeric", digits: 2 },
  { key: "stoch_rsi_kd", label: "K/D", kind: "dot" },
  { key: "ms", label: "M/S", kind: "dot" },
  { key: "angle", label: "Angle", kind: "dot" },
  { key: "sig_angle", label: "SigAngle", kind: "dot" },
  { key: "ppo", label: "PPO", kind: "dot" },
  { key: "flip_age", label: "FlipAge", kind: "numeric", digits: 0 },
  { key: "adx", label: "ADX", kind: "numeric", digits: 1 },
  { key: "adx_angle", label: "ADXAngle", kind: "dot" },
  { key: "atr", label: "ATR", kind: "numeric", digits: 1 },
  { key: "atr_angle", label: "ATRAngle", kind: "dot" },
  { key: "frama_state", label: "FRAMA", kind: "state" },
  { key: "vidya_state", label: "VIDYA", kind: "state" },
  { key: "range_filter_state", label: "Range", kind: "state" },
  { key: "range_filter_phase", label: "RangePhase", kind: "state" },
  { key: "market_state", label: "Market", kind: "state" },
  { key: "market_bias", label: "Bias", kind: "state" },
  { key: "market_phase", label: "Phase", kind: "state" },
  { key: "market_confidence", label: "Conf", kind: "numeric", digits: 0 },
  { key: "market_state_age", label: "Age", kind: "numeric", digits: 0 },
  { key: "di_plus", label: "+DI", kind: "numeric", digits: 1 },
  { key: "di_minus", label: "-DI", kind: "numeric", digits: 1 },
];

const state = {
  presets: [],
  selectedPreset: null,
  builderPreset: null,
  builderPresetName: null,
  builderPresetDirty: false,
  builderTarget: { tabName: "Long Entry", groupIndex: 0 },
  snapshot: null,
  snapshotLoading: false,
  currentJobId: null,
  currentRunId: null,
  currentResult: null,
  currentTrades: [],
  currentTradeDetail: null,
  selectedTradeId: null,
  jobTimer: null,
  ruleUi: {},
  groupLogicModalTab: null,
  entryFilterModalTab: null,
  entryFilterModalGroupIndex: null,
};

const TAKER_BIAS_WINDOW_FIELDS = new Set([
  "taker_bias_state",
  "taker_trade_count",
  "taker_delta_pct",
  "taker_buy_share_pct",
  "taker_buy_volume",
  "taker_sell_volume",
  "taker_delta_volume",
]);

const $ = (id) => document.getElementById(id);

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function fmt(value, digits = 2) {
  const num = Number(value);
  return Number.isFinite(num) ? num.toFixed(digits) : value ?? "-";
}

function parsePrimitive(raw) {
  const value = String(raw ?? "").trim();
  if (value === "") return null;
  if (value === "true") return true;
  if (value === "false") return false;
  const num = Number(value);
  return Number.isFinite(num) && value !== "" ? num : value;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function toUtcDateTimeLocalValue(value) {
  const raw = String(value ?? "").trim();
  if (!raw) return "";
  const match = raw.match(/^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2})(?::\d{2}(?:\.\d+)?)?(?:\s*UTC)?$/i);
  if (match) {
    return `${match[1]}T${match[2]}`;
  }
  if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(raw)) {
    return raw;
  }
  return raw.replace(" ", "T").slice(0, 16);
}

function fromUtcDateTimeLocalValue(value) {
  const raw = String(value ?? "").trim();
  if (!raw) return "";
  const match = raw.match(/^(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2})(?::(\d{2}))?$/);
  if (!match) return raw.replace("T", " ");
  return `${match[1]} ${match[2]}:${match[3] || "00"}`;
}

const PARAMETER_INDICATOR_SPECS = Object.freeze({
  ema: Object.freeze({
    key: "ema_indicators",
    fields: Object.freeze(["ema_1_length", "ema_2_length"]),
    defaults: Object.freeze({ ema_1_length: 9, ema_2_length: 21 }),
  }),
  rsi: Object.freeze({
    key: "rsi_indicators",
    fields: Object.freeze(["length", "smoothing"]),
    defaults: Object.freeze({ length: 14, smoothing: 3 }),
  }),
});

function parameterIndicatorSpec(kindOrKey) {
  if (kindOrKey && typeof kindOrKey === "object" && Array.isArray(kindOrKey.fields)) return kindOrKey;
  const key = String(kindOrKey || "").trim();
  if (!key) return null;
  if (PARAMETER_INDICATOR_SPECS[key]) return PARAMETER_INDICATOR_SPECS[key];
  return Object.values(PARAMETER_INDICATOR_SPECS).find((spec) => spec.key === key) || null;
}

function defaultParameterizedIndicatorSettings(kindOrKey) {
  const spec = parameterIndicatorSpec(kindOrKey);
  return {
    defaults: spec ? { ...spec.defaults } : {},
    overrides: {},
  };
}

function coerceParameterizedIndicatorConfig(kindOrKey, rawConfig, fallback = null) {
  const spec = parameterIndicatorSpec(kindOrKey);
  if (!spec) return {};
  const base = fallback && typeof fallback === "object" ? fallback : spec.defaults;
  const raw = rawConfig && typeof rawConfig === "object" ? rawConfig : {};
  const next = {};
  spec.fields.forEach((field) => {
    const fallbackValue = Number(base?.[field] ?? spec.defaults[field] ?? 1);
    const coerced = Math.max(1, Number(raw[field] ?? fallbackValue));
    next[field] = Number.isFinite(coerced) ? coerced : spec.defaults[field];
  });
  return next;
}

function normalizeParameterizedIndicatorSettings(kindOrKey, rawSettings) {
  const spec = parameterIndicatorSpec(kindOrKey);
  if (!spec) return { defaults: {}, overrides: {} };
  const raw = rawSettings && typeof rawSettings === "object" ? rawSettings : {};
  const defaultsSource = raw.defaults && typeof raw.defaults === "object" ? raw.defaults : raw;
  const defaults = coerceParameterizedIndicatorConfig(spec, defaultsSource);
  const rawOverrides = raw.overrides && typeof raw.overrides === "object" ? raw.overrides : {};
  const overrides = {};
  Object.entries(rawOverrides).forEach(([timeframe, config]) => {
    const tf = String(timeframe || "").trim();
    if (!tf) return;
    overrides[tf] = coerceParameterizedIndicatorConfig(spec, config, defaults);
  });
  return {
    defaults,
    overrides,
  };
}

function ensurePresetParameterizedIndicatorSettings(preset, kindOrKey) {
  const spec = parameterIndicatorSpec(kindOrKey);
  if (!spec) return { defaults: {}, overrides: {} };
  if (!preset) return normalizeParameterizedIndicatorSettings(spec, {});
  preset.settings = preset.settings || {};
  preset.settings[spec.key] = normalizeParameterizedIndicatorSettings(spec, preset.settings[spec.key]);
  return preset.settings[spec.key];
}

function currentParameterizedIndicatorSettings(kindOrKey, preset = null, timeframe = null) {
  const spec = parameterIndicatorSpec(kindOrKey);
  if (!spec) return {};
  const sourcePreset = preset || state.builderPreset || state.selectedPreset?.preset || null;
  const normalized = normalizeParameterizedIndicatorSettings(spec, sourcePreset?.settings?.[spec.key]);
  const tf = String(timeframe || "").trim();
  if (tf && normalized.overrides[tf]) return { ...normalized.overrides[tf] };
  return { ...normalized.defaults };
}

function sameParameterizedIndicatorConfig(spec, left, right) {
  return spec.fields.every((field) => Number(left?.[field]) === Number(right?.[field]));
}

function setPresetParameterizedIndicatorSettings(kindOrKey, preset, timeframe, nextConfig) {
  const spec = parameterIndicatorSpec(kindOrKey);
  if (!spec || !preset) return {};
  const normalized = ensurePresetParameterizedIndicatorSettings(preset, spec);
  const tf = String(timeframe || "").trim();
  if (!tf || tf === "default") {
    normalized.defaults = coerceParameterizedIndicatorConfig(spec, nextConfig, normalized.defaults);
    Object.keys(normalized.overrides).forEach((overrideTf) => {
      normalized.overrides[overrideTf] = coerceParameterizedIndicatorConfig(spec, normalized.overrides[overrideTf], normalized.defaults);
      if (sameParameterizedIndicatorConfig(spec, normalized.overrides[overrideTf], normalized.defaults)) {
        delete normalized.overrides[overrideTf];
      }
    });
    preset.settings[spec.key] = normalized;
    return normalized.defaults;
  }
  const config = coerceParameterizedIndicatorConfig(spec, nextConfig, normalized.defaults);
  if (sameParameterizedIndicatorConfig(spec, config, normalized.defaults)) {
    delete normalized.overrides[tf];
  } else {
    normalized.overrides[tf] = config;
  }
  preset.settings[spec.key] = normalized;
  return currentParameterizedIndicatorSettings(spec, preset, tf);
}

function parameterizedIndicatorOverrideCount(kindOrKey, preset = null) {
  const spec = parameterIndicatorSpec(kindOrKey);
  if (!spec) return 0;
  const sourcePreset = preset || state.builderPreset || state.selectedPreset?.preset || null;
  const normalized = normalizeParameterizedIndicatorSettings(spec, sourcePreset?.settings?.[spec.key]);
  return Object.keys(normalized.overrides).length;
}

function hasPresetParameterizedIndicatorOverride(kindOrKey, preset = null, timeframe = null) {
  const spec = parameterIndicatorSpec(kindOrKey);
  if (!spec) return false;
  const sourcePreset = preset || state.builderPreset || state.selectedPreset?.preset || null;
  const normalized = normalizeParameterizedIndicatorSettings(spec, sourcePreset?.settings?.[spec.key]);
  const tf = String(timeframe || "").trim();
  return Boolean(tf && normalized.overrides[tf]);
}

function formatParameterizedIndicatorConfig(kindOrKey, config) {
  const spec = parameterIndicatorSpec(kindOrKey);
  if (!spec) return "";
  const normalized = coerceParameterizedIndicatorConfig(spec, config);
  return spec.fields.map((field) => normalized[field]).join("/");
}

function defaultEmaIndicatorSettings() {
  return defaultParameterizedIndicatorSettings("ema");
}

function defaultRsiIndicatorSettings() {
  return defaultParameterizedIndicatorSettings("rsi");
}

function coerceEmaPair(rawPair, fallback = null) {
  return coerceParameterizedIndicatorConfig("ema", rawPair, fallback);
}

function normalizeEmaIndicatorSettings(rawSettings) {
  return normalizeParameterizedIndicatorSettings("ema", rawSettings);
}

function coerceRsiConfig(rawConfig, fallback = null) {
  return coerceParameterizedIndicatorConfig("rsi", rawConfig, fallback);
}

function normalizeRsiIndicatorSettings(rawSettings) {
  return normalizeParameterizedIndicatorSettings("rsi", rawSettings);
}

function ensurePresetEmaIndicatorSettings(preset) {
  return ensurePresetParameterizedIndicatorSettings(preset, "ema");
}

function ensurePresetRsiIndicatorSettings(preset) {
  return ensurePresetParameterizedIndicatorSettings(preset, "rsi");
}

function currentEmaSettings(preset = null, timeframe = null) {
  return currentParameterizedIndicatorSettings("ema", preset, timeframe);
}

function currentRsiSettings(preset = null, timeframe = null) {
  return currentParameterizedIndicatorSettings("rsi", preset, timeframe);
}

function setPresetEmaSettings(preset, timeframe, nextPair) {
  return setPresetParameterizedIndicatorSettings("ema", preset, timeframe, nextPair);
}

function setPresetRsiSettings(preset, timeframe, nextConfig) {
  return setPresetParameterizedIndicatorSettings("rsi", preset, timeframe, nextConfig);
}

function normalizeKnownTimeframe(value) {
  const tf = String(value || "").trim();
  return TIMEFRAME_OPTIONS.includes(tf) ? tf : "";
}

function detectedPresetTimeframes(preset = null) {
  const sourcePreset = preset || state.builderPreset || state.selectedPreset?.preset || null;
  const detected = new Set();
  const tabs = sourcePreset?.tabs && typeof sourcePreset.tabs === "object" ? sourcePreset.tabs : {};
  Object.values(tabs).forEach((tabPayload) => {
    const groups = Array.isArray(tabPayload?.groups) ? tabPayload.groups : [];
    groups.forEach((group) => {
      const rules = Array.isArray(group?.rules) ? group.rules : [];
      rules.forEach((rule) => {
        const tf = normalizeKnownTimeframe(rule?.timeframe);
        if (tf) detected.add(tf);
      });
    });
  });
  Object.values(PARAMETER_INDICATOR_SPECS).forEach((spec) => {
    const overrides = normalizeParameterizedIndicatorSettings(spec, sourcePreset?.settings?.[spec.key]).overrides;
    Object.keys(overrides).forEach((timeframe) => {
      const tf = normalizeKnownTimeframe(timeframe);
      if (tf) detected.add(tf);
    });
  });
  if (!detected.size) {
    Object.entries(sourcePreset?.settings?.timeframes || {}).forEach(([timeframe, enabled]) => {
      const tf = normalizeKnownTimeframe(timeframe);
      if (tf && enabled) detected.add(tf);
    });
  }
  if (!detected.size) detected.add("1m");
  return TIMEFRAME_OPTIONS.filter((tf) => detected.has(tf));
}

function syncPresetTimeframes(preset = null) {
  const sourcePreset = preset || state.builderPreset || state.selectedPreset?.preset || null;
  if (!sourcePreset) return {};
  sourcePreset.settings = sourcePreset.settings || {};
  const detected = new Set(detectedPresetTimeframes(sourcePreset));
  sourcePreset.settings.timeframes = Object.fromEntries(TIMEFRAME_OPTIONS.map((tf) => [tf, detected.has(tf)]));
  return sourcePreset.settings.timeframes;
}

function renderDetectedTimeframeScope(preset = null) {
  const detected = detectedPresetTimeframes(preset);
  return `
    <div class="builder-scope-summary">
      <div class="eyebrow">Auto-Detected TF Scope</div>
      <div class="muted">Derived from current rules plus timeframe-specific EMA and RSI overrides. Backtests still add 1m base data internally when needed.</div>
      <div class="pill-row">${detected.map((tf) => `<span class="pill">${escapeHtml(tf)}</span>`).join("")}</div>
    </div>
  `;
}

function emaOverrideCount(preset = null) {
  return parameterizedIndicatorOverrideCount("ema", preset);
}

function rsiOverrideCount(preset = null) {
  return parameterizedIndicatorOverrideCount("rsi", preset);
}

function hasPresetEmaOverride(preset = null, timeframe = null) {
  return hasPresetParameterizedIndicatorOverride("ema", preset, timeframe);
}

function hasPresetRsiOverride(preset = null, timeframe = null) {
  return hasPresetParameterizedIndicatorOverride("rsi", preset, timeframe);
}

function formatEmaPair(pair) {
  return formatParameterizedIndicatorConfig("ema", pair);
}

function formatRsiConfig(config) {
  return formatParameterizedIndicatorConfig("rsi", config);
}

function emaDisplayConfig(preset = null, timeframe = null) {
  const settings = currentEmaSettings(preset, timeframe);
  const fast = settings.ema_1_length;
  const slow = settings.ema_2_length;
  const hasSpecificTimeframe = Boolean(String(timeframe || "").trim());
  return {
    fastLength: fast,
    slowLength: slow,
    fastShort: hasSpecificTimeframe ? `EMA ${fast}` : "Fast EMA",
    slowShort: hasSpecificTimeframe ? `EMA ${slow}` : "Slow EMA",
    stackShort: hasSpecificTimeframe ? `Stack ${fast}/${slow}` : "Stack",
    fastRule: hasSpecificTimeframe ? `Fast EMA [${fast}]` : "Fast EMA",
    slowRule: hasSpecificTimeframe ? `Slow EMA [${slow}]` : "Slow EMA",
    stackRule: hasSpecificTimeframe ? `EMA Stack [${fast}/${slow}]` : "EMA Stack",
    crossUpRule: hasSpecificTimeframe ? `EMA Cross Up [${fast}/${slow}]` : "EMA Cross Up",
    crossDownRule: hasSpecificTimeframe ? `EMA Cross Down [${fast}/${slow}]` : "EMA Cross Down",
    fastAngleRule: hasSpecificTimeframe ? `Fast EMA Angle [${fast}]` : "Fast EMA Angle",
    slowAngleRule: hasSpecificTimeframe ? `Slow EMA Angle [${slow}]` : "Slow EMA Angle",
    fastSlopeRule: hasSpecificTimeframe ? `Fast EMA Slope [${fast}]` : "Fast EMA Slope",
    slowSlopeRule: hasSpecificTimeframe ? `Slow EMA Slope [${slow}]` : "Slow EMA Slope",
    spreadRule: hasSpecificTimeframe ? `EMA Spread [${fast}-${slow}]` : "EMA Spread",
    spreadPctRule: hasSpecificTimeframe ? `EMA Spread % [${fast}/${slow}]` : "EMA Spread %",
    spreadDeltaRule: hasSpecificTimeframe ? `EMA Spread Δ [${fast}-${slow}]` : "EMA Spread Δ",
    spreadPctDeltaRule: hasSpecificTimeframe ? `EMA Spread Δ% [${fast}/${slow}]` : "EMA Spread Δ%",
    spreadTrendRule: hasSpecificTimeframe ? `EMA Spread Trend [${fast}/${slow}]` : "EMA Spread Trend",
    pressureRule: hasSpecificTimeframe ? `EMA Pressure [${fast}/${slow}]` : "EMA Pressure",
  };
}

function compactFieldLabel(field, timeframe = null, preset = null) {
  const ema = emaDisplayConfig(preset, timeframe);
  const labels = {
    ema_1: ema.fastShort,
    ema_2: ema.slowShort,
    ema_stack: ema.stackShort,
    ema_1_angle_state: `${ema.fastShort} Angle`,
    ema_2_angle_state: `${ema.slowShort} Angle`,
    ema_1_slope: `${ema.fastShort} Slope`,
    ema_2_slope: `${ema.slowShort} Slope`,
    ema_spread: `Spread ${ema.fastLength}-${ema.slowLength}`,
    ema_spread_pct: `Spread % ${ema.fastLength}/${ema.slowLength}`,
    ema_spread_delta: `Δ Spread ${ema.fastLength}-${ema.slowLength}`,
    ema_spread_pct_delta: `Δ Spread % ${ema.fastLength}/${ema.slowLength}`,
    ema_spread_trend: `Spread Trend ${ema.fastLength}/${ema.slowLength}`,
    ema_stack_pressure: `EMA Pressure ${ema.fastLength}/${ema.slowLength}`,
    rsi: "RSI",
    rsi_smooth: "RSI Smooth",
    rsi_state: "RSI State",
  };
  return labels[field] || fieldLabel(field, timeframe, preset);
}

function snapshotColumnLabel(columnKey) {
  if (columnKey === "ema_1") return "Fast EMA";
  if (columnKey === "ema_2") return "Slow EMA";
  if (columnKey === "ema_stack") return "Stack";
  const column = SNAPSHOT_COLUMNS.find((item) => item.key === columnKey);
  return column?.label || fieldLabel(columnKey);
}

function fieldLabel(field, timeframe = null, preset = null) {
  const ema = emaDisplayConfig(preset, timeframe);
  const labels = {
    ema_1: ema.fastRule,
    ema_2: ema.slowRule,
    ema_stack: ema.stackRule,
    ema_cross_up: ema.crossUpRule,
    ema_cross_down: ema.crossDownRule,
    ema_1_angle_state: ema.fastAngleRule,
    ema_2_angle_state: ema.slowAngleRule,
    ema_1_slope: ema.fastSlopeRule,
    ema_2_slope: ema.slowSlopeRule,
    ema_spread: ema.spreadRule,
    ema_spread_pct: ema.spreadPctRule,
    ema_spread_delta: ema.spreadDeltaRule,
    ema_spread_pct_delta: ema.spreadPctDeltaRule,
    ema_spread_trend: ema.spreadTrendRule,
    ema_stack_pressure: ema.pressureRule,
    ms: "M/S",
    angle: "Angle",
    sig_angle: "SigAngle",
    ppo: "PPO",
    flip_age: "FlipAge",
    macd: "MACD",
    signal: "Signal",
    rsi: "RSI",
    rsi_smooth: "RSI Smooth",
    rsi_state: "RSI State",
    stoch_rsi_k: "Stoch K",
    stoch_rsi_d: "Stoch D",
    stoch_rsi_kd: "Stoch K/D",
    ms_cross: "MACD×Signal",
    macd0_cross: "MACD×0",
    adx: "ADX",
    adx_angle: "ADXAngle",
    atr: "ATR",
    atr_angle: "ATRAngle",
    di_plus: "+DI",
    di_minus: "-DI",
    di_spread: "DI Spread",
    di_cross: "DI Cross",
    taker_buy_volume: "Taker Buy Vol",
    taker_sell_volume: "Taker Sell Vol",
    taker_delta_volume: "Taker Delta Vol",
    taker_delta_pct: "Taker Delta %",
    taker_buy_share_pct: "Taker Buy %",
    taker_trade_count: "Taker Trades",
    taker_bias_state: "Taker Bias",
    frama_state: "FRAMA",
    frama_break_up: "FRAMA Break Up",
    frama_break_down: "FRAMA Break Down",
    frama_mid_cross: "FRAMA Mid Cross",
    vidya_state: "VIDYA",
    vidya_line: "VIDYA Line",
    vidya_raw: "VIDYA Raw",
    vidya_upper: "VIDYA Upper",
    vidya_lower: "VIDYA Lower",
    vidya_trend_up: "VIDYA Trend Up",
    vidya_trend_down: "VIDYA Trend Down",
    vidya_up_volume: "VIDYA Buy Volume",
    vidya_down_volume: "VIDYA Sell Volume",
    vidya_delta_pct: "VIDYA Δ%",
    vidya_slope: "VIDYA Slope",
    vidya_angle_deg: "VIDYA Angle",
    vidya_angle_state: "VIDYA Angle State",
    vidya_angle_accel: "VIDYA Angle Accel",
    vidya_angle_deg_norm: "VIDYA Angle Norm",
    vidya_active_liq_low_count: "VIDYA Active Liq Low Count",
    vidya_active_liq_high_count: "VIDYA Active Liq High Count",
    vidya_nearest_liq_low: "VIDYA Nearest Liq Low",
    vidya_nearest_liq_high: "VIDYA Nearest Liq High",
    vidya_dist_to_liq_low_pct: "VIDYA Dist to Liq Low %",
    vidya_dist_to_liq_high_pct: "VIDYA Dist to Liq High %",
    vidya_new_liq_low: "VIDYA New Liq Low",
    vidya_new_liq_high: "VIDYA New Liq High",
    vidya_liq_low_taken: "VIDYA Liq Low Taken",
    vidya_liq_high_taken: "VIDYA Liq High Taken",
    range_filter_buy: "Range Buy",
    range_filter_sell: "Range Sell",
    range_filter_long_cond: "Range Long Cond",
    range_filter_short_cond: "Range Short Cond",
    range_filter_cond_ini: "Range CondIni",
    range_filter_state: "Range",
    range_filter_phase: "Range Phase",
    range_filter_line: "Range Line",
    range_filter_upper: "Range Upper",
    range_filter_lower: "Range Lower",
    range_filter_smooth_range: "Range Smooth Range",
    range_filter_upward_count: "Range Up Count",
    range_filter_downward_count: "Range Down Count",
    market_state: "Market",
    market_bias: "Bias",
    market_phase: "Phase",
    market_confidence: "Confidence",
    market_state_age: "Market Age",
    market_direction_score: "Market Direction Score",
    market_energy_score: "Market Energy Score",
    market_alignment_bonus: "Market Alignment Bonus",
    market_macd_gap_norm: "Market MACD Gap Norm",
  };
  return labels[field] || field;
}

const DOT_FIELDS = new Set(["ms", "angle", "sig_angle", "ppo", "adx_angle", "atr_angle", "rsi_state", "stoch_rsi_kd"]);
const CROSS_FIELDS = {
  ms_cross: { description: "MACD crosses Signal. Use UP for bullish cross and DOWN for bearish cross." },
  macd0_cross: { description: "MACD crosses the zero line. Use UP for a bullish zero-line cross and DOWN for bearish." },
  di_cross: { description: "+DI crosses -DI. Use UP when +DI moves above -DI and DOWN for the opposite." },
};
const FIXED_EVENT_FIELDS = {
  ema_cross_up: "Fast EMA crossed above Slow EMA on this bar.",
  ema_cross_down: "Fast EMA crossed below Slow EMA on this bar.",
  range_filter_buy: "Fresh Range Filter BUY event.",
  range_filter_sell: "Fresh Range Filter SELL event.",
  range_filter_long_cond: "Range Filter long condition is active on this bar.",
  range_filter_short_cond: "Range Filter short condition is active on this bar.",
  frama_break_up: "Fresh FRAMA break-up event.",
  frama_break_down: "Fresh FRAMA break-down event.",
  frama_mid_cross: "Fresh FRAMA mid-line cross event.",
  vidya_trend_up: "Fresh VIDYA trend-up event.",
  vidya_trend_down: "Fresh VIDYA trend-down event.",
  vidya_new_liq_low: "New VIDYA liquidity low detected.",
  vidya_new_liq_high: "New VIDYA liquidity high detected.",
  vidya_liq_low_taken: "VIDYA liquidity low has been taken.",
  vidya_liq_high_taken: "VIDYA liquidity high has been taken.",
};
const ENUM_STATE_FIELDS = {
  ema_stack: { options: ["BUY", "SELL", "NEUTRAL"], description: "Current fast EMA versus slow EMA directional stack." },
  ema_1_angle_state: { options: ["UP", "DOWN", "FLAT"], description: "Current fast EMA angle state." },
  ema_2_angle_state: { options: ["UP", "DOWN", "FLAT"], description: "Current slow EMA angle state." },
  ema_spread_trend: {
    options: ["EXPANDING", "CONTRACTING", "FLAT"],
    description: "Whether the absolute distance between fast and slow EMA is widening, narrowing, or stable.",
  },
  ema_stack_pressure: {
    options: ["BULL_ACCELERATING", "BULL_DECELERATING", "BEAR_ACCELERATING", "BEAR_DECELERATING", "NEUTRAL"],
    description: "Directional EMA pressure based on current stack plus spread dynamics.",
  },
  ms: { options: ["GREEN", "RED"], description: "Current MACD/Signal color state." },
  angle: { options: ["GREEN", "RED"], description: "Current MACD angle color state." },
  sig_angle: { options: ["GREEN", "RED"], description: "Current signal-angle color state." },
  ppo: { options: ["GREEN", "RED"], description: "Current PPO color state." },
  rsi_state: { options: ["GREEN", "RED"], description: "GREEN when RSI is above RSI Smooth, RED otherwise." },
  stoch_rsi_kd: { options: ["GREEN", "RED"], description: "GREEN when Stoch RSI %K is above %D, RED otherwise." },
  adx_angle: { options: ["GREEN", "RED"], description: "Current ADX-angle color state." },
  atr_angle: { options: ["GREEN", "RED"], description: "Current ATR-angle color state." },
  taker_bias_state: { options: ["BUY", "SELL", "NEUTRAL"], description: "Candle-level taker aggressor bias from Binance taker-buy volume versus total volume." },
  frama_state: { options: ["BUY", "SELL", "NEUTRAL"], description: "Current FRAMA state." },
  vidya_state: { options: ["BUY", "SELL"], description: "Current VIDYA state." },
  range_filter_state: { options: ["BUY", "SELL", "NEUTRAL"], description: "Current Range Filter directional state." },
  range_filter_phase: { options: ["BUY_STRONG", "BUY_WEAK", "SELL_STRONG", "SELL_WEAK", "NEUTRAL"], description: "Current Range Filter phase label." },
  market_state: {
    options: ["BULL_EXPANSION", "BULL_PULLBACK", "BULL_EXHAUSTION", "BEAR_EXPANSION", "BEAR_PULLBACK", "BEAR_EXHAUSTION", "TRANSITION", "COMPRESSION"],
    description: "Current market-state label from the backend.",
  },
  market_bias: { options: ["LONG", "SHORT", "NEUTRAL"], description: "Current higher-level directional bias." },
  market_phase: {
    options: ["EXPANSION", "PULLBACK", "EXHAUSTION", "TRANSITION", "COMPRESSION"],
    description: "Current market-phase label from the backend.",
  },
  vidya_angle_state: { options: ["UP", "DOWN", "FLAT"], description: "Current VIDYA angle state." },
  vidya_angle_accel: { options: ["ACCEL", "DECEL", "STABLE"], description: "Current VIDYA angle acceleration state." },
};
const TEXT_STATE_FIELDS = {};
const NUMERIC_FIELDS = new Set([
  "ema_1",
  "ema_2",
  "ema_1_slope",
  "ema_2_slope",
  "ema_spread",
  "ema_spread_pct",
  "ema_spread_delta",
  "ema_spread_pct_delta",
  "flip_age",
  "macd",
  "signal",
  "rsi",
  "rsi_smooth",
  "stoch_rsi_k",
  "stoch_rsi_d",
  "adx",
  "atr",
  "di_plus",
  "di_minus",
  "di_spread",
  "taker_buy_volume",
  "taker_sell_volume",
  "taker_delta_volume",
  "taker_delta_pct",
  "taker_buy_share_pct",
  "taker_trade_count",
  "range_filter_line",
  "range_filter_upper",
  "range_filter_lower",
  "range_filter_smooth_range",
  "range_filter_cond_ini",
  "range_filter_upward_count",
  "range_filter_downward_count",
  "vidya_line",
  "vidya_raw",
  "vidya_upper",
  "vidya_lower",
  "vidya_up_volume",
  "vidya_down_volume",
  "vidya_delta_pct",
  "vidya_slope",
  "vidya_angle_deg",
  "vidya_angle_deg_norm",
  "vidya_active_liq_low_count",
  "vidya_active_liq_high_count",
  "vidya_nearest_liq_low",
  "vidya_nearest_liq_high",
  "vidya_dist_to_liq_low_pct",
  "vidya_dist_to_liq_high_pct",
  "market_confidence",
  "market_state_age",
  "market_direction_score",
  "market_energy_score",
  "market_alignment_bonus",
  "market_macd_gap_norm",
]);

function fieldDescription(field) {
  if (CROSS_FIELDS[field]?.description) return CROSS_FIELDS[field].description;
  if (FIXED_EVENT_FIELDS[field]) return FIXED_EVENT_FIELDS[field];
  if (ENUM_STATE_FIELDS[field]?.description) return ENUM_STATE_FIELDS[field].description;
  if (TEXT_STATE_FIELDS[field]) return TEXT_STATE_FIELDS[field];
  if (field === "rsi" || field === "rsi_smooth") return `${fieldLabel(field)} is a numeric RSI line on the selected timeframe.`;
  if (field === "stoch_rsi_k" || field === "stoch_rsi_d") return `${fieldLabel(field)} is a numeric Stochastic RSI line on the selected timeframe.`;
  if (NUMERIC_FIELDS.has(field)) return `${fieldLabel(field)} is treated as a numeric comparison on the selected timeframe.`;
  return `This rule evaluates ${fieldLabel(field)} on the selected timeframe.`;
}

function getFieldMeta(field) {
  if (DOT_FIELDS.has(field)) {
    return {
      family: "dot",
      modes: ["state", "event"],
      stateOps: ["=", "!="],
      eventOps: ["TURNED", "FLIPPED"],
      stateValues: ["GREEN", "RED"],
      turnedValues: ["GREEN", "RED"],
      description: fieldDescription(field),
    };
  }
  if (CROSS_FIELDS[field]) {
    return {
      family: "cross",
      modes: ["event"],
      eventOps: ["CROSS"],
      crossValues: ["UP", "DOWN"],
      description: fieldDescription(field),
    };
  }
  if (FIXED_EVENT_FIELDS[field]) {
    return {
      family: "event-fixed",
      modes: ["event"],
      eventOps: ["EVENT"],
      description: fieldDescription(field),
    };
  }
  if (ENUM_STATE_FIELDS[field]) {
    return {
      family: "state-enum",
      modes: ["state"],
      stateOps: ["=", "!="],
      stateValues: ENUM_STATE_FIELDS[field].options,
      description: fieldDescription(field),
    };
  }
  if (TEXT_STATE_FIELDS[field]) {
    return {
      family: "state-text",
      modes: ["state"],
      stateOps: ["=", "!="],
      description: fieldDescription(field),
    };
  }
  if (NUMERIC_FIELDS.has(field)) {
    return {
      family: "numeric",
      modes: ["state"],
      stateOps: STATE_OP_OPTIONS,
      description: fieldDescription(field),
    };
  }
  return {
    family: "generic",
    modes: ["state", "event"],
    stateOps: STATE_OP_OPTIONS,
    eventOps: EVENT_OP_OPTIONS,
    description: fieldDescription(field),
  };
}

function getRuleValueSpec(meta, mode, op) {
  if (meta.family === "dot") {
    if (mode === "state") return { kind: "select", label: "State", options: meta.stateValues, defaultValue: meta.stateValues[0] };
    if (op === "TURNED") return { kind: "select", label: "Turned To", options: meta.turnedValues, defaultValue: meta.turnedValues[0] };
    return { kind: "none", label: "Value" };
  }
  if (meta.family === "cross") {
    return { kind: "select", label: "Direction", options: meta.crossValues, defaultValue: meta.crossValues[0] };
  }
  if (meta.family === "event-fixed") {
    return { kind: "none", label: "Value" };
  }
  if (meta.family === "state-enum") {
    return { kind: "select", label: "State", options: meta.stateValues, defaultValue: meta.stateValues[0] };
  }
  if (meta.family === "state-text") {
    return { kind: "text", label: "State", placeholder: "Enter backend label" };
  }
  if (meta.family === "numeric") {
    return { kind: "number", label: "Threshold", placeholder: "Numeric value" };
  }
  if (mode === "state") {
    return { kind: "text", label: "Value", placeholder: "Value" };
  }
  if (op === "TURNED" || op === "CROSS") {
    return { kind: "text", label: "Value", placeholder: "Value" };
  }
  return { kind: "none", label: "Value" };
}

function normalizeRuleConfig(rule) {
  if (!rule) return rule;
  const isTakerBiasWindowField = TAKER_BIAS_WINDOW_FIELDS.has(String(rule.field || ""));
  const rawWindowBars = Number(rule.window_bars ?? 1);
  rule.window_bars = Math.max(1, Math.min(10, Number.isFinite(rawWindowBars) ? rawWindowBars : 1));
  if (!isTakerBiasWindowField) {
    rule.window_bars = 1;
  }
  const meta = getFieldMeta(rule.field);
  const modeOptions = meta.modes || ["state", "event"];
  if (!modeOptions.includes(rule.mode)) {
    rule.mode = modeOptions[0];
  }
  const operatorOptions = rule.mode === "state" ? (meta.stateOps || STATE_OP_OPTIONS) : (meta.eventOps || EVENT_OP_OPTIONS);
  if (!operatorOptions.includes(rule.op)) {
    rule.op = operatorOptions[0];
  }
  const valueSpec = getRuleValueSpec(meta, rule.mode, rule.op);
  if (valueSpec.kind === "none") {
    rule.value = null;
  } else if (valueSpec.kind === "select" && Array.isArray(valueSpec.options) && valueSpec.options.length) {
    if (!valueSpec.options.map(String).includes(String(rule.value))) {
      rule.value = valueSpec.defaultValue ?? valueSpec.options[0];
    }
  } else if (valueSpec.kind === "number") {
    if (rule.value === null || rule.value === undefined || rule.value === "") {
      rule.value = 0;
    }
  } else if ((rule.value === null || rule.value === undefined) && valueSpec.placeholder) {
    rule.value = "";
  }
  if (rule.mode !== "event") {
    rule.is_trigger = false;
  }
  if (rule.is_sequence_canceler) {
    rule.is_trigger = false;
    rule.is_final_gate = false;
  } else if (rule.is_final_gate) {
    rule.is_sequence_canceler = false;
  }
  return rule;
}

function getRuleUiSchema(rule, groupJoin = "OR") {
  const normalized = normalizeRuleConfig({ ...rule });
  const meta = getFieldMeta(normalized.field);
  const modeOptions = meta.modes || ["state", "event"];
  const operatorOptions = normalized.mode === "state" ? (meta.stateOps || STATE_OP_OPTIONS) : (meta.eventOps || EVENT_OP_OPTIONS);
  const valueSpec = getRuleValueSpec(meta, normalized.mode, normalized.op);
  const isSequence = String(groupJoin || "OR").toUpperCase() === "SEQUENCE";
  return {
    meta,
    normalized,
    modeOptions,
    operatorOptions,
    valueSpec,
    showMode: modeOptions.length > 1,
    showOperator: operatorOptions.length > 1,
    showValue: valueSpec.kind !== "none",
    triggerEnabled: normalized.mode === "event",
    finalGateEnabled: isSequence,
    sequenceCancelerEnabled: isSequence,
    stillValidEnabled: String(normalized.bars_ago_mode || "OFF").toUpperCase() !== "OFF",
  };
}

function ruleHelpText(rule, groupJoin, control) {
  const schema = getRuleUiSchema(rule, groupJoin);
  const fieldName = fieldLabel(schema.normalized.field, schema.normalized.timeframe);
  if (control === "field") return schema.meta.description;
  if (control === "mode") {
    if (!schema.showMode) return `${fieldName} only works as ${schema.normalized.mode.toUpperCase()} logic here.`;
    return schema.normalized.mode === "event"
      ? `Event mode checks for something new happening on this bar, not just the current persistent state.`
      : `State mode checks the current value/state on the selected timeframe.`;
  }
  if (control === "operator") {
    if (!schema.showOperator) return `${fieldName} uses ${schema.normalized.op} automatically in this rule family.`;
    if (schema.normalized.mode === "event") {
      return `Choose the event behavior that makes sense for ${fieldName}. Only valid event operators are shown here.`;
    }
    return `Choose the comparison that makes sense for ${fieldName}. Only valid state operators are shown here.`;
  }
  if (control === "value") {
    if (!schema.showValue) return `${fieldName} does not need a value in this situation.`;
    if (schema.valueSpec.kind === "select") return `Choose from the valid values for ${fieldName} in this exact situation.`;
    if (schema.valueSpec.kind === "number") return `Enter the numeric threshold used for ${fieldName}.`;
    return `Enter the exact value label expected for ${fieldName}.`;
  }
  if (control === "trigger") {
    return schema.triggerEnabled
      ? `Mark this event as the trigger event for the group.`
      : `Trigger only makes sense on event rules, so it is disabled here.`;
  }
  if (control === "final-gate") {
    return schema.finalGateEnabled
      ? `Inside a SEQUENCE group, Final Gate checks this rule only when the final sequence step completes.`
      : `Final Gate only matters inside a SEQUENCE group, so it is disabled here.`;
  }
  if (control === "sequence-canceler") {
    return schema.sequenceCancelerEnabled
      ? `Inside a SEQUENCE group, Sequence Canceler watches this rule on every bar while the sequence is in progress. If it turns TRUE, the whole sequence resets.`
      : `Sequence Canceler only matters inside a SEQUENCE group, so it is disabled here.`;
  }
  if (control === "still-valid") {
    return schema.stillValidEnabled
      ? `After the Bars Ago timing matches, require the rule to still be valid now.`
      : `Require Still Valid only matters when Bars Ago timing is enabled.`;
  }
  if (control === "bars-ago") {
    return `Advanced timing filter: require an event to have happened EXACT or WITHIN N builder steps ago.`;
  }
  if (control === "window-bars") {
    return `For Taker Bias only: evaluate the rule over the last N confirmed closed bars instead of a single candle.`;
  }
  return schema.meta.description;
}

function renderRuleControl(label, controlHtml, className = "", helpText = "") {
  const titleAttr = helpText ? ` title="${escapeHtml(helpText)}"` : "";
  const cls = className ? ` ${className}` : "";
  return `<label class="${`builder-rule-cell${cls}`.trim()}"${titleAttr}><span>${escapeHtml(label)}</span>${controlHtml}</label>`;
}

function opLabel(rule) {
  if (!rule) return "";
  const valuePart = rule.value === null || rule.value === undefined || rule.value === "" ? "" : ` ${rule.value}`;
  return `${rule.op}${valuePart}`;
}

function ruleSummary(rule) {
  if (!rule) return "";
  const schema = getRuleUiSchema(rule);
  const normalized = schema.normalized;
  const parts = [normalized.timeframe];
  if (schema.meta.family === "cross") {
    parts.push(`${fieldLabel(normalized.field, normalized.timeframe)} ${String(normalized.value || "").toUpperCase()}`.trim());
  } else if (schema.meta.family === "event-fixed") {
    parts.push(fieldLabel(normalized.field, normalized.timeframe));
  } else {
    parts.push(fieldLabel(normalized.field, normalized.timeframe));
    parts.push(opLabel({ op: normalized.op, value: schema.showValue ? normalized.value : "" }));
  }
  if (normalized.is_trigger && normalized.mode === "event") parts.push("★");
  if (normalized.is_final_gate) parts.push("Final");
  if (normalized.is_sequence_canceler) parts.push("Cancel");
  if ((normalized.bars_ago_mode || "OFF") !== "OFF") parts.push(`${normalized.bars_ago_mode} ${normalized.bars_ago_n || 0}`);
  if (TAKER_BIAS_WINDOW_FIELDS.has(String(normalized.field || "")) && Number(normalized.window_bars || 1) > 1) parts.push(`W${normalized.window_bars}`);
  return parts.filter(Boolean).join(" · ");
}

function fmtSnapshotValue(value, digits = 2) {
  if (value === null || value === undefined || value === "") return "—";
  const num = Number(value);
  if (Number.isFinite(num) && String(value).trim() !== "") return num.toFixed(digits);
  return String(value);
}

function boolish(value) {
  return value === true || value === "true" || value === 1 || value === "1";
}

function defaultRule() {
  return {
    timeframe: "1m",
    mode: "event",
    field: "range_filter_buy",
    op: "EVENT",
    value: null,
    is_trigger: false,
    is_final_gate: false,
    is_sequence_canceler: false,
    bars_ago_mode: "OFF",
    bars_ago_n: 0,
    window_bars: 1,
    require_still_valid: false,
  };
}

function sequenceRuleDisplayMeta(rules = [], ruleIndex = 0) {
  const normalizedRules = Array.isArray(rules) ? rules.map((rule) => normalizeRuleConfig({ ...(rule || {}) })) : [];
  const targetRule = normalizedRules[ruleIndex] || null;
  if (!targetRule) return { kind: "step", stepNumber: ruleIndex + 1, totalSteps: normalizedRules.length || 1 };
  if (targetRule.is_sequence_canceler) return { kind: "canceler", stepNumber: null, totalSteps: null };
  if (targetRule.is_final_gate) return { kind: "gate", stepNumber: null, totalSteps: null };
  const seqRules = normalizedRules.filter((rule) => !rule.is_final_gate && !rule.is_sequence_canceler);
  const seqIndexes = [];
  normalizedRules.forEach((rule, index) => {
    if (!rule.is_final_gate && !rule.is_sequence_canceler) seqIndexes.push(index);
  });
  const seqPosition = Math.max(0, seqIndexes.indexOf(ruleIndex));
  return { kind: "step", stepNumber: seqPosition + 1, totalSteps: seqRules.length || 1 };
}

function ruleUiKey(tabName, groupIndex, ruleIndex) {
  return `${tabName}::${groupIndex}::${ruleIndex}`;
}

function isRuleAdvancedOpen(tabName, groupIndex, ruleIndex, rule = null) {
  const key = ruleUiKey(tabName, groupIndex, ruleIndex);
  if (Object.prototype.hasOwnProperty.call(state.ruleUi, key)) {
    return !!state.ruleUi[key];
  }
  const targetRule = rule || state.builderPreset?.tabs?.[tabName]?.groups?.[groupIndex]?.rules?.[ruleIndex];
  return !!targetRule && String(targetRule.bars_ago_mode || "OFF").toUpperCase() !== "OFF";
}

function alphaGroupKey(index) {
  let current = Number(index);
  if (!Number.isFinite(current) || current < 0) current = 0;
  let value = "";
  do {
    value = String.fromCharCode(65 + (current % 26)) + value;
    current = Math.floor(current / 26) - 1;
  } while (current >= 0);
  return value;
}

function nextGroupKey(groups = []) {
  const used = new Set((groups || []).map((group) => String(group?.group_key || "").trim().toUpperCase()).filter(Boolean));
  let index = 0;
  while (used.has(alphaGroupKey(index))) index += 1;
  return alphaGroupKey(index);
}

function normalizeAdvancedLogic(tab) {
  const validKeys = new Set((tab?.groups || []).map((group, index) => String(group?.group_key || alphaGroupKey(index)).trim()).filter(Boolean));
  const fallbackTarget = Array.from(validKeys)[0] || "";
  const refs = (tab?.groups || []).map((group, index) => ({
    key: String(group?.group_key || alphaGroupKey(index)).trim() || alphaGroupKey(index),
  }));
  const raw = tab?.advanced_logic && typeof tab.advanced_logic === "object" ? tab.advanced_logic : {};
  const routes = Array.isArray(raw.routes) ? raw.routes : [];
  tab.advanced_logic = {
    enabled: boolish(raw.enabled),
    routes: routes.map((route, routeIndex) => {
      const target = String(route?.target_group_key || "").trim();
      const normalizedRoute = {
        route_id: String(route?.route_id || `logic-${Date.now().toString(36)}-${routeIndex}`),
        expression: String(route?.expression || "").trim(),
        target_group_key: validKeys.has(target) ? target : fallbackTarget,
        exclusive: boolish(route?.exclusive),
        note: String(route?.note || ""),
      };
      normalizedRoute.conditions = normalizeLogicConditions(route, refs);
      syncLogicRouteExpression(normalizedRoute);
      return normalizedRoute;
    }),
  };
  return tab.advanced_logic;
}

function groupLabel(group, index) {
  const key = String(group?.group_key || alphaGroupKey(index)).trim() || alphaGroupKey(index);
  return `${key} · Group ${index + 1}`;
}

function logicRouteId() {
  return `logic-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

function defaultPresetTemplate(name = "new") {
  return {
    version: 1,
    settings: {
      symbol: "BTCUSDT",
      start: "",
      end: "",
      warmup_min: 600,
      cache_dir: "data_cache",
    price_source: "LAST",
    macd_impl: "TRADINGVIEW",
    adx_impl: "TRADINGVIEW",
    ema_indicators: defaultEmaIndicatorSettings(),
    rsi_indicators: defaultRsiIndicatorSettings(),
    live_forming: true,
      latch_until_flip: true,
      bt_mode: "Bar Close (1m candles)",
      timeframes: {
        "1m": false,
        "5m": false,
        "15m": false,
        "30m": false,
        "1h": false,
      },
    },
    tabs: TAB_NAMES.reduce((acc, tabName) => {
      acc[tabName] = { groups_join: "AND", groups: [], advanced_logic: { enabled: false, routes: [] } };
      return acc;
    }, {}),
    entry_filters: {
      "Long Entry": {
        enabled: false,
        mode: "OFF",
        expansion_atr_mult: 2.0,
        hard_skip_atr_mult: 3.0,
        confirm_bars: 3,
        reference_range: "FULL_CANDLE",
        touch_type: "WICK",
        min_retrace_pct: 50.0,
        max_retrace_pct: 100.0,
        require_next_close_beyond_mid: true,
        require_setup_still_valid: true,
        apply_to_reversals: true,
        groups: {},
      },
      "Short Entry": {
        enabled: false,
        mode: "OFF",
        expansion_atr_mult: 2.0,
        hard_skip_atr_mult: 3.0,
        confirm_bars: 3,
        reference_range: "FULL_CANDLE",
        touch_type: "WICK",
        min_retrace_pct: 50.0,
        max_retrace_pct: 100.0,
        require_next_close_beyond_mid: true,
        require_setup_still_valid: true,
        apply_to_reversals: true,
        groups: {},
      },
    },
    meta: {
      strategy_note: `Preset scaffold for ${name}`,
    },
  };
}

function normalizePreset(rawPreset, name = "preset") {
  const preset = clone(rawPreset || defaultPresetTemplate(name));
  preset.version = Number.isFinite(Number(preset.version)) ? Number(preset.version) : 1;
  preset.settings = { ...defaultPresetTemplate(name).settings, ...(preset.settings || {}) };
  preset.settings.timeframes = { ...defaultPresetTemplate(name).settings.timeframes, ...(preset.settings.timeframes || {}) };
  preset.settings.ema_indicators = normalizeEmaIndicatorSettings(preset.settings.ema_indicators);
  preset.settings.rsi_indicators = normalizeRsiIndicatorSettings(preset.settings.rsi_indicators);
  preset.tabs = preset.tabs || {};
  TAB_NAMES.forEach((tabName) => {
    const tab = preset.tabs[tabName] || { groups_join: "AND", groups: [], advanced_logic: { enabled: false, routes: [] } };
    tab.groups_join = tab.groups_join || "AND";
    tab.groups = Array.isArray(tab.groups) ? tab.groups : [];
    const assignedKeys = new Set();
    tab.groups = tab.groups.map((group, groupIndex) => {
      let groupKey = String(group?.group_key || "").trim().toUpperCase();
      if (!groupKey || assignedKeys.has(groupKey)) {
        groupKey = alphaGroupKey(groupIndex);
        while (assignedKeys.has(groupKey)) {
          groupKey = alphaGroupKey(assignedKeys.size + groupIndex + 1);
        }
      }
      assignedKeys.add(groupKey);
      return {
        group_key: groupKey,
        rules_join: group?.rules_join || "OR",
        rules: Array.isArray(group?.rules) ? group.rules.map((rule) => ({ ...defaultRule(), ...(rule || {}) })) : [],
      };
    });
    normalizeAdvancedLogic(tab);
    preset.tabs[tabName] = tab;
  });
  const rawFilters = preset.entry_filters || {};
  preset.entry_filters = {};
  ["Long Entry", "Short Entry"].forEach((tabName) => {
    const rawTab = rawFilters[tabName] || {};
    const tabFilter = normalizeEntryFilterConfig(rawTab);
    const rawGroupFilters = rawTab?.groups || {};
    const nextGroupFilters = {};
    const groups = preset.tabs?.[tabName]?.groups || [];
    groups.forEach((group, groupIndex) => {
      const groupKey = String(group?.group_key || alphaGroupKey(groupIndex)).trim().toUpperCase();
      const rawGroup = rawGroupFilters[groupKey] ?? rawGroupFilters[String(groupIndex)] ?? null;
      if (rawGroup) {
        nextGroupFilters[groupKey] = normalizeEntryFilterConfig(rawGroup);
      }
    });
    tabFilter.groups = nextGroupFilters;
    preset.entry_filters[tabName] = tabFilter;
  });
  preset.meta = { strategy_note: "", ...(preset.meta || {}) };
  syncPresetTimeframes(preset);
  return preset;
}

function normalizePresetForBuilder(rawPreset, name = "preset") {
  const preset = normalizePreset(rawPreset, name);
  TAB_NAMES.forEach((tabName) => {
    const tab = preset.tabs?.[tabName] || { groups_join: "AND", groups: [], advanced_logic: { enabled: false, routes: [] } };
    tab.groups = (Array.isArray(tab.groups) ? tab.groups : []).map((group) => ({
      group_key: String(group?.group_key || nextGroupKey(tab.groups)).trim().toUpperCase(),
      rules_join: group?.rules_join || "OR",
      rules: Array.isArray(group?.rules) ? group.rules.map((rule) => ({ ...defaultRule(), ...(rule || {}) })) : [],
    }));
    normalizeAdvancedLogic(tab);
    preset.tabs[tabName] = tab;
  });
  return preset;
}

function defaultEntryFilterConfig() {
  return clone(defaultPresetTemplate("entry_filter").entry_filters["Long Entry"]);
}

function normalizeEntryFilterMode(mode) {
  const raw = String(mode || "OFF").toUpperCase().trim();
  if (["RETRACE", "PULLBACK", "PRICE_PULLBACK", "REQUIRE_PULLBACK"].includes(raw)) return "RETRACE_ALWAYS";
  if (["OFF", "ANTI_CHASE", "RETRACE_ON_EXPANSION", "RETRACE_ALWAYS"].includes(raw)) return raw;
  return "OFF";
}

function normalizeEntryFilterReferenceRange(value) {
  const raw = String(value || "FULL_CANDLE").toUpperCase().trim();
  return raw === "BODY_ONLY" ? "BODY_ONLY" : "FULL_CANDLE";
}

function normalizeEntryFilterTouchType(value) {
  const raw = String(value || "WICK").toUpperCase().trim();
  return raw === "CLOSE" ? "CLOSE" : "WICK";
}

function entryFilterRequiresAtr(config) {
  const cfg = config || {};
  const mode = normalizeEntryFilterMode(cfg.mode);
  if (mode === "ANTI_CHASE" || mode === "RETRACE_ON_EXPANSION") return true;
  return Number(cfg.hard_skip_atr_mult || 0) > 0;
}

function normalizeEntryFilterConfig(rawConfig) {
  const defaults = defaultEntryFilterConfig();
  const raw = { ...(rawConfig || {}) };
  delete raw.groups;
  const next = { ...defaults, ...raw };
  next.enabled = Boolean(next.enabled);
  next.mode = normalizeEntryFilterMode(next.mode);
  next.expansion_atr_mult = Math.max(0.1, Number(next.expansion_atr_mult || defaults.expansion_atr_mult));
  next.hard_skip_atr_mult = Number(next.hard_skip_atr_mult ?? defaults.hard_skip_atr_mult);
  if (!Number.isFinite(next.hard_skip_atr_mult)) next.hard_skip_atr_mult = defaults.hard_skip_atr_mult;
  if (next.hard_skip_atr_mult <= 0) {
    next.hard_skip_atr_mult = 0;
  } else if (["ANTI_CHASE", "RETRACE_ON_EXPANSION"].includes(next.mode)) {
    next.hard_skip_atr_mult = Math.max(next.expansion_atr_mult, next.hard_skip_atr_mult);
  }
  next.confirm_bars = Math.max(1, Number(next.confirm_bars || defaults.confirm_bars));
  next.reference_range = normalizeEntryFilterReferenceRange(next.reference_range);
  next.touch_type = normalizeEntryFilterTouchType(next.touch_type);
  next.min_retrace_pct = Math.max(0, Math.min(100, Number(next.min_retrace_pct ?? defaults.min_retrace_pct)));
  next.max_retrace_pct = Math.max(0, Math.min(100, Number(next.max_retrace_pct ?? defaults.max_retrace_pct)));
  if (next.max_retrace_pct < next.min_retrace_pct) next.max_retrace_pct = next.min_retrace_pct;
  next.require_next_close_beyond_mid = Boolean(next.require_next_close_beyond_mid);
  next.require_setup_still_valid = Boolean(next.require_setup_still_valid);
  next.apply_to_reversals = Boolean(next.apply_to_reversals);
  if (!next.enabled) next.mode = "OFF";
  delete next.groups;
  return next;
}

function entryFilterTabKey(tabName) {
  return tabName === "Short Entry" ? "Short Entry" : "Long Entry";
}

function entryFilterGroupKey(tabName, groupIndex) {
  return String(state.builderPreset?.tabs?.[tabName]?.groups?.[groupIndex]?.group_key || "").trim().toUpperCase();
}

function ensureEntryFilterConfig(tabName) {
  const key = entryFilterTabKey(tabName);
  if (!state.builderPreset) return normalizeEntryFilterConfig({});
  state.builderPreset.entry_filters = state.builderPreset.entry_filters || {};
  const next = normalizeEntryFilterConfig(state.builderPreset.entry_filters[key] || {});
  next.groups = { ...((state.builderPreset.entry_filters[key] || {}).groups || {}) };
  state.builderPreset.entry_filters[key] = next;
  return next;
}

function getGroupEntryFilterConfig(tabName, groupIndex) {
  const tabConfig = ensureEntryFilterConfig(tabName);
  const groupKey = entryFilterGroupKey(tabName, groupIndex);
  if (!groupKey) return null;
  const rawGroup = tabConfig.groups?.[groupKey];
  if (!rawGroup) return null;
  const next = normalizeEntryFilterConfig(rawGroup);
  tabConfig.groups[groupKey] = next;
  return next;
}

function effectiveEntryFilterConfig(tabName, groupIndex = null) {
  if (groupIndex === null || groupIndex === undefined) return ensureEntryFilterConfig(tabName);
  return getGroupEntryFilterConfig(tabName, groupIndex) || ensureEntryFilterConfig(tabName);
}

function ensureGroupEntryFilterConfig(tabName, groupIndex) {
  const tabConfig = ensureEntryFilterConfig(tabName);
  const groupKey = entryFilterGroupKey(tabName, groupIndex);
  if (!groupKey) return normalizeEntryFilterConfig(tabConfig);
  tabConfig.groups = tabConfig.groups || {};
  const rawGroup = tabConfig.groups[groupKey];
  if (rawGroup) {
    const next = normalizeEntryFilterConfig(rawGroup);
    tabConfig.groups[groupKey] = next;
    return next;
  }
  const seeded = normalizeEntryFilterConfig(tabConfig);
  tabConfig.groups[groupKey] = seeded;
  return tabConfig.groups[groupKey];
}

function entryFilterIsActive(config) {
  const cfg = normalizeEntryFilterConfig(config || {});
  return Boolean(cfg.enabled) && normalizeEntryFilterMode(cfg.mode) !== "OFF";
}

function entryFilterButtonLabel(tabName) {
  return `Default Entry Filter: ${entryFilterIsActive(effectiveEntryFilterConfig(tabName)) ? "On" : "Off"}`;
}

function groupEntryFilterButtonLabel(tabName, groupIndex) {
  const groupCfg = getGroupEntryFilterConfig(tabName, groupIndex);
  if (!groupCfg) {
    return `Group Entry Filter: Inherit ${entryFilterIsActive(effectiveEntryFilterConfig(tabName)) ? "On" : "Off"}`;
  }
  return `Group Entry Filter: ${entryFilterIsActive(groupCfg) ? "On" : "Off"}`;
}

function entryFilterSummaryText(config) {
  const cfg = { ...defaultEntryFilterConfig(), ...(config || {}) };
  if (!cfg.enabled || String(cfg.mode || "OFF").toUpperCase() === "OFF") return "Off";
  return `Anti-chase · ${fmt(cfg.expansion_atr_mult, 2)} ATR · wait ${fmt(cfg.confirm_bars, 0)} bar${Number(cfg.confirm_bars) === 1 ? "" : "s"} · retrace ≤ ${fmt(cfg.max_retrace_pct, 0)}%`;
}

function renderEntryFilterModal(tabName, groupIndex = null) {
  if (groupIndex === null || groupIndex === undefined) {
    groupIndex = state.entryFilterModalGroupIndex;
  }
  const isGroupTarget = groupIndex !== null && groupIndex !== undefined;
  const cfg = isGroupTarget ? ensureGroupEntryFilterConfig(tabName, groupIndex) : ensureEntryFilterConfig(tabName);
  const groupLabelText = isGroupTarget ? groupLabel(state.builderPreset?.tabs?.[tabName]?.groups?.[groupIndex], groupIndex) : "";
  const isActive = entryFilterIsActive(cfg);
  return `
    <div class="downloader-field-modal">
      <div class="downloader-field-modal-note">
        Anti-Chase Filter uses ATR-based giant-candle avoidance after the rules already turned TRUE on the entry bar. It delays expansion candles instead of rewriting Bars Ago logic.
        ${isGroupTarget
          ? `<br />This filter applies only to ${escapeHtml(tabName)} · ${escapeHtml(groupLabelText || `Group ${Number(groupIndex) + 1}`)}.`
          : `<br />This is the default Anti-Chase Filter for ${escapeHtml(tabName)}.`}
      </div>
      <label class="toggle toggle-inline">
        <input type="checkbox" data-entry-filter-prop="enabled" ${isActive ? "checked" : ""} />
        <span>Anti-Chase Filter On</span>
      </label>
      <div class="pill-row">
        <span class="pill">${escapeHtml(`Status: ${isActive ? "On" : "Off"}`)}</span>
        <span class="pill">Uses ATR internally</span>
      </div>
      <div class="detail-grid">
        <label>
          <span>Expansion Threshold (ATR×)</span>
          <input type="number" step="0.1" min="0.1" data-entry-filter-prop="expansion_atr_mult" value="${cfg.expansion_atr_mult}" />
        </label>
        <label>
          <span>Hard Skip Threshold (ATR×)</span>
          <input type="number" step="0.1" min="0.1" data-entry-filter-prop="hard_skip_atr_mult" value="${cfg.hard_skip_atr_mult}" />
        </label>
        <label>
          <span>Confirmation Bars</span>
          <input type="number" step="1" min="1" data-entry-filter-prop="confirm_bars" value="${cfg.confirm_bars}" />
        </label>
        <label>
          <span>Max Retrace Into Signal (%)</span>
          <input type="number" step="1" min="0" max="100" data-entry-filter-prop="max_retrace_pct" value="${cfg.max_retrace_pct}" />
        </label>
        <label class="toggle toggle-inline">
          <input type="checkbox" data-entry-filter-prop="require_next_close_beyond_mid" ${cfg.require_next_close_beyond_mid ? "checked" : ""} />
          <span>Require confirmation close beyond midpoint</span>
        </label>
        <label class="toggle toggle-inline">
          <input type="checkbox" data-entry-filter-prop="require_setup_still_valid" ${cfg.require_setup_still_valid ? "checked" : ""} />
          <span>Require setup still valid on confirmation bar</span>
        </label>
        <label class="toggle toggle-inline">
          <input type="checkbox" data-entry-filter-prop="apply_to_reversals" ${cfg.apply_to_reversals ? "checked" : ""} />
          <span>Apply to reverse entries too</span>
        </label>
      </div>
      <div class="editor-actions">
        <button class="secondary-button" type="button" data-entry-filter-action="reset">Reset Defaults</button>
      </div>
    </div>
  `;
}

function entryFilterModalTitle(tabName, groupIndex = null) {
  if (groupIndex === null || groupIndex === undefined) return `Anti-Chase Filter · ${tabName}`;
  const label = groupLabel(state.builderPreset?.tabs?.[tabName]?.groups?.[groupIndex], groupIndex);
  return `Anti-Chase Filter · ${tabName} · ${label}`;
}

function openEntryFilterModal(tabName, groupIndex = null) {
  if (!state.builderPreset) return;
  state.entryFilterModalTab = tabName;
  state.entryFilterModalGroupIndex = groupIndex;
  showDetailModal(entryFilterModalTitle(tabName, groupIndex), renderEntryFilterModal(tabName, groupIndex), { mode: "entry-filter", wide: true });
}

function rerenderEntryFilterModal() {
  if (!state.entryFilterModalTab) return;
  showDetailModal(
    entryFilterModalTitle(state.entryFilterModalTab, state.entryFilterModalGroupIndex),
    renderEntryFilterModal(state.entryFilterModalTab, state.entryFilterModalGroupIndex),
    { mode: "entry-filter", wide: true }
  );
}

function updateEntryFilterProp(tabName, prop, rawValue, inputType = "text", groupIndex = null) {
  const cfg = groupIndex === null || groupIndex === undefined
    ? ensureEntryFilterConfig(tabName)
    : ensureGroupEntryFilterConfig(tabName, groupIndex);
  if (inputType === "checkbox") {
    cfg[prop] = Boolean(rawValue);
  } else if (["expansion_atr_mult", "hard_skip_atr_mult", "confirm_bars", "max_retrace_pct"].includes(prop)) {
    cfg[prop] = Number(rawValue || 0);
  } else {
    cfg[prop] = rawValue;
  }
  if (prop === "enabled") {
    cfg.mode = cfg.enabled ? "ANTI_CHASE" : "OFF";
  }
  if (prop === "mode") {
    cfg.mode = String(cfg.mode || "OFF").toUpperCase() === "ANTI_CHASE" ? "ANTI_CHASE" : "OFF";
    cfg.enabled = cfg.mode === "ANTI_CHASE";
  }
  if (groupIndex === null || groupIndex === undefined) {
    ensureEntryFilterConfig(tabName);
  } else {
    ensureGroupEntryFilterConfig(tabName, groupIndex);
  }
  state.builderPresetDirty = true;
  renderBuilder();
  syncJsonEditorFromBuilder();
  const targetText = groupIndex === null || groupIndex === undefined
    ? tabName
    : `${tabName} ${groupLabel(state.builderPreset?.tabs?.[tabName]?.groups?.[groupIndex], groupIndex)}`;
  setBuilderStatus(`Updated Anti-Chase Filter for ${targetText}.`);
  requestBuilderLiveRefresh();
}

function entryFilterSummaryText(config) {
  const cfg = normalizeEntryFilterConfig(config || {});
  if (!entryFilterIsActive(cfg)) return "Off";
  if (cfg.mode === "ANTI_CHASE") {
    return `Anti-chase · ${fmt(cfg.expansion_atr_mult, 2)} ATR · wait ${fmt(cfg.confirm_bars, 0)} bar${Number(cfg.confirm_bars) === 1 ? "" : "s"} · retrace ≤ ${fmt(cfg.max_retrace_pct, 0)}%`;
  }
  const prefix = cfg.mode === "RETRACE_ON_EXPANSION"
    ? `Expansion retrace · ${fmt(cfg.expansion_atr_mult, 2)} ATR`
    : "Retrace gate";
  const scope = cfg.reference_range === "BODY_ONLY" ? "body" : "full candle";
  const touch = cfg.touch_type === "CLOSE" ? "close" : "wick";
  return `${prefix} · ${scope} · ${touch} · ${fmt(cfg.min_retrace_pct, 0)}-${fmt(cfg.max_retrace_pct, 0)}% · wait ${fmt(cfg.confirm_bars, 0)} bar${Number(cfg.confirm_bars) === 1 ? "" : "s"}`;
}

function entryFilterButtonLabel(tabName) {
  return `Default Entry Filter: ${entryFilterIsActive(effectiveEntryFilterConfig(tabName)) ? "On" : "Off"}`;
}

function groupEntryFilterButtonLabel(tabName, groupIndex) {
  const groupCfg = getGroupEntryFilterConfig(tabName, groupIndex);
  if (!groupCfg) {
    return `Group Entry Filter: Inherit ${entryFilterIsActive(effectiveEntryFilterConfig(tabName)) ? "On" : "Off"}`;
  }
  return `Group Entry Filter: ${entryFilterIsActive(groupCfg) ? "On" : "Off"}`;
}

function renderEntryFilterModal(tabName, groupIndex = null) {
  if (groupIndex === null || groupIndex === undefined) {
    groupIndex = state.entryFilterModalGroupIndex;
  }
  const isGroupTarget = groupIndex !== null && groupIndex !== undefined;
  const cfg = isGroupTarget ? ensureGroupEntryFilterConfig(tabName, groupIndex) : ensureEntryFilterConfig(tabName);
  const groupLabelText = isGroupTarget ? groupLabel(state.builderPreset?.tabs?.[tabName]?.groups?.[groupIndex], groupIndex) : "";
  const isActive = entryFilterIsActive(cfg);
  return `
    <div class="downloader-field-modal">
      <div class="downloader-field-modal-note">
        Entry Confirmation Filter runs after the group or sequence has already turned TRUE on the trigger bar. It decides whether to enter now, wait for a required retrace, or block the entry.
        ${isGroupTarget
          ? `<br />This filter applies only to ${escapeHtml(tabName)} · ${escapeHtml(groupLabelText || `Group ${Number(groupIndex) + 1}`)}.`
          : `<br />This is the default Entry Confirmation Filter for ${escapeHtml(tabName)}.`}
      </div>
      <label class="toggle toggle-inline">
        <input type="checkbox" data-entry-filter-prop="enabled" ${isActive ? "checked" : ""} />
        <span>Entry Filter On</span>
      </label>
      <div class="pill-row">
        <span class="pill">${escapeHtml(`Status: ${isActive ? "On" : "Off"}`)}</span>
        <span class="pill">${escapeHtml(entryFilterRequiresAtr(cfg) ? "Uses ATR" : "Price-only")}</span>
      </div>
      <div class="detail-grid">
        <label>
          <span>Mode</span>
          <select data-entry-filter-prop="mode">
            <option value="OFF" ${cfg.mode === "OFF" ? "selected" : ""}>Off</option>
            <option value="ANTI_CHASE" ${cfg.mode === "ANTI_CHASE" ? "selected" : ""}>Legacy Anti-Chase</option>
            <option value="RETRACE_ON_EXPANSION" ${cfg.mode === "RETRACE_ON_EXPANSION" ? "selected" : ""}>Retrace Only On Expansion</option>
            <option value="RETRACE_ALWAYS" ${cfg.mode === "RETRACE_ALWAYS" ? "selected" : ""}>Always Require Pullback</option>
          </select>
        </label>
        <label>
          <span>Expansion Threshold (ATR×)</span>
          <input type="number" step="0.1" min="0.1" data-entry-filter-prop="expansion_atr_mult" value="${cfg.expansion_atr_mult}" />
        </label>
        <label>
          <span>Hard Skip Threshold (ATR×, 0=off)</span>
          <input type="number" step="0.1" min="0" data-entry-filter-prop="hard_skip_atr_mult" value="${cfg.hard_skip_atr_mult}" />
        </label>
        <label>
          <span>Max Wait Bars</span>
          <input type="number" step="1" min="1" data-entry-filter-prop="confirm_bars" value="${cfg.confirm_bars}" />
        </label>
        <label>
          <span>Measure Pullback By</span>
          <select data-entry-filter-prop="reference_range">
            <option value="FULL_CANDLE" ${cfg.reference_range === "FULL_CANDLE" ? "selected" : ""}>Full Candle</option>
            <option value="BODY_ONLY" ${cfg.reference_range === "BODY_ONLY" ? "selected" : ""}>Body Only</option>
          </select>
        </label>
        <label>
          <span>Count Pullback As</span>
          <select data-entry-filter-prop="touch_type">
            <option value="WICK" ${cfg.touch_type === "WICK" ? "selected" : ""}>Wick Touch</option>
            <option value="CLOSE" ${cfg.touch_type === "CLOSE" ? "selected" : ""}>Close</option>
          </select>
        </label>
        <label>
          <span>Minimum Pullback (%)</span>
          <input type="number" step="1" min="0" max="100" data-entry-filter-prop="min_retrace_pct" value="${cfg.min_retrace_pct}" />
        </label>
        <label>
          <span>Maximum Pullback (%)</span>
          <input type="number" step="1" min="0" max="100" data-entry-filter-prop="max_retrace_pct" value="${cfg.max_retrace_pct}" />
        </label>
        <label class="toggle toggle-inline">
          <input type="checkbox" data-entry-filter-prop="require_next_close_beyond_mid" ${cfg.require_next_close_beyond_mid ? "checked" : ""} />
          <span>Require close beyond midpoint (legacy anti-chase)</span>
        </label>
        <label class="toggle toggle-inline">
          <input type="checkbox" data-entry-filter-prop="require_setup_still_valid" ${cfg.require_setup_still_valid ? "checked" : ""} />
          <span>Require setup still active while waiting</span>
        </label>
        <label class="toggle toggle-inline">
          <input type="checkbox" data-entry-filter-prop="apply_to_reversals" ${cfg.apply_to_reversals ? "checked" : ""} />
          <span>Apply to reverse entries too</span>
        </label>
      </div>
      <div class="editor-actions">
        <button class="secondary-button" type="button" data-entry-filter-action="reset">Reset Defaults</button>
      </div>
    </div>
  `;
}

function entryFilterModalTitle(tabName, groupIndex = null) {
  if (groupIndex === null || groupIndex === undefined) return `Entry Confirmation Filter · ${tabName}`;
  const label = groupLabel(state.builderPreset?.tabs?.[tabName]?.groups?.[groupIndex], groupIndex);
  return `Entry Confirmation Filter · ${tabName} · ${label}`;
}

function updateEntryFilterProp(tabName, prop, rawValue, inputType = "text", groupIndex = null) {
  const cfg = groupIndex === null || groupIndex === undefined
    ? ensureEntryFilterConfig(tabName)
    : ensureGroupEntryFilterConfig(tabName, groupIndex);
  if (inputType === "checkbox") {
    cfg[prop] = Boolean(rawValue);
  } else if (["expansion_atr_mult", "hard_skip_atr_mult", "confirm_bars", "min_retrace_pct", "max_retrace_pct"].includes(prop)) {
    cfg[prop] = Number(rawValue || 0);
  } else {
    cfg[prop] = rawValue;
  }
  if (prop === "enabled") {
    cfg.mode = cfg.enabled ? normalizeEntryFilterMode(cfg.mode || "RETRACE_ON_EXPANSION") : "OFF";
  }
  if (prop === "mode") {
    cfg.mode = normalizeEntryFilterMode(cfg.mode);
    cfg.enabled = cfg.mode !== "OFF";
  }
  if (groupIndex === null || groupIndex === undefined) {
    ensureEntryFilterConfig(tabName);
  } else {
    ensureGroupEntryFilterConfig(tabName, groupIndex);
  }
  state.builderPresetDirty = true;
  renderBuilder();
  syncJsonEditorFromBuilder();
  const targetText = groupIndex === null || groupIndex === undefined
    ? tabName
    : `${tabName} ${groupLabel(state.builderPreset?.tabs?.[tabName]?.groups?.[groupIndex], groupIndex)}`;
  setBuilderStatus(`Updated Entry Filter for ${targetText}.`);
  requestBuilderLiveRefresh();
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || `Request failed: ${response.status}`);
  return data;
}

function setView(viewName) {
  document.querySelectorAll(".view").forEach((node) => {
    node.classList.toggle("is-active", node.id === `${viewName}-view`);
  });
  document.querySelectorAll(".nav-link").forEach((node) => {
    node.classList.toggle("is-active", node.dataset.view === viewName);
  });
  const titles = {
    overview: "Overview",
    presets: "Presets",
    builder: "Rule Builder",
    backtest: "Backtest",
  };
  $("view-title").textContent = titles[viewName] || (viewName.charAt(0).toUpperCase() + viewName.slice(1));
}

function setEditorStatus(message, isError = false) {
  const node = $("editor-status");
  node.textContent = message;
  node.style.color = isError ? "var(--danger)" : "";
}

function setBuilderStatus(message, isError = false) {
  const node = $("builder-status");
  node.textContent = message;
  node.style.color = isError ? "var(--danger)" : "";
}

function setSnapshotStatus(message, isError = false) {
  const node = $("snapshot-status");
  node.textContent = message;
  node.style.color = isError ? "var(--danger)" : "";
}

function currentBuilderPresetIdentity() {
  const editorName = $("editor-preset-name")?.value?.trim() || "";
  const builderName = String(state.builderPresetName || "").trim();
  const selectedName = String(state.selectedPreset?.name || "").trim();
  return editorName || builderName || selectedName || "";
}

function syncBuilderHeader() {
  const identity = currentBuilderPresetIdentity();
  $("builder-working-preset-pill").textContent = state.builderPreset
    ? (identity ? `Preset: ${identity}` : "Blank Draft")
    : "No preset loaded";
  $("builder-target-pill").textContent = `Target: ${activeTargetLabel()}`;
}

function syncJsonEditorFromBuilder() {
  if (!state.builderPreset) return;
  $("preset-json-editor").value = JSON.stringify(normalizePreset(state.builderPreset, builderPresetName()), null, 2);
}

function activeBuilderTabName() {
  const target = ensureBuilderTarget();
  return target?.tabName || TAB_NAMES[0];
}

function activeBuilderView() {
  return $("builder-view")?.classList.contains("is-active");
}

function closeBuilderPresetTray() {
  const tray = $("builder-toolbar-disclosure");
  if (tray) tray.open = false;
}

function requestBuilderLiveRefresh() {
  if (!state.builderPreset || !activeBuilderView()) return;
  if (typeof loadSnapshotForSelectedPreset !== "function") return;
  Promise.resolve(loadSnapshotForSelectedPreset()).catch((error) => {
    setSnapshotStatus(error?.message || String(error), true);
  });
}

function loadPresetIntoEditor(name, preset) {
  $("editor-preset-name").value = name || "";
  $("preset-json-editor").value = JSON.stringify(preset, null, 2);
  setEditorStatus("Loaded selected preset JSON.");
}

function loadPresetIntoBuilder(name, preset, statusMessage = "Visual builder loaded.") {
  state.builderPreset = normalizePresetForBuilder(preset, name);
  TAB_NAMES.forEach((tabName) => {
    const groups = state.builderPreset?.tabs?.[tabName]?.groups || [];
    groups.forEach((group) => {
      (group.rules || []).forEach((rule) => normalizeRuleConfig(rule));
    });
  });
  state.builderPresetName = name || null;
  state.builderPresetDirty = false;
  ensureBuilderTarget();
  renderBuilder();
  renderSnapshotPanel();
  syncJsonEditorFromBuilder();
  $("editor-preset-name").value = name || "";
  if ($("builder-preset-select")) $("builder-preset-select").value = name || "";
  syncBuilderHeader();
  setBuilderStatus(statusMessage);
  requestBuilderLiveRefresh();
}

function createEmptyGroup(existingGroups = []) {
  return { group_key: nextGroupKey(existingGroups), rules_join: "OR", rules: [] };
}

function ensureBuilderTarget() {
  const preset = state.builderPreset;
  if (!preset) return null;
  const targetTab = state.builderTarget?.tabName && TAB_NAMES.includes(state.builderTarget.tabName)
    ? state.builderTarget.tabName
    : TAB_NAMES[0];
  const tab = preset.tabs[targetTab];
  if (!tab.groups.length) {
    tab.groups.push(createEmptyGroup(tab.groups));
  }
  let groupIndex = Number(state.builderTarget?.groupIndex ?? 0);
  if (!Number.isFinite(groupIndex) || groupIndex < 0 || groupIndex >= tab.groups.length) {
    groupIndex = 0;
  }
  state.builderTarget = { tabName: targetTab, groupIndex };
  return state.builderTarget;
}

function groupOptionsForTab(tabName) {
  const tab = state.builderPreset?.tabs?.[tabName];
  const groups = tab?.groups || [];
  return groups.map((group, index) => ({ value: index, label: groupLabel(group, index) }));
}

function ensureTargetGroup() {
  const target = ensureBuilderTarget();
  if (!target || !state.builderPreset) return null;
  const tab = state.builderPreset.tabs[target.tabName];
  if (!tab.groups.length) tab.groups.push(createEmptyGroup(tab.groups));
  if (!tab.groups[target.groupIndex]) {
    target.groupIndex = 0;
    if (!tab.groups.length) tab.groups.push(createEmptyGroup(tab.groups));
  }
  return tab.groups[target.groupIndex];
}

function setBuilderTarget(tabName, groupIndex = 0) {
  state.builderTarget = { tabName, groupIndex: Number(groupIndex) || 0 };
  ensureBuilderTarget();
  renderBuilder();
  renderSnapshotPanel();
  syncBuilderHeader();
  focusTargetGroup();
}

function insertRuleIntoTarget(rule, message = "Rule added from current values.") {
  const targetGroup = ensureTargetGroup();
  if (!targetGroup) return;
  const nextRule = { ...defaultRule(), ...(rule || {}) };
  normalizeRuleConfig(nextRule);
  targetGroup.rules.push(nextRule);
  renderBuilder();
  renderSnapshotPanel();
  syncJsonEditorFromBuilder();
  syncBuilderHeader();
  setBuilderStatus(message);
  focusTargetGroup();
}

function duplicateRuleAt(tabName, groupIndex, ruleIndex) {
  const rule = state.builderPreset?.tabs?.[tabName]?.groups?.[groupIndex]?.rules?.[ruleIndex];
  if (!rule) return;
  const nextRule = clone(rule);
  normalizeRuleConfig(nextRule);
  state.builderPreset.tabs[tabName].groups[groupIndex].rules.splice(ruleIndex + 1, 0, nextRule);
}

function activeTargetLabel() {
  const target = ensureBuilderTarget();
  if (!target) return "No target";
  const group = state.builderPreset?.tabs?.[target.tabName]?.groups?.[target.groupIndex];
  return `${target.tabName} / ${groupLabel(group, target.groupIndex)}`;
}

function focusTargetGroup() {
  const target = ensureBuilderTarget();
  if (!target) return;
  const selector = `.builder-group[data-tab="${CSS.escape(target.tabName)}"][data-group-index="${target.groupIndex}"]`;
  const node = document.querySelector(selector);
  if (!node) return;
  node.classList.remove("is-focused");
  node.scrollIntoView({ behavior: "smooth", block: "nearest", inline: "center" });
  window.setTimeout(() => node.classList.add("is-focused"), 20);
  window.setTimeout(() => node.classList.remove("is-focused"), 1500);
}

function snapshotRows() {
  return Array.isArray(state.snapshot?.rows) ? state.snapshot.rows : [];
}

function renderOverviewCards(health, presetMeta) {
  const cards = [
    { label: "Backend", value: health.status === "ok" ? "Ready" : "Offline" },
    { label: "UI Mode", value: "Web V2" },
    { label: "Presets", value: String(presetMeta.count || 0) },
    { label: "Last Preset", value: presetMeta.last_preset || "None" },
  ];
  $("overview-cards").innerHTML = cards.map((card) => `<article class="metric-card"><div class="eyebrow">${card.label}</div><strong>${card.value}</strong></article>`).join("");
}

function renderPresetList(items) {
  const list = $("preset-list");
  $("preset-count").textContent = `${items.length} presets`;
  if (!items.length) {
    list.innerHTML = `<div class="empty-state">No presets found in presets.json.</div>`;
    return;
  }
  list.innerHTML = items.map((item) => `
    <button class="preset-item ${state.selectedPreset?.name === item.name ? "is-selected" : ""}" data-preset="${item.name}">
      <h4>${item.name}</h4>
      <div class="muted">${item.symbol} | ${item.timeframes.join(", ") || "No TFs"}</div>
      <div class="muted">${item.total_rules} rules | ${item.bt_mode || "Unknown mode"}</div>
    </button>`).join("");
  list.querySelectorAll(".preset-item").forEach((node) => node.addEventListener("click", () => selectPreset(node.dataset.preset)));
}

function ruleRow(rule) {
  return `<div class="pill"><strong>${rule.timeframe}</strong><span>${rule.field}</span><span>${rule.op}</span>${rule.is_trigger ? "<span>trigger</span>" : ""}</div>`;
}

function renderPresetDetail(detail) {
  const preset = detail.preset;
  state.selectedPreset = { name: detail.name, preset };
  $("selected-preset-name").textContent = detail.name;
  const settings = preset.settings || {};
  const tabs = preset.tabs || {};
  const filters = normalizePreset(preset, detail.name).entry_filters || {};
  const normalizedPreset = normalizePreset(preset, detail.name);
  const emaDefaults = currentEmaSettings(normalizedPreset, null);
  const rsiDefaults = currentRsiSettings(normalizedPreset, null);
  const emaOverrides = emaOverrideCount(normalizedPreset);
  const rsiOverrides = rsiOverrideCount(normalizedPreset);
  const timeframes = detectedPresetTimeframes(normalizedPreset);
  const blocks = [
    `<section class="detail-block"><div class="eyebrow">Window</div><div>${settings.symbol || "BTCUSDT"} | ${settings.start || "?"} -> ${settings.end || "?"}</div><div class="pill-row">${timeframes.map((tf) => `<span class="pill">${tf}</span>`).join("")}<span class="pill">Default EMA: ${formatEmaPair(emaDefaults)}</span><span class="pill">Default RSI: ${formatRsiConfig(rsiDefaults)}</span>${emaOverrides ? `<span class="pill">${emaOverrides} EMA TF override${emaOverrides === 1 ? "" : "s"}</span>` : ""}${rsiOverrides ? `<span class="pill">${rsiOverrides} RSI TF override${rsiOverrides === 1 ? "" : "s"}</span>` : ""}</div></section>`,
    `<section class="detail-block"><div class="eyebrow">Entry Filters</div><div class="pill-row"><span class="pill">Long Entry default: ${escapeHtml(entryFilterSummaryText(filters["Long Entry"]))}</span><span class="pill">Short Entry default: ${escapeHtml(entryFilterSummaryText(filters["Short Entry"]))}</span></div><div class="pill-row">${["Long Entry", "Short Entry"].flatMap((tabName) => {
      const groups = tabs?.[tabName]?.groups || [];
      return groups
        .filter((group) => Boolean(filters?.[tabName]?.groups?.[String(group?.group_key || "").trim().toUpperCase()]))
        .map((group, groupIndex) => `<span class="pill">${escapeHtml(tabName)} ${escapeHtml(groupLabel(group, groupIndex))}: ${escapeHtml(entryFilterSummaryText(filters?.[tabName]?.groups?.[String(group?.group_key || "").trim().toUpperCase()]))}</span>`);
    }).join("") || `<span class="muted">No group-specific ATR overrides.</span>`}</div></section>`,
    `<section class="detail-block"><div class="eyebrow">Tabs</div>${Object.entries(tabs).map(([tabName, tabPayload]) => {
      const groups = tabPayload.groups || [];
      return `<div class="detail-block"><strong>${tabName}</strong><div class="muted">Groups join: ${tabPayload.groups_join || "AND"}</div><div class="pill-row">${groups.flatMap((group) => group.rules || []).map((rule) => ruleRow(rule)).join("") || `<span class="muted">No rules</span>`}</div></div>`;
    }).join("")}</section>`,
  ];
  $("preset-detail").innerHTML = blocks.join("");
  renderPresetList(state.presets);
  syncBacktestForm();
  applyBacktestFormState(presetBacktestFormState(preset));
  setBacktestFormStatus(`Loaded preset ${detail.name} into Backtest.`);
  loadPresetIntoEditor(detail.name, preset);
}

const BACKTEST_FORM_DEFAULTS = Object.freeze({
  start: "",
  end: "",
  stop_loss_value: 0,
  take_profit_value: 0,
  break_even_after_mfe_pct: 0,
  trailing_stop_pct: 0,
  order_notional: 100,
  fee_pct: 0.04,
  equity_curve_stride: 15,
  allow_reverse: true,
  capture_trade_details: false,
});

function normalizeBacktestFormState(raw = {}) {
  const next = { ...BACKTEST_FORM_DEFAULTS, ...(raw || {}) };
  return {
    start: String(next.start || "").trim(),
    end: String(next.end || "").trim(),
    stop_loss_value: Number(next.stop_loss_value || 0),
    take_profit_value: Number(next.take_profit_value || 0),
    break_even_after_mfe_pct: Number(next.break_even_after_mfe_pct || 0),
    trailing_stop_pct: Number(next.trailing_stop_pct || 0),
    order_notional: Number(next.order_notional || 100),
    fee_pct: Number(next.fee_pct || 0.04),
    equity_curve_stride: Math.max(1, Number(next.equity_curve_stride || 15)),
    allow_reverse: Boolean(next.allow_reverse),
    capture_trade_details: Boolean(next.capture_trade_details),
  };
}

function setBacktestFormStatus(message, isError = false) {
  const node = $("backtest-form-status");
  if (!node) return;
  node.textContent = message || "Ready.";
  node.style.color = isError ? "var(--danger)" : "";
}

function presetBacktestFormState(preset) {
  const settings = preset?.settings || {};
  const saved = settings?.backtest_defaults || {};
  return normalizeBacktestFormState({
    ...saved,
    start: saved.start || settings.start || "",
    end: saved.end || settings.end || "",
  });
}

function applyBacktestFormState(raw) {
  const values = normalizeBacktestFormState(raw);
  $("start-input").value = toUtcDateTimeLocalValue(values.start);
  $("end-input").value = toUtcDateTimeLocalValue(values.end);
  $("stop-loss-input").value = String(values.stop_loss_value);
  $("take-profit-input").value = String(values.take_profit_value);
  $("break-even-input").value = String(values.break_even_after_mfe_pct);
  $("trailing-stop-input").value = String(values.trailing_stop_pct);
  $("position-size-input").value = String(values.order_notional);
  $("commission-input").value = String(values.fee_pct);
  $("equity-stride-input").value = String(values.equity_curve_stride);
  $("allow-reverse-input").checked = Boolean(values.allow_reverse);
  $("capture-details-input").checked = Boolean(values.capture_trade_details);
}

function collectBacktestFormState() {
  return normalizeBacktestFormState({
    start: fromUtcDateTimeLocalValue($("start-input").value),
    end: fromUtcDateTimeLocalValue($("end-input").value),
    stop_loss_value: $("stop-loss-input").value,
    take_profit_value: $("take-profit-input").value,
    break_even_after_mfe_pct: $("break-even-input").value,
    trailing_stop_pct: $("trailing-stop-input").value,
    order_notional: $("position-size-input").value,
    fee_pct: $("commission-input").value,
    equity_curve_stride: $("equity-stride-input").value,
    allow_reverse: $("allow-reverse-input").checked,
    capture_trade_details: $("capture-details-input").checked,
  });
}

async function ensureBacktestPresetLoaded(name) {
  const clean = String(name || "").trim();
  if (!clean) return null;
  if (state.selectedPreset?.name === clean && state.selectedPreset?.preset) return state.selectedPreset;
  await selectPreset(clean);
  return state.selectedPreset;
}

function buildPresetFromBacktestForm(basePreset, name) {
  const preset = normalizePreset(clone(basePreset || defaultPresetTemplate(name)), name);
  const form = collectBacktestFormState();
  preset.settings = preset.settings || {};
  preset.settings.start = form.start || "";
  preset.settings.end = form.end || "";
  preset.settings.backtest_defaults = {
    start: form.start || "",
    end: form.end || "",
    stop_loss_value: form.stop_loss_value,
    take_profit_value: form.take_profit_value,
    break_even_after_mfe_pct: form.break_even_after_mfe_pct,
    trailing_stop_pct: form.trailing_stop_pct,
    order_notional: form.order_notional,
    fee_pct: form.fee_pct,
    equity_curve_stride: form.equity_curve_stride,
    allow_reverse: form.allow_reverse,
    capture_trade_details: form.capture_trade_details,
  };
  return preset;
}

async function handleBacktestPresetSelectChange() {
  const name = $("preset-select").value;
  if (!name) return;
  try {
    await ensureBacktestPresetLoaded(name);
    if (state.selectedPreset?.preset) {
      applyBacktestFormState(presetBacktestFormState(state.selectedPreset.preset));
      setBacktestFormStatus(`Loaded saved Backtest values from ${name}.`);
    }
  } catch (error) {
    setBacktestFormStatus(error.message, true);
  }
}

async function handleBacktestPresetLoad() {
  const name = $("preset-select").value;
  if (!name) {
    setBacktestFormStatus("Choose a preset first.", true);
    return;
  }
  await handleBacktestPresetSelectChange();
}

async function handleBacktestPresetSave() {
  const name = String($("preset-select").value || "").trim();
  if (!name) {
    setBacktestFormStatus("Choose a preset first.", true);
    return;
  }
  try {
    const loaded = await ensureBacktestPresetLoaded(name);
    const nextPreset = buildPresetFromBacktestForm(loaded?.preset, name);
    await savePresetPayload(name, nextPreset, Boolean($("editor-set-last")?.checked ?? true));
    await reloadPresetsAndSelect(name, { reloadBuilder: false });
    setBacktestFormStatus(`Saved current Backtest form into preset ${name}.`);
  } catch (error) {
    setBacktestFormStatus(error.message, true);
  }
}

async function handleBacktestPresetSaveAs() {
  const baseName = String($("preset-select").value || state.selectedPreset?.name || "preset_copy").trim() || "preset_copy";
  const nextName = String(window.prompt("Save backtest form as preset:", `${baseName}_bt`) || "").trim();
  if (!nextName) return;
  try {
    const loaded = await ensureBacktestPresetLoaded($("preset-select").value || state.selectedPreset?.name || "");
    const nextPreset = buildPresetFromBacktestForm(loaded?.preset, nextName);
    await savePresetPayload(nextName, nextPreset, Boolean($("editor-set-last")?.checked ?? true));
    await reloadPresetsAndSelect(nextName, { reloadBuilder: false });
    setBacktestFormStatus(`Saved current Backtest form as preset ${nextName}.`);
  } catch (error) {
    setBacktestFormStatus(error.message, true);
  }
}

function syncBacktestForm() {
  const select = $("preset-select");
  const currentName = String(state.selectedPreset?.name || select.value || "").trim();
  select.innerHTML = state.presets.map((item) => `<option value="${item.name}" ${item.name === currentName ? "selected" : ""}>${item.name}</option>`).join("");
  if (currentName && state.presets.some((item) => item.name === currentName)) {
    select.value = currentName;
  }
  const builderSelect = $("builder-preset-select");
  if (builderSelect) {
    const activeName = state.builderPresetName || state.selectedPreset?.name || "";
    builderSelect.innerHTML = state.presets.map((item) => `<option value="${item.name}" ${item.name === activeName ? "selected" : ""}>${item.name}</option>`).join("");
  }
  const selected = state.presets.find((item) => item.name === (select.value || state.selectedPreset?.name));
  if (selected) {
    $("start-input").placeholder = selected.start || "";
    $("end-input").placeholder = selected.end || "";
  }
}

async function selectPreset(name) {
  const detail = await api(`/api/v2/presets/${encodeURIComponent(name)}`);
  renderPresetDetail(detail);
}

function renderMetrics(summary) {
  const tradeCount =
    summary.num_trades ??
    summary.trades ??
    ((Number.isFinite(summary.wins) || Number.isFinite(summary.losses))
      ? (Number(summary.wins || 0) + Number(summary.losses || 0))
      : null);
  const important = [
    ["Engine", summary.strategy_plan_engine || "legacy"],
    ["Return %", fmt(summary.return_pct)],
    ["Trades", fmt(tradeCount, 0)],
    ["Win Rate %", fmt(summary.win_rate_pct)],
    ["Profit Factor", fmt(summary.profit_factor)],
    ["Max DD %", fmt(summary.max_drawdown_pct)],
    ["Total Sec", fmt(summary.timing_total_sec)],
    ["Simulation Sec", fmt(summary.timing_simulation_sec)],
  ];
  $("result-metrics").innerHTML = important.map(([label, value]) => `<article class="metric-card"><div class="eyebrow">${label}</div><strong>${value}</strong></article>`).join("");
}

function renderTrades(trades) {
  const body = $("trades-table-body");
  const rows = Array.isArray(trades) ? trades : [];
  state.currentTrades = rows;
  $("trades-table-status").textContent = rows.length ? `${rows.length} trade${rows.length === 1 ? "" : "s"} loaded. Click a row for the full report.` : "Base backtest trades render here as soon as the core run finishes.";
  if (!rows.length) {
    body.innerHTML = `<tr><td colspan="6" class="muted">No trades captured.</td></tr>`;
    return;
  }
  body.innerHTML = rows.map((trade) => `
    <tr data-trade-id="${trade.trade_id}" class="${Number(trade.trade_id) === Number(state.selectedTradeId) ? "is-selected" : ""}">
      <td>${trade.trade_id}</td>
      <td>${trade.side}</td>
      <td>${trade.entry_time}</td>
      <td>${trade.exit_time}</td>
      <td>${fmt(trade.net_pnl)}</td>
      <td>${fmt(trade.return_pct)}</td>
    </tr>`).join("");
}

function renderActivity(eventActivity) {
  const node = $("event-activity");
  const entries = Object.entries(eventActivity || {});
  if (!entries.length) {
    node.innerHTML = `<div class="empty-state">No event activity in this window.</div>`;
    return;
  }
  node.innerHTML = entries.map(([label, meta]) => `
    <article class="activity-item">
      <strong>${label}</strong>
      <span class="muted">Count: ${meta.count}</span>
      <span class="muted">${(meta.sample_times || []).slice(-2).join(" | ") || "No samples"}</span>
    </article>`).join("");
}

function drawEquity(points) {
  const canvas = $("equity-canvas");
  const ctx = canvas.getContext("2d");
  const width = canvas.width;
  const height = canvas.height;
  ctx.clearRect(0, 0, width, height);
  ctx.fillStyle = "#08111a";
  ctx.fillRect(0, 0, width, height);
  ctx.strokeStyle = "rgba(255,255,255,0.08)";
  ctx.lineWidth = 1;
  for (let i = 1; i < 4; i += 1) {
    const y = (height / 4) * i;
    ctx.beginPath();
    ctx.moveTo(0, y);
    ctx.lineTo(width, y);
    ctx.stroke();
  }
  if (!points.length) return;
  const values = points.map((point) => Number(point.equity_mark ?? point.equity ?? 0));
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = Math.max(1e-9, max - min);
  const toX = (index) => (index / Math.max(1, values.length - 1)) * (width - 40) + 20;
  const toY = (value) => height - ((value - min) / range) * (height - 40) - 20;
  const gradient = ctx.createLinearGradient(0, 0, 0, height);
  gradient.addColorStop(0, "rgba(121,228,210,0.28)");
  gradient.addColorStop(1, "rgba(121,228,210,0.02)");
  ctx.beginPath();
  values.forEach((value, index) => {
    const x = toX(index);
    const y = toY(value);
    if (index === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.lineWidth = 3;
  ctx.strokeStyle = "#79e4d2";
  ctx.stroke();
  ctx.lineTo(toX(values.length - 1), height - 20);
  ctx.lineTo(toX(0), height - 20);
  ctx.closePath();
  ctx.fillStyle = gradient;
  ctx.fill();
}

function clearTradeInspector(message = "Trade report will appear here after you select a trade from the table above.") {
  state.currentTradeDetail = null;
  $("trade-inspector-status").textContent = message;
  $("trade-inspector-grid").innerHTML = `<div class="empty-state">${escapeHtml(message)}</div>`;
  $("trade-chart-status").textContent = "The chart focuses on the selected trade window.";
  drawTradeChart(null);
}

function drawTradeChart(chart) {
  const canvas = $("trade-chart-canvas");
  const ctx = canvas.getContext("2d");
  const width = canvas.width;
  const height = canvas.height;
  ctx.clearRect(0, 0, width, height);
  ctx.fillStyle = "#08111a";
  ctx.fillRect(0, 0, width, height);
  ctx.strokeStyle = "rgba(255,255,255,0.08)";
  ctx.lineWidth = 1;
  for (let i = 1; i < 4; i += 1) {
    const y = (height / 4) * i;
    ctx.beginPath();
    ctx.moveTo(0, y);
    ctx.lineTo(width, y);
    ctx.stroke();
  }

  const rows = Array.isArray(chart?.rows) ? chart.rows : [];
  if (!rows.length) {
    ctx.fillStyle = "rgba(158,176,198,0.9)";
    ctx.font = '14px "Segoe UI Variable", "Segoe UI", sans-serif';
    ctx.fillText("No trade chart data available.", 24, 32);
    return;
  }

  const lows = rows.map((row) => Number(row.low ?? row.close ?? 0)).filter((value) => Number.isFinite(value));
  const highs = rows.map((row) => Number(row.high ?? row.close ?? 0)).filter((value) => Number.isFinite(value));
  const closes = rows.map((row) => Number(row.close ?? row.high ?? row.low ?? 0));
  const min = Math.min(...lows);
  const max = Math.max(...highs);
  const range = Math.max(1e-9, max - min);
  const toX = (index) => (index / Math.max(1, rows.length - 1)) * (width - 50) + 25;
  const toY = (value) => height - ((value - min) / range) * (height - 46) - 24;
  const markerPrice = (row, key) => {
    const direct = Number(row?.[key]);
    if (Number.isFinite(direct)) return direct;
    const fallback = Number(row?.close ?? row?.high ?? row?.low ?? 0);
    return Number.isFinite(fallback) ? fallback : min;
  };

  const insideIndices = rows.map((row, index) => row.inside_trade ? index : -1).filter((value) => value >= 0);
  if (insideIndices.length) {
    const x0 = toX(insideIndices[0]);
    const x1 = toX(insideIndices[insideIndices.length - 1]);
    ctx.fillStyle = "rgba(121,228,210,0.08)";
    ctx.fillRect(x0, 14, Math.max(4, x1 - x0), height - 28);
  }

  ctx.beginPath();
  closes.forEach((value, index) => {
    const x = toX(index);
    const y = toY(Number.isFinite(value) ? value : min);
    if (index === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.lineWidth = 2.5;
  ctx.strokeStyle = "#79e4d2";
  ctx.stroke();

  rows.forEach((row, index) => {
    const x = toX(index);
    if (row.is_entry_bar) {
      const y = toY(markerPrice(row, "entry_marker_price"));
      ctx.beginPath();
      ctx.arc(x, y, 5, 0, Math.PI * 2);
      ctx.fillStyle = "#6aa4ff";
      ctx.fill();
      ctx.strokeStyle = "rgba(255,255,255,0.65)";
      ctx.lineWidth = 1.5;
      ctx.stroke();
    }
    if (row.is_exit_bar) {
      const y = toY(markerPrice(row, "exit_marker_price"));
      ctx.beginPath();
      ctx.arc(x, y, 4, 0, Math.PI * 2);
      ctx.fillStyle = "#ff7d90";
      ctx.fill();
      ctx.strokeStyle = "rgba(255,255,255,0.65)";
      ctx.lineWidth = 1.5;
      ctx.stroke();
    }
  });

  ctx.fillStyle = "rgba(158,176,198,0.9)";
  ctx.font = '12px "Segoe UI Variable", "Segoe UI", sans-serif';
  ctx.fillText(`Low ${fmt(min, 2)}`, 20, height - 8);
  ctx.fillText(`High ${fmt(max, 2)}`, width - 120, 18);
}

function renderTradeInspector(detail) {
  const trade = detail?.trade;
  if (!trade) {
    clearTradeInspector();
    return;
  }
  state.currentTradeDetail = detail;
  $("trade-inspector-status").textContent = `Trade #${trade.trade_id} · ${trade.side} · ${trade.entry_time} → ${trade.exit_time}`;
  const cards = [
    ["Side", trade.side],
    ["Entry", `${trade.entry_time} @ ${fmt(trade.entry_price, 4)}`],
    ["Exit", `${trade.exit_time} @ ${fmt(trade.exit_price, 4)}`],
    ["Net PnL", fmt(trade.net_pnl, 4)],
    ["Return %", fmt(trade.return_pct, 4)],
    ["Qty", fmt(trade.qty, 6)],
    ["Fees", fmt(trade.fees, 6)],
    ["Duration Min", fmt(trade.duration_min, 0)],
    ["MFE %", fmt(trade.mfe_pct, 4)],
    ["MAE %", fmt(trade.mae_pct, 4)],
    ["Gross PnL", fmt(trade.gross_pnl, 4)],
    ["Engine", trade.engine_type || "-"],
  ];
  $("trade-inspector-grid").innerHTML = `
    ${cards.map(([label, value]) => `
      <article class="trade-inspector-card">
        <div class="eyebrow">${escapeHtml(label)}</div>
        <strong>${escapeHtml(value ?? "-")}</strong>
      </article>
    `).join("")}
    <article class="trade-inspector-card full trade-reason-block">
      <div class="eyebrow">Trade Notes</div>
      <div class="trade-reason-grid">
        <div class="trade-reason-item">
          <div class="eyebrow">Entry Reason</div>
          <p>${escapeHtml(trade.entry_reason || "-")}</p>
        </div>
        <div class="trade-reason-item">
          <div class="eyebrow">Exit Reason</div>
          <p>${escapeHtml(trade.exit_reason || "-")}</p>
        </div>
      </div>
    </article>
  `;
  const chart = detail.chart || {};
  const rows = Array.isArray(chart.rows) ? chart.rows : [];
  $("trade-chart-status").textContent = rows.length
    ? `${chart.window_start_utc || rows[0]?.timestamp_utc || ""} → ${chart.window_end_utc || rows[rows.length - 1]?.timestamp_utc || ""}${chart.stride && chart.stride > 1 ? ` · stride ${chart.stride}` : ""}`
    : "No chart data available for this trade.";
  drawTradeChart(chart);
}

function formatTradeUtcStamp(value) {
  const raw = String(value || "").trim();
  return raw ? raw.replace("T", " ").replace("+00:00", " UTC") : "-";
}

function tradeBarTags(row) {
  const tags = [];
  if (row?.is_signal_bar) tags.push({ text: "SIGNAL", css: "is-signal", position: "top" });
  if (row?.is_order_bar) tags.push({ text: "ORDER", css: "is-order", position: "top" });
  if (row?.is_exit_signal_bar) tags.push({ text: "EXIT SIG", css: "is-exit-signal", position: "top" });
  if (row?.is_exit_order_bar) tags.push({ text: "EXIT ORD", css: "is-exit-order", position: "top" });
  if (row?.is_entry_bar) tags.push({ text: "ENTRY", css: "is-entry", position: "bottom" });
  if (row?.is_exit_bar) tags.push({ text: "EXIT", css: "is-exit", position: "bottom" });
  const extraTags = Array.isArray(row?.event_tags) ? row.event_tags : [];
  extraTags.forEach((tag) => {
    const text = String(tag?.text || "").trim();
    if (!text) return;
    tags.push({
      text,
      css: String(tag?.css || "is-seq-step"),
      position: String(tag?.position || "top"),
    });
  });
  return tags;
}

function renderTradeBarTags(row) {
  const tags = tradeBarTags(row);
  if (!tags.length) return `<span class="muted">-</span>`;
  return `<div class="trade-bar-tags">${tags.map((tag) => `<span class="trade-bar-tag ${tag.css}">${escapeHtml(tag.text)}</span>`).join("")}</div>`;
}

function renderTradeLifecycleStrip(trade, chart) {
  const host = $("trade-lifecycle-strip");
  if (!host) return;
  const events = Array.isArray(chart?.events) ? chart.events : [];
  if (!events.length) {
    host.innerHTML = `<div class="empty-state">No lifecycle events were captured for this trade.</div>`;
    return;
  }
  host.innerHTML = events.map((event) => `
    <article class="trade-lifecycle-item ${escapeHtml(String(event.css || ""))}">
      <div class="eyebrow">${escapeHtml(event.label || event.key || "Event")}</div>
      <strong>${escapeHtml(formatTradeUtcStamp(event.time_utc))}</strong>
      <span class="muted">${event.window_row_index !== null && event.window_row_index !== undefined ? `Window bar ${Number(event.window_row_index) + 1}` : "Outside current chart window"}</span>
    </article>
  `).join("");
}

function renderRuleTrace(traceText) {
  const text = String(traceText || "").trim();
  if (!text) return `<div class="empty-state">No rule trace captured for this side.</div>`;
  const lines = text.split(/\r?\n/).filter((line) => line.trim().length);
  return `
    <div class="trade-trace-list">
      ${lines.map((rawLine) => {
        const trimmed = rawLine.trim();
        const badges = [];
        let clean = trimmed;
        if (clean.includes("[TRIGGER]")) {
          badges.push(`<span class="trade-trace-badge is-trigger">TRIGGER</span>`);
          clean = clean.replace("[TRIGGER]", "").replace(/\s{2,}/g, " ").trim();
        }
        if (clean.includes("[FINAL GATE]")) {
          badges.push(`<span class="trade-trace-badge is-gate">FINAL GATE</span>`);
          clean = clean.replace("[FINAL GATE]", "").replace(/\s{2,}/g, " ").trim();
        }
        if (clean.includes("[SEQ CANCEL]")) {
          badges.push(`<span class="trade-trace-badge is-cancel">SEQ CANCEL</span>`);
          clean = clean.replace("[SEQ CANCEL]", "").replace(/\s{2,}/g, " ").trim();
        }
        let lineClass = "trade-trace-line";
        if (clean.startsWith("Tab:") || clean.startsWith("TF[")) lineClass += " is-header";
        else if (clean.startsWith("Group ")) lineClass += " is-group";
        else if (clean.startsWith("Sequence progress:") || clean.startsWith("Sequence reset:")) lineClass += " is-sequence";
        else if (rawLine.startsWith("      ")) lineClass += " is-detail";
        return `
          <div class="${lineClass}">
            ${badges.join("")}
            <span>${escapeHtml(clean)}</span>
          </div>
        `;
      }).join("")}
    </div>
  `;
}

function renderTradeBarTimeline(detail) {
  const body = $("trade-bar-table-body");
  const status = $("trade-bar-table-status");
  if (!body || !status) return;
  const rows = Array.isArray(detail?.chart?.rows) ? detail.chart.rows : [];
  if (!rows.length) {
    body.innerHTML = `<tr><td colspan="7" class="muted">No bar timeline available.</td></tr>`;
    status.textContent = "No bar data available for the selected trade.";
    return;
  }
  status.textContent = `${rows.length} bar${rows.length === 1 ? "" : "s"} around the selected trade.`;
  body.innerHTML = rows.map((row, index) => {
    const tags = tradeBarTags(row);
    const rowClass = [
      row?.inside_trade ? "is-inside-trade" : "",
      tags.length ? "is-event-bar" : "",
    ].filter(Boolean).join(" ");
    return `
      <tr class="${rowClass}">
        <td>${index + 1}</td>
        <td>${escapeHtml(formatTradeUtcStamp(row.timestamp_utc))}</td>
        <td>${escapeHtml(fmt(row.open, 4))}</td>
        <td>${escapeHtml(fmt(row.high, 4))}</td>
        <td>${escapeHtml(fmt(row.low, 4))}</td>
        <td>${escapeHtml(fmt(row.close, 4))}</td>
        <td>${renderTradeBarTags(row)}</td>
      </tr>
    `;
  }).join("");
}

function clearTradeInspector(message = "Trade report will appear here after you select a trade from the table above.") {
  state.currentTradeDetail = null;
  $("trade-inspector-status").textContent = message;
  $("trade-inspector-grid").innerHTML = `<div class="empty-state">${escapeHtml(message)}</div>`;
  $("trade-chart-status").textContent = "The chart focuses on the selected trade window.";
  if ($("trade-lifecycle-strip")) $("trade-lifecycle-strip").innerHTML = `<div class="empty-state">${escapeHtml(message)}</div>`;
  if ($("trade-bar-table-body")) $("trade-bar-table-body").innerHTML = `<tr><td colspan="7" class="muted">${escapeHtml(message)}</td></tr>`;
  if ($("trade-bar-table-status")) $("trade-bar-table-status").textContent = "Bars around the selected trade will appear here.";
  drawTradeChart(null);
}

function drawTradeChart(chart) {
  const canvas = $("trade-chart-canvas");
  const ctx = canvas.getContext("2d");
  const width = canvas.width;
  const height = canvas.height;
  ctx.clearRect(0, 0, width, height);
  ctx.fillStyle = "#08111a";
  ctx.fillRect(0, 0, width, height);

  const rows = Array.isArray(chart?.rows) ? chart.rows : [];
  if (!rows.length) {
    ctx.fillStyle = "rgba(158,176,198,0.9)";
    ctx.font = '14px "Segoe UI Variable", "Segoe UI", sans-serif';
    ctx.fillText("No trade chart data available.", 24, 32);
    return;
  }

  const numeric = (value, fallback = null) => {
    const num = Number(value);
    if (Number.isFinite(num)) return num;
    return fallback;
  };
  const highs = rows.map((row) => numeric(row.high, numeric(row.close, 0))).filter((value) => Number.isFinite(value));
  const lows = rows.map((row) => numeric(row.low, numeric(row.close, 0))).filter((value) => Number.isFinite(value));
  const min = Math.min(...lows);
  const max = Math.max(...highs);
  const range = Math.max(1e-9, max - min);
  const chartLeft = 36;
  const chartRight = width - 18;
  const chartTop = 24;
  const chartBottom = height - 34;
  const plotWidth = Math.max(10, chartRight - chartLeft);
  const plotHeight = Math.max(10, chartBottom - chartTop);
  const slotWidth = plotWidth / Math.max(1, rows.length);
  const candleWidth = Math.max(3, Math.min(14, slotWidth * 0.58));
  const toX = (index) => chartLeft + slotWidth * index + slotWidth / 2;
  const toY = (value) => chartBottom - ((value - min) / range) * plotHeight;

  ctx.strokeStyle = "rgba(255,255,255,0.06)";
  ctx.lineWidth = 1;
  for (let step = 0; step <= 4; step += 1) {
    const y = chartTop + (plotHeight / 4) * step;
    ctx.beginPath();
    ctx.moveTo(chartLeft, y);
    ctx.lineTo(chartRight, y);
    ctx.stroke();
  }

  const insideIndices = rows.map((row, index) => row.inside_trade ? index : -1).filter((value) => value >= 0);
  if (insideIndices.length) {
    const x0 = toX(insideIndices[0]) - slotWidth / 2;
    const x1 = toX(insideIndices[insideIndices.length - 1]) + slotWidth / 2;
    ctx.fillStyle = "rgba(121,228,210,0.07)";
    ctx.fillRect(x0, chartTop, Math.max(4, x1 - x0), plotHeight);
  }

  const drawBadge = (x, y, text, fill, stroke) => {
    ctx.font = '10px "Segoe UI Variable", "Segoe UI", sans-serif';
    const padX = 6;
    const textWidth = ctx.measureText(text).width;
    const boxWidth = textWidth + padX * 2;
    const boxHeight = 16;
    ctx.fillStyle = fill;
    ctx.strokeStyle = stroke;
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.roundRect(x - boxWidth / 2, y - boxHeight / 2, boxWidth, boxHeight, 8);
    ctx.fill();
    ctx.stroke();
    ctx.fillStyle = "#f5f8ff";
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";
    ctx.fillText(text, x, y + 0.5);
  };

  rows.forEach((row, index) => {
    const x = toX(index);
    const open = numeric(row.open, numeric(row.close, min));
    const close = numeric(row.close, open);
    const high = numeric(row.high, Math.max(open, close));
    const low = numeric(row.low, Math.min(open, close));
    const color = close >= open ? "#79e4d2" : "#ff7d90";
    const wickTop = toY(high);
    const wickBottom = toY(low);
    const bodyTop = toY(Math.max(open, close));
    const bodyBottom = toY(Math.min(open, close));
    const bodyHeight = Math.max(2, bodyBottom - bodyTop);

    ctx.beginPath();
    ctx.moveTo(x, wickTop);
    ctx.lineTo(x, wickBottom);
    ctx.strokeStyle = color;
    ctx.lineWidth = 1.5;
    ctx.stroke();

    ctx.fillStyle = color;
    ctx.fillRect(x - candleWidth / 2, bodyTop, candleWidth, bodyHeight);

    const topTags = tradeBarTags(row).filter((tag) => tag.position === "top");
    const bottomTags = tradeBarTags(row).filter((tag) => tag.position === "bottom");
    topTags.forEach((tag, tagIndex) => {
      const fill = tag.css === "is-signal" || tag.css === "is-seq-trigger" ? "rgba(255, 209, 102, 0.88)"
        : tag.css === "is-order" || tag.css === "is-exit-order" ? "rgba(183, 148, 246, 0.88)"
        : tag.css === "is-seq-complete" ? "rgba(134, 239, 172, 0.92)"
        : tag.css === "is-seq-step" ? "rgba(121, 228, 210, 0.88)"
        : "rgba(255, 125, 144, 0.88)";
      drawBadge(x, chartTop + 12 + tagIndex * 18, tag.text, fill, "rgba(255,255,255,0.14)");
    });
    bottomTags.forEach((tag, tagIndex) => {
      const fill = tag.css === "is-entry" ? "rgba(106, 164, 255, 0.9)" : "rgba(255, 125, 144, 0.9)";
      drawBadge(x, chartBottom - 12 - tagIndex * 18, tag.text, fill, "rgba(255,255,255,0.14)");
    });
  });

  const entryRow = rows.find((row) => Number.isFinite(Number(row.entry_marker_price)));
  const exitRow = rows.find((row) => Number.isFinite(Number(row.exit_marker_price)));
  [
    { row: entryRow, key: "entry_marker_price", color: "rgba(106,164,255,0.65)", label: "Entry" },
    { row: exitRow, key: "exit_marker_price", color: "rgba(255,125,144,0.65)", label: "Exit" },
  ].forEach((item) => {
    const price = numeric(item?.row?.[item.key]);
    if (!Number.isFinite(price)) return;
    const y = toY(price);
    ctx.save();
    ctx.setLineDash([6, 4]);
    ctx.beginPath();
    ctx.moveTo(chartLeft, y);
    ctx.lineTo(chartRight, y);
    ctx.strokeStyle = item.color;
    ctx.lineWidth = 1;
    ctx.stroke();
    ctx.restore();
    ctx.fillStyle = item.color;
    ctx.font = '11px "Segoe UI Variable", "Segoe UI", sans-serif';
    ctx.textAlign = "left";
    ctx.textBaseline = "bottom";
    ctx.fillText(`${item.label} ${fmt(price, 2)}`, chartLeft + 6, y - 4);
  });

  ctx.fillStyle = "rgba(158,176,198,0.9)";
  ctx.font = '12px "Segoe UI Variable", "Segoe UI", sans-serif';
  ctx.textAlign = "left";
  ctx.textBaseline = "alphabetic";
  ctx.fillText(`Low ${fmt(min, 2)}`, chartLeft, height - 10);
  ctx.textAlign = "right";
  ctx.fillText(`High ${fmt(max, 2)}`, chartRight, 16);
}

function renderTradeInspector(detail) {
  const trade = detail?.trade;
  if (!trade) {
    clearTradeInspector();
    return;
  }
  state.currentTradeDetail = detail;
  $("trade-inspector-status").textContent = `Trade #${trade.trade_id} | ${trade.side} | ${formatTradeUtcStamp(trade.entry_time)} -> ${formatTradeUtcStamp(trade.exit_time)}`;
  const cards = [
    ["Side", trade.side],
    ["Entry", `${formatTradeUtcStamp(trade.entry_time)} @ ${fmt(trade.entry_price, 4)}`],
    ["Exit", `${formatTradeUtcStamp(trade.exit_time)} @ ${fmt(trade.exit_price, 4)}`],
    ["Net PnL", fmt(trade.net_pnl, 4)],
    ["Return %", fmt(trade.return_pct, 4)],
    ["Qty", fmt(trade.qty, 6)],
    ["Fees", fmt(trade.fees, 6)],
    ["Duration Min", fmt(trade.duration_min, 0)],
    ["MFE %", fmt(trade.mfe_pct, 4)],
    ["MAE %", fmt(trade.mae_pct, 4)],
    ["Gross PnL", fmt(trade.gross_pnl, 4)],
    ["Engine", trade.engine_type || "-"],
  ];
  $("trade-inspector-grid").innerHTML = `
    ${cards.map(([label, value]) => `
      <article class="trade-inspector-card">
        <div class="eyebrow">${escapeHtml(label)}</div>
        <strong>${escapeHtml(value ?? "-")}</strong>
      </article>
    `).join("")}
    <article class="trade-inspector-card full trade-reason-block">
      <div class="eyebrow">Trade Notes</div>
      <div class="trade-reason-grid">
        <div class="trade-reason-item">
          <div class="eyebrow">Entry Reason</div>
          <p>${escapeHtml(trade.entry_reason || "-")}</p>
        </div>
        <div class="trade-reason-item">
          <div class="eyebrow">Exit Reason</div>
          <p>${escapeHtml(trade.exit_reason || "-")}</p>
        </div>
      </div>
    </article>
    <article class="trade-inspector-card full">
      <div class="eyebrow">Rule Trace</div>
      <div class="trade-trace-columns">
        <section class="trade-trace-panel">
          <div class="eyebrow">Entry Trace</div>
          ${renderRuleTrace(trade.entry_rule_trace)}
        </section>
        <section class="trade-trace-panel">
          <div class="eyebrow">Exit Trace</div>
          ${renderRuleTrace(trade.exit_rule_trace)}
        </section>
      </div>
    </article>
  `;
  const chart = detail.chart || {};
  const rows = Array.isArray(chart.rows) ? chart.rows : [];
  $("trade-chart-status").textContent = rows.length
    ? `${formatTradeUtcStamp(chart.window_start_utc || rows[0]?.timestamp_utc || "")} -> ${formatTradeUtcStamp(chart.window_end_utc || rows[rows.length - 1]?.timestamp_utc || "")}${chart.stride && chart.stride > 1 ? ` | stride ${chart.stride}` : ""}`
    : "No chart data available for this trade.";
  renderTradeLifecycleStrip(trade, chart);
  renderTradeBarTimeline(detail);
  drawTradeChart(chart);
}

async function hydrateRunTrades(runId) {
  state.currentRunId = runId || null;
  state.currentTrades = [];
  state.selectedTradeId = null;
  renderTrades([]);
  clearTradeInspector("Loading full trade list...");
  if (!runId) return;
  try {
    const payload = await api(`/api/v2/backtests/${encodeURIComponent(runId)}/trades`);
    const trades = Array.isArray(payload.trades) ? payload.trades : [];
    renderTrades(trades);
    if (trades.length) {
      await selectTradeRow(trades[0].trade_id);
    } else {
      clearTradeInspector("No trades were captured in this run.");
    }
  } catch (error) {
    $("trades-table-status").textContent = error.message;
    clearTradeInspector(error.message);
  }
}

async function selectTradeRow(tradeId) {
  if (!state.currentRunId) return;
  state.selectedTradeId = Number(tradeId) || null;
  renderTrades(state.currentTrades);
  $("trade-inspector-status").textContent = `Loading trade #${tradeId}...`;
  try {
    const detail = await api(`/api/v2/backtests/${encodeURIComponent(state.currentRunId)}/trades/${encodeURIComponent(tradeId)}`);
    if (Number(state.selectedTradeId) !== Number(tradeId)) return;
    renderTradeInspector(detail);
  } catch (error) {
    clearTradeInspector(error.message);
  }
}

function snapshotValueClass(column, value) {
  const upper = String(value ?? "").toUpperCase();
  if (column.kind === "dot" || column.kind === "state") {
    if (["BUY", "UP", "GREEN", "LONG", "BULLISH", "ACCEL"].includes(upper)) return "is-positive";
    if (["SELL", "DOWN", "RED", "SHORT", "BEARISH", "DECEL"].includes(upper)) return "is-negative";
    if (upper === "NEUTRAL") return "is-neutral";
    if (upper.includes("BULL")) return "is-positive";
    if (upper.includes("BEAR")) return "is-negative";
    if (upper.includes("PULLBACK")) return "is-accent";
  }
  return "";
}

function renderSnapshotCell(row, column) {
  const value = column.key === "timestamp_utc" ? row.timestamp_utc : row.values?.[column.key];
  const display = column.key === "timestamp_utc"
    ? (value ? String(value).replace("T", " ").replace("+00:00", " UTC") : "—")
    : fmtSnapshotValue(value, column.digits ?? 2);
  const css = snapshotValueClass(column, value);
  return `<td class="snapshot-cell ${css}" data-snapshot-cell="1" data-timeframe="${row.timeframe}" data-column="${column.key}" title="${escapeHtml(display)}">${escapeHtml(display)}</td>`;
}

function renderSnapshotTimeframeCell(row) {
  const timeframe = String(row?.timeframe || "").trim();
  const emaDefaults = currentEmaSettings(state.builderPreset, null);
  const emaPair = currentEmaSettings(state.builderPreset, timeframe);
  const emaOverride = hasPresetEmaOverride(state.builderPreset, timeframe);
  const rsiDefaults = currentRsiSettings(state.builderPreset, null);
  const rsiConfig = currentRsiSettings(state.builderPreset, timeframe);
  const rsiOverride = hasPresetRsiOverride(state.builderPreset, timeframe);
  const notes = [
    emaOverride
      ? `Custom EMA pair for ${timeframe}. Set it back to ${formatEmaPair(emaDefaults)} to remove the override.`
      : `Using the preset default EMA pair ${formatEmaPair(emaDefaults)}.`,
    rsiOverride
      ? `Custom RSI config for ${timeframe}. Set it back to ${formatRsiConfig(rsiDefaults)} to remove the override.`
      : `Using the preset default RSI config ${formatRsiConfig(rsiDefaults)}.`,
  ];
  return `
    <td class="snapshot-timeframe snapshot-timeframe-cell" title="${escapeHtml(notes.join(" "))}">
      <div class="snapshot-timeframe-main">${escapeHtml(timeframe)}</div>
      <div class="snapshot-timeframe-meta">
        <span class="snapshot-timeframe-pair">EMA ${escapeHtml(formatEmaPair(emaPair))}</span>
        <span class="snapshot-timeframe-badge ${emaOverride ? "is-override" : "is-default"}">${emaOverride ? "EMA TF" : "EMA Default"}</span>
        <span class="snapshot-timeframe-pair">RSI ${escapeHtml(formatRsiConfig(rsiConfig))}</span>
        <span class="snapshot-timeframe-badge ${rsiOverride ? "is-override" : "is-default"}">${rsiOverride ? "RSI TF" : "RSI Default"}</span>
      </div>
      <div class="snapshot-ema-inline">
        <label>
          <span>Fast</span>
          <input type="number" min="1" step="1" data-snapshot-ema-timeframe="${escapeHtml(timeframe)}" data-snapshot-ema-key="ema_1_length" value="${escapeHtml(emaPair.ema_1_length)}" />
        </label>
        <label>
          <span>Slow</span>
          <input type="number" min="1" step="1" data-snapshot-ema-timeframe="${escapeHtml(timeframe)}" data-snapshot-ema-key="ema_2_length" value="${escapeHtml(emaPair.ema_2_length)}" />
        </label>
      </div>
      <div class="snapshot-rsi-inline">
        <label>
          <span>RSI Len</span>
          <input type="number" min="1" step="1" data-snapshot-rsi-timeframe="${escapeHtml(timeframe)}" data-snapshot-rsi-key="length" value="${escapeHtml(rsiConfig.length)}" />
        </label>
        <label>
          <span>Smooth</span>
          <input type="number" min="1" step="1" data-snapshot-rsi-timeframe="${escapeHtml(timeframe)}" data-snapshot-rsi-key="smoothing" value="${escapeHtml(rsiConfig.smoothing)}" />
        </label>
      </div>
    </td>
  `;
}

function renderSnapshotPanel() {
  const panel = $("snapshot-panel");
  if (!panel) {
    syncBuilderHeader();
    return;
  }
  const preset = state.builderPreset;
  if (!preset) {
    panel.innerHTML = `<div class="empty-state">Load a preset to inspect current values and build rules from them.</div>`;
    syncBuilderHeader();
    return;
  }

  ensureBuilderTarget();
  const targetTab = state.builderTarget?.tabName || TAB_NAMES[0];
  const groups = groupOptionsForTab(targetTab);
  const groupOptions = groups.length ? groups : [{ value: 0, label: "Group 1" }];
  const emaSettings = currentEmaSettings(preset, null);
  const rsiSettings = currentRsiSettings(preset, null);
  const emaOverrides = emaOverrideCount(preset);
  const rsiOverrides = rsiOverrideCount(preset);
  const snapshot = state.snapshot;
  const rows = snapshotRows();
  const summaryBits = [];
  if (snapshot?.snapshot_source) summaryBits.push(snapshot.snapshot_source);
  if (snapshot?.snapshot_source_note) summaryBits.push(snapshot.snapshot_source_note);
  if (snapshot?.snapshot_as_of_utc) summaryBits.push(`as of ${String(snapshot.snapshot_as_of_utc).replace("T", " ").replace("+00:00", " UTC")}`);
  summaryBits.push(`Default EMA ${formatEmaPair(emaSettings)}`);
  summaryBits.push(`Default RSI ${formatRsiConfig(rsiSettings)}`);
  if (emaOverrides) summaryBits.push(`${emaOverrides} EMA TF override${emaOverrides === 1 ? "" : "s"}`);
  if (rsiOverrides) summaryBits.push(`${rsiOverrides} RSI TF override${rsiOverrides === 1 ? "" : "s"}`);

  panel.innerHTML = `
    <section class="builder-card">
      <div class="builder-head">
        <div>
          <h4>Quick Rule Insert</h4>
          <div class="muted">Right-click a value for rule suggestions. Double-click for the default action.</div>
        </div>
        <div class="pill-row">
          <span class="pill">Target: ${escapeHtml(activeTargetLabel())}</span>
          ${summaryBits.length ? `<span class="pill">${escapeHtml(summaryBits.join(" · "))}</span>` : ""}
        </div>
      </div>
      <div class="builder-grid">
        <label>
          <span>Insert Into Tab</span>
          <select id="snapshot-target-tab">${renderSelect(TAB_NAMES, targetTab)}</select>
        </label>
        <label>
          <span>Insert Into Group</span>
          <select id="snapshot-target-group">${groupOptions.map((option) => `<option value="${option.value}" ${Number(option.value) === Number(state.builderTarget.groupIndex) ? "selected" : ""}>${option.label}</option>`).join("")}</select>
        </label>
        <label>
          <span>Default Fast EMA Length</span>
          <input type="number" min="1" step="1" id="snapshot-ema-1-length" data-snapshot-setting="ema_1_length" value="${emaSettings.ema_1_length || 9}" />
        </label>
        <label>
          <span>Default Slow EMA Length</span>
          <input type="number" min="1" step="1" id="snapshot-ema-2-length" data-snapshot-setting="ema_2_length" value="${emaSettings.ema_2_length || 21}" />
        </label>
        <label>
          <span>Default RSI Length</span>
          <input type="number" min="1" step="1" id="snapshot-rsi-length" data-snapshot-rsi-setting="length" value="${rsiSettings.length || 14}" />
        </label>
        <label>
          <span>Default RSI Smoothing</span>
          <input type="number" min="1" step="1" id="snapshot-rsi-smoothing" data-snapshot-rsi-setting="smoothing" value="${rsiSettings.smoothing || 3}" />
        </label>
      </div>
      <div class="builder-group-target">Default EMA and RSI settings cascade into every timeframe. Use the inputs inside a TF row to create or remove row-specific overrides.</div>
    </section>
    ${rows.length ? `
      <div class="table-wrap snapshot-table-wrap">
        <table class="data-table snapshot-table">
          <thead>
            <tr>
              <th>TF</th>
              ${SNAPSHOT_COLUMNS.map((column) => `<th>${snapshotColumnLabel(column.key)}</th>`).join("")}
            </tr>
          </thead>
          <tbody>
            ${rows.map((row) => `
              <tr>
                ${renderSnapshotTimeframeCell(row)}
                ${SNAPSHOT_COLUMNS.map((column) => renderSnapshotCell(row, column)).join("")}
              </tr>
            `).join("")}
          </tbody>
        </table>
      </div>
    ` : `<div class="empty-state">${state.snapshotLoading ? "Loading snapshot..." : "No snapshot rows are available yet."}</div>`}
  `;
  syncBuilderHeader();
}

function builderSnapshotPayload() {
  if (!state.builderPreset) return null;
  const name = builderPresetName();
  return {
    name,
    preset: normalizePreset(clone(state.builderPreset), name),
  };
}

async function loadSnapshotForSelectedPreset() {
  const useBuilderPreview = activeBuilderView() && Boolean(state.builderPreset);
  if (!useBuilderPreview && !state.selectedPreset?.name) {
    state.snapshot = null;
    renderSnapshotPanel();
    return;
  }
  state.snapshotLoading = true;
  setSnapshotStatus(useBuilderPreview ? "Loading builder preview values..." : "Loading current values...");
  renderSnapshotPanel();
  try {
    const snapshot = useBuilderPreview
      ? await api("/api/v2/presets/preview-snapshot", {
        method: "POST",
        body: JSON.stringify(builderSnapshotPayload()),
      })
      : await api(`/api/v2/presets/${encodeURIComponent(state.selectedPreset.name)}/snapshot`);
    state.snapshot = snapshot;
    const sourceBits = [snapshot.snapshot_source, snapshot.snapshot_source_note].filter(Boolean);
    const prefix = useBuilderPreview ? "Builder values ready" : "Values ready";
    setSnapshotStatus(sourceBits.length ? `${prefix} (${sourceBits.join(", ")}).` : `${prefix}.`);
  } catch (error) {
    state.snapshot = null;
    setSnapshotStatus(error.message, true);
  } finally {
    state.snapshotLoading = false;
    renderSnapshotPanel();
  }
}

function renderSelect(options, value, labelFn = null) {
  return options.map((option) => {
    const optionValue = option && typeof option === "object" ? option.value : option;
    const optionLabel = option && typeof option === "object"
      ? (option.label ?? (labelFn ? labelFn(optionValue) : optionValue))
      : (labelFn ? labelFn(optionValue) : optionValue);
    return `<option value="${escapeHtml(optionValue)}" ${String(optionValue) === String(value) ? "selected" : ""}>${escapeHtml(optionLabel)}</option>`;
  }).join("");
}

function renderTimeframeChecks(timeframes) {
  return TIMEFRAME_OPTIONS.map((tf) => `
    <label class="toggle toggle-inline">
      <input type="checkbox" data-builder-type="timeframe" data-timeframe="${tf}" ${timeframes?.[tf] ? "checked" : ""} />
      <span>${tf}</span>
    </label>`).join("");
}

function renderRuleEditor(tabName, groupIndex, ruleIndex, rule, groupJoin = "OR", totalRules = 0, groupRules = []) {
  const schema = getRuleUiSchema(rule, groupJoin);
  const normalizedRule = schema.normalized;
  const isSequence = String(groupJoin || "OR").toUpperCase() === "SEQUENCE";
  const sequenceMeta = isSequence ? sequenceRuleDisplayMeta(groupRules, ruleIndex) : null;
  const advancedOpen = isRuleAdvancedOpen(tabName, groupIndex, ruleIndex, normalizedRule);
  const barsAgoMode = normalizedRule.bars_ago_mode || "OFF";
  const barsAgoActive = String(barsAgoMode).toUpperCase() !== "OFF";
  const barsAgoBadge = barsAgoActive ? `${barsAgoMode} ${normalizedRule.bars_ago_n || 0}` : "Off";
  const takerWindowEnabled = TAKER_BIAS_WINDOW_FIELDS.has(String(normalizedRule.field || ""));
  const takerWindowBars = Math.max(1, Number(normalizedRule.window_bars || 1));
  const controls = [];
  controls.push(renderRuleControl(
    "Timeframe",
    `<select data-builder-type="rule-prop" data-prop="timeframe" data-tab="${tabName}" data-group-index="${groupIndex}" data-rule-index="${ruleIndex}" title="${escapeHtml(`Choose the timeframe used for this rule.`)}">${renderSelect(TIMEFRAME_OPTIONS, normalizedRule.timeframe)}</select>`,
    "builder-rule-cell-timeframe",
    "Choose the timeframe used for this rule."
  ));
  if (schema.showMode) {
    controls.push(renderRuleControl(
      "Mode",
      `<select data-builder-type="rule-prop" data-prop="mode" data-tab="${tabName}" data-group-index="${groupIndex}" data-rule-index="${ruleIndex}" title="${escapeHtml(ruleHelpText(normalizedRule, groupJoin, "mode"))}">${renderSelect(schema.modeOptions, normalizedRule.mode)}</select>`,
      "builder-rule-cell-mode",
      ruleHelpText(normalizedRule, groupJoin, "mode")
    ));
  }
  controls.push(renderRuleControl(
    "Field",
    `<select data-builder-type="rule-prop" data-prop="field" data-tab="${tabName}" data-group-index="${groupIndex}" data-rule-index="${ruleIndex}" title="${escapeHtml(ruleHelpText(normalizedRule, groupJoin, "field"))}">${renderSelect(FIELD_OPTIONS, normalizedRule.field, (optionValue) => fieldLabel(optionValue, normalizedRule.timeframe))}</select>`,
    "builder-rule-cell-field",
    ruleHelpText(normalizedRule, groupJoin, "field")
  ));
  if (schema.showOperator) {
    controls.push(renderRuleControl(
      "Operator",
      `<select data-builder-type="rule-prop" data-prop="op" data-tab="${tabName}" data-group-index="${groupIndex}" data-rule-index="${ruleIndex}" title="${escapeHtml(ruleHelpText(normalizedRule, groupJoin, "operator"))}">${renderSelect(schema.operatorOptions, normalizedRule.op)}</select>`,
      "builder-rule-cell-operator",
      ruleHelpText(normalizedRule, groupJoin, "operator")
    ));
  }
  if (schema.showValue) {
    let controlHtml = "";
    if (schema.valueSpec.kind === "select") {
      controlHtml = `<select data-builder-type="rule-prop" data-prop="value" data-tab="${tabName}" data-group-index="${groupIndex}" data-rule-index="${ruleIndex}" title="${escapeHtml(ruleHelpText(normalizedRule, groupJoin, "value"))}">${renderSelect(schema.valueSpec.options || [], normalizedRule.value)}</select>`;
    } else if (schema.valueSpec.kind === "number") {
      const ruleValue = normalizedRule.value === null || normalizedRule.value === undefined ? "" : normalizedRule.value;
      controlHtml = `<input type="number" step="any" data-builder-type="rule-prop" data-prop="value" data-tab="${tabName}" data-group-index="${groupIndex}" data-rule-index="${ruleIndex}" value="${ruleValue}" placeholder="${escapeHtml(schema.valueSpec.placeholder || "Numeric value")}" title="${escapeHtml(ruleHelpText(normalizedRule, groupJoin, "value"))}" />`;
    } else {
      const ruleValue = normalizedRule.value === null || normalizedRule.value === undefined ? "" : normalizedRule.value;
      controlHtml = `<input data-builder-type="rule-prop" data-prop="value" data-tab="${tabName}" data-group-index="${groupIndex}" data-rule-index="${ruleIndex}" value="${ruleValue}" placeholder="${escapeHtml(schema.valueSpec.placeholder || "Value")}" title="${escapeHtml(ruleHelpText(normalizedRule, groupJoin, "value"))}" />`;
    }
    controls.push(renderRuleControl(
      schema.valueSpec.label || "Value",
      controlHtml,
      "builder-rule-cell-value",
      ruleHelpText(normalizedRule, groupJoin, "value")
    ));
  }
  if (takerWindowEnabled) {
    controls.push(renderRuleControl(
      "Closed Window",
      `<input type="number" min="1" max="10" step="1" data-builder-type="rule-prop" data-prop="window_bars" data-tab="${tabName}" data-group-index="${groupIndex}" data-rule-index="${ruleIndex}" value="${takerWindowBars}" title="${escapeHtml(ruleHelpText(normalizedRule, groupJoin, "window-bars"))}" />`,
      "builder-rule-cell-value",
      ruleHelpText(normalizedRule, groupJoin, "window-bars")
    ));
  }
  return `
    <div class="builder-rule" data-tab="${tabName}" data-group-index="${groupIndex}" data-rule-index="${ruleIndex}">
      <div class="builder-row spread">
        <div>
          <strong>Rule ${ruleIndex + 1}</strong>
          <div class="builder-rule-summary">${escapeHtml(ruleSummary(normalizedRule))}</div>
          ${isSequence && sequenceMeta?.kind === "step" ? `<div class="builder-sequence-note">Sequence step ${sequenceMeta.stepNumber} of ${sequenceMeta.totalSteps || totalRules || ruleIndex + 1}</div>` : ""}
          ${isSequence && sequenceMeta?.kind === "gate" ? `<div class="builder-sequence-note">Final gate: checked only when the last real sequence step completes.</div>` : ""}
          ${isSequence && sequenceMeta?.kind === "canceler" ? `<div class="builder-sequence-note">Sequence canceler: if this rule turns true while the setup is in progress, the whole sequence resets.</div>` : ""}
        </div>
        <div class="builder-row">
          ${isSequence ? `
            <button class="mini-button" type="button" data-builder-action="move-rule-up" data-tab="${tabName}" data-group-index="${groupIndex}" data-rule-index="${ruleIndex}" ${ruleIndex === 0 ? "disabled" : ""}>Up</button>
            <button class="mini-button" type="button" data-builder-action="move-rule-down" data-tab="${tabName}" data-group-index="${groupIndex}" data-rule-index="${ruleIndex}" ${ruleIndex >= totalRules - 1 ? "disabled" : ""}>Down</button>
          ` : ""}
          <button class="mini-button ${barsAgoActive ? "is-active" : ""}" type="button" data-builder-action="toggle-rule-advanced" data-tab="${tabName}" data-group-index="${groupIndex}" data-rule-index="${ruleIndex}" title="${escapeHtml(ruleHelpText(normalizedRule, groupJoin, "bars-ago"))}">Bars Ago: ${escapeHtml(barsAgoBadge)}</button>
          <button class="mini-button" type="button" data-builder-action="duplicate-rule" data-tab="${tabName}" data-group-index="${groupIndex}" data-rule-index="${ruleIndex}">Duplicate</button>
          <button class="mini-button danger" type="button" data-builder-action="delete-rule" data-tab="${tabName}" data-group-index="${groupIndex}" data-rule-index="${ruleIndex}">Delete Rule</button>
        </div>
      </div>
      <div class="builder-grid builder-rule-grid">
        ${controls.join("")}
      </div>
      <div class="builder-row">
        <label class="toggle toggle-inline" title="${escapeHtml(ruleHelpText(normalizedRule, groupJoin, "trigger"))}"><input type="checkbox" data-builder-type="rule-bool" data-prop="is_trigger" data-tab="${tabName}" data-group-index="${groupIndex}" data-rule-index="${ruleIndex}" ${normalizedRule.is_trigger ? "checked" : ""} ${schema.triggerEnabled ? "" : "disabled"} /><span>Trigger</span></label>
        <label class="toggle toggle-inline" title="${escapeHtml(ruleHelpText(normalizedRule, groupJoin, "final-gate"))}"><input type="checkbox" data-builder-type="rule-bool" data-prop="is_final_gate" data-tab="${tabName}" data-group-index="${groupIndex}" data-rule-index="${ruleIndex}" ${normalizedRule.is_final_gate ? "checked" : ""} ${schema.finalGateEnabled ? "" : "disabled"} /><span>Final Gate</span></label>
        <label class="toggle toggle-inline" title="${escapeHtml(ruleHelpText(normalizedRule, groupJoin, "sequence-canceler"))}"><input type="checkbox" data-builder-type="rule-bool" data-prop="is_sequence_canceler" data-tab="${tabName}" data-group-index="${groupIndex}" data-rule-index="${ruleIndex}" ${normalizedRule.is_sequence_canceler ? "checked" : ""} ${schema.sequenceCancelerEnabled ? "" : "disabled"} /><span>Sequence Canceler</span></label>
        <label class="toggle toggle-inline" title="${escapeHtml(ruleHelpText(normalizedRule, groupJoin, "still-valid"))}"><input type="checkbox" data-builder-type="rule-bool" data-prop="require_still_valid" data-tab="${tabName}" data-group-index="${groupIndex}" data-rule-index="${ruleIndex}" ${normalizedRule.require_still_valid ? "checked" : ""} ${schema.stillValidEnabled ? "" : "disabled"} /><span>Require Still Valid</span></label>
      </div>
      ${advancedOpen ? `
        <div class="builder-advanced-panel">
          <div class="builder-advanced-title">Bars Ago</div>
          <div class="builder-grid builder-grid-compact">
            <label title="${escapeHtml(ruleHelpText(normalizedRule, groupJoin, "bars-ago"))}"><span>Mode</span><select data-builder-type="rule-prop" data-prop="bars_ago_mode" data-tab="${tabName}" data-group-index="${groupIndex}" data-rule-index="${ruleIndex}">${renderSelect(["OFF", "EXACT", "WITHIN"], barsAgoMode)}</select></label>
            <label title="${escapeHtml(ruleHelpText(normalizedRule, groupJoin, "bars-ago"))}"><span>N</span><input type="number" min="0" step="1" data-builder-type="rule-prop" data-prop="bars_ago_n" data-tab="${tabName}" data-group-index="${groupIndex}" data-rule-index="${ruleIndex}" value="${normalizedRule.bars_ago_n || 0}" /></label>
          </div>
        </div>
      ` : ""}
    </div>`;
}

function renderGroupEditor(tabName, group, groupIndex) {
  const isTarget = state.builderTarget?.tabName === tabName && Number(state.builderTarget?.groupIndex) === Number(groupIndex);
  const rulesJoin = group.rules_join || "OR";
  const isSequence = String(rulesJoin).toUpperCase() === "SEQUENCE";
  const rules = Array.isArray(group.rules) ? group.rules : [];
  const isEntryTab = tabName === "Long Entry" || tabName === "Short Entry";
  const groupFilterActive = isEntryTab ? entryFilterIsActive(effectiveEntryFilterConfig(tabName, groupIndex)) : false;
  return `
    <div class="builder-group ${isTarget ? "is-target" : ""}" data-tab="${tabName}" data-group-index="${groupIndex}">
      <div class="builder-row spread">
        <div>
          <strong>${escapeHtml(groupLabel(group, groupIndex))}</strong>
          <div class="builder-group-target">${isTarget ? "Current insert target" : "Click this group to make it the insert target"}</div>
          ${isSequence ? `<div class="builder-sequence-note">Sequence mode: rules run in order from top to bottom.</div>` : ""}
        </div>
        <div class="builder-row">
          ${isTarget ? `<span class="pill builder-active-pill">Active Group</span>` : ""}
          ${isEntryTab ? `<button class="mini-button ${groupFilterActive ? "is-active" : ""}" type="button" data-builder-action="edit-group-entry-filter" data-tab="${tabName}" data-group-index="${groupIndex}">${escapeHtml(groupEntryFilterButtonLabel(tabName, groupIndex))}</button>` : ""}
          <button class="mini-button danger" type="button" data-builder-action="delete-group" data-tab="${tabName}" data-group-index="${groupIndex}">Delete Group</button>
        </div>
      </div>
      <div class="builder-grid">
        <label><span>Rules Join</span><select data-builder-type="group-join" data-tab="${tabName}" data-group-index="${groupIndex}">${renderSelect(RULE_JOIN_OPTIONS, rulesJoin)}</select></label>
      </div>
      <div class="builder-rule-list">
        ${rules.map((rule, ruleIndex) => renderRuleEditor(tabName, groupIndex, ruleIndex, rule, rulesJoin, rules.length, rules)).join("") || `<div class="empty-state">No rules in this group yet. Right click a sticky indicator chip above to add one.</div>`}
      </div>
    </div>`;
}

function renderTabEditor(tabName, tab) {
  const logic = ensureTabAdvancedLogic(tabName) || { enabled: false, routes: [] };
  const isEntryTab = tabName === "Long Entry" || tabName === "Short Entry";
  const defaultFilterActive = isEntryTab ? entryFilterIsActive(effectiveEntryFilterConfig(tabName)) : false;
  return `
    <section class="builder-card" data-tab="${tabName}">
      <div class="builder-head">
        <div>
          <h4>${tabName}</h4>
          <div class="muted">Add empty groups here, then build rules from the sticky verification panel. Click a group card anywhere to target it.</div>
        </div>
        <div class="builder-row">
          <span class="pill">${logic.enabled ? `Advanced Logic On · ${logic.routes.length} route${logic.routes.length === 1 ? "" : "s"}` : "Advanced Logic Off"}</span>
          ${isEntryTab ? `<button class="mini-button ${defaultFilterActive ? "is-active" : ""}" type="button" data-builder-action="edit-tab-entry-filter" data-tab="${tabName}">${escapeHtml(entryFilterButtonLabel(tabName))}</button>` : ""}
          <button class="mini-button" type="button" data-builder-action="open-group-logic" data-tab="${tabName}">Advanced Logic</button>
          <button class="mini-button" type="button" data-builder-action="add-group" data-tab="${tabName}">Add Group</button>
        </div>
      </div>
      <div class="builder-grid">
        <label><span>Groups Join</span><select data-builder-type="tab-join" data-tab="${tabName}">${renderSelect(GROUP_JOIN_OPTIONS, tab.groups_join || "AND")}</select></label>
      </div>
      <div class="builder-group-list">
        ${(tab.groups || []).map((group, groupIndex) => renderGroupEditor(tabName, group, groupIndex)).join("") || `<div class="empty-state">No groups in this tab yet.</div>`}
      </div>
    </section>`;
}

function renderBuilder() {
  const preset = state.builderPreset;
  if (!preset) {
    $("preset-builder").innerHTML = `<div class="empty-state">Builder is empty. Use Preset Tray -> Load Preset when you want to edit one here.</div>`;
    return;
  }

  syncPresetTimeframes(preset);
  const settings = preset.settings || {};
  const activeTab = activeBuilderTabName();
  const emaSettings = currentEmaSettings(preset, null);
  $("preset-builder").innerHTML = `
    <section class="builder-card">
      <div class="builder-head">
        <div>
          <h4>Strategy Sections</h4>
          <div class="muted">Choose which tab you want to edit. The selected tab fills the canvas.</div>
        </div>
        <span class="pill">Active Section: ${escapeHtml(activeTab)}</span>
      </div>
      <div class="builder-tab-row">
        ${TAB_NAMES.map((tabName) => {
          const count = ((preset.tabs?.[tabName]?.groups || []).flatMap((group) => group.rules || [])).length;
          return `<button class="builder-tab-button ${tabName === activeTab ? "is-active" : ""}" type="button" data-builder-action="set-active-tab" data-tab="${tabName}">${tabName} (${count})</button>`;
        }).join("")}
      </div>
    </section>
    <section class="builder-card">
      <div class="builder-head">
        <div>
          <h4>Settings</h4>
          <div class="muted">Core backtest window and auto-detected timeframe scope.</div>
        </div>
      </div>
      <div class="builder-grid">
        <label><span>Symbol</span><input data-builder-type="setting" data-setting="symbol" value="${settings.symbol || ""}" /></label>
        <label><span>Start UTC</span><input type="datetime-local" step="60" data-builder-type="setting" data-setting="start" value="${toUtcDateTimeLocalValue(settings.start)}" /></label>
        <label><span>End UTC</span><input type="datetime-local" step="60" data-builder-type="setting" data-setting="end" value="${toUtcDateTimeLocalValue(settings.end)}" /></label>
        <label><span>Warmup Minutes</span><input type="number" min="0" step="1" data-builder-type="setting" data-setting="warmup_min" value="${settings.warmup_min || 0}" /></label>
        <label><span>Backtest Mode</span><select data-builder-type="setting" data-setting="bt_mode">${renderSelect(BACKTEST_MODE_OPTIONS, settings.bt_mode || BACKTEST_MODE_OPTIONS[0])}</select></label>
        <label><span>Default Fast EMA Length</span><input type="number" min="1" step="1" data-builder-type="ema-setting" data-ema-setting="ema_1_length" value="${emaSettings.ema_1_length || 9}" /></label>
        <label><span>Default Slow EMA Length</span><input type="number" min="1" step="1" data-builder-type="ema-setting" data-ema-setting="ema_2_length" value="${emaSettings.ema_2_length || 21}" /></label>
        <label><span>Strategy Note</span><input data-builder-type="meta" data-meta="strategy_note" value="${preset.meta?.strategy_note || ""}" /></label>
      </div>
      ${renderDetectedTimeframeScope(preset)}
    </section>
    ${renderTabEditor(activeTab, preset.tabs?.[activeTab] || { groups_join: "AND", groups: [] })}
  `;
}

function updateBuilderRule(tabName, groupIndex, ruleIndex, prop, value) {
  const rule = state.builderPreset?.tabs?.[tabName]?.groups?.[groupIndex]?.rules?.[ruleIndex];
  if (!rule) return;
  rule[prop] = value;
  normalizeRuleConfig(rule);
}

function builderPresetName() {
  return currentBuilderPresetIdentity() || "preset";
}

async function savePresetPayload(name, preset, setLast) {
  const normalized = normalizePreset(preset, name);
  await api("/api/v2/presets", {
    method: "POST",
    body: JSON.stringify({ name, preset: normalized, set_last: setLast }),
  });
}

async function reloadPresetsAndSelect(name, options = {}) {
  const reloadBuilder = options.reloadBuilder !== false;
  const presetMeta = await api("/api/v2/presets");
  state.presets = presetMeta.items || [];
  renderPresetList(state.presets);
  syncBacktestForm();
  if (name) {
    await selectPreset(name);
    if (state.selectedPreset && reloadBuilder) {
      loadPresetIntoBuilder(state.selectedPreset.name, state.selectedPreset.preset, `Loaded preset ${state.selectedPreset.name} into the builder.`);
    }
  }
}

function handleBuilderClick(event) {
  if (!state.builderPreset) return;
  const actionNode = event.target.closest("[data-builder-action]");
  if (!actionNode) {
    if (event.target.closest("input, select, textarea, button, summary")) return;
    const groupNode = event.target.closest(".builder-group");
    if (groupNode) {
      setBuilderTarget(groupNode.dataset.tab, Number(groupNode.dataset.groupIndex));
      setBuilderStatus(`Insert target set to ${groupNode.dataset.tab} / Group ${Number(groupNode.dataset.groupIndex) + 1}.`);
    }
    return;
  }
  const action = actionNode.dataset.builderAction;
  const tabName = actionNode.dataset.tab;
  const groupIndex = Number(actionNode.dataset.groupIndex);
  if (action === "set-active-tab") {
    state.builderTarget = { tabName, groupIndex: 0 };
    ensureBuilderTarget();
    renderBuilder();
    renderSnapshotPanel();
    syncJsonEditorFromBuilder();
    syncBuilderHeader();
    setBuilderStatus(`Editing section ${tabName}.`);
    focusTargetGroup();
    return;
  }
  const tab = state.builderPreset.tabs?.[tabName];
  if (!tab) return;
  let mutated = false;

  if (action === "open-group-logic") {
    openGroupLogicModal(tabName);
    return;
  }
  if (action === "edit-tab-entry-filter") {
    openEntryFilterModal(tabName);
    return;
  }
  if (action === "edit-group-entry-filter") {
    openEntryFilterModal(tabName, Number(actionNode.dataset.groupIndex || 0));
    return;
  }

  if (action === "add-group") {
    tab.groups.push(createEmptyGroup(tab.groups));
    mutated = true;
  } else if (action === "delete-group") {
    tab.groups.splice(groupIndex, 1);
    mutated = true;
  } else if (action === "toggle-rule-advanced") {
    const ruleIndex = Number(actionNode.dataset.ruleIndex);
    const key = ruleUiKey(tabName, groupIndex, ruleIndex);
    state.ruleUi[key] = !isRuleAdvancedOpen(tabName, groupIndex, ruleIndex);
  } else if (action === "move-rule-up" || action === "move-rule-down") {
    const ruleIndex = Number(actionNode.dataset.ruleIndex);
    const rules = tab.groups[groupIndex]?.rules || [];
    const swapIndex = action === "move-rule-up" ? ruleIndex - 1 : ruleIndex + 1;
    if (swapIndex >= 0 && swapIndex < rules.length) {
      [rules[ruleIndex], rules[swapIndex]] = [rules[swapIndex], rules[ruleIndex]];
      mutated = true;
    }
  } else if (action === "duplicate-rule") {
    const ruleIndex = Number(actionNode.dataset.ruleIndex);
    duplicateRuleAt(tabName, groupIndex, ruleIndex);
    mutated = true;
  } else if (action === "delete-rule") {
    const ruleIndex = Number(actionNode.dataset.ruleIndex);
    tab.groups[groupIndex]?.rules.splice(ruleIndex, 1);
    mutated = true;
  }

  normalizeAdvancedLogic(tab);
  if (mutated) {
    state.builderPresetDirty = true;
  }
  ensureBuilderTarget();
  renderBuilder();
  renderSnapshotPanel();
  syncJsonEditorFromBuilder();
  setBuilderStatus("Visual builder updated.");
  if (mutated) {
    requestBuilderLiveRefresh();
  }
}

function handleBuilderChange(event) {
  if (!state.builderPreset) return;
  const kind = event.target.dataset.builderType;
  if (!kind) return;
  let statusMessage = "Visual builder updated.";

  if (kind === "setting") {
    const key = event.target.dataset.setting;
    if (event.target.type === "number") {
      state.builderPreset.settings[key] = Number(event.target.value || 0);
    } else if (event.target.type === "datetime-local" && (key === "start" || key === "end")) {
      state.builderPreset.settings[key] = fromUtcDateTimeLocalValue(event.target.value);
    } else {
      state.builderPreset.settings[key] = event.target.value;
    }
  } else if (kind === "meta") {
    const key = event.target.dataset.meta;
    state.builderPreset.meta[key] = event.target.value;
  } else if (kind === "ema-setting") {
    const key = event.target.dataset.emaSetting;
    const nextPair = currentEmaSettings(state.builderPreset, null);
    nextPair[key] = Math.max(1, Number(event.target.value || 1));
    const updated = setPresetEmaSettings(state.builderPreset, "default", nextPair);
    statusMessage = `Default EMA pair updated to ${formatEmaPair(updated)}.`;
  } else if (kind === "timeframe") {
    state.builderPreset.settings.timeframes[event.target.dataset.timeframe] = event.target.checked;
  } else if (kind === "tab-join") {
    state.builderPreset.tabs[event.target.dataset.tab].groups_join = event.target.value;
  } else if (kind === "group-join") {
    const groupIndex = Number(event.target.dataset.groupIndex);
    state.builderPreset.tabs[event.target.dataset.tab].groups[groupIndex].rules_join = event.target.value;
  } else if (kind === "rule-prop") {
    const groupIndex = Number(event.target.dataset.groupIndex);
    const ruleIndex = Number(event.target.dataset.ruleIndex);
    const prop = event.target.dataset.prop;
    const value = prop === "bars_ago_n" || prop === "window_bars" ? Number(event.target.value || (prop === "window_bars" ? 1 : 0)) : prop === "value" ? parsePrimitive(event.target.value) : event.target.value;
    updateBuilderRule(event.target.dataset.tab, groupIndex, ruleIndex, prop, value);
  } else if (kind === "rule-bool") {
    const groupIndex = Number(event.target.dataset.groupIndex);
    const ruleIndex = Number(event.target.dataset.ruleIndex);
    updateBuilderRule(event.target.dataset.tab, groupIndex, ruleIndex, event.target.dataset.prop, event.target.checked);
  }

  state.builderPresetDirty = true;
  ensureBuilderTarget();
  renderBuilder();
  renderSnapshotPanel();
  syncJsonEditorFromBuilder();
  setBuilderStatus(statusMessage);
  requestBuilderLiveRefresh();
}

async function handleBuilderSave() {
  if (!state.builderPreset) {
    setBuilderStatus("No visual preset loaded.", true);
    return;
  }
  const name = builderPresetName();
  try {
    await savePresetPayload(name, state.builderPreset, $("editor-set-last").checked);
    state.builderPresetDirty = false;
    setBuilderStatus(`Saved preset ${name}.`);
    setEditorStatus(`Saved preset ${name}.`);
    await reloadPresetsAndSelect(name, { reloadBuilder: false });
    requestBuilderLiveRefresh();
  } catch (error) {
    setBuilderStatus(error.message, true);
  }
}

async function handleBuilderSaveAs() {
  if (!state.builderPreset) {
    setBuilderStatus("No visual preset loaded.", true);
    return;
  }
  const currentName = builderPresetName();
  const nextNameRaw = window.prompt("Save preset as:", currentName ? `${currentName}_copy` : "preset_copy");
  const nextName = String(nextNameRaw || "").trim();
  if (!nextName) return;
  try {
    await savePresetPayload(nextName, state.builderPreset, $("editor-set-last").checked);
    setBuilderStatus(`Saved preset as ${nextName}.`);
    setEditorStatus(`Saved preset as ${nextName}.`);
    await reloadPresetsAndSelect(nextName);
  } catch (error) {
    setBuilderStatus(error.message, true);
  }
}

function handleBuilderReset() {
  if (!state.selectedPreset) {
    setBuilderStatus("No preset selected.", true);
    return;
  }
  loadPresetIntoBuilder(state.selectedPreset.name, state.selectedPreset.preset, `Reloaded preset ${state.selectedPreset.name}.`);
  focusTargetGroup();
}

async function handleBuilderPresetLoad() {
  const name = $("builder-preset-select").value;
  if (!name) {
    setBuilderStatus("Choose a preset first.", true);
    return;
  }
  await selectPreset(name);
  if (state.selectedPreset) {
    loadPresetIntoBuilder(state.selectedPreset.name, state.selectedPreset.preset, `Loaded preset ${state.selectedPreset.name} into the builder.`);
    closeBuilderPresetTray();
  }
  setView("builder");
  focusTargetGroup();
}

function handleOpenBuilder() {
  if (!state.builderPreset && state.selectedPreset) {
    loadPresetIntoBuilder(state.selectedPreset.name, state.selectedPreset.preset, `Loaded preset ${state.selectedPreset.name} into the builder.`);
  }
  setView("builder");
  if (state.builderPreset) {
    focusTargetGroup();
  } else {
    setBuilderStatus("No preset is available yet.", true);
  }
}

function makeRule(timeframe, mode, field, op, value, extra = {}) {
  return {
    ...defaultRule(),
    timeframe,
    mode,
    field,
    op,
    value,
    ...extra,
  };
}

function promptAndAddNumericRule(timeframe, field, label, defaultOp, defaultValue) {
  const raw = window.prompt(`${label} rule\nExample: >= 25`, `${defaultOp} ${defaultValue}`);
  if (!raw) return;
  const trimmed = raw.trim();
  const match = trimmed.match(/^(<=|>=|!=|=|<|>)\s*(.+)$/);
  if (!match) {
    setBuilderStatus(`Could not parse numeric rule: ${trimmed}`, true);
    return;
  }
  const value = Number(match[2]);
  if (!Number.isFinite(value)) {
    setBuilderStatus(`Numeric value required for ${label}.`, true);
    return;
  }
  insertRuleIntoTarget(makeRule(timeframe, "state", field, match[1], value), `${label} rule added.`);
}

function promptAndAddStateRule(timeframe, field, label, defaultOp, defaultValue) {
  const raw = window.prompt(`${label} rule\nExample: = BUY`, `${defaultOp} ${defaultValue}`);
  if (!raw) return;
  const trimmed = raw.trim();
  const match = trimmed.match(/^(<=|>=|!=|=|<|>)\s*(.+)$/);
  if (!match) {
    setBuilderStatus(`Could not parse state rule: ${trimmed}`, true);
    return;
  }
  insertRuleIntoTarget(makeRule(timeframe, "state", field, match[1], match[2]), `${label} rule added.`);
}

function groupsForTab(tabName) {
  return state.builderPreset?.tabs?.[tabName]?.groups || [];
}

function groupReferencesForTab(tabName) {
  return groupsForTab(tabName).map((group, index) => ({
    index,
    key: String(group?.group_key || alphaGroupKey(index)).trim() || alphaGroupKey(index),
    label: groupLabel(group, index),
  }));
}

function logicExpressionTokens(expression) {
  return Array.from(new Set((String(expression || "").toUpperCase().match(/\b[A-Z][A-Z0-9]*\b/g) || [])));
}

function logicConditionId() {
  return `logic-cond-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 7)}`;
}

function parseLogicExpressionToConditions(expression, refs) {
  const validKeys = new Set((refs || []).map((ref) => String(ref?.key || "").trim()).filter(Boolean));
  const tokens = String(expression || "").toUpperCase().match(/\b[A-Z][A-Z0-9]*\b/g) || [];
  const conditions = [];
  let nextJoin = "AND";
  for (let i = 0; i < tokens.length; i += 1) {
    const token = tokens[i];
    if (token === "AND" || token === "OR") {
      nextJoin = token;
      continue;
    }
    let expected = true;
    let key = token;
    if (token === "NOT") {
      const maybeKey = tokens[i + 1];
      if (!maybeKey) continue;
      expected = false;
      key = maybeKey;
      i += 1;
    }
    if (!validKeys.has(key)) continue;
    conditions.push({
      condition_id: logicConditionId(),
      join: conditions.length ? nextJoin : "AND",
      group_key: key,
      expected,
    });
    nextJoin = "AND";
  }
  return conditions;
}

function logicConditionsToExpression(conditions) {
  const parts = [];
  (Array.isArray(conditions) ? conditions : []).forEach((condition, index) => {
    const key = String(condition?.group_key || "").trim().toUpperCase();
    if (!key) return;
    if (index > 0) parts.push(String(condition?.join || "AND").toUpperCase() === "OR" ? "OR" : "AND");
    parts.push(condition?.expected === false ? `NOT ${key}` : key);
  });
  return parts.join(" ").trim();
}

function normalizeLogicConditions(route, refs) {
  const validKeys = new Set((refs || []).map((ref) => String(ref?.key || "").trim()).filter(Boolean));
  const fallbackKey = Array.from(validKeys)[0] || "";
  const rawConditions = Array.isArray(route?.conditions) ? route.conditions : [];
  let conditions = rawConditions
    .map((condition, index) => {
      const key = String(condition?.group_key || "").trim().toUpperCase();
      return {
        condition_id: String(condition?.condition_id || logicConditionId()),
        join: index === 0 ? "AND" : (String(condition?.join || "AND").toUpperCase() === "OR" ? "OR" : "AND"),
        group_key: validKeys.has(key) ? key : fallbackKey,
        expected: condition?.expected !== false,
      };
    })
    .filter((condition) => condition.group_key);
  if (!conditions.length) {
    conditions = parseLogicExpressionToConditions(route?.expression, refs);
  }
  if (!conditions.length && fallbackKey) {
    conditions = [{
      condition_id: logicConditionId(),
      join: "AND",
      group_key: fallbackKey,
      expected: true,
    }];
  }
  return conditions;
}

function syncLogicRouteExpression(route) {
  route.expression = logicConditionsToExpression(route?.conditions || []);
}

function defaultLogicRoute(tabName) {
  const refs = groupReferencesForTab(tabName);
  const firstKey = refs[0]?.key || alphaGroupKey(0);
  return {
    route_id: logicRouteId(),
    expression: firstKey,
    conditions: firstKey ? [{
      condition_id: logicConditionId(),
      join: "AND",
      group_key: firstKey,
      expected: true,
    }] : [],
    target_group_key: firstKey,
    exclusive: false,
    note: "",
  };
}

function ensureTabAdvancedLogic(tabName) {
  const tab = state.builderPreset?.tabs?.[tabName];
  if (!tab) return null;
  return normalizeAdvancedLogic(tab);
}

function logicRouteSummary(route) {
  const expression = String(route?.expression || "").trim() || "—";
  const target = String(route?.target_group_key || "").trim() || "—";
  return `${expression} → ${target}${route?.exclusive ? " (exclusive)" : ""}`;
}

function renderGroupLogicRoute(tabName, route, routeIndex, refs) {
  const validKeys = new Set(refs.map((ref) => ref.key));
  const missing = logicExpressionTokens(route.expression).filter((token) => !validKeys.has(token) && !["AND", "OR", "NOT"].includes(token));
  const previewText = route.expression?.trim()
    ? `If ${route.expression.trim()} is TRUE, then use ${route.target_group_key || "—"}${route.exclusive ? " and switch off the previous active branch" : ""}.`
    : "Build a TRUE/FALSE condition, then choose which group should become active.";
  return `
    <div class="logic-route-card" data-route-id="${escapeHtml(route.route_id)}">
      <div class="builder-row spread">
        <div>
          <strong>Route ${routeIndex + 1}</strong>
          <div class="builder-rule-summary">${escapeHtml(previewText)}</div>
        </div>
        <button class="mini-button danger" type="button" data-logic-action="delete-route" data-route-id="${escapeHtml(route.route_id)}">Delete Route</button>
      </div>
      <div class="logic-token-row">
        ${refs.map((ref) => `<button class="logic-token-chip" type="button" data-logic-action="insert-token" data-route-id="${escapeHtml(route.route_id)}" data-token="${escapeHtml(ref.key)}">${escapeHtml(ref.key)}</button>`).join("")}
        ${refs.map((ref) => `<button class="logic-token-chip subtle" type="button" data-logic-action="insert-token" data-route-id="${escapeHtml(route.route_id)}" data-token="${escapeHtml(`NOT ${ref.key}`)}">NOT ${escapeHtml(ref.key)}</button>`).join("")}
        ${["AND", "OR", "(", ")", "NOT"].map((token) => `<button class="logic-token-chip subtle" type="button" data-logic-action="insert-token" data-route-id="${escapeHtml(route.route_id)}" data-token="${escapeHtml(token)}">${escapeHtml(token)}</button>`).join("")}
      </div>
      <div class="builder-grid logic-route-grid">
        <label>
          <span>1. When This Becomes TRUE</span>
          <input data-logic-prop="expression" data-route-id="${escapeHtml(route.route_id)}" value="${escapeHtml(route.expression)}" placeholder="Example: A AND NOT B" />
        </label>
        <label>
          <span>2. Then Use Group</span>
          <select data-logic-prop="target_group_key" data-route-id="${escapeHtml(route.route_id)}">
            ${refs.length ? refs.map((ref) => `<option value="${escapeHtml(ref.key)}" ${ref.key === route.target_group_key ? "selected" : ""}>${escapeHtml(ref.label)}</option>`).join("") : `<option value="">No groups yet</option>`}
          </select>
        </label>
      </div>
      <div class="builder-row">
        <label class="toggle toggle-inline">
          <input type="checkbox" data-logic-prop="exclusive" data-route-id="${escapeHtml(route.route_id)}" ${route.exclusive ? "checked" : ""} />
          <span>3. Switch Off Previous Active Branch</span>
        </label>
      </div>
      <div class="logic-route-status ${missing.length ? "is-warning" : ""}">
        ${missing.length ? `Unknown group symbols in expression: ${escapeHtml(missing.join(", "))}` : "Each group letter means whether that group is currently TRUE or FALSE. To require FALSE, use NOT before the group, for example: A AND NOT B."}
      </div>
    </div>
  `;
}

function renderGroupLogicModal(tabName) {
  const refs = groupReferencesForTab(tabName);
  const logic = ensureTabAdvancedLogic(tabName) || { enabled: false, routes: [] };
  return `
    <div class="logic-modal-shell">
      <div class="logic-modal-intro">
        <p class="muted">Each group letter is a TRUE/FALSE result. Build a boolean condition here, and when it becomes TRUE, the chosen target group becomes the one to use. If you want a group to be FALSE, write NOT before it, for example: A AND NOT B.</p>
      </div>
      <div class="logic-node-board">
        ${refs.length ? refs.map((ref) => `<div class="logic-node"><div class="logic-node-key">${escapeHtml(ref.key)}</div><div class="logic-node-label">${escapeHtml(ref.label)}</div><div class="logic-node-bool">TRUE output: ${escapeHtml(ref.key)} | FALSE output: NOT ${escapeHtml(ref.key)}</div></div>`).join("") : `<div class="empty-state">Add at least one group in ${escapeHtml(tabName)} to build advanced routing.</div>`}
      </div>
      <div class="builder-row logic-modal-actions">
        <label class="toggle toggle-inline">
          <input type="checkbox" data-logic-global="enabled" ${logic.enabled ? "checked" : ""} />
          <span>Use these TRUE/FALSE routes in this section</span>
        </label>
        <button class="mini-button" type="button" data-logic-action="add-route" ${refs.length ? "" : "disabled"}>Add Logic Route</button>
      </div>
      <div class="logic-route-list">
        ${logic.routes.length ? logic.routes.map((route, routeIndex) => renderGroupLogicRoute(tabName, route, routeIndex, refs)).join("") : `<div class="empty-state">No advanced routes yet. Add one when you want a TRUE/FALSE route from group results.</div>`}
      </div>
    </div>
  `;
}

function logicRoutePreviewFromConditions(route) {
  const conditions = Array.isArray(route?.conditions) ? route.conditions : [];
  if (!conditions.length) return "Choose group results, set them to TRUE or FALSE, then choose the target group.";
  const expression = conditions.map((condition, index) => {
    const fragment = `${condition?.group_key || "-"} is ${condition?.expected === false ? "FALSE" : "TRUE"}`;
    return index === 0 ? fragment : `${condition?.join || "AND"} ${fragment}`;
  }).join(" ");
  return `If ${expression}, then use ${route?.target_group_key || "-"}${route?.exclusive ? " and switch off the previous active branch" : ""}.`;
}

function renderLogicConditionRow(route, condition, conditionIndex, refs) {
  const conditionId = escapeHtml(condition?.condition_id || "");
  const routeId = escapeHtml(route?.route_id || "");
  return `
    <div class="logic-condition-row">
      ${conditionIndex > 0 ? `
        <label>
          <span>Join</span>
          <select data-logic-condition-prop="join" data-route-id="${routeId}" data-condition-id="${conditionId}">
            <option value="AND" ${String(condition?.join || "AND").toUpperCase() === "AND" ? "selected" : ""}>AND</option>
            <option value="OR" ${String(condition?.join || "AND").toUpperCase() === "OR" ? "selected" : ""}>OR</option>
          </select>
        </label>
      ` : `<div class="logic-condition-anchor">IF</div>`}
      <label>
        <span>Group</span>
        <select data-logic-condition-prop="group_key" data-route-id="${routeId}" data-condition-id="${conditionId}">
          ${refs.map((ref) => `<option value="${escapeHtml(ref.key)}" ${ref.key === condition?.group_key ? "selected" : ""}>${escapeHtml(ref.label)}</option>`).join("")}
        </select>
      </label>
      <div class="logic-truth-pick">
        <span>Must Be</span>
        <div class="logic-truth-buttons">
          <button class="logic-truth-button ${condition?.expected !== false ? "is-active" : ""}" type="button" data-logic-action="set-condition-expected" data-route-id="${routeId}" data-condition-id="${conditionId}" data-expected="true">TRUE</button>
          <button class="logic-truth-button ${condition?.expected === false ? "is-active" : ""}" type="button" data-logic-action="set-condition-expected" data-route-id="${routeId}" data-condition-id="${conditionId}" data-expected="false">FALSE</button>
        </div>
      </div>
      <div class="logic-condition-actions">
        <button class="mini-button" type="button" data-logic-action="delete-condition" data-route-id="${routeId}" data-condition-id="${conditionId}" ${route?.conditions?.length > 1 ? "" : "disabled"}>Delete</button>
      </div>
    </div>
  `;
}

function logicRouteSummary(route) {
  const conditions = Array.isArray(route?.conditions) ? route.conditions : [];
  const expression = conditions.length
    ? conditions.map((condition, index) => {
      const fragment = `${condition?.group_key || "-"} is ${condition?.expected === false ? "FALSE" : "TRUE"}`;
      return index === 0 ? fragment : `${condition?.join || "AND"} ${fragment}`;
    }).join(" ")
    : (String(route?.expression || "").trim() || "-");
  const target = String(route?.target_group_key || "").trim() || "-";
  return `${expression} -> ${target}${route?.exclusive ? " (exclusive)" : ""}`;
}

function renderGroupLogicRoute(tabName, route, routeIndex, refs) {
  const conditions = Array.isArray(route?.conditions) ? route.conditions : [];
  const previewText = logicRoutePreviewFromConditions(route);
  return `
    <div class="logic-route-card" data-route-id="${escapeHtml(route.route_id)}">
      <div class="builder-row spread">
        <div>
          <strong>Route ${routeIndex + 1}</strong>
          <div class="builder-rule-summary">${escapeHtml(previewText)}</div>
        </div>
        <button class="mini-button danger" type="button" data-logic-action="delete-route" data-route-id="${escapeHtml(route.route_id)}">Delete Route</button>
      </div>
      <div class="logic-condition-list">
        ${conditions.map((condition, conditionIndex) => renderLogicConditionRow(route, condition, conditionIndex, refs)).join("")}
      </div>
      <div class="builder-row">
        <button class="mini-button" type="button" data-logic-action="add-condition" data-route-id="${escapeHtml(route.route_id)}" ${refs.length ? "" : "disabled"}>Add Group Condition</button>
      </div>
      <div class="builder-grid logic-route-grid">
        <label>
          <span>Then Use Group</span>
          <select data-logic-prop="target_group_key" data-route-id="${escapeHtml(route.route_id)}">
            ${refs.length ? refs.map((ref) => `<option value="${escapeHtml(ref.key)}" ${ref.key === route.target_group_key ? "selected" : ""}>${escapeHtml(ref.label)}</option>`).join("") : `<option value="">No groups yet</option>`}
          </select>
        </label>
      </div>
      <div class="builder-row">
        <label class="toggle toggle-inline">
          <input type="checkbox" data-logic-prop="exclusive" data-route-id="${escapeHtml(route.route_id)}" ${route.exclusive ? "checked" : ""} />
          <span>Switch Off Previous Active Branch</span>
        </label>
      </div>
      <div class="logic-route-status">
        Pick a group, choose whether it must be TRUE or FALSE, then combine rows with AND / OR.
      </div>
    </div>
  `;
}

function renderGroupLogicModal(tabName) {
  const refs = groupReferencesForTab(tabName);
  const logic = ensureTabAdvancedLogic(tabName) || { enabled: false, routes: [] };
  return `
    <div class="logic-modal-shell">
      <div class="logic-modal-intro">
        <p class="muted">Choose group results directly. Each row says whether a group must be TRUE or FALSE, then AND / OR combines those rows. If the whole route becomes TRUE, the chosen target group becomes the one to use.</p>
      </div>
      <div class="logic-node-board">
        ${refs.length ? refs.map((ref) => `<div class="logic-node"><div class="logic-node-key">${escapeHtml(ref.key)}</div><div class="logic-node-label">${escapeHtml(ref.label)}</div><div class="logic-node-bool">Pick ${escapeHtml(ref.key)} as TRUE or FALSE in a route row.</div></div>`).join("") : `<div class="empty-state">Add at least one group in ${escapeHtml(tabName)} to build advanced routing.</div>`}
      </div>
      <div class="builder-row logic-modal-actions">
        <label class="toggle toggle-inline">
          <input type="checkbox" data-logic-global="enabled" ${logic.enabled ? "checked" : ""} />
          <span>Use these TRUE/FALSE routes in this section</span>
        </label>
        <button class="mini-button" type="button" data-logic-action="add-route" ${refs.length ? "" : "disabled"}>Add Logic Route</button>
      </div>
      <div class="logic-route-list">
        ${logic.routes.length ? logic.routes.map((route, routeIndex) => renderGroupLogicRoute(tabName, route, routeIndex, refs)).join("") : `<div class="empty-state">No advanced routes yet. Add one when you want a TRUE/FALSE route from group results.</div>`}
      </div>
    </div>
  `;
}

function openGroupLogicModal(tabName) {
  if (!state.builderPreset?.tabs?.[tabName]) return;
  state.groupLogicModalTab = tabName;
  showDetailModal(`Advanced Group Logic · ${tabName}`, renderGroupLogicModal(tabName), { mode: "group-logic", wide: true });
}

function rerenderGroupLogicModal() {
  if (!state.groupLogicModalTab) return;
  showDetailModal(`Advanced Group Logic · ${state.groupLogicModalTab}`, renderGroupLogicModal(state.groupLogicModalTab), { mode: "group-logic", wide: true });
}

function insertLogicToken(route, token) {
  const normalized = String(token || "").trim();
  if (!normalized) return;
  const current = String(route.expression || "").trim();
  route.expression = current ? `${current} ${normalized}` : normalized;
}

function updateLogicRoute(tabName, routeId, prop, value) {
  const logic = ensureTabAdvancedLogic(tabName);
  const route = logic?.routes?.find((item) => String(item.route_id) === String(routeId));
  if (!route) return;
  route[prop] = value;
}

function showDetailModal(title, html, options = {}) {
  $("detail-modal-title").textContent = title;
  $("detail-modal-body").innerHTML = html;
  const backdrop = $("detail-modal-backdrop");
  const shell = document.querySelector("#detail-modal-backdrop .detail-modal");
  backdrop.dataset.mode = String(options.mode || "");
  if (shell) shell.classList.toggle("is-wide", !!options.wide);
  backdrop.hidden = false;
}

function hideDetailModal() {
  const backdrop = $("detail-modal-backdrop");
  const shell = document.querySelector("#detail-modal-backdrop .detail-modal");
  backdrop.hidden = true;
  backdrop.dataset.mode = "";
  if (shell) shell.classList.remove("is-wide");
  state.groupLogicModalTab = null;
  state.entryFilterModalTab = null;
  state.entryFilterModalGroupIndex = null;
}

function detailRows(entries) {
  return `
    <div class="detail-grid">
      ${entries.map(([label, value]) => `
        <div class="detail-item">
          <div class="eyebrow">${escapeHtml(label)}</div>
          <strong>${escapeHtml(value === null || value === undefined || value === "" ? "—" : value)}</strong>
        </div>
      `).join("")}
    </div>
  `;
}

function showEmaDetails(row) {
  const values = row.values || {};
  const ema = emaDisplayConfig(null, row.timeframe);
  const price = Number(values.price);
  const ema1 = Number(values.ema_1);
  const ema2 = Number(values.ema_2);
  const priceVsFast = Number.isFinite(price) && Number.isFinite(ema1) ? price - ema1 : null;
  const priceVsSlow = Number.isFinite(price) && Number.isFinite(ema2) ? price - ema2 : null;
  showDetailModal(`EMA Pair Details · ${row.timeframe}`, detailRows([
    ["Fast EMA Length", ema.fastLength],
    ["Slow EMA Length", ema.slowLength],
    [fieldLabel("ema_1", row.timeframe), fmtSnapshotValue(values.ema_1, 3)],
    [fieldLabel("ema_2", row.timeframe), fmtSnapshotValue(values.ema_2, 3)],
    [fieldLabel("ema_stack", row.timeframe), values.ema_stack],
    [fieldLabel("ema_1_angle_state", row.timeframe), values.ema_1_angle_state],
    [fieldLabel("ema_2_angle_state", row.timeframe), values.ema_2_angle_state],
    [fieldLabel("ema_1_slope", row.timeframe), fmtSnapshotValue(values.ema_1_slope, 6)],
    [fieldLabel("ema_2_slope", row.timeframe), fmtSnapshotValue(values.ema_2_slope, 6)],
    [fieldLabel("ema_spread", row.timeframe), fmtSnapshotValue(values.ema_spread, 6)],
    [fieldLabel("ema_spread_pct", row.timeframe), fmtSnapshotValue(values.ema_spread_pct, 3)],
    [fieldLabel("ema_spread_delta", row.timeframe), fmtSnapshotValue(values.ema_spread_delta, 6)],
    [fieldLabel("ema_spread_pct_delta", row.timeframe), fmtSnapshotValue(values.ema_spread_pct_delta, 3)],
    [fieldLabel("ema_spread_trend", row.timeframe), values.ema_spread_trend],
    [fieldLabel("ema_stack_pressure", row.timeframe), values.ema_stack_pressure],
    [fieldLabel("ema_cross_up", row.timeframe), boolish(values.ema_cross_up) ? "YES" : "no"],
    [fieldLabel("ema_cross_down", row.timeframe), boolish(values.ema_cross_down) ? "YES" : "no"],
    ["Price", fmtSnapshotValue(values.price, 3)],
    ["Price vs Fast EMA", fmtSnapshotValue(priceVsFast, 3)],
    ["Price vs Slow EMA", fmtSnapshotValue(priceVsSlow, 3)],
  ]));
}

function showStochRsiDetails(row) {
  const values = row.values || {};
  let k = values.stoch_rsi_k;
  k = k === null || k === undefined ? null : Number(k);
  if (!Number.isFinite(k)) k = null;
  let d = values.stoch_rsi_d;
  d = d === null || d === undefined ? null : Number(d);
  if (!Number.isFinite(d)) d = null;
  const gap = k !== null && d !== null ? k - d : null;
  showDetailModal(`Stoch RSI Details · ${row.timeframe}`, detailRows([
    ["Stoch K", fmtSnapshotValue(values.stoch_rsi_k, 3)],
    ["Stoch D", fmtSnapshotValue(values.stoch_rsi_d, 3)],
    ["K/D", values.stoch_rsi_kd || "-"],
    ["K-D Gap", fmtSnapshotValue(gap, 3)],
    ["Overbought", k !== null && k >= 80 ? "YES" : "no"],
    ["Oversold", k !== null && k <= 20 ? "YES" : "no"],
  ]));
}

function showRsiDetails(row) {
  const values = row.values || {};
  let rsi = values.rsi;
  rsi = rsi === null || rsi === undefined ? null : Number(rsi);
  if (!Number.isFinite(rsi)) rsi = null;
  let smooth = values.rsi_smooth;
  smooth = smooth === null || smooth === undefined ? null : Number(smooth);
  if (!Number.isFinite(smooth)) smooth = null;
  const gap = rsi !== null && smooth !== null ? rsi - smooth : null;
  showDetailModal(`RSI Details · ${row.timeframe}`, detailRows([
    ["RSI", fmtSnapshotValue(values.rsi, 3)],
    ["RSI Smooth", fmtSnapshotValue(values.rsi_smooth, 3)],
    ["State", values.rsi_state || "-"],
    ["RSI-Smooth Gap", fmtSnapshotValue(gap, 3)],
    ["Overbought", rsi !== null && rsi >= 70 ? "YES" : "no"],
    ["Oversold", rsi !== null && rsi <= 30 ? "YES" : "no"],
  ]));
}

function showVidyaDetails(row) {
  const values = row.values || {};
  showDetailModal(`VIDYA Details · ${row.timeframe}`, detailRows([
    ["State", values.vidya_state],
    ["Line", fmtSnapshotValue(values.vidya_line, 3)],
    ["Δ%", fmtSnapshotValue(values.vidya_delta_pct, 3)],
    ["Slope", fmtSnapshotValue(values.vidya_slope, 6)],
    ["Angle State", values.vidya_angle_state],
    ["Angle Norm", fmtSnapshotValue(values.vidya_angle_deg_norm, 3)],
    ["Angle Accel", values.vidya_angle_accel],
    ["Trend Up Event", boolish(values.vidya_trend_up) ? "YES" : "no"],
    ["Trend Down Event", boolish(values.vidya_trend_down) ? "YES" : "no"],
    ["Active Liq Lows", fmtSnapshotValue(values.vidya_active_liq_low_count, 0)],
    ["Active Liq Highs", fmtSnapshotValue(values.vidya_active_liq_high_count, 0)],
    ["Nearest Liq Low", fmtSnapshotValue(values.vidya_nearest_liq_low, 3)],
    ["Nearest Liq High", fmtSnapshotValue(values.vidya_nearest_liq_high, 3)],
    ["Dist to Liq Low %", fmtSnapshotValue(values.vidya_dist_to_liq_low_pct, 3)],
    ["Dist to Liq High %", fmtSnapshotValue(values.vidya_dist_to_liq_high_pct, 3)],
    ["New Liq Low", boolish(values.vidya_new_liq_low) ? "YES" : "no"],
    ["New Liq High", boolish(values.vidya_new_liq_high) ? "YES" : "no"],
    ["Liq Low Taken", boolish(values.vidya_liq_low_taken) ? "YES" : "no"],
    ["Liq High Taken", boolish(values.vidya_liq_high_taken) ? "YES" : "no"],
  ]));
}

function showRangeDetails(row) {
  const values = row.values || {};
  showDetailModal(`Range Details · ${row.timeframe}`, detailRows([
    ["Display State", values.range_filter_display_state || values.range_filter_state],
    ["Display Age", fmtSnapshotValue(values.range_filter_display_age, 0)],
    ["State", values.range_filter_state],
    ["State Age", fmtSnapshotValue(values.range_filter_state_age, 0)],
    ["Phase", values.range_filter_phase],
    ["Line", fmtSnapshotValue(values.range_filter_line, 3)],
    ["Smooth Range", fmtSnapshotValue(values.range_filter_smooth_range, 3)],
    ["Upward Count", fmtSnapshotValue(values.range_filter_upward_count, 0)],
    ["Downward Count", fmtSnapshotValue(values.range_filter_downward_count, 0)],
    ["Range Buy Event", boolish(values.range_filter_buy) ? "YES" : "no"],
    ["Range Sell Event", boolish(values.range_filter_sell) ? "YES" : "no"],
    ["Range Buy Age", fmtSnapshotValue(values.range_filter_buy_age, 0)],
    ["Range Sell Age", fmtSnapshotValue(values.range_filter_sell_age, 0)],
  ]));
}

function showMarketDetails(row) {
  const values = row.values || {};
  showDetailModal(`Market Details · ${row.timeframe}`, detailRows([
    ["Market", values.market_state],
    ["Bias", values.market_bias],
    ["Phase", values.market_phase],
    ["Confidence", fmtSnapshotValue(values.market_confidence, 0)],
    ["Age", fmtSnapshotValue(values.market_state_age, 0)],
    ["VIDYA", values.vidya_state],
    ["Range", values.range_filter_state],
  ]));
}

function hideContextMenu() {
  const menu = $("context-menu");
  menu.classList.remove("is-open");
  menu.innerHTML = "";
}

function showContextMenu(items, x, y) {
  const menu = $("context-menu");
  menu.innerHTML = items.map((item) => {
    if (item.type === "separator") return `<div class="context-separator"></div>`;
    if (item.disabled) return `<div class="context-item is-disabled">${escapeHtml(item.label)}</div>`;
    return `<button class="context-item" type="button" data-context-action="${escapeHtml(item.id)}">${escapeHtml(item.label)}</button>`;
  }).join("");
  menu.classList.add("is-open");
  const width = Math.min(360, Math.max(280, menu.offsetWidth || 320));
  const height = Math.min(window.innerHeight - 24, Math.max(120, menu.offsetHeight || 240));
  const left = Math.min(x, window.innerWidth - width - 12);
  const top = Math.min(y, window.innerHeight - height - 12);
  menu.style.left = `${Math.max(12, left)}px`;
  menu.style.top = `${Math.max(12, top)}px`;
  const map = new Map(items.filter((item) => item.type !== "separator" && !item.disabled).map((item) => [item.id, item.onClick]));
  menu.querySelectorAll("[data-context-action]").forEach((node) => {
    node.addEventListener("click", () => {
      hideContextMenu();
      const handler = map.get(node.dataset.contextAction);
      if (handler) handler();
    });
  });
}

function handleGlobalContextMenuScroll(event) {
  const menu = $("context-menu");
  if (!menu?.classList.contains("is-open")) return;
  const target = event.target;
  if (target === menu || target?.closest?.("#context-menu")) return;
  hideContextMenu();
}

function handleDetailModalClick(event) {
  const backdrop = $("detail-modal-backdrop");
  if (backdrop?.dataset.mode !== "group-logic") return;
  const actionNode = event.target.closest("[data-logic-action]");
  if (!actionNode || !state.groupLogicModalTab) return;
  const tabName = state.groupLogicModalTab;
  const tab = state.builderPreset?.tabs?.[tabName];
  const logic = ensureTabAdvancedLogic(tabName);
  if (!tab || !logic) return;
  const routeId = actionNode.dataset.routeId;
  if (actionNode.dataset.logicAction === "add-route") {
    logic.routes.push(defaultLogicRoute(tabName));
  } else if (actionNode.dataset.logicAction === "delete-route") {
    logic.routes = logic.routes.filter((route) => String(route.route_id) !== String(routeId));
  } else if (actionNode.dataset.logicAction === "insert-token") {
    const route = logic.routes.find((item) => String(item.route_id) === String(routeId));
    if (route) insertLogicToken(route, actionNode.dataset.token);
  }
  normalizeAdvancedLogic(tab);
  rerenderGroupLogicModal();
  renderBuilder();
  syncJsonEditorFromBuilder();
  setBuilderStatus("Advanced group logic updated.");
}

function handleDetailModalInput(event) {
  const backdrop = $("detail-modal-backdrop");
  if (backdrop?.dataset.mode !== "group-logic") return;
  if (!state.groupLogicModalTab) return;
  const prop = event.target.dataset.logicProp;
  const routeId = event.target.dataset.routeId;
  if (!prop || !routeId) return;
  const value = event.target.type === "checkbox" ? event.target.checked : event.target.value;
  updateLogicRoute(state.groupLogicModalTab, routeId, prop, value);
}

function handleDetailModalChange(event) {
  const backdrop = $("detail-modal-backdrop");
  if (backdrop?.dataset.mode !== "group-logic") return;
  if (!state.groupLogicModalTab) return;
  const tabName = state.groupLogicModalTab;
  const tab = state.builderPreset?.tabs?.[tabName];
  const logic = ensureTabAdvancedLogic(tabName);
  if (!tab || !logic) return;

  if (event.target.dataset.logicGlobal === "enabled") {
    logic.enabled = event.target.checked;
  }

  const prop = event.target.dataset.logicProp;
  const routeId = event.target.dataset.routeId;
  if (prop && routeId) {
    const value = event.target.type === "checkbox" ? event.target.checked : event.target.value;
    updateLogicRoute(tabName, routeId, prop, value);
  }

  normalizeAdvancedLogic(tab);
  rerenderGroupLogicModal();
  renderBuilder();
  syncJsonEditorFromBuilder();
  setBuilderStatus("Advanced group logic updated.");
}

function addLogicConditionToRoute(route, tabName) {
  const refs = groupReferencesForTab(tabName);
  const fallbackKey = refs[0]?.key || alphaGroupKey(0);
  route.conditions = Array.isArray(route?.conditions) ? route.conditions : [];
  route.conditions.push({
    condition_id: logicConditionId(),
    join: route.conditions.length ? "AND" : "AND",
    group_key: fallbackKey,
    expected: true,
  });
  syncLogicRouteExpression(route);
}

function updateLogicCondition(tabName, routeId, conditionId, prop, value) {
  const logic = ensureTabAdvancedLogic(tabName);
  const route = logic?.routes?.find((item) => String(item.route_id) === String(routeId));
  const condition = route?.conditions?.find((item) => String(item.condition_id) === String(conditionId));
  if (!route || !condition) return;
  if (prop === "join") {
    condition.join = String(value || "AND").toUpperCase() === "OR" ? "OR" : "AND";
  } else if (prop === "group_key") {
    condition.group_key = String(value || "").trim().toUpperCase();
  } else if (prop === "expected") {
    condition.expected = value === true || String(value).toLowerCase() === "true";
  }
  syncLogicRouteExpression(route);
}

function deleteLogicCondition(tabName, routeId, conditionId) {
  const logic = ensureTabAdvancedLogic(tabName);
  const route = logic?.routes?.find((item) => String(item.route_id) === String(routeId));
  if (!route) return;
  route.conditions = (Array.isArray(route.conditions) ? route.conditions : []).filter((item) => String(item.condition_id) !== String(conditionId));
  if (!route.conditions.length) addLogicConditionToRoute(route, tabName);
  route.conditions.forEach((condition, index) => {
    condition.join = index === 0 ? "AND" : (String(condition?.join || "AND").toUpperCase() === "OR" ? "OR" : "AND");
  });
  syncLogicRouteExpression(route);
}

function handleDetailModalClick(event) {
  const backdrop = $("detail-modal-backdrop");
  if (backdrop?.dataset.mode !== "group-logic") return;
  const actionNode = event.target.closest("[data-logic-action]");
  if (!actionNode || !state.groupLogicModalTab) return;
  const tabName = state.groupLogicModalTab;
  const tab = state.builderPreset?.tabs?.[tabName];
  const logic = ensureTabAdvancedLogic(tabName);
  if (!tab || !logic) return;
  const routeId = actionNode.dataset.routeId;
  const conditionId = actionNode.dataset.conditionId;
  if (actionNode.dataset.logicAction === "add-route") {
    logic.routes.push(defaultLogicRoute(tabName));
  } else if (actionNode.dataset.logicAction === "delete-route") {
    logic.routes = logic.routes.filter((route) => String(route.route_id) !== String(routeId));
  } else if (actionNode.dataset.logicAction === "add-condition") {
    const route = logic.routes.find((item) => String(item.route_id) === String(routeId));
    if (route) addLogicConditionToRoute(route, tabName);
  } else if (actionNode.dataset.logicAction === "delete-condition") {
    deleteLogicCondition(tabName, routeId, conditionId);
  } else if (actionNode.dataset.logicAction === "set-condition-expected") {
    updateLogicCondition(tabName, routeId, conditionId, "expected", actionNode.dataset.expected);
  }
  normalizeAdvancedLogic(tab);
  rerenderGroupLogicModal();
  renderBuilder();
  syncJsonEditorFromBuilder();
  setBuilderStatus("Advanced group logic updated.");
}

function handleDetailModalInput(event) {
  const backdrop = $("detail-modal-backdrop");
  if (backdrop?.dataset.mode !== "group-logic") return;
  if (!state.groupLogicModalTab) return;
  const conditionProp = event.target.dataset.logicConditionProp;
  const conditionId = event.target.dataset.conditionId;
  const routeId = event.target.dataset.routeId;
  if (conditionProp && routeId && conditionId) {
    updateLogicCondition(state.groupLogicModalTab, routeId, conditionId, conditionProp, event.target.value);
    return;
  }
  const prop = event.target.dataset.logicProp;
  if (!prop || !routeId) return;
  const value = event.target.type === "checkbox" ? event.target.checked : event.target.value;
  updateLogicRoute(state.groupLogicModalTab, routeId, prop, value);
}

function handleDetailModalChange(event) {
  const backdrop = $("detail-modal-backdrop");
  if (backdrop?.dataset.mode !== "group-logic") return;
  if (!state.groupLogicModalTab) return;
  const tabName = state.groupLogicModalTab;
  const tab = state.builderPreset?.tabs?.[tabName];
  const logic = ensureTabAdvancedLogic(tabName);
  if (!tab || !logic) return;

  if (event.target.dataset.logicGlobal === "enabled") {
    logic.enabled = event.target.checked;
  }

  const conditionProp = event.target.dataset.logicConditionProp;
  const conditionId = event.target.dataset.conditionId;
  const routeId = event.target.dataset.routeId;
  if (conditionProp && routeId && conditionId) {
    updateLogicCondition(tabName, routeId, conditionId, conditionProp, event.target.value);
  }

  const prop = event.target.dataset.logicProp;
  if (prop && routeId) {
    const value = event.target.type === "checkbox" ? event.target.checked : event.target.value;
    updateLogicRoute(tabName, routeId, prop, value);
  }

  normalizeAdvancedLogic(tab);
  rerenderGroupLogicModal();
  renderBuilder();
  syncJsonEditorFromBuilder();
  setBuilderStatus("Advanced group logic updated.");
}

function menuId(prefix) {
  return `${prefix}-${Math.random().toString(36).slice(2, 10)}`;
}

function dotMenuItems(timeframe, field, currentValue) {
  return [
    { id: menuId("a"), label: `Add STATE ${fieldLabel(field, timeframe)} = GREEN`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", field, "=", "GREEN")) },
    { id: menuId("b"), label: `Add STATE ${fieldLabel(field, timeframe)} = RED`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", field, "=", "RED")) },
    { type: "separator" },
    { id: menuId("c"), label: `Add EVENT ${fieldLabel(field, timeframe)} TURNED GREEN`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", field, "TURNED", "GREEN")) },
    { id: menuId("d"), label: `Add EVENT ${fieldLabel(field, timeframe)} TURNED RED`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", field, "TURNED", "RED")) },
    { id: menuId("e"), label: `Add EVENT ${fieldLabel(field, timeframe)} FLIPPED`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", field, "FLIPPED", null)) },
    { type: "separator" },
    { id: menuId("f"), label: `Add ★TRIGGER ${fieldLabel(field, timeframe)} TURNED GREEN`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", field, "TURNED", "GREEN", { is_trigger: true })) },
    { id: menuId("g"), label: `Add ★TRIGGER ${fieldLabel(field, timeframe)} TURNED RED`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", field, "TURNED", "RED", { is_trigger: true })) },
    { id: menuId("h"), label: `Add ★TRIGGER ${fieldLabel(field, timeframe)} FLIPPED`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", field, "FLIPPED", null, { is_trigger: true })) },
    { type: "separator" },
    { id: menuId("i"), label: "Add STATE (custom)...", onClick: () => promptAndAddStateRule(timeframe, field, fieldLabel(field, timeframe), "=", currentValue || "GREEN") },
  ];
}

function buildSnapshotMenuItems(row, columnKey) {
  const timeframe = row.timeframe;
  const values = row.values || {};
  if (["ms", "angle", "sig_angle", "ppo", "adx_angle", "atr_angle", "rsi_state", "stoch_rsi_kd"].includes(columnKey)) {
    const items = dotMenuItems(timeframe, columnKey, values[columnKey]);
    if (columnKey === "ms") {
      items.push(
        { type: "separator" },
        { id: menuId("j"), label: "Add EVENT MACD×SIGNAL CROSS UP", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "ms_cross", "CROSS", "UP")) },
        { id: menuId("k"), label: "Add EVENT MACD×SIGNAL CROSS DOWN", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "ms_cross", "CROSS", "DOWN")) },
        { id: menuId("l"), label: "Add ★TRIGGER MACD×SIGNAL CROSS UP", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "ms_cross", "CROSS", "UP", { is_trigger: true })) },
        { id: menuId("m"), label: "Add ★TRIGGER MACD×SIGNAL CROSS DOWN", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "ms_cross", "CROSS", "DOWN", { is_trigger: true })) },
      );
    }
    return items;
  }
  if (columnKey === "rsi" || columnKey === "rsi_smooth") {
    const field = columnKey;
    const pretty = fieldLabel(field, timeframe);
    return [
      { id: menuId("a"), label: "View RSI Details", onClick: () => showRsiDetails(row) },
      { type: "separator" },
      { id: menuId("b"), label: `Add STATE ${pretty} >= 70`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", field, ">=", 70)) },
      { id: menuId("c"), label: `Add STATE ${pretty} <= 30`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", field, "<=", 30)) },
      { id: menuId("d"), label: `Add STATE ${pretty} >= 50`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", field, ">=", 50)) },
      { type: "separator" },
      { id: menuId("e"), label: `Add STATE ${pretty} (custom)...`, onClick: () => promptAndAddNumericRule(timeframe, field, pretty, ">=", fmtSnapshotValue(values[field], 2)) },
      { id: menuId("f"), label: `Add STATE ${fieldLabel("rsi_state", timeframe)} = GREEN`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "rsi_state", "=", "GREEN")) },
      { id: menuId("g"), label: `Add STATE ${fieldLabel("rsi_state", timeframe)} = RED`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "rsi_state", "=", "RED")) },
    ];
  }
  if (["taker_bias_state", "taker_trade_count", "taker_delta_pct", "taker_buy_share_pct", "taker_buy_volume", "taker_sell_volume", "taker_delta_volume"].includes(columnKey)) {
    return [
      { id: menuId("a"), label: "View Aggressive Pressure Details", onClick: () => showTakerBiasDetails(row) },
      { type: "separator" },
      { id: menuId("b"), label: `Add STATE ${fieldLabel("taker_bias_state", timeframe)} = BUY`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "taker_bias_state", "=", "BUY")) },
      { id: menuId("c"), label: `Add STATE ${fieldLabel("taker_bias_state", timeframe)} = SELL`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "taker_bias_state", "=", "SELL")) },
      { id: menuId("d"), label: `Add STATE ${fieldLabel("taker_bias_state", timeframe)} = NEUTRAL`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "taker_bias_state", "=", "NEUTRAL")) },
      { type: "separator" },
      { id: menuId("e"), label: `Add STATE ${fieldLabel("taker_delta_pct", timeframe)} > 0`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "taker_delta_pct", ">", 0)) },
      { id: menuId("f"), label: `Add STATE ${fieldLabel("taker_delta_pct", timeframe)} < 0`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "taker_delta_pct", "<", 0)) },
      { id: menuId("g"), label: `Add STATE ${fieldLabel("taker_delta_pct", timeframe)} (custom)...`, onClick: () => promptAndAddNumericRule(timeframe, "taker_delta_pct", fieldLabel("taker_delta_pct", timeframe), ">", fmtSnapshotValue(values.taker_delta_pct, 2)) },
      { id: menuId("h"), label: `Add STATE ${fieldLabel("taker_buy_share_pct", timeframe)} (custom)...`, onClick: () => promptAndAddNumericRule(timeframe, "taker_buy_share_pct", fieldLabel("taker_buy_share_pct", timeframe), ">=", fmtSnapshotValue(values.taker_buy_share_pct, 2)) },
      { id: menuId("i"), label: `Add STATE ${fieldLabel("taker_trade_count", timeframe)} (custom)...`, onClick: () => promptAndAddNumericRule(timeframe, "taker_trade_count", fieldLabel("taker_trade_count", timeframe), ">=", fmtSnapshotValue(values.taker_trade_count, 0)) },
      { type: "separator" },
      { id: menuId("j"), label: `Add STATE ${fieldLabel("taker_buy_volume", timeframe)} (custom)...`, onClick: () => promptAndAddNumericRule(timeframe, "taker_buy_volume", fieldLabel("taker_buy_volume", timeframe), ">=", fmtSnapshotValue(values.taker_buy_volume, 4)) },
      { id: menuId("k"), label: `Add STATE ${fieldLabel("taker_sell_volume", timeframe)} (custom)...`, onClick: () => promptAndAddNumericRule(timeframe, "taker_sell_volume", fieldLabel("taker_sell_volume", timeframe), ">=", fmtSnapshotValue(values.taker_sell_volume, 4)) },
      { id: menuId("l"), label: `Add STATE ${fieldLabel("taker_delta_volume", timeframe)} > 0`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "taker_delta_volume", ">", 0)) },
      { id: menuId("m"), label: `Add STATE ${fieldLabel("taker_delta_volume", timeframe)} < 0`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "taker_delta_volume", "<", 0)) },
      { id: menuId("n"), label: `Add STATE ${fieldLabel("taker_delta_volume", timeframe)} (custom)...`, onClick: () => promptAndAddNumericRule(timeframe, "taker_delta_volume", fieldLabel("taker_delta_volume", timeframe), ">", fmtSnapshotValue(values.taker_delta_volume, 4)) },
    ];
  }
  if (columnKey === "stoch_rsi_k" || columnKey === "stoch_rsi_d") {
    const field = columnKey;
    const pretty = fieldLabel(field, timeframe);
    return [
      { id: menuId("a"), label: "View Stoch RSI Details", onClick: () => showStochRsiDetails(row) },
      { type: "separator" },
      { id: menuId("b"), label: `Add STATE ${pretty} >= 80`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", field, ">=", 80)) },
      { id: menuId("c"), label: `Add STATE ${pretty} <= 20`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", field, "<=", 20)) },
      { id: menuId("d"), label: `Add STATE ${pretty} >= 50`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", field, ">=", 50)) },
      { type: "separator" },
      { id: menuId("e"), label: `Add STATE ${pretty} (custom)...`, onClick: () => promptAndAddNumericRule(timeframe, field, pretty, ">=", fmtSnapshotValue(values[field], 2)) },
      { id: menuId("f"), label: `Add STATE ${fieldLabel("stoch_rsi_kd", timeframe)} = GREEN`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "stoch_rsi_kd", "=", "GREEN")) },
      { id: menuId("g"), label: `Add STATE ${fieldLabel("stoch_rsi_kd", timeframe)} = RED`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "stoch_rsi_kd", "=", "RED")) },
    ];
  }
  if (["ema_1", "ema_2", "ema_stack", "ema_spread", "ema_spread_delta", "ema_spread_trend", "ema_stack_pressure"].includes(columnKey)) {
    const items = [
      { id: menuId("a"), label: "View EMA Details", onClick: () => showEmaDetails(row) },
    ];
    if (columnKey === "ema_1" || columnKey === "ema_2") {
      const angleField = columnKey === "ema_1" ? "ema_1_angle_state" : "ema_2_angle_state";
      const slopeField = columnKey === "ema_1" ? "ema_1_slope" : "ema_2_slope";
      const pretty = fieldLabel(columnKey, timeframe);
      items.push(
        { type: "separator" },
        { id: menuId("b"), label: `Add STATE ${fieldLabel(angleField, timeframe)} = UP`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", angleField, "=", "UP")) },
        { id: menuId("c"), label: `Add STATE ${fieldLabel(angleField, timeframe)} = DOWN`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", angleField, "=", "DOWN")) },
        { id: menuId("d"), label: `Add STATE ${pretty} (custom)...`, onClick: () => promptAndAddNumericRule(timeframe, columnKey, pretty, ">=", fmtSnapshotValue(values[columnKey], 2)) },
        { id: menuId("e"), label: `Add STATE ${fieldLabel(slopeField, timeframe)} (custom)...`, onClick: () => promptAndAddNumericRule(timeframe, slopeField, fieldLabel(slopeField, timeframe), ">=", fmtSnapshotValue(values[slopeField], 6)) },
      );
      return items;
    }
    items.push(
      { type: "separator" },
      { id: menuId("b"), label: `Add STATE ${fieldLabel("ema_stack", timeframe)} = BUY`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "ema_stack", "=", "BUY")) },
      { id: menuId("c"), label: `Add STATE ${fieldLabel("ema_stack", timeframe)} = SELL`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "ema_stack", "=", "SELL")) },
      { id: menuId("d"), label: `Add STATE ${fieldLabel("ema_stack", timeframe)} = NEUTRAL`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "ema_stack", "=", "NEUTRAL")) },
      { type: "separator" },
      { id: menuId("e"), label: `Add EVENT ${fieldLabel("ema_cross_up", timeframe)}`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "ema_cross_up", "EVENT", null)) },
      { id: menuId("f"), label: `Add EVENT ${fieldLabel("ema_cross_down", timeframe)}`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "ema_cross_down", "EVENT", null)) },
      { id: menuId("g"), label: `Add ★TRIGGER ${fieldLabel("ema_cross_up", timeframe)}`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "ema_cross_up", "EVENT", null, { is_trigger: true })) },
      { id: menuId("h"), label: `Add ★TRIGGER ${fieldLabel("ema_cross_down", timeframe)}`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "ema_cross_down", "EVENT", null, { is_trigger: true })) },
      { type: "separator" },
      { id: menuId("i"), label: `Add STATE ${fieldLabel("ema_1_angle_state", timeframe)} = UP`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "ema_1_angle_state", "=", "UP")) },
      { id: menuId("j"), label: `Add STATE ${fieldLabel("ema_1_angle_state", timeframe)} = DOWN`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "ema_1_angle_state", "=", "DOWN")) },
      { id: menuId("k"), label: `Add STATE ${fieldLabel("ema_2_angle_state", timeframe)} = UP`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "ema_2_angle_state", "=", "UP")) },
      { id: menuId("l"), label: `Add STATE ${fieldLabel("ema_2_angle_state", timeframe)} = DOWN`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "ema_2_angle_state", "=", "DOWN")) },
      { type: "separator" },
      { id: menuId("m"), label: `Add STATE ${fieldLabel("ema_spread", timeframe)} (custom)...`, onClick: () => promptAndAddNumericRule(timeframe, "ema_spread", fieldLabel("ema_spread", timeframe), ">=", fmtSnapshotValue(values.ema_spread, 6)) },
      { id: menuId("n"), label: `Add STATE ${fieldLabel("ema_spread_pct", timeframe)} (custom)...`, onClick: () => promptAndAddNumericRule(timeframe, "ema_spread_pct", fieldLabel("ema_spread_pct", timeframe), ">=", fmtSnapshotValue(values.ema_spread_pct, 3)) },
      { id: menuId("o"), label: `Add STATE ${fieldLabel("ema_spread_delta", timeframe)} > 0`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "ema_spread_delta", ">", 0)) },
      { id: menuId("p"), label: `Add STATE ${fieldLabel("ema_spread_delta", timeframe)} < 0`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "ema_spread_delta", "<", 0)) },
      { id: menuId("q"), label: `Add STATE ${fieldLabel("ema_spread_pct_delta", timeframe)} (custom)...`, onClick: () => promptAndAddNumericRule(timeframe, "ema_spread_pct_delta", fieldLabel("ema_spread_pct_delta", timeframe), ">=", fmtSnapshotValue(values.ema_spread_pct_delta, 3)) },
      { id: menuId("r"), label: `Add STATE ${fieldLabel("ema_spread_trend", timeframe)} = EXPANDING`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "ema_spread_trend", "=", "EXPANDING")) },
      { id: menuId("s"), label: `Add STATE ${fieldLabel("ema_spread_trend", timeframe)} = CONTRACTING`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "ema_spread_trend", "=", "CONTRACTING")) },
      { id: menuId("t"), label: `Add STATE ${fieldLabel("ema_stack_pressure", timeframe)} = BULL_ACCELERATING`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "ema_stack_pressure", "=", "BULL_ACCELERATING")) },
      { id: menuId("u"), label: `Add STATE ${fieldLabel("ema_stack_pressure", timeframe)} = BEAR_ACCELERATING`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "ema_stack_pressure", "=", "BEAR_ACCELERATING")) },
    );
    return items;
  }
  if (columnKey === "frama_state") {
    return [
      { id: menuId("a"), label: "Add STATE FRAMA = BUY", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "frama_state", "=", "BUY")) },
      { id: menuId("b"), label: "Add STATE FRAMA = SELL", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "frama_state", "=", "SELL")) },
      { id: menuId("c"), label: "Add STATE FRAMA = NEUTRAL", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "frama_state", "=", "NEUTRAL")) },
      { type: "separator" },
      { id: menuId("d"), label: "Add EVENT FRAMA BREAK UP", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "frama_break_up", "EVENT", null)) },
      { id: menuId("e"), label: "Add EVENT FRAMA BREAK DOWN", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "frama_break_down", "EVENT", null)) },
      { id: menuId("f"), label: "Add EVENT FRAMA MID CROSS", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "frama_mid_cross", "EVENT", null)) },
      { type: "separator" },
      { id: menuId("g"), label: "Add ★TRIGGER FRAMA BREAK UP", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "frama_break_up", "EVENT", null, { is_trigger: true })) },
      { id: menuId("h"), label: "Add ★TRIGGER FRAMA BREAK DOWN", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "frama_break_down", "EVENT", null, { is_trigger: true })) },
    ];
  }
  if (columnKey === "vidya_state") {
    return [
      { id: menuId("a"), label: "View VIDYA Details", onClick: () => showVidyaDetails(row) },
      { type: "separator" },
      { id: menuId("b"), label: "Add STATE VIDYA = BUY", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "vidya_state", "=", "BUY")) },
      { id: menuId("c"), label: "Add STATE VIDYA = SELL", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "vidya_state", "=", "SELL")) },
      { id: menuId("d"), label: "Add STATE VIDYA Angle State = UP", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "vidya_angle_state", "=", "UP")) },
      { id: menuId("e"), label: "Add STATE VIDYA Angle State = DOWN", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "vidya_angle_state", "=", "DOWN")) },
      { type: "separator" },
      { id: menuId("f"), label: "Add EVENT VIDYA TREND UP", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "vidya_trend_up", "EVENT", null)) },
      { id: menuId("g"), label: "Add EVENT VIDYA TREND DOWN", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "vidya_trend_down", "EVENT", null)) },
      { id: menuId("h"), label: "Add ★TRIGGER VIDYA TREND UP", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "vidya_trend_up", "EVENT", null, { is_trigger: true })) },
      { id: menuId("i"), label: "Add ★TRIGGER VIDYA TREND DOWN", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "vidya_trend_down", "EVENT", null, { is_trigger: true })) },
      { type: "separator" },
      { id: menuId("j"), label: "Add STATE VIDYA Active Liq Low Count >= 1", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "vidya_active_liq_low_count", ">=", 1)) },
      { id: menuId("k"), label: "Add STATE VIDYA Active Liq High Count >= 1", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "vidya_active_liq_high_count", ">=", 1)) },
      { id: menuId("l"), label: "Add STATE VIDYA Dist to Liq Low % <= 1.0", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "vidya_dist_to_liq_low_pct", "<=", 1.0)) },
      { id: menuId("m"), label: "Add STATE VIDYA Dist to Liq High % <= 1.0", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "vidya_dist_to_liq_high_pct", "<=", 1.0)) },
      { type: "separator" },
      { id: menuId("n"), label: "Add STATE VIDYA Δ% (custom)...", onClick: () => promptAndAddNumericRule(timeframe, "vidya_delta_pct", "VIDYA Δ%", ">=", fmtSnapshotValue(values.vidya_delta_pct, 3)) },
      { id: menuId("o"), label: "Add STATE VIDYA Slope (custom)...", onClick: () => promptAndAddNumericRule(timeframe, "vidya_slope", "VIDYA Slope", ">=", fmtSnapshotValue(values.vidya_slope, 6)) },
      { id: menuId("p"), label: "Add STATE VIDYA Angle Norm (custom)...", onClick: () => promptAndAddNumericRule(timeframe, "vidya_angle_deg_norm", "VIDYA Angle Norm", ">=", fmtSnapshotValue(values.vidya_angle_deg_norm, 3)) },
    ];
  }
  if (columnKey === "range_filter_state" || columnKey === "range_filter_phase") {
    return [
      { id: menuId("a"), label: "View Range Details", onClick: () => showRangeDetails(row) },
      { type: "separator" },
      { id: menuId("b"), label: "Add STATE RANGE = BUY", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "range_filter_state", "=", "BUY")) },
      { id: menuId("c"), label: "Add STATE RANGE = SELL", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "range_filter_state", "=", "SELL")) },
      { id: menuId("d"), label: "Add STATE RANGE = NEUTRAL", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "range_filter_state", "=", "NEUTRAL")) },
      { id: menuId("e"), label: "Add STATE RANGE PHASE = BUY_STRONG", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "range_filter_phase", "=", "BUY_STRONG")) },
      { id: menuId("f"), label: "Add STATE RANGE PHASE = BUY_WEAK", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "range_filter_phase", "=", "BUY_WEAK")) },
      { id: menuId("g"), label: "Add STATE RANGE PHASE = SELL_STRONG", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "range_filter_phase", "=", "SELL_STRONG")) },
      { id: menuId("h"), label: "Add STATE RANGE PHASE = SELL_WEAK", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "range_filter_phase", "=", "SELL_WEAK")) },
      { type: "separator" },
      { id: menuId("i"), label: "Add EVENT RANGE BUY", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "range_filter_buy", "EVENT", null)) },
      { id: menuId("j"), label: "Add EVENT RANGE SELL", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "range_filter_sell", "EVENT", null)) },
      { id: menuId("k"), label: "Add ★TRIGGER RANGE BUY", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "range_filter_buy", "EVENT", null, { is_trigger: true })) },
      { id: menuId("l"), label: "Add ★TRIGGER RANGE SELL", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "range_filter_sell", "EVENT", null, { is_trigger: true })) },
      { type: "separator" },
      { id: menuId("m"), label: "Add STATE RANGE Line (custom)...", onClick: () => promptAndAddNumericRule(timeframe, "range_filter_line", "Range Line", ">=", fmtSnapshotValue(values.range_filter_line, 3)) },
      { id: menuId("n"), label: "Add STATE RANGE Smooth Range (custom)...", onClick: () => promptAndAddNumericRule(timeframe, "range_filter_smooth_range", "Range Smooth Range", ">=", fmtSnapshotValue(values.range_filter_smooth_range, 3)) },
    ];
  }
  if (["market_state", "market_bias", "market_phase", "market_confidence", "market_state_age"].includes(columnKey)) {
    return [
      { id: menuId("a"), label: "Show Market Details", onClick: () => showMarketDetails(row) },
      { type: "separator" },
      values.market_state ? { id: menuId("b"), label: `Add STATE Market = ${values.market_state}`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "market_state", "=", values.market_state)) } : null,
      values.market_bias ? { id: menuId("c"), label: `Add STATE Bias = ${values.market_bias}`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "market_bias", "=", values.market_bias)) } : null,
      values.market_phase ? { id: menuId("d"), label: `Add STATE Phase = ${values.market_phase}`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "market_phase", "=", values.market_phase)) } : null,
      values.market_confidence !== null && values.market_confidence !== undefined ? { id: menuId("e"), label: `Add STATE Confidence >= ${Math.round(Number(values.market_confidence))}`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "market_confidence", ">=", Math.round(Number(values.market_confidence)))) } : null,
      values.market_state_age !== null && values.market_state_age !== undefined ? { id: menuId("f"), label: `Add STATE Age >= ${Math.round(Number(values.market_state_age))}`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "market_state_age", ">=", Math.round(Number(values.market_state_age)))) } : null,
    ].filter(Boolean);
  }
  if (columnKey === "flip_age") {
    return [
      { id: menuId("a"), label: "Add STATE FlipAge <= 1", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "flip_age", "<=", 1)) },
      { id: menuId("b"), label: "Add STATE FlipAge <= 3", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "flip_age", "<=", 3)) },
      { id: menuId("c"), label: "Add STATE FlipAge <= 5", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "flip_age", "<=", 5)) },
      { type: "separator" },
      { id: menuId("d"), label: "Add STATE FlipAge (custom)...", onClick: () => promptAndAddNumericRule(timeframe, "flip_age", "FlipAge", "<=", fmtSnapshotValue(values.flip_age, 0)) },
    ];
  }
  if (columnKey === "macd" || columnKey === "signal") {
    const field = columnKey;
    const pretty = fieldLabel(field, timeframe);
    const items = [
      { id: menuId("a"), label: `Add STATE ${pretty} > 0`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", field, ">", 0)) },
      { id: menuId("b"), label: `Add STATE ${pretty} < 0`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", field, "<", 0)) },
      { id: menuId("c"), label: `Add STATE ${pretty} (custom)...`, onClick: () => promptAndAddNumericRule(timeframe, field, pretty, ">", fmtSnapshotValue(values[field], 3)) },
    ];
    if (field === "macd") {
      items.push(
        { type: "separator" },
        { id: menuId("d"), label: "Add EVENT MACD×0 CROSS UP", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "macd0_cross", "CROSS", "UP")) },
        { id: menuId("e"), label: "Add EVENT MACD×0 CROSS DOWN", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "macd0_cross", "CROSS", "DOWN")) },
        { id: menuId("f"), label: "Add ★TRIGGER MACD×0 CROSS UP", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "macd0_cross", "CROSS", "UP", { is_trigger: true })) },
        { id: menuId("g"), label: "Add ★TRIGGER MACD×0 CROSS DOWN", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "macd0_cross", "CROSS", "DOWN", { is_trigger: true })) },
      );
    }
    return items;
  }
  if (columnKey === "adx") {
    return [
      { id: menuId("a"), label: "Add STATE ADX >= 15", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "adx", ">=", 15)) },
      { id: menuId("b"), label: "Add STATE ADX >= 20", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "adx", ">=", 20)) },
      { id: menuId("c"), label: "Add STATE ADX >= 25", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "adx", ">=", 25)) },
      { id: menuId("d"), label: "Add STATE ADX >= 30", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "adx", ">=", 30)) },
      { type: "separator" },
      { id: menuId("e"), label: "Add STATE ADX (custom)...", onClick: () => promptAndAddNumericRule(timeframe, "adx", "ADX", ">=", fmtSnapshotValue(values.adx, 1)) },
    ];
  }
  if (columnKey === "atr") {
    return [
      { id: menuId("a"), label: "Add STATE ATR >= 50", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "atr", ">=", 50)) },
      { id: menuId("b"), label: "Add STATE ATR >= 100", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "atr", ">=", 100)) },
      { id: menuId("c"), label: "Add STATE ATR >= 200", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "atr", ">=", 200)) },
      { type: "separator" },
      { id: menuId("d"), label: "Add STATE ATR (custom)...", onClick: () => promptAndAddNumericRule(timeframe, "atr", "ATR", ">=", fmtSnapshotValue(values.atr, 1)) },
    ];
  }
  if (columnKey === "di_plus" || columnKey === "di_minus") {
    const field = columnKey;
    const pretty = fieldLabel(field, timeframe);
    return [
      { id: menuId("a"), label: `Add STATE ${pretty} >= 10`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", field, ">=", 10)) },
      { id: menuId("b"), label: `Add STATE ${pretty} >= 20`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", field, ">=", 20)) },
      { id: menuId("c"), label: `Add STATE ${pretty} >= 30`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", field, ">=", 30)) },
      { type: "separator" },
      { id: menuId("d"), label: "Add STATE DI+ > DI-", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "di_spread", ">", 0)) },
      { id: menuId("e"), label: "Add STATE DI- > DI+", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "di_spread", "<", 0)) },
      { type: "separator" },
      { id: menuId("f"), label: `Add STATE ${pretty} (custom)...`, onClick: () => promptAndAddNumericRule(timeframe, field, pretty, ">=", fmtSnapshotValue(values[field], 1)) },
      { type: "separator" },
      { id: menuId("g"), label: "Add EVENT DI+×DI- CROSS UP", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "di_cross", "CROSS", "UP")) },
      { id: menuId("h"), label: "Add EVENT DI+×DI- CROSS DOWN", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "di_cross", "CROSS", "DOWN")) },
      { id: menuId("i"), label: "Add ★TRIGGER DI+×DI- CROSS UP", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "di_cross", "CROSS", "UP", { is_trigger: true })) },
      { id: menuId("j"), label: "Add ★TRIGGER DI+×DI- CROSS DOWN", onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "di_cross", "CROSS", "DOWN", { is_trigger: true })) },
    ];
  }
  return [
    { id: menuId("a"), label: `Add STATE ${fieldLabel(columnKey, timeframe)} = ${values[columnKey] ?? "current"}`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", columnKey, "=", values[columnKey])) },
  ];
}

function defaultRuleFromSnapshotCell(row, columnKey) {
  const timeframe = row.timeframe;
  const values = row.values || {};
  const value = values[columnKey];
  if (["ms", "angle", "sig_angle", "ppo", "adx_angle", "atr_angle", "rsi_state", "stoch_rsi_kd", "frama_state", "vidya_state", "range_filter_state", "range_filter_phase", "market_state", "market_bias", "market_phase"].includes(columnKey) && value !== null && value !== undefined && value !== "") {
    return makeRule(timeframe, "state", columnKey, "=", value);
  }
  if (columnKey === "flip_age" && Number.isFinite(Number(value))) {
    return makeRule(timeframe, "state", "flip_age", "<=", Math.round(Number(value)));
  }
  if (columnKey === "market_confidence" && Number.isFinite(Number(value))) {
    return makeRule(timeframe, "state", "market_confidence", ">=", Math.round(Number(value)));
  }
  if (columnKey === "market_state_age" && Number.isFinite(Number(value))) {
    return makeRule(timeframe, "state", "market_state_age", ">=", Math.round(Number(value)));
  }
  if (["adx", "atr", "di_plus", "di_minus"].includes(columnKey) && Number.isFinite(Number(value))) {
    return makeRule(timeframe, "state", columnKey, ">=", Number(value));
  }
  if (["macd", "signal"].includes(columnKey) && Number.isFinite(Number(value))) {
    return makeRule(timeframe, "state", columnKey, Number(value) >= 0 ? ">" : "<", 0);
  }
  if (["rsi", "rsi_smooth"].includes(columnKey) && value !== null && value !== undefined && Number.isFinite(Number(value))) {
    const num = Number(value);
    if (num >= 70) return makeRule(timeframe, "state", columnKey, ">=", 70);
    if (num <= 30) return makeRule(timeframe, "state", columnKey, "<=", 30);
    return makeRule(timeframe, "state", columnKey, num >= 50 ? ">=" : "<=", Math.round(num * 100) / 100);
  }
  if (["stoch_rsi_k", "stoch_rsi_d"].includes(columnKey) && value !== null && value !== undefined && Number.isFinite(Number(value))) {
    const num = Number(value);
    if (num >= 80) return makeRule(timeframe, "state", columnKey, ">=", 80);
    if (num <= 20) return makeRule(timeframe, "state", columnKey, "<=", 20);
    return makeRule(timeframe, "state", columnKey, num >= 50 ? ">=" : "<=", Math.round(num * 100) / 100);
  }
  return null;
}

function rowForTimeframe(timeframe) {
  return snapshotRows().find((row) => row.timeframe === timeframe) || null;
}

function handleSnapshotPanelChange(event) {
  if (event.target.id === "snapshot-target-tab") {
    setBuilderTarget(event.target.value, 0);
    return;
  }
  if (event.target.id === "snapshot-target-group") {
    setBuilderTarget(state.builderTarget.tabName, Number(event.target.value || 0));
    return;
  }
  if (event.target.dataset.snapshotSetting && state.builderPreset) {
    const key = event.target.dataset.snapshotSetting;
    const nextPair = currentEmaSettings(state.builderPreset, null);
    nextPair[key] = Math.max(1, Number(event.target.value || 1));
    const updated = setPresetEmaSettings(state.builderPreset, "default", nextPair);
    state.builderPresetDirty = true;
    ensureBuilderTarget();
    renderBuilder();
    renderSnapshotPanel();
    syncJsonEditorFromBuilder();
    setBuilderStatus(`Updated default EMA pair to ${formatEmaPair(updated)}.`);
    requestBuilderLiveRefresh();
    return;
  }
  if (event.target.dataset.snapshotRsiSetting && state.builderPreset) {
    const key = event.target.dataset.snapshotRsiSetting;
    const nextConfig = currentRsiSettings(state.builderPreset, null);
    nextConfig[key] = Math.max(1, Number(event.target.value || 1));
    const updated = setPresetRsiSettings(state.builderPreset, "default", nextConfig);
    state.builderPresetDirty = true;
    ensureBuilderTarget();
    renderBuilder();
    renderSnapshotPanel();
    syncJsonEditorFromBuilder();
    setBuilderStatus(`Updated default RSI config to ${formatRsiConfig(updated)}.`);
    requestBuilderLiveRefresh();
    return;
  }
  if (event.target.dataset.snapshotEmaTimeframe && event.target.dataset.snapshotEmaKey && state.builderPreset) {
    const timeframe = String(event.target.dataset.snapshotEmaTimeframe || "").trim();
    const key = event.target.dataset.snapshotEmaKey;
    const nextPair = currentEmaSettings(state.builderPreset, timeframe);
    nextPair[key] = Math.max(1, Number(event.target.value || 1));
    const updated = setPresetEmaSettings(state.builderPreset, timeframe, nextPair);
    const isOverride = hasPresetEmaOverride(state.builderPreset, timeframe);
    state.builderPresetDirty = true;
    ensureBuilderTarget();
    renderBuilder();
    renderSnapshotPanel();
    syncJsonEditorFromBuilder();
    setBuilderStatus(
      isOverride
        ? `Updated ${timeframe} EMA pair to ${formatEmaPair(updated)}.`
        : `${timeframe} EMA pair now follows the default ${formatEmaPair(updated)}.`
    );
    requestBuilderLiveRefresh();
    return;
  }
  if (event.target.dataset.snapshotRsiTimeframe && event.target.dataset.snapshotRsiKey && state.builderPreset) {
    const timeframe = String(event.target.dataset.snapshotRsiTimeframe || "").trim();
    const key = event.target.dataset.snapshotRsiKey;
    const nextConfig = currentRsiSettings(state.builderPreset, timeframe);
    nextConfig[key] = Math.max(1, Number(event.target.value || 1));
    const updated = setPresetRsiSettings(state.builderPreset, timeframe, nextConfig);
    const isOverride = hasPresetRsiOverride(state.builderPreset, timeframe);
    state.builderPresetDirty = true;
    ensureBuilderTarget();
    renderBuilder();
    renderSnapshotPanel();
    syncJsonEditorFromBuilder();
    setBuilderStatus(
      isOverride
        ? `Updated ${timeframe} RSI config to ${formatRsiConfig(updated)}.`
        : `${timeframe} RSI now follows the default ${formatRsiConfig(updated)}.`
    );
    requestBuilderLiveRefresh();
  }
}

function handleSnapshotContextMenu(event) {
  const cell = event.target.closest("[data-snapshot-cell='1']");
  if (!cell) return;
  event.preventDefault();
  const row = rowForTimeframe(cell.dataset.timeframe);
  if (!row) return;
  const items = buildSnapshotMenuItems(row, cell.dataset.column);
  if (!items.length) return;
  showContextMenu(items, event.clientX, event.clientY);
}

function handleSnapshotDoubleClick(event) {
  const cell = event.target.closest("[data-snapshot-cell='1']");
  if (!cell) return;
  const row = rowForTimeframe(cell.dataset.timeframe);
  if (!row) return;
  const column = cell.dataset.column;
  if (["ema_1", "ema_2", "ema_stack", "ema_spread", "ema_spread_delta", "ema_spread_trend", "ema_stack_pressure"].includes(column)) {
    showEmaDetails(row);
    return;
  }
  if (["rsi", "rsi_smooth", "rsi_state"].includes(column) && typeof showDetailModal === "function" && typeof detailRows === "function") {
    showRsiDetails(row);
    return;
  }
  if (["stoch_rsi_k", "stoch_rsi_d", "stoch_rsi_kd"].includes(column)) {
    showStochRsiDetails(row);
    return;
  }
  if (column === "vidya_state") {
    showVidyaDetails(row);
    return;
  }
  if (column === "range_filter_state" || column === "range_filter_phase") {
    showRangeDetails(row);
    return;
  }
  if (["market_state", "market_bias", "market_phase", "market_confidence", "market_state_age"].includes(column)) {
    showMarketDetails(row);
    return;
  }
  const rule = defaultRuleFromSnapshotCell(row, column);
  if (rule) {
    insertRuleIntoTarget(rule, `${fieldLabel(rule.field, rule.timeframe)} rule added from current value.`);
  }
}

function buildRuleCardMenuItems(rule, tabName, groupIndex, ruleIndex, groupJoin) {
  const items = [];
  if (rule.mode === "event") {
    items.push({
      id: menuId("a"),
      label: `${rule.is_trigger ? "Unset" : "Set"} ★ Trigger`,
      onClick: () => {
        rule.is_trigger = !rule.is_trigger;
        normalizeRuleConfig(rule);
        renderBuilder();
        syncJsonEditorFromBuilder();
        setBuilderStatus("Trigger flag updated.");
      },
    });
  }
  items.push(
    {
      id: menuId("b"),
      label: "Bars Ago OFF",
      onClick: () => {
        rule.bars_ago_mode = "OFF";
        rule.bars_ago_n = 0;
        rule.require_still_valid = false;
        normalizeRuleConfig(rule);
        renderBuilder();
        syncJsonEditorFromBuilder();
        setBuilderStatus("Bars Ago disabled.");
      },
    },
    {
      id: menuId("c"),
      label: "Bars Ago EXACT 1",
      onClick: () => {
        rule.bars_ago_mode = "EXACT";
        rule.bars_ago_n = 1;
        normalizeRuleConfig(rule);
        renderBuilder();
        syncJsonEditorFromBuilder();
        setBuilderStatus("Bars Ago EXACT 1 applied.");
      },
    },
    {
      id: menuId("d"),
      label: "Bars Ago WITHIN 3",
      onClick: () => {
        rule.bars_ago_mode = "WITHIN";
        rule.bars_ago_n = 3;
        normalizeRuleConfig(rule);
        renderBuilder();
        syncJsonEditorFromBuilder();
        setBuilderStatus("Bars Ago WITHIN 3 applied.");
      },
    },
    {
      id: menuId("e"),
      label: `${rule.require_still_valid ? "Unset" : "Set"} Require Still Valid`,
      onClick: () => {
        rule.require_still_valid = !rule.require_still_valid;
        normalizeRuleConfig(rule);
        renderBuilder();
        syncJsonEditorFromBuilder();
        setBuilderStatus("Require Still Valid updated.");
      },
    },
  );
  if ((groupJoin || "OR").toUpperCase() === "SEQUENCE") {
    items.push({
      id: menuId("f"),
      label: `${rule.is_final_gate ? "Unset" : "Set"} Final-Bar Gate`,
      onClick: () => {
        rule.is_final_gate = !rule.is_final_gate;
        normalizeRuleConfig(rule);
        renderBuilder();
        syncJsonEditorFromBuilder();
        setBuilderStatus("Final gate updated.");
      },
    });
    items.push({
      id: menuId("f2"),
      label: `${rule.is_sequence_canceler ? "Unset" : "Set"} Sequence Canceler`,
      onClick: () => {
        rule.is_sequence_canceler = !rule.is_sequence_canceler;
        normalizeRuleConfig(rule);
        renderBuilder();
        syncJsonEditorFromBuilder();
        setBuilderStatus("Sequence canceler updated.");
      },
    });
  }
  items.push(
    { type: "separator" },
    {
      id: menuId("g"),
      label: "Duplicate Rule",
      onClick: () => {
        duplicateRuleAt(tabName, groupIndex, ruleIndex);
        renderBuilder();
        syncJsonEditorFromBuilder();
        setBuilderStatus("Rule duplicated.");
      },
    },
    {
      id: menuId("h"),
      label: "Delete Rule",
      onClick: () => {
        state.builderPreset.tabs[tabName].groups[groupIndex].rules.splice(ruleIndex, 1);
        ensureBuilderTarget();
        renderBuilder();
        renderSnapshotPanel();
        syncJsonEditorFromBuilder();
        setBuilderStatus("Rule deleted.");
      },
    },
  );
  return items;
}

function handleBuilderContextMenu(event) {
  const ruleNode = event.target.closest(".builder-rule");
  if (!ruleNode || !state.builderPreset) return;
  event.preventDefault();
  const tabName = ruleNode.dataset.tab;
  const groupIndex = Number(ruleNode.dataset.groupIndex);
  const ruleIndex = Number(ruleNode.dataset.ruleIndex);
  const rule = state.builderPreset?.tabs?.[tabName]?.groups?.[groupIndex]?.rules?.[ruleIndex];
  if (!rule) return;
  const groupJoin = state.builderPreset?.tabs?.[tabName]?.groups?.[groupIndex]?.rules_join || "OR";
  showContextMenu(buildRuleCardMenuItems(rule, tabName, groupIndex, ruleIndex, groupJoin), event.clientX, event.clientY);
}

async function loadInitialData() {
  try {
    const [health, presetMeta] = await Promise.all([api("/api/v2/health"), api("/api/v2/presets")]);
    $("health-pill").textContent = "Backend Ready";
    state.presets = presetMeta.items || [];
    renderOverviewCards(health, presetMeta);
    renderPresetList(state.presets);
    syncBacktestForm();
    if (presetMeta.last_preset) await selectPreset(presetMeta.last_preset);
    else if (state.presets.length) await selectPreset(state.presets[0].name);
    else {
      const fresh = defaultPresetTemplate("new");
      loadPresetIntoEditor("", fresh);
      loadPresetIntoBuilder("", fresh, "No saved presets available. Working in a blank draft.");
    }
    if (state.selectedPreset) {
      loadPresetIntoBuilder(state.selectedPreset.name, state.selectedPreset.preset, `Loaded preset ${state.selectedPreset.name} into the builder.`);
    }
    renderBuilder();
    renderSnapshotPanel();
    syncBuilderHeader();
  } catch (error) {
    $("health-pill").textContent = "Backend Error";
    $("health-pill").style.color = "#ff7d90";
    $("overview-cards").innerHTML = `<div class="empty-state">${error.message}</div>`;
    setEditorStatus(error.message, true);
    setBuilderStatus(error.message, true);
    setSnapshotStatus(error.message, true);
  }
}

async function submitBacktest(event) {
  event.preventDefault();
  const presetName = $("preset-select").value;
  if (!presetName) return;
  const formState = collectBacktestFormState();
  const overrides = {
    start: formState.start || null,
    end: formState.end || null,
    stop_loss_mode: "PERCENT",
    stop_loss_value: formState.stop_loss_value,
    take_profit_mode: "PERCENT",
    take_profit_value: formState.take_profit_value,
    break_even_after_mfe_pct: formState.break_even_after_mfe_pct,
    trailing_stop_pct: formState.trailing_stop_pct,
    order_notional: formState.order_notional,
    fee_pct: formState.fee_pct,
    equity_curve_stride: formState.equity_curve_stride,
    allow_reverse: formState.allow_reverse,
    capture_trade_details: formState.capture_trade_details,
  };
  setBacktestFormStatus(`Running backtest for ${presetName}...`);
  const payload = await api("/api/v2/backtests", {
    method: "POST",
    body: JSON.stringify({ preset_name: presetName, overrides }),
  });
  state.currentJobId = payload.job_id;
  $("job-pill").textContent = `Job ${payload.job_id.slice(0, 8)} running`;
  pollJob();
  setView("backtest");
}

async function pollJob() {
  if (!state.currentJobId) return;
  try {
    const data = await api(`/api/v2/jobs/${state.currentJobId}`);
    $("job-status-text").textContent = data.state;
    $("job-message").textContent = data.error || data.message || "Working...";
    $("job-progress-fill").style.width = `${Math.max(0, Math.min(100, data.progress_pct || 0))}%`;
    if (data.state === "completed" && data.result) {
      $("job-pill").textContent = `Last job ${data.job_id.slice(0, 8)} complete`;
      $("result-bundle-path").textContent = data.result.verification_bundle || "Result ready";
      renderMetrics(data.result.summary || {});
      renderTrades(data.result.sample_trades || []);
      renderActivity(data.result.event_activity || {});
      drawEquity(data.result.equity_curve_preview || []);
      clearTimeout(state.jobTimer);
      state.jobTimer = null;
      return;
    }
    if (data.state === "failed") {
      $("job-pill").textContent = `Job ${data.job_id.slice(0, 8)} failed`;
      clearTimeout(state.jobTimer);
      state.jobTimer = null;
      return;
    }
  } catch (error) {
    $("job-message").textContent = error.message;
  }
  state.jobTimer = setTimeout(pollJob, 1200);
}

function handleNewBlankPreset() {
  const suggested = `preset_${Date.now()}`;
  const fresh = defaultPresetTemplate(suggested);
  $("editor-preset-name").value = suggested;
  loadPresetIntoEditor(suggested, fresh);
  loadPresetIntoBuilder(suggested, fresh);
  setBuilderStatus("Blank preset scaffold created.");
}

function handleLoadSelectedJson() {
  if (!state.selectedPreset) {
    setEditorStatus("No preset selected yet.", true);
    return;
  }
  loadPresetIntoEditor(state.selectedPreset.name, state.selectedPreset.preset);
}

async function handleSavePreset(event) {
  event.preventDefault();
  const name = builderPresetName();
  if (!name) {
    setEditorStatus("Preset name is required.", true);
    return;
  }
  let parsed;
  try {
    parsed = JSON.parse($("preset-json-editor").value);
  } catch (error) {
    setEditorStatus(`Invalid JSON: ${error.message}`, true);
    return;
  }
  try {
    await savePresetPayload(name, parsed, $("editor-set-last").checked);
    setEditorStatus(`Saved preset ${name}.`);
    setBuilderStatus(`Saved preset ${name}.`);
    await reloadPresetsAndSelect(name);
  } catch (error) {
    setEditorStatus(error.message, true);
  }
}

async function handleSavePresetAs() {
  const currentName = builderPresetName();
  const nextName = window.prompt("Save preset as:", currentName ? `${currentName}_copy` : "preset_copy");
  if (!nextName) return;
  let parsed;
  try {
    parsed = JSON.parse($("preset-json-editor").value);
  } catch (error) {
    setEditorStatus(`Invalid JSON: ${error.message}`, true);
    return;
  }
  try {
    await savePresetPayload(nextName.trim(), parsed, $("editor-set-last").checked);
    setEditorStatus(`Saved preset as ${nextName.trim()}.`);
    setBuilderStatus(`Saved preset as ${nextName.trim()}.`);
    await reloadPresetsAndSelect(nextName.trim());
  } catch (error) {
    setEditorStatus(error.message, true);
  }
}

async function handleDeletePreset() {
  const name = state.selectedPreset?.name || $("editor-preset-name").value.trim();
  if (!name) {
    setEditorStatus("Select a preset first.", true);
    return;
  }
  if (!window.confirm(`Delete preset "${name}"?`)) return;
  try {
    await api(`/api/v2/presets/${encodeURIComponent(name)}`, { method: "DELETE" });
    setEditorStatus(`Deleted preset ${name}.`);
    setBuilderStatus(`Deleted preset ${name}.`);
    const presetMeta = await api("/api/v2/presets");
    state.presets = presetMeta.items || [];
    renderPresetList(state.presets);
    syncBacktestForm();
    if (presetMeta.last_preset) {
      await selectPreset(presetMeta.last_preset);
    } else if (state.presets.length) {
      await selectPreset(state.presets[0].name);
    } else {
      state.selectedPreset = null;
      const fresh = defaultPresetTemplate("new");
      loadPresetIntoEditor("", fresh);
      loadPresetIntoBuilder("", fresh, "No saved presets left. Builder switched to a blank draft.");
      $("preset-detail").innerHTML = `<p class="muted">Choose a preset to inspect its timeframes, rules, and saved settings.</p>`;
    }
  } catch (error) {
    setEditorStatus(error.message, true);
  }
}

function handleEntryFilterModalClickInternal(event) {
  const actionNode = event.target.closest("[data-entry-filter-action]");
  if (!actionNode || !state.entryFilterModalTab || !state.builderPreset) return false;
  const groupIndex = state.entryFilterModalGroupIndex;
  const isGroupTarget = groupIndex !== null && groupIndex !== undefined;
  if (actionNode.dataset.entryFilterAction === "reset") {
    if (isGroupTarget) {
      const cfg = ensureGroupEntryFilterConfig(state.entryFilterModalTab, groupIndex);
      Object.assign(cfg, defaultEntryFilterConfig());
    } else {
      const key = entryFilterTabKey(state.entryFilterModalTab);
      state.builderPreset.entry_filters[key] = { ...defaultEntryFilterConfig(), groups: { ...((state.builderPreset.entry_filters?.[key] || {}).groups || {}) } };
    }
    if (isGroupTarget) {
      ensureGroupEntryFilterConfig(state.entryFilterModalTab, groupIndex);
    } else {
      ensureEntryFilterConfig(state.entryFilterModalTab);
    }
    state.builderPresetDirty = true;
  } else {
    return false;
  }
  rerenderEntryFilterModal();
  renderBuilder();
  syncJsonEditorFromBuilder();
  const targetText = isGroupTarget
    ? `${state.entryFilterModalTab} ${groupLabel(state.builderPreset?.tabs?.[state.entryFilterModalTab]?.groups?.[groupIndex], groupIndex)}`
    : state.entryFilterModalTab;
  setBuilderStatus(`Updated Entry Filter for ${targetText}.`);
  requestBuilderLiveRefresh();
  return true;
}

function handleEntryFilterModalFormChange(event) {
  if (!state.entryFilterModalTab || !state.builderPreset) return false;
  const prop = event.target.dataset.entryFilterProp;
  if (!prop) return false;
  updateEntryFilterProp(
    state.entryFilterModalTab,
    prop,
    event.target.type === "checkbox" ? event.target.checked : event.target.value,
    event.target.type,
    state.entryFilterModalGroupIndex,
  );
  rerenderEntryFilterModal();
  return true;
}

const originalHandleDetailModalClick = handleDetailModalClick;
handleDetailModalClick = function (event) {
  const backdrop = $("detail-modal-backdrop");
  if (backdrop?.dataset.mode === "entry-filter" && handleEntryFilterModalClickInternal(event)) return;
  return originalHandleDetailModalClick(event);
};

const originalHandleDetailModalInput = handleDetailModalInput;
handleDetailModalInput = function (event) {
  const backdrop = $("detail-modal-backdrop");
  if (backdrop?.dataset.mode === "entry-filter") return;
  return originalHandleDetailModalInput(event);
};

const originalHandleDetailModalChange = handleDetailModalChange;
handleDetailModalChange = function (event) {
  const backdrop = $("detail-modal-backdrop");
  if (backdrop?.dataset.mode === "entry-filter" && handleEntryFilterModalFormChange(event)) return;
  return originalHandleDetailModalChange(event);
};

function installNav() {
  document.querySelectorAll(".nav-link").forEach((node) => node.addEventListener("click", () => {
    setView(node.dataset.view);
    closeSidebar();
  }));
}

function setSidebarOpen(open) {
  document.body.classList.toggle("sidebar-open", Boolean(open));
}

function closeSidebar() {
  setSidebarOpen(false);
}

function installSidebarDrawer() {
  $("nav-toggle-button")?.addEventListener("click", () => {
    setSidebarOpen(!document.body.classList.contains("sidebar-open"));
  });
  $("sidebar-backdrop")?.addEventListener("click", () => closeSidebar());
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") closeSidebar();
  });
}

document.addEventListener("DOMContentLoaded", async () => {
  installNav();
  installSidebarDrawer();
  $("backtest-form").addEventListener("submit", submitBacktest);
  $("preset-select").addEventListener("change", handleBacktestPresetSelectChange);
  $("backtest-load-preset-button")?.addEventListener("click", handleBacktestPresetLoad);
  $("backtest-save-preset-button")?.addEventListener("click", handleBacktestPresetSave);
  $("backtest-save-as-button")?.addEventListener("click", handleBacktestPresetSaveAs);
  $("delete-preset-button").addEventListener("click", handleDeletePreset);
  $("open-builder-button").addEventListener("click", handleOpenBuilder);
  $("snapshot-refresh-button").addEventListener("click", loadSnapshotForSelectedPreset);
  $("builder-load-button").addEventListener("click", handleBuilderPresetLoad);
  $("builder-preset-select").addEventListener("change", handleBuilderPresetLoad);
  $("builder-save-button").addEventListener("click", handleBuilderSave);
  $("builder-save-as-button").addEventListener("click", handleBuilderSaveAs);
  $("builder-reset-button").addEventListener("click", handleBuilderReset);
  $("trades-table-body").addEventListener("click", (event) => {
    const row = event.target.closest("[data-trade-id]");
    if (!row) return;
    void selectTradeRow(Number(row.dataset.tradeId));
  });
  $("preset-builder").addEventListener("click", handleBuilderClick);
  $("preset-builder").addEventListener("change", handleBuilderChange);
  $("preset-builder").addEventListener("contextmenu", handleBuilderContextMenu);
  $("snapshot-panel")?.addEventListener("change", handleSnapshotPanelChange);
  $("snapshot-panel")?.addEventListener("contextmenu", handleSnapshotContextMenu);
  $("snapshot-panel")?.addEventListener("dblclick", handleSnapshotDoubleClick);
  $("detail-modal-close").addEventListener("click", hideDetailModal);
  $("detail-modal-body").addEventListener("click", handleDetailModalClick);
  $("detail-modal-body").addEventListener("input", handleDetailModalInput);
  $("detail-modal-body").addEventListener("change", handleDetailModalChange);
  $("detail-modal-backdrop").addEventListener("click", (event) => {
    if (event.target.id === "detail-modal-backdrop") hideDetailModal();
  });
  document.addEventListener("click", (event) => {
    if (!event.target.closest("#context-menu")) hideContextMenu();
  });
  window.addEventListener("resize", hideContextMenu);
  window.addEventListener("scroll", handleGlobalContextMenuScroll, true);
  await loadInitialData();
});
