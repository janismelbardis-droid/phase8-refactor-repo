(function () {
  "use strict";

  const byId = (id) => document.getElementById(id);
  const LIVE_RECONNECT_MS = 1500;
  const LIVE_CONTEXT_DEFAULT_TIMEFRAMES = Object.freeze(["1m", "5m", "15m", "30m", "1h"]);

  let activeSnapshotTimeframe = null;
  let livePaused = false;
  let liveFetchInFlight = false;
  let livePulseTimer = null;
  let liveSocket = null;
  let liveSocketUrl = null;
  let liveSocketConnected = false;
  let liveReconnectTimer = null;
  let pendingPausedSnapshot = null;
  let liveBootstrapError = "";
  let liveSnapshotPayload = null;
  let liveSnapshotLoading = false;
  let indicatorRefreshCarryByTf = {};
  let indicatorRefreshCarryTimer = null;
  let verifyEmaEditorOpen = false;
  let verifyRsiEditorOpen = false;
  let takerBiasDetailState = {
    row: null,
    window: 3,
  };
  const LIVE_CONTEXT_STORAGE_KEY = "ui-v2-live-context-v1";
  const MARKET_AUDIT_PLAY_MS = 700;
  const MARKET_AUDIT_LIVE_REFRESH_MS = 2000;
  const VERIFY_TRIGGER_CARRY_TTL_MS = 45000;
  const VERIFY_TRIGGER_CARRY_FIELDS = [
    "frama_display_age",
    "frama_trigger_age",
    "frama_trigger_side",
    "frama_break_up_age",
    "frama_break_down_age",
    "frama_mid_cross_age",
    "range_filter_display_age",
    "range_filter_buy_age",
    "range_filter_sell_age",
  ];
  let marketAuditState = {
    open: false,
    loading: false,
    error: "",
    payload: null,
    payloadCache: {},
    source: "live",
    timeframe: "5m",
    bars: 300,
    activeIndex: 0,
    playTimer: null,
    liveRefreshTimer: null,
    followLive: true,
    fetchInFlight: false,
    requestSeq: 0,
    fetchController: null,
    chartScrollLeft: 0,
    syncingChartScroll: false,
    renderQueued: false,
  };

  function sanitizeText(value) {
    return String(value ?? "")
      .replaceAll("MACDÃƒÆ’Ã¢â‚¬â€Signal", "MACD x Signal")
      .replaceAll("MACDÃƒÆ’Ã¢â‚¬â€SIGNAL", "MACD x SIGNAL")
      .replaceAll("MACDÃƒÆ’Ã¢â‚¬â€0", "MACD x 0")
      .replaceAll("DI+ÃƒÆ’Ã¢â‚¬â€DI-", "DI+ x DI-")
      .replaceAll("VIDYA ÃƒÅ½Ã¢â‚¬Â%", "VIDYA Delta %")
      .replaceAll("ÃƒÅ½Ã¢â‚¬Â%", "Delta %")
      .replaceAll("ÃƒÂ¢Ã‹Å“Ã¢â‚¬Â¦TRIGGER", "Trigger")
      .replaceAll("ÃƒÂ¢Ã‹Å“Ã¢â‚¬Â¦", "Trigger")
      .replaceAll("Ãƒâ€šÃ‚Â·", " | ")
      .replaceAll("ÃƒÂ¢Ã¢â€šÂ¬Ã¢â‚¬Â", "-")
      .replaceAll("Ã‚Â·", " | ")
      .replaceAll("Ã¢â‚¬â€", "-")
      .replaceAll("Ã¢Ëœâ€¦TRIGGER", "Trigger")
      .replaceAll("Ã¢Ëœâ€¦", "Trigger");
  }

  function sanitizeTree(root) {
    if (!root) return;
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
    const nodes = [];
    while (walker.nextNode()) nodes.push(walker.currentNode);
    nodes.forEach((node) => {
      const next = sanitizeText(node.nodeValue);
      if (next !== node.nodeValue) node.nodeValue = next;
    });
    root.querySelectorAll?.("[title]")?.forEach((node) => {
      const current = node.getAttribute("title");
      const next = sanitizeText(current);
      if (next !== current) node.setAttribute("title", next);
    });
  }

  function verifyEditorRenderLocked() {
    return Boolean(verifyEmaEditorOpen || verifyRsiEditorOpen);
  }

  function cloneJsonSafe(value, fallback = null) {
    try {
      return JSON.parse(JSON.stringify(value));
    } catch (error) {
      return fallback;
    }
  }

  const LIVE_PARAMETER_INDICATOR_DEFAULTS = Object.freeze({
    ema: Object.freeze({ ema_1_length: 9, ema_2_length: 21 }),
    rsi: Object.freeze({ length: 14, smoothing: 3 }),
  });

  function liveTimeframeOptions() {
    return Array.isArray(TIMEFRAME_OPTIONS) && TIMEFRAME_OPTIONS.length
      ? TIMEFRAME_OPTIONS
      : LIVE_CONTEXT_DEFAULT_TIMEFRAMES.slice();
  }

  function normalizeLiveTimeframeMap(sourceMap, fallbackMap = null) {
    const options = liveTimeframeOptions();
    const source = sourceMap && typeof sourceMap === "object" ? sourceMap : {};
    const fallback = fallbackMap && typeof fallbackMap === "object" ? fallbackMap : {};
    const next = {};
    let hasEnabled = false;
    options.forEach((tf) => {
      const enabled = Object.prototype.hasOwnProperty.call(source, tf)
        ? Boolean(source[tf])
        : Boolean(fallback[tf]);
      next[tf] = enabled;
      if (enabled) hasEnabled = true;
    });
    if (!hasEnabled) {
      const fallbackTf = options.find((tf) => Boolean(fallback[tf])) || options[0] || "1m";
      next[fallbackTf] = true;
    }
    return next;
  }

  function isBuilderViewActive() {
    if (typeof activeBuilderView === "function") {
      try {
        return Boolean(activeBuilderView());
      } catch (error) {
        /* noop */
      }
    }
    return Boolean(byId("builder-view")?.classList.contains("is-active"));
  }

  function normalizeLiveIndicatorSettingsValue(kind, rawValue) {
    const key = String(kind || "").trim();
    const defaults = LIVE_PARAMETER_INDICATOR_DEFAULTS[key];
    if (!defaults) return { defaults: {}, overrides: {} };
    if (key === "ema" && typeof normalizeEmaIndicatorSettings === "function") {
      return normalizeEmaIndicatorSettings(rawValue);
    }
    if (key === "rsi" && typeof normalizeRsiIndicatorSettings === "function") {
      return normalizeRsiIndicatorSettings(rawValue);
    }
    const defaultsSource = rawValue?.defaults && typeof rawValue.defaults === "object" ? rawValue.defaults : (rawValue || {});
    const nextDefaults = {};
    Object.keys(defaults).forEach((field) => {
      const normalizedValue = Math.max(1, Number(defaultsSource?.[field] ?? defaults[field]));
      nextDefaults[field] = Number.isFinite(normalizedValue) ? normalizedValue : defaults[field];
    });
    return {
      defaults: nextDefaults,
      overrides: {},
    };
  }

  function liveContextSeedSettings() {
    const preset = state?.builderPreset || state?.selectedPreset?.preset || null;
    const presetSettings = preset?.settings || {};
    const normalizedEma = normalizeLiveIndicatorSettingsValue("ema", presetSettings.ema_indicators);
    const normalizedRsi = normalizeLiveIndicatorSettingsValue("rsi", presetSettings.rsi_indicators);
    return {
      symbol: String(liveSnapshotPayload?.symbol || presetSettings.symbol || "BTCUSDT").trim().toUpperCase() || "BTCUSDT",
      timeframes: normalizeLiveTimeframeMap(presetSettings.timeframes),
      price_source: String(presetSettings.price_source || "LAST").trim().toUpperCase() || "LAST",
      macd_impl: String(presetSettings.macd_impl || "TRADINGVIEW").trim().toUpperCase() || "TRADINGVIEW",
      adx_impl: String(presetSettings.adx_impl || "TRADINGVIEW").trim().toUpperCase() || "TRADINGVIEW",
      live_forming: Boolean(presetSettings.live_use_forming ?? presetSettings.live_forming ?? true),
      ema_indicators: normalizedEma,
      rsi_indicators: normalizedRsi,
    };
  }

  function normalizeLiveContext(raw) {
    const source = raw && typeof raw === "object" ? raw : {};
    const seeded = liveContextSeedSettings();
    const sourceSettings = source.settings && typeof source.settings === "object" ? source.settings : source;
    const normalizedEma = normalizeLiveIndicatorSettingsValue("ema", sourceSettings.ema_indicators || seeded.ema_indicators);
    const normalizedRsi = normalizeLiveIndicatorSettingsValue("rsi", sourceSettings.rsi_indicators || seeded.rsi_indicators);
    return {
      session_name: String(source.session_name || source.name || "live_market").trim() || "live_market",
      settings: {
        symbol: String(sourceSettings.symbol || seeded.symbol).trim().toUpperCase() || "BTCUSDT",
        timeframes: normalizeLiveTimeframeMap(sourceSettings.timeframes, seeded.timeframes),
        price_source: String(sourceSettings.price_source || seeded.price_source || "LAST").trim().toUpperCase() || "LAST",
        macd_impl: String(sourceSettings.macd_impl || seeded.macd_impl || "TRADINGVIEW").trim().toUpperCase() || "TRADINGVIEW",
        adx_impl: String(sourceSettings.adx_impl || seeded.adx_impl || "TRADINGVIEW").trim().toUpperCase() || "TRADINGVIEW",
        live_forming: Boolean(sourceSettings.live_use_forming ?? sourceSettings.live_forming ?? seeded.live_forming ?? true),
        ema_indicators: normalizedEma,
        rsi_indicators: normalizedRsi,
      },
    };
  }

  function readStoredLiveContext() {
    try {
      const raw = window.localStorage.getItem(LIVE_CONTEXT_STORAGE_KEY);
      if (!raw) return null;
      return normalizeLiveContext(JSON.parse(raw));
    } catch (error) {
      return null;
    }
  }

  let liveContextState = readStoredLiveContext() || normalizeLiveContext({});

  function persistLiveContext() {
    try {
      window.localStorage.setItem(LIVE_CONTEXT_STORAGE_KEY, JSON.stringify(liveContextState));
    } catch (error) {
      /* noop */
    }
  }

  function liveContextPresetView() {
    return { settings: cloneJsonSafe(liveContextState?.settings, {}) || {} };
  }

  function normalizedBuilderLiveContext() {
    if (!state?.builderPreset?.settings || typeof state.builderPreset.settings !== "object") return null;
    return normalizeLiveContext({
      session_name: liveContextState?.session_name || "live_market",
      settings: state.builderPreset.settings,
    });
  }

  function syncLiveContextFromBuilderSettings(options = {}) {
    const normalized = normalizedBuilderLiveContext();
    if (!normalized) return false;
    const prevJson = JSON.stringify(liveContextState?.settings || {});
    const nextJson = JSON.stringify(normalized.settings || {});
    if (prevJson === nextJson) return false;
    liveContextState = normalized;
    persistLiveContext();
    if (options.refresh !== false && isBuilderViewActive() && !livePaused && !liveFetchInFlight) {
      void refreshSnapshotOnce(false);
    } else {
      updateLiveControls();
      renderVerifyHost();
    }
    return true;
  }

  function snapshotRowsSafe() {
    return Array.isArray(liveSnapshotPayload?.rows) ? liveSnapshotPayload.rows : [];
  }

  function clearIndicatorRefreshCarry() {
    indicatorRefreshCarryByTf = {};
    if (indicatorRefreshCarryTimer) {
      window.clearTimeout(indicatorRefreshCarryTimer);
      indicatorRefreshCarryTimer = null;
    }
  }

  function beginIndicatorRefreshCarry() {
    const carry = {};
    snapshotRowsSafe().forEach((row) => {
      const tf = String(row?.timeframe || "").trim();
      const values = row?.values && typeof row.values === "object" ? row.values : null;
      if (!tf || !values) return;
      const protectedValues = {};
      VERIFY_TRIGGER_CARRY_FIELDS.forEach((field) => {
        const value = values[field];
        if (value !== null && value !== undefined && value !== "") {
          protectedValues[field] = value;
        }
      });
      if (Object.keys(protectedValues).length) {
        carry[tf] = protectedValues;
      }
    });
    indicatorRefreshCarryByTf = carry;
    if (indicatorRefreshCarryTimer) {
      window.clearTimeout(indicatorRefreshCarryTimer);
      indicatorRefreshCarryTimer = null;
    }
    if (Object.keys(carry).length) {
      indicatorRefreshCarryTimer = window.setTimeout(() => {
        clearIndicatorRefreshCarry();
      }, VERIFY_TRIGGER_CARRY_TTL_MS);
    }
  }

  function shouldUseIndicatorRefreshCarry() {
    return Object.keys(indicatorRefreshCarryByTf).length > 0
      && (liveFetchInFlight || liveSnapshotLoading || currentLivePhase() !== "live");
  }

  function mergeIndicatorRefreshCarryRow(row) {
    if (!row || typeof row !== "object" || !shouldUseIndicatorRefreshCarry()) return row;
    const tf = String(row.timeframe || "").trim();
    const carry = indicatorRefreshCarryByTf[tf];
    const values = row?.values && typeof row.values === "object" ? row.values : null;
    if (!tf || !carry || !values) return row;
    let changed = false;
    const mergedValues = { ...values };
    VERIFY_TRIGGER_CARRY_FIELDS.forEach((field) => {
      const currentValue = mergedValues[field];
      if ((currentValue === null || currentValue === undefined || currentValue === "") && carry[field] !== undefined) {
        mergedValues[field] = carry[field];
        changed = true;
      }
    });
    return changed ? { ...row, values: mergedValues } : row;
  }

  function escapeAuditHtml(value) {
    if (typeof escapeHtml === "function") return escapeHtml(String(value ?? ""));
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function hasUnsavedBuilderDraft() {
    return Boolean(state?.builderPreset && state?.builderPresetDirty);
  }

  function activeLivePresetName() {
    const name = String(state?.builderPresetName || state?.selectedPreset?.name || "").trim();
    if (!name) return null;
    if (hasUnsavedBuilderDraft()) return null;
    const known = Array.isArray(state?.presets) ? state.presets.some((item) => String(item?.name || "") === name) : false;
    return known ? name : null;
  }

  function hasBuilderDraft() {
    return Boolean(state?.builderPreset);
  }

  function currentLiveSessionSpec() {
    return {
      mode: "live_context",
      sessionName: String(liveContextState?.session_name || "live_market").trim() || "live_market",
      requestBody: {
        session_name: String(liveContextState?.session_name || "live_market").trim() || "live_market",
        settings: cloneJsonSafe(liveContextState?.settings, liveContextSeedSettings()),
      },
      socketPresetName: null,
    };
  }

  function stopMarketAuditPlayback() {
    if (marketAuditState.playTimer) {
      window.clearInterval(marketAuditState.playTimer);
      marketAuditState.playTimer = null;
    }
  }

  function stopMarketAuditLiveRefresh() {
    if (marketAuditState.liveRefreshTimer) {
      window.clearInterval(marketAuditState.liveRefreshTimer);
      marketAuditState.liveRefreshTimer = null;
    }
  }

  function stopMarketAuditFetch() {
    if (marketAuditState.fetchController && typeof marketAuditState.fetchController.abort === "function") {
      try {
        marketAuditState.fetchController.abort();
      } catch (error) {
        /* noop */
      }
    }
    marketAuditState.fetchController = null;
    marketAuditState.fetchInFlight = false;
    marketAuditState.loading = false;
  }

  function marketAuditChartShell() {
    return document.querySelector("#detail-modal-backdrop .market-audit-chart-shell");
  }

  function captureMarketAuditChartScroll() {
    const shell = marketAuditChartShell();
    if (!shell) return;
    marketAuditState.chartScrollLeft = Number(shell.scrollLeft || 0);
  }

  function marketAuditShellNearRight(shell) {
    if (!shell) return true;
    const maxScroll = Math.max(0, Number(shell.scrollWidth || 0) - Number(shell.clientWidth || 0));
    return (maxScroll - Number(shell.scrollLeft || 0)) <= 24;
  }

  function restoreMarketAuditChartScroll() {
    const shell = marketAuditChartShell();
    if (!shell) return;
    marketAuditState.syncingChartScroll = true;
    if (shouldFollowMarketAuditLive()) {
      shell.scrollLeft = Math.max(0, Number(shell.scrollWidth || 0) - Number(shell.clientWidth || 0));
      marketAuditState.chartScrollLeft = Number(shell.scrollLeft || 0);
    } else {
      const maxScroll = Math.max(0, Number(shell.scrollWidth || 0) - Number(shell.clientWidth || 0));
      shell.scrollLeft = Math.max(0, Math.min(maxScroll, Number(marketAuditState.chartScrollLeft || 0)));
    }
    window.requestAnimationFrame(() => {
      marketAuditState.syncingChartScroll = false;
    });
  }

  function shouldFollowMarketAuditLive() {
    return Boolean(
      marketAuditState.open
      && normalizeMarketAuditSource(marketAuditState.source) === "live"
      && marketAuditState.followLive
      && !marketAuditState.playTimer
    );
  }

  function ensureMarketAuditLiveRefresh() {
    stopMarketAuditLiveRefresh();
    if (!shouldFollowMarketAuditLive()) return;
    marketAuditState.liveRefreshTimer = window.setInterval(() => {
      if (marketAuditState.loading || marketAuditState.fetchInFlight) return;
      void loadMarketAudit({ background: true, forceLatest: true });
    }, MARKET_AUDIT_LIVE_REFRESH_MS);
  }

  function disableMarketAuditLiveFollow() {
    if (normalizeMarketAuditSource(marketAuditState.source) !== "live") return;
    marketAuditState.followLive = false;
    stopMarketAuditLiveRefresh();
  }

  function syncMarketAuditIndex() {
    const count = Number(marketAuditState?.payload?.bar_count || marketAuditState?.payload?.rows?.length || 0);
    if (!count) {
      marketAuditState.activeIndex = 0;
      stopMarketAuditPlayback();
      return 0;
    }
    marketAuditState.activeIndex = Math.max(0, Math.min(count - 1, Number(marketAuditState.activeIndex || 0)));
    return marketAuditState.activeIndex;
  }

  function marketAuditRows() {
    return Array.isArray(marketAuditState?.payload?.rows) ? marketAuditState.payload.rows : [];
  }

  function activeMarketAuditRow() {
    const rows = marketAuditRows();
    if (!rows.length) return null;
    return rows[syncMarketAuditIndex()] || null;
  }

  function activeSavedAuditPresetName() {
    const selectedName = String(state?.selectedPreset?.name || "").trim();
    if (selectedName) return selectedName;
    const builderName = String(state?.builderPresetName || "").trim();
    if (builderName && !hasUnsavedBuilderDraft()) return builderName;
    return null;
  }

  function normalizeMarketAuditSource(source) {
    const value = String(source || "").trim().toLowerCase();
    if (value === "saved" || value === "preview" || value === "live") return value;
    return "live";
  }

  function marketAuditAvailableSources() {
    const sources = [
      {
        key: "live",
        label: "Live Market",
        description: "Independent live market context",
        available: true,
      },
      {
        key: "preview",
        label: "Builder Draft",
        description: "Current unsaved builder draft",
        available: Boolean(builderPreviewRequestPayload()?.preset),
      },
      {
        key: "saved",
        label: "Saved Preset",
        description: "Currently selected saved preset",
        available: Boolean(activeSavedAuditPresetName()),
      },
    ];
    return sources;
  }

  function marketAuditRowHasValidOhlc(row) {
    const open = Number(row?.open);
    const high = Number(row?.high);
    const low = Number(row?.low);
    const close = Number(row?.close);
    if (![open, high, low, close].every((value) => Number.isFinite(value))) return false;
    if (high < low) return false;
    if (high < Math.max(open, close)) return false;
    if (low > Math.min(open, close)) return false;
    return true;
  }

  function marketAuditMedian(values) {
    const numeric = Array.isArray(values)
      ? values.map((value) => Number(value)).filter((value) => Number.isFinite(value)).sort((a, b) => a - b)
      : [];
    if (!numeric.length) return 0;
    return numeric[Math.floor(numeric.length / 2)];
  }

  function normalizeMarketAuditChartRow(row, previousClose = NaN) {
    let open = Number(row?.open);
    let high = Number(row?.high);
    let low = Number(row?.low);
    let close = Number(row?.close);
    const prev = Number(previousClose);
    if (!Number.isFinite(open) && Number.isFinite(prev)) open = prev;
    if (!Number.isFinite(close) && Number.isFinite(open)) close = open;
    if (!Number.isFinite(high) && Number.isFinite(open) && Number.isFinite(close)) high = Math.max(open, close);
    if (!Number.isFinite(low) && Number.isFinite(open) && Number.isFinite(close)) low = Math.min(open, close);
    const volumeRaw = Number(row?.volume);
    const next = {
      ...row,
      open,
      high,
      low,
      close,
      volume: Number.isFinite(volumeRaw) && volumeRaw >= 0 ? volumeRaw : 0,
      _chartValid: false,
    };
    if (![open, high, low, close].every((value) => Number.isFinite(value))) {
      return next;
    }
    next.high = Math.max(high, open, close);
    next.low = Math.min(low, open, close);
    next._chartValid = true;
    return next;
  }

  function normalizeMarketAuditChartRows(rows) {
    const list = Array.isArray(rows) ? rows : [];
    const normalized = [];
    let previousClose = NaN;
    list.forEach((row) => {
      const next = normalizeMarketAuditChartRow(row, previousClose);
      normalized.push(next);
      if (next._chartValid) previousClose = next.close;
    });
    return normalized;
  }

  function marketAuditCacheKey(spec = null, timeframe = null) {
    const source = normalizeMarketAuditSource(spec?.source || marketAuditState.source);
    const tf = String(timeframe || spec?.requestBody?.timeframe || marketAuditState.timeframe || "5m").trim() || "5m";
    if (source === "live") {
      const sessionName = String(spec?.sessionName || liveContextState?.session_name || "live_market").trim() || "live_market";
      return `live:${sessionName}:${tf}`;
    }
    if (source === "saved") {
      const presetName = String(spec?.presetName || activeSavedAuditPresetName() || "saved").trim() || "saved";
      return `saved:${presetName}:${tf}`;
    }
    const previewName = String(spec?.name || builderPreviewRequestPayload()?.name || "builder_preview").trim() || "builder_preview";
    return `preview:${previewName}:${tf}`;
  }

  function marketAuditPayloadStats(payload) {
    const rows = Array.isArray(payload?.rows) ? payload.rows : [];
    const rowCount = Number(payload?.bar_count || rows.length || 0);
    const valid = Number.isFinite(Number(payload?.ohlc_valid_bars))
      ? Number(payload.ohlc_valid_bars)
      : rows.filter((row) => marketAuditRowHasValidOhlc(row)).length;
    const chartRows = normalizeMarketAuditChartRows(rows);
    const chartValid = chartRows.filter((row) => row?._chartValid).length;
    const chartRanges = chartRows
      .filter((row) => row?._chartValid)
      .map((row) => Number(row.high) - Number(row.low))
      .filter((value) => Number.isFinite(value) && value >= 0);
    const medianRange = marketAuditMedian(chartRanges);
    const maxRange = chartRanges.length ? Math.max(...chartRanges) : 0;
    const invalid = Number.isFinite(Number(payload?.ohlc_invalid_bars))
      ? Number(payload.ohlc_invalid_bars)
      : Math.max(0, rowCount - valid);
    const anchorMs = Number(Date.parse(String(payload?.anchor_timestamp_utc || ""))) || 0;
    return {
      rowCount,
      valid,
      chartValid,
      invalid,
      anchorMs,
      medianRange,
      maxRange,
      rangeSpikeRatio: medianRange > 0 ? (maxRange / medianRange) : 0,
    };
  }

  function cacheMarketAuditPayload(cacheKey, payload) {
    if (!cacheKey || !payload) return;
    const next = cloneJsonSafe(payload, payload);
    if (!next) return;
    marketAuditState.payloadCache[cacheKey] = next;
  }

  function restoreCachedMarketAuditPayload(cacheKey, options = {}) {
    const cached = marketAuditState.payloadCache?.[cacheKey];
    if (!cached) return false;
    marketAuditState.payload = cloneJsonSafe(cached, cached) || cached;
    const count = Number(marketAuditState?.payload?.bar_count || marketAuditState?.payload?.rows?.length || 0);
    if (!count) {
      marketAuditState.activeIndex = 0;
      return true;
    }
    if (Boolean(options?.forceLatest) || shouldFollowMarketAuditLive()) {
      marketAuditState.activeIndex = count - 1;
      return true;
    }
    marketAuditState.activeIndex = Math.max(0, Math.min(count - 1, Number(marketAuditState.activeIndex || 0)));
    return true;
  }

  function chooseMarketAuditPayload(cacheKey, nextPayload, options = {}) {
    if (!cacheKey) return nextPayload;
    const previous = marketAuditState.payloadCache?.[cacheKey] || null;
    if (!previous) return nextPayload;
    if (!nextPayload) return previous;
    if (!String(cacheKey).startsWith("live:")) return nextPayload;
    const prevStats = marketAuditPayloadStats(previous);
    const nextStats = marketAuditPayloadStats(nextPayload);
    if (!nextStats.rowCount && prevStats.rowCount) return previous;
    if (!Boolean(options?.background)) return nextPayload;
    if (nextStats.valid === 0 && prevStats.valid > 0) return previous;
    if (nextStats.chartValid === 0 && prevStats.chartValid > 0) return previous;
    if (nextStats.anchorMs && prevStats.anchorMs && nextStats.anchorMs < prevStats.anchorMs) return previous;
    if (nextStats.anchorMs <= prevStats.anchorMs) {
      if (nextStats.valid < prevStats.valid) return previous;
      if (nextStats.chartValid < prevStats.chartValid) return previous;
      if (nextStats.rowCount < prevStats.rowCount) return previous;
    }
    return nextPayload;
  }

  function marketAuditRequestSpec(options = {}) {
    const source = normalizeMarketAuditSource(marketAuditState.source);
    if (source === "live") {
      const requestBody = {
        source: "live",
        session_name: String(liveContextState?.session_name || "live_market").trim() || "live_market",
        timeframe: marketAuditState.timeframe,
        bars: marketAuditState.bars,
      };
      if (!hasAnySnapshotValues()) {
        requestBody.settings = cloneJsonSafe(liveContextState?.settings, liveContextSeedSettings());
      }
      return {
        source: "live",
        sessionName: String(liveContextState?.session_name || "live_market").trim() || "live_market",
        requestBody,
      };
    }
    if (source === "saved") {
      const presetName = activeSavedAuditPresetName();
      if (!presetName) return null;
      return {
        source: "saved",
        presetName,
        requestBody: {
          source: "saved",
          preset_name: presetName,
          timeframe: marketAuditState.timeframe,
          bars: marketAuditState.bars,
        },
      };
    }
    const preview = builderPreviewRequestPayload();
    if (!preview?.preset) return null;
    return {
      source: "preview",
      name: String(preview.name || "builder_preview").trim() || "builder_preview",
      requestBody: {
        source: "preview",
        name: String(preview.name || "builder_preview").trim() || "builder_preview",
        preset: preview.preset,
        timeframe: marketAuditState.timeframe,
        bars: marketAuditState.bars,
      },
    };
  }

  function marketAuditSessionLabel() {
    const spec = marketAuditRequestSpec();
    if (!spec) return "Source is unavailable";
    if (spec.source === "live") {
      return `Live market | ${sanitizeText(liveContextState?.settings?.symbol || liveSnapshotPayload?.symbol || "BTCUSDT")}`;
    }
    if (spec.source === "saved") {
      return `Saved preset | ${sanitizeText(spec.presetName)}`;
    }
    return `Builder draft | ${sanitizeText(spec.name)}`;
  }

  function marketAuditSourceButtons() {
    return marketAuditAvailableSources().map((source) => `
      <button
        class="mini-button ${source.key === normalizeMarketAuditSource(marketAuditState.source) ? "is-active" : ""}"
        type="button"
        data-market-audit-source="${sanitizeText(source.key)}"
        ${source.available ? "" : "disabled"}
        title="${escapeAuditHtml(source.description)}"
      >${escapeAuditHtml(source.label)}</button>
    `).join("");
  }

  function marketAuditTimeframeButtons() {
    const payloadTfs = Array.isArray(marketAuditState?.payload?.available_timeframes)
      ? marketAuditState.payload.available_timeframes
      : [];
    const base = payloadTfs.length ? payloadTfs : selectableTimeframes();
    return base.map((tf) => `
      <button
        class="verify-tf-button ${String(tf) === String(marketAuditState.timeframe) ? "is-active" : ""}"
        type="button"
        data-market-audit-timeframe="${sanitizeText(tf)}"
      >${sanitizeText(tf)}</button>
    `).join("");
  }

  function marketAuditMarkersForIndex(index) {
    const markers = Array.isArray(marketAuditState?.payload?.markers) ? marketAuditState.payload.markers : [];
    return markers.filter((marker) => Number(marker?.bar_index) === Number(index));
  }

  function marketAuditRecentMarkers(index, limit = 10) {
    const markers = Array.isArray(marketAuditState?.payload?.markers) ? marketAuditState.payload.markers : [];
    return markers
      .filter((marker) => Number(marker?.bar_index) <= Number(index))
      .slice(-Math.max(1, Number(limit || 10)));
  }

  function marketAuditTone(value) {
    const text = String(value ?? "").toUpperCase();
    if (!text) return "";
    if (["UPTREND", "HH_HL", "EXPANSION", "CONFIRM_LONG", "WATCH_LONG", "SUPPORTIVE", "BUY", "LONG"].includes(text)) return "is-positive";
    if (["DOWNTREND", "LH_LL", "FAIL_LONG", "FAIL_SHORT", "WATCH_SHORT", "CONFIRM_SHORT", "WEAK", "SELL", "SHORT", "BROKEN_UPTREND", "BROKEN_DOWNTREND"].includes(text)) return "is-negative";
    if (["BALANCE", "FLAT", "MIXED", "NONE", "DRY", "NEUTRAL", "PULLBACK", "EXHAUSTION", "CLIMACTIC"].includes(text)) return "is-neutral";
    return "";
  }

  function renderAuditStateCard(label, value) {
    const tone = marketAuditTone(value);
    return `
      <article class="market-audit-state-card ${tone}">
        <div class="eyebrow">${escapeAuditHtml(label)}</div>
        <strong>${escapeAuditHtml(value ?? "--")}</strong>
      </article>
    `;
  }

  function renderMarketAuditSelectedMarkers(markers, emptyText = "No context changes on this selected candle.") {
    return `
      <div class="market-audit-chart-marker-strip">
        <div class="eyebrow">Selected Candle Changes</div>
        <div class="market-audit-marker-list">
          ${markers.length
            ? markers.map((marker) => `<span class="market-audit-marker-chip is-${escapeAuditHtml(marker.kind || "state_change")}">${escapeAuditHtml(marker.label || marker.field || "Marker")}</span>`).join("")
            : `<span class="muted">${escapeAuditHtml(emptyText)}</span>`}
        </div>
      </div>
    `;
  }

  function formatMarketAuditAxisLabel(timestamp, includeDate = false) {
    if (!timestamp) return "";
    const date = new Date(timestamp);
    if (!Number.isFinite(date.getTime())) return "";
    try {
      return new Intl.DateTimeFormat(undefined, includeDate
        ? { month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit", hour12: false }
        : { hour: "2-digit", minute: "2-digit", hour12: false }).format(date);
    } catch (error) {
      return includeDate
        ? sanitizeText(timestamp).slice(5, 16).replace("T", " ")
        : sanitizeText(timestamp).slice(11, 16);
    }
  }

  function renderMarketAuditChart(payload, activeIndex) {
    const rows = normalizeMarketAuditChartRows(payload?.rows);
    if (!rows.length) {
      return '<div class="empty-state">No candle data in this audit window yet.</div>';
    }
    const selectedGlobalIndex = Math.max(0, Math.min(rows.length - 1, Number(activeIndex || 0)));
    const maxVisibleBars = Math.min(90, Math.max(36, rows.length));
    const visibleCount = Math.min(rows.length, maxVisibleBars);
    const rightContext = Math.min(12, Math.max(4, Math.floor(visibleCount * 0.18)));
    const leftContext = Math.max(0, visibleCount - rightContext - 1);
    const startIndex = Math.max(0, Math.min(selectedGlobalIndex - leftContext, rows.length - visibleCount));
    const visibleRows = rows.slice(startIndex, startIndex + visibleCount);
    const markersByIndex = new Map();
    (Array.isArray(payload?.markers) ? payload.markers : []).forEach((marker) => {
      const index = Number(marker?.bar_index);
      if (!Number.isFinite(index)) return;
      const list = markersByIndex.get(index) || [];
      list.push(marker);
      markersByIndex.set(index, list);
    });
    const selectedMarkers = markersByIndex.get(selectedGlobalIndex) || [];
    const maxMarkerCount = Array.from(markersByIndex.values()).reduce((best, list) => Math.max(best, Array.isArray(list) ? list.length : 0), 0);
    const markerBandHeight = maxMarkerCount > 0 ? (12 + Math.max(0, maxMarkerCount - 1) * 10) : 0;
    const chartHeight = 320 + markerBandHeight;
    const chartPaddingLeft = 52;
    const chartPaddingRight = 20;
    const chartPaddingTop = 18 + markerBandHeight;
    const chartPaddingBottom = 34;
    const priceZoneBottom = chartHeight - 98;
    const volumeZoneTop = chartHeight - 80;
    const volumeZoneBottom = chartHeight - chartPaddingBottom;
    const candleStep = 12;
    const candleWidth = 5.5;
    const width = Math.max(860, chartPaddingLeft + chartPaddingRight + visibleRows.length * candleStep);
    const priceBodyHeight = priceZoneBottom - chartPaddingTop;
    const validPriceRows = visibleRows.filter((row) => Boolean(row?._chartValid));
    const rowRanges = validPriceRows
      .map((row) => Number(row.high) - Number(row.low))
      .filter((value) => Number.isFinite(value) && value >= 0);
    const medianRange = marketAuditMedian(rowRanges);
    let scaleRows = validPriceRows;
    if (medianRange > 0 && validPriceRows.length >= 12) {
      const filteredRows = validPriceRows.filter((row) => (Number(row.high) - Number(row.low)) <= (medianRange * 8));
      if (filteredRows.length >= Math.max(8, Math.floor(validPriceRows.length * 0.7))) {
        scaleRows = filteredRows;
      }
    }
    const highs = scaleRows.map((row) => Number(row.high)).filter((value) => Number.isFinite(value));
    const lows = scaleRows.map((row) => Number(row.low)).filter((value) => Number.isFinite(value));
    const volumes = visibleRows.map((row) => Number(row.volume)).filter((value) => Number.isFinite(value) && value >= 0);
    let minLow = lows.length ? Math.min(...lows) : 0;
    let maxHigh = highs.length ? Math.max(...highs) : 1;
    const rawPriceRange = Math.max(1e-9, maxHigh - minLow);
    const scalePadding = Math.max(rawPriceRange * 0.08, medianRange * 0.6, 1e-6);
    minLow -= scalePadding;
    maxHigh += scalePadding;
    const priceRange = Math.max(1e-9, maxHigh - minLow);
    const maxVolume = volumes.length ? Math.max(...volumes) : 0;
    const scaleY = (price) => {
      const num = Number(price);
      if (!Number.isFinite(num)) return chartHeight / 2;
      const normalized = (num - minLow) / priceRange;
      const clamped = Math.max(-0.12, Math.min(1.12, normalized));
      return chartPaddingTop + (priceBodyHeight - clamped * priceBodyHeight);
    };
    const scaleVolumeY = (volume) => {
      const num = Number(volume);
      if (!Number.isFinite(num) || maxVolume <= 0) return volumeZoneBottom;
      const normalized = Math.max(0, num) / maxVolume;
      return volumeZoneBottom - (normalized * (volumeZoneBottom - volumeZoneTop));
    };
    const selected = rows[selectedGlobalIndex] || null;
    const selectedHigh = Number(selected?.market_protected_high);
    const selectedLow = Number(selected?.market_protected_low);
    const selectedSupportZoneLow = Number(selected?.market_support_zone_low);
    const selectedSupportZoneHigh = Number(selected?.market_support_zone_high);
    const selectedResistanceZoneLow = Number(selected?.market_resistance_zone_low);
    const selectedResistanceZoneHigh = Number(selected?.market_resistance_zone_high);
    const selectedBoundaryHigh = Number(selected?.market_boundary_high);
    const selectedBoundaryLow = Number(selected?.market_boundary_low);

    const gridLines = [
      maxHigh,
      (maxHigh + minLow) / 2,
      minLow,
    ].map((price) => `
      <line class="market-audit-grid-line" x1="${chartPaddingLeft - 8}" y1="${scaleY(price)}" x2="${width - chartPaddingRight + 4}" y2="${scaleY(price)}"></line>
    `).join("");

    const candles = visibleRows.map((row, localIndex) => {
      const globalIndex = startIndex + localIndex;
      const x = chartPaddingLeft + localIndex * candleStep;
      const open = Number(row.open);
      const close = Number(row.close);
      const high = Number(row.high);
      const low = Number(row.low);
      const volume = Number(row.volume);
      const validOhlc = Boolean(row?._chartValid);
      const openY = validOhlc ? scaleY(open) : null;
      const closeY = validOhlc ? scaleY(close) : null;
      const highY = validOhlc ? scaleY(high) : null;
      const lowY = validOhlc ? scaleY(low) : null;
      const bodyTop = validOhlc ? Math.min(openY, closeY) : null;
      const bodyHeightPx = validOhlc ? Math.max(1.5, Math.abs(openY - closeY)) : null;
      const toneClass = close >= open ? "is-up" : "is-down";
      const selectedClass = globalIndex === selectedGlobalIndex ? "is-selected" : "";
      const barMarkers = markersByIndex.get(globalIndex) || [];
      const volumeTop = scaleVolumeY(volume);
      const markerDots = barMarkers.map((marker, markerIndex) => {
        const dotY = chartPaddingTop - 8 - markerIndex * 10;
        const dotTone = marker.kind === "event" ? "event" : "state";
        return `<circle class="market-audit-marker-dot is-${dotTone}" cx="${x}" cy="${dotY}" r="3.2" data-market-audit-select="${globalIndex}"><title>${escapeAuditHtml(marker.label || marker.field || "Marker")}</title></circle>`;
      }).join("");
      return `
        <g class="market-audit-candle ${toneClass} ${selectedClass}">
          ${globalIndex === selectedGlobalIndex ? `<rect class="market-audit-selection-band" x="${x - candleStep / 2}" y="0" width="${candleStep}" height="${chartHeight}"></rect>` : ""}
          <rect class="market-audit-volume-bar" x="${x - candleWidth / 2}" y="${volumeTop}" width="${candleWidth}" height="${Math.max(1, volumeZoneBottom - volumeTop)}" rx="1.2"></rect>
          ${validOhlc ? `<line class="market-audit-wick" x1="${x}" y1="${highY}" x2="${x}" y2="${lowY}" data-market-audit-select="${globalIndex}"></line>` : ""}
          ${validOhlc ? `<rect class="market-audit-body" x="${x - candleWidth / 2}" y="${bodyTop}" width="${candleWidth}" height="${bodyHeightPx}" rx="1.5" data-market-audit-select="${globalIndex}"></rect>` : ""}
          ${markerDots}
        </g>
      `;
    }).join("");

    const referenceLines = [];
    const referenceZones = [];
    if (Number.isFinite(selectedSupportZoneLow) && Number.isFinite(selectedSupportZoneHigh)) {
      const y1 = scaleY(selectedSupportZoneHigh);
      const y2 = scaleY(selectedSupportZoneLow);
      referenceZones.push(`<rect class="market-audit-zone-band is-support" x="${chartPaddingLeft - 8}" y="${Math.min(y1, y2)}" width="${width - chartPaddingLeft - chartPaddingRight + 12}" height="${Math.max(2, Math.abs(y2 - y1))}"></rect>`);
    }
    if (Number.isFinite(selectedResistanceZoneLow) && Number.isFinite(selectedResistanceZoneHigh)) {
      const y1 = scaleY(selectedResistanceZoneHigh);
      const y2 = scaleY(selectedResistanceZoneLow);
      referenceZones.push(`<rect class="market-audit-zone-band is-resistance" x="${chartPaddingLeft - 8}" y="${Math.min(y1, y2)}" width="${width - chartPaddingLeft - chartPaddingRight + 12}" height="${Math.max(2, Math.abs(y2 - y1))}"></rect>`);
    }
    if (Number.isFinite(selectedHigh)) {
      referenceLines.push(`<line class="market-audit-ref-line is-protected-high" x1="${chartPaddingLeft - 8}" y1="${scaleY(selectedHigh)}" x2="${width - chartPaddingRight + 4}" y2="${scaleY(selectedHigh)}"></line>`);
    }
    if (Number.isFinite(selectedLow)) {
      referenceLines.push(`<line class="market-audit-ref-line is-protected-low" x1="${chartPaddingLeft - 8}" y1="${scaleY(selectedLow)}" x2="${width - chartPaddingRight + 4}" y2="${scaleY(selectedLow)}"></line>`);
    }
    if (Number.isFinite(selectedBoundaryHigh)) {
      referenceLines.push(`<line class="market-audit-ref-line is-boundary-high" x1="${chartPaddingLeft - 8}" y1="${scaleY(selectedBoundaryHigh)}" x2="${width - chartPaddingRight + 4}" y2="${scaleY(selectedBoundaryHigh)}"></line>`);
    }
    if (Number.isFinite(selectedBoundaryLow)) {
      referenceLines.push(`<line class="market-audit-ref-line is-boundary-low" x1="${chartPaddingLeft - 8}" y1="${scaleY(selectedBoundaryLow)}" x2="${width - chartPaddingRight + 4}" y2="${scaleY(selectedBoundaryLow)}"></line>`);
    }

    const priceAxisLabels = [
      { price: maxHigh, label: displayValue(maxHigh, 2) },
      { price: (maxHigh + minLow) / 2, label: displayValue((maxHigh + minLow) / 2, 2) },
      { price: minLow, label: displayValue(minLow, 2) },
    ].map((item) => `
      <text class="market-audit-axis-label" x="4" y="${scaleY(item.price) + 4}">${escapeAuditHtml(item.label)}</text>
    `).join("");

    const tickCount = Math.min(6, Math.max(3, visibleRows.length >= 40 ? 6 : 4));
    const xAxisLabels = Array.from({ length: tickCount }, (_, tickIndex) => {
      const localIndex = tickCount === 1
        ? visibleRows.length - 1
        : Math.round((visibleRows.length - 1) * (tickIndex / (tickCount - 1)));
      const row = visibleRows[Math.max(0, Math.min(visibleRows.length - 1, localIndex))];
      const x = chartPaddingLeft + localIndex * candleStep;
      const includeDate = tickIndex === 0 || tickIndex === tickCount - 1;
      return `
        <g class="market-audit-x-tick">
          <line class="market-audit-x-tick-line" x1="${x}" y1="${volumeZoneTop}" x2="${x}" y2="${volumeZoneBottom + 4}"></line>
          <text class="market-audit-x-axis-label" x="${x}" y="${chartHeight - 10}" text-anchor="middle">${escapeAuditHtml(formatMarketAuditAxisLabel(row?.timestamp_utc, includeDate))}</text>
        </g>
      `;
    }).join("");

    const rangeText = visibleRows.length
      ? `${sanitizeText(formatMarketAuditAxisLabel(visibleRows[0]?.timestamp_utc, true))} → ${sanitizeText(formatMarketAuditAxisLabel(visibleRows[visibleRows.length - 1]?.timestamp_utc, true))}`
      : "";

    return `
      <div class="market-audit-chart-shell">
        ${rangeText ? `<div class="market-audit-chart-range">${escapeAuditHtml(rangeText)}</div>` : ""}
        ${renderMarketAuditSelectedMarkers(selectedMarkers)}
        <svg class="market-audit-chart" viewBox="0 0 ${width} ${chartHeight}" width="${width}" height="${chartHeight}">
          <rect x="0" y="0" width="${width}" height="${chartHeight}" rx="20" class="market-audit-chart-bg"></rect>
          ${gridLines}
          ${referenceZones.join("")}
          ${referenceLines.join("")}
          <line class="market-audit-volume-divider" x1="${chartPaddingLeft - 8}" y1="${volumeZoneTop}" x2="${width - chartPaddingRight + 4}" y2="${volumeZoneTop}"></line>
          ${candles}
          ${priceAxisLabels}
          ${xAxisLabels}
        </svg>
      </div>
    `;
  }

  function renderMarketAuditTimeframeSummary(payload) {
    const contexts = payload?.latest_context_by_timeframe || {};
    const entries = Object.entries(contexts);
    if (!entries.length) {
      return '<div class="empty-state">No higher-timeframe context available yet.</div>';
    }
    return `
      <div class="market-audit-context-list">
        ${entries.map(([tf, item]) => `
          <article class="market-audit-context-card ${String(tf) === String(marketAuditState.timeframe) ? "is-active" : ""}">
            <div class="market-audit-context-head">
              <strong>${escapeAuditHtml(tf)}</strong>
              <span class="muted">${escapeAuditHtml(item?.timestamp_utc || "")}</span>
            </div>
            <div class="market-audit-context-grid">
              <span>${escapeAuditHtml(item?.market_trend_state || "--")}</span>
              <span>${escapeAuditHtml(item?.market_structure_state || "--")}</span>
              <span>${escapeAuditHtml(item?.market_phase_state || "--")}</span>
              <span>${escapeAuditHtml(item?.market_breakout_signal || "--")}</span>
            </div>
          </article>
        `).join("")}
      </div>
    `;
  }

  function formatMarketAuditWindowLabel(start, end) {
    const left = formatMarketAuditAxisLabel(start, true);
    const right = formatMarketAuditAxisLabel(end, true);
    if (!left && !right) return "--";
    if (!left) return right;
    if (!right) return left;
    return `${left} -> ${right}`;
  }

  function renderMarketAuditBasisCard(payload, row) {
    const hh = row?.market_structure_swing_hh ? "Y" : "N";
    const hl = row?.market_structure_swing_hl ? "Y" : "N";
    const lh = row?.market_structure_swing_lh ? "Y" : "N";
    const ll = row?.market_structure_swing_ll ? "Y" : "N";
    const ohlcLoaded = Number(payload?.bar_count || 0);
    const ohlcValid = Number(payload?.ohlc_valid_bars || 0);
    const includesForming = Boolean(payload?.includes_forming_bar);
    return `
      <section class="market-audit-card">
        <div class="eyebrow">Context Basis</div>
        <div class="market-audit-kv">
          <span>Visible Window</span><strong>${escapeAuditHtml(formatMarketAuditWindowLabel(payload?.window_start_utc, payload?.window_end_utc))}</strong>
          <span>Analysis Window</span><strong>${escapeAuditHtml(formatMarketAuditWindowLabel(payload?.analysis_window_start_utc, payload?.analysis_window_end_utc || payload?.request_end_utc))}</strong>
          <span>Lookback</span><strong>${escapeAuditHtml(`${displayValue(payload?.analysis_lookback_minutes, 0)}m`)}</strong>
          <span>Warmup</span><strong>${escapeAuditHtml(`${displayValue(payload?.analysis_warmup_minutes, 0)}m`)}</strong>
          <span>Requested Bars</span><strong>${escapeAuditHtml(displayValue(payload?.requested_bars, 0))}</strong>
          <span>Loaded Bars</span><strong>${escapeAuditHtml(displayValue(payload?.bar_count, 0))}</strong>
          <span>OHLC Valid</span><strong>${escapeAuditHtml(`${displayValue(ohlcValid, 0)} / ${displayValue(ohlcLoaded, 0)}`)}</strong>
          <span>Includes Forming</span><strong>${escapeAuditHtml(includesForming ? "YES" : "NO")}</strong>
          <span>Structure Basis</span><strong>${escapeAuditHtml(row?.market_structure_basis || "--")}</strong>
          <span>Structure Context</span><strong>${escapeAuditHtml(row?.market_structure_context || "--")}</strong>
          <span>Market Type</span><strong>${escapeAuditHtml(row?.market_market_type || "--")}</strong>
          <span>Trend Regime</span><strong>${escapeAuditHtml(row?.market_trend_regime || "--")}</strong>
          <span>Pivot Bias</span><strong>${escapeAuditHtml(row?.market_structure_pivot_bias || "--")}</strong>
          <span>Pivot Count</span><strong>${escapeAuditHtml(displayValue(row?.market_structure_pivot_count, 0))}</strong>
          <span>HH / HL / LH / LL</span><strong>${escapeAuditHtml(`${hh} / ${hl} / ${lh} / ${ll}`)}</strong>
          <span>Last Swing High</span><strong>${escapeAuditHtml(displayValue(row?.market_structure_last_high, 2))}</strong>
          <span>Last Swing Low</span><strong>${escapeAuditHtml(displayValue(row?.market_structure_last_low, 2))}</strong>
        </div>
        <div class="eyebrow market-audit-subhead">Structure Note</div>
        <strong>${escapeAuditHtml(row?.market_structure_note || "--")}</strong>
      </section>
    `;
  }

  function renderMarketAuditDynamicSections(payload) {
    const row = activeMarketAuditRow();
    const index = syncMarketAuditIndex();
    const markers = row ? marketAuditMarkersForIndex(index) : [];
    const recentMarkers = row ? marketAuditRecentMarkers(index, 10) : [];
    const reasons = Array.isArray(row?.market_context_reason_list) ? row.market_context_reason_list : [];
    const progressText = payload?.bar_count
      ? `Candle ${index + 1} / ${payload.bar_count} | ${sanitizeText(row?.timestamp_utc || "")}`
      : "No candle selected";

    return {
      progressText,
      stateGridHtml: `
        <div class="market-audit-state-grid">
          ${renderAuditStateCard("Market", row?.market_state || "--")}
          ${renderAuditStateCard("Trend", row?.market_trend_state || "--")}
          ${renderAuditStateCard("Structure", row?.market_structure_state || "--")}
          ${renderAuditStateCard("Phase", row?.market_phase_state || "--")}
          ${renderAuditStateCard("Pullback", row?.market_pullback_state || "--")}
          ${renderAuditStateCard("Breakout", row?.market_breakout_signal || "--")}
          ${renderAuditStateCard("Volume", row?.market_volume_context || "--")}
        </div>
      `,
      chartHtml: renderMarketAuditChart(payload, index),
      detailsHtml: `
        <div class="market-audit-detail-grid">
          <section class="market-audit-card">
            <div class="eyebrow">Why</div>
            <strong>${escapeAuditHtml(row?.market_context_note || "--")}</strong>
            <ul class="market-audit-reason-list">
              ${reasons.length ? reasons.map((reason) => `<li>${escapeAuditHtml(reason)}</li>`).join("") : '<li class="muted">No context reasons for this candle.</li>'}
            </ul>
          </section>
          <section class="market-audit-card">
            <div class="eyebrow">Selected Candle</div>
            <div class="muted">This is the active bar in the audit window: latest in live-follow, or the candle you clicked / replayed to.</div>
            <div class="market-audit-kv">
              <span>Open</span><strong>${escapeAuditHtml(displayValue(row?.open, 2))}</strong>
              <span>High</span><strong>${escapeAuditHtml(displayValue(row?.high, 2))}</strong>
              <span>Low</span><strong>${escapeAuditHtml(displayValue(row?.low, 2))}</strong>
              <span>Close</span><strong>${escapeAuditHtml(displayValue(row?.close, 2))}</strong>
              <span>Volume</span><strong>${escapeAuditHtml(displayValue(row?.volume, 2))}</strong>
              <span>Protected High</span><strong>${escapeAuditHtml(displayValue(row?.market_protected_high, 2))}</strong>
              <span>Protected Low</span><strong>${escapeAuditHtml(displayValue(row?.market_protected_low, 2))}</strong>
              <span>Support Zone</span><strong>${escapeAuditHtml(`${displayValue(row?.market_support_zone_low, 2)} -> ${displayValue(row?.market_support_zone_high, 2)}`)}</strong>
              <span>Resistance Zone</span><strong>${escapeAuditHtml(`${displayValue(row?.market_resistance_zone_low, 2)} -> ${displayValue(row?.market_resistance_zone_high, 2)}`)}</strong>
              <span>Base Top</span><strong>${escapeAuditHtml(displayValue(row?.market_boundary_high, 2))}</strong>
              <span>Base Bottom</span><strong>${escapeAuditHtml(displayValue(row?.market_boundary_low, 2))}</strong>
            </div>
          </section>
          ${renderMarketAuditBasisCard(payload, row)}
          <section class="market-audit-card">
            <div class="eyebrow">Markers At Candle</div>
            <div class="market-audit-marker-list">
              ${markers.length ? markers.map((marker) => `<span class="market-audit-marker-chip is-${escapeAuditHtml(marker.kind || "state")}">${escapeAuditHtml(marker.label || marker.field || "Marker")}</span>`).join("") : '<span class="muted">No transition or trigger markers on this candle.</span>'}
            </div>
            <div class="eyebrow market-audit-subhead">Recent Markers</div>
            <div class="market-audit-marker-stack">
              ${recentMarkers.length ? recentMarkers.map((marker) => `
                <div class="market-audit-marker-row">
                  <strong>${escapeAuditHtml(marker.label || marker.field || "Marker")}</strong>
                  <span>${escapeAuditHtml(marker.timestamp_utc || "")}</span>
                </div>
              `).join("") : '<div class="muted">No recent markers yet.</div>'}
            </div>
          </section>
        </div>
        <section class="market-audit-card" id="market-audit-context-host">
          <div class="eyebrow">Timeframe Context</div>
          ${renderMarketAuditTimeframeSummary(payload)}
        </section>
      `,
    };
  }

  function renderMarketAuditModal() {
    const payload = marketAuditState.payload;
    const isLiveSource = normalizeMarketAuditSource(marketAuditState.source) === "live";
    const playLabel = marketAuditState.playTimer ? (isLiveSource ? "Pause Replay" : "Pause") : (isLiveSource ? "Replay" : "Play");
    const dynamic = payload ? renderMarketAuditDynamicSections(payload) : null;
    const progressText = dynamic?.progressText || "No candle selected";
    const index = payload ? Number(marketAuditState.activeIndex || 0) : 0;

    return `
      <div class="market-audit-modal">
        <div class="market-audit-toolbar">
          <div>
            <div class="eyebrow">Market Audit Module</div>
            <div class="market-audit-caption">${escapeAuditHtml(marketAuditSessionLabel())}${payload?.audit_source_note ? ` | ${escapeAuditHtml(payload.audit_source_note)}` : ""}</div>
          </div>
          <div class="market-audit-toolbar-controls">
            <div class="market-audit-source-row">${marketAuditSourceButtons()}</div>
            <div class="verify-tf-row">${marketAuditTimeframeButtons()}</div>
            <label class="market-audit-bars-input">
              <span>Bars</span>
              <input type="number" min="1" max="1200" step="1" data-market-audit-bars value="${escapeAuditHtml(marketAuditState.bars)}" />
            </label>
            <button class="mini-button" type="button" data-market-audit-action="reload">Reload</button>
          </div>
        </div>
        ${marketAuditState.loading ? `<div class="empty-state">Loading market audit for ${escapeAuditHtml(marketAuditState.timeframe)}...</div>` : ""}
        ${marketAuditState.error ? `<div class="downloader-field-modal-note">${escapeAuditHtml(marketAuditState.error)}</div>` : ""}
        ${payload ? `
          <div class="market-audit-playbar">
            <div class="market-audit-play-controls">
              <button class="mini-button" type="button" data-market-audit-action="prev" ${index <= 0 ? "disabled" : ""}>Prev</button>
              <button class="mini-button" type="button" data-market-audit-action="play">${playLabel}</button>
              <button class="mini-button" type="button" data-market-audit-action="next" ${index >= (Number(payload.bar_count || 1) - 1) ? "disabled" : ""}>Next</button>
              <button class="mini-button" type="button" data-market-audit-action="jump-end" ${index >= (Number(payload.bar_count || 1) - 1) ? "disabled" : ""}>Latest</button>
              ${normalizeMarketAuditSource(marketAuditState.source) === "live"
                ? `<button class="mini-button ${marketAuditState.followLive ? "is-active" : ""}" type="button" data-market-audit-action="follow-live">${marketAuditState.followLive ? "Follow Live On" : "Follow Live Off"}</button>`
                : ""}
            </div>
            <div class="market-audit-progress" id="market-audit-progress">${escapeAuditHtml(progressText)}</div>
          </div>
          <div id="market-audit-state-grid-host">${dynamic?.stateGridHtml || ""}</div>
          <div id="market-audit-chart-host">${dynamic?.chartHtml || ""}</div>
          <div id="market-audit-details-host">${dynamic?.detailsHtml || ""}</div>
        ` : (!marketAuditState.loading && !marketAuditState.error ? '<div class="empty-state">Choose a market-audit source, then load a candle-by-candle audit window.</div>' : "")}
      </div>
    `;
  }

  function updateMarketAuditSelectionOnly() {
    const backdrop = byId("detail-modal-backdrop");
    if (backdrop?.dataset.mode !== "market-audit" || backdrop.hidden) return false;
    const payload = marketAuditState.payload;
    if (!payload) return false;
    const progressNode = byId("market-audit-progress");
    const stateGridHost = byId("market-audit-state-grid-host");
    const chartHost = byId("market-audit-chart-host");
    const detailsHost = byId("market-audit-details-host");
    if (!progressNode || !stateGridHost || !chartHost || !detailsHost) return false;
    captureMarketAuditChartScroll();
    const dynamic = renderMarketAuditDynamicSections(payload);
    progressNode.textContent = sanitizeText(dynamic.progressText);
    stateGridHost.innerHTML = dynamic.stateGridHtml;
    chartHost.innerHTML = dynamic.chartHtml;
    detailsHost.innerHTML = dynamic.detailsHtml;
    window.requestAnimationFrame(restoreMarketAuditChartScroll);
    return true;
  }

  function renderOrUpdateMarketAuditModal() {
    if (marketAuditState.renderQueued) return;
    marketAuditState.renderQueued = true;
    window.requestAnimationFrame(() => {
      marketAuditState.renderQueued = false;
      renderOrUpdateMarketAuditModalNow();
    });
  }

  function renderOrUpdateMarketAuditModalNow() {
    const title = `Market Audit - ${sanitizeText(marketAuditState.timeframe || "-")}`;
    let html = "";
    try {
      html = renderMarketAuditModal();
    } catch (error) {
      console.error("Market audit modal render failed", error);
      marketAuditState.loading = false;
      marketAuditState.fetchInFlight = false;
      marketAuditState.error = error?.message || String(error || "Unable to render market audit.");
      html = `
        <div class="market-audit-modal">
          <div class="market-audit-toolbar">
            <div>
              <div class="eyebrow">Market Audit Module</div>
              <div class="market-audit-caption">${escapeAuditHtml(marketAuditSessionLabel())}</div>
            </div>
          </div>
          <div class="downloader-field-modal-note">Market Audit render failed: ${escapeAuditHtml(marketAuditState.error)}</div>
        </div>
      `;
    }
    const backdrop = byId("detail-modal-backdrop");
    const body = byId("detail-modal-body");
    const titleNode = byId("detail-modal-title");
    const shell = document.querySelector("#detail-modal-backdrop .detail-modal");
    if (backdrop && body && titleNode && !backdrop.hidden && backdrop.dataset.mode === "market-audit") {
      captureMarketAuditChartScroll();
      titleNode.textContent = sanitizeText(title);
      body.innerHTML = html;
      if (shell) shell.classList.add("is-wide");
      window.requestAnimationFrame(restoreMarketAuditChartScroll);
      return;
    }
    if (typeof showDetailModal === "function") {
      showDetailModal(title, html, { mode: "market-audit", wide: true });
      sanitizeTree(byId("detail-modal-backdrop"));
      window.requestAnimationFrame(restoreMarketAuditChartScroll);
    }
  }

  async function loadMarketAudit(options = {}) {
    const spec = marketAuditRequestSpec(options);
    const background = Boolean(options?.background);
    const forceLatest = Boolean(options?.forceLatest);
    const keepIndex = Boolean(options?.keepIndex);
    const cacheKey = marketAuditCacheKey(spec);
    const prevIndex = Number(marketAuditState.activeIndex || 0);
    const requestId = Number(marketAuditState.requestSeq || 0) + 1;
    marketAuditState.requestSeq = requestId;
    if (marketAuditState.fetchController && typeof marketAuditState.fetchController.abort === "function") {
      try {
        marketAuditState.fetchController.abort();
      } catch (error) {
        /* noop */
      }
    }
    const controller = typeof AbortController === "function" ? new AbortController() : null;
    marketAuditState.fetchController = controller;
    marketAuditState.fetchInFlight = true;
    marketAuditState.error = "";
    if (!background) {
      marketAuditState.loading = true;
      renderOrUpdateMarketAuditModal();
    }
    if (!spec || typeof api !== "function") {
      marketAuditState.loading = false;
      marketAuditState.error = "The selected market-audit source is unavailable right now.";
      marketAuditState.payload = null;
      marketAuditState.fetchInFlight = false;
      marketAuditState.fetchController = null;
      renderOrUpdateMarketAuditModal();
      return;
    }
    try {
      const payload = await api("/api/v2/market-audit", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(spec.requestBody),
        signal: controller?.signal,
      });
      if (requestId !== Number(marketAuditState.requestSeq || 0)) return;
      const chosenPayload = chooseMarketAuditPayload(cacheKey, payload || null, { background });
      marketAuditState.payload = chosenPayload || null;
      if (chosenPayload) {
        cacheMarketAuditPayload(cacheKey, chosenPayload);
      }
      marketAuditState.error = "";
      const count = Number(chosenPayload?.bar_count || chosenPayload?.rows?.length || 0);
      if (!count) {
        marketAuditState.activeIndex = 0;
      } else if (forceLatest || (normalizeMarketAuditSource(marketAuditState.source) === "live" && marketAuditState.followLive)) {
        marketAuditState.activeIndex = count - 1;
      } else if (keepIndex) {
        marketAuditState.activeIndex = Math.max(0, Math.min(count - 1, prevIndex));
      } else {
        marketAuditState.activeIndex = count - 1;
      }
    } catch (error) {
      if (controller?.signal?.aborted) return;
      if (requestId !== Number(marketAuditState.requestSeq || 0)) return;
      marketAuditState.error = error?.message || String(error || "Unable to load market audit.");
    } finally {
      if (requestId === Number(marketAuditState.requestSeq || 0)) {
        marketAuditState.loading = false;
        marketAuditState.fetchInFlight = false;
        marketAuditState.fetchController = null;
        ensureMarketAuditLiveRefresh();
        renderOrUpdateMarketAuditModal();
      }
    }
  }

  function openMarketAuditModal() {
    stopMarketAuditPlayback();
    stopMarketAuditLiveRefresh();
    stopMarketAuditFetch();
    marketAuditState.open = true;
    marketAuditState.source = normalizeMarketAuditSource(marketAuditState.source);
    marketAuditState.followLive = marketAuditState.source === "live";
    marketAuditState.timeframe = String(activeSnapshotTimeframe || marketAuditState.timeframe || "5m");
    restoreCachedMarketAuditPayload(marketAuditCacheKey(), { forceLatest: marketAuditState.followLive });
    renderOrUpdateMarketAuditModal();
    void loadMarketAudit({ background: Boolean(marketAuditState.payload), forceLatest: marketAuditState.followLive });
  }

  function stepMarketAudit(delta) {
    const count = Number(marketAuditState?.payload?.bar_count || marketAuditState?.payload?.rows?.length || 0);
    if (!count) return;
    disableMarketAuditLiveFollow();
    marketAuditState.activeIndex = Math.max(0, Math.min(count - 1, Number(marketAuditState.activeIndex || 0) + Number(delta || 0)));
    if (!updateMarketAuditSelectionOnly()) renderOrUpdateMarketAuditModal();
  }

  function toggleMarketAuditPlayback() {
    const count = Number(marketAuditState?.payload?.bar_count || marketAuditState?.payload?.rows?.length || 0);
    if (!count) return;
    if (marketAuditState.playTimer) {
      stopMarketAuditPlayback();
      ensureMarketAuditLiveRefresh();
      renderOrUpdateMarketAuditModal();
      return;
    }
    disableMarketAuditLiveFollow();
    marketAuditState.playTimer = window.setInterval(() => {
      const maxIndex = Math.max(0, count - 1);
      if (Number(marketAuditState.activeIndex || 0) >= maxIndex) {
        stopMarketAuditPlayback();
        if (!updateMarketAuditSelectionOnly()) renderOrUpdateMarketAuditModal();
        return;
      }
      marketAuditState.activeIndex += 1;
      if (!updateMarketAuditSelectionOnly()) renderOrUpdateMarketAuditModal();
    }, MARKET_AUDIT_PLAY_MS);
    if (!updateMarketAuditSelectionOnly()) renderOrUpdateMarketAuditModal();
  }

  function handleMarketAuditModalClick(event) {
    const backdrop = byId("detail-modal-backdrop");
    if (backdrop?.dataset.mode !== "market-audit") return;
    const sourceNode = event.target.closest("[data-market-audit-source]");
    if (sourceNode) {
      const nextSource = normalizeMarketAuditSource(sourceNode.dataset.marketAuditSource);
      if (nextSource !== marketAuditState.source) {
        stopMarketAuditPlayback();
        stopMarketAuditLiveRefresh();
        marketAuditState.source = nextSource;
        marketAuditState.followLive = nextSource === "live";
        restoreCachedMarketAuditPayload(marketAuditCacheKey(), { forceLatest: marketAuditState.followLive });
        renderOrUpdateMarketAuditModal();
        void loadMarketAudit({ background: Boolean(marketAuditState.payload), forceLatest: marketAuditState.followLive });
      }
      return;
    }
    const timeframeNode = event.target.closest("[data-market-audit-timeframe]");
    if (timeframeNode) {
      const nextTf = String(timeframeNode.dataset.marketAuditTimeframe || "").trim();
      if (nextTf && nextTf !== marketAuditState.timeframe) {
        stopMarketAuditPlayback();
        marketAuditState.timeframe = nextTf;
        if (normalizeMarketAuditSource(marketAuditState.source) === "live") {
          marketAuditState.followLive = true;
        }
        restoreCachedMarketAuditPayload(marketAuditCacheKey(null, nextTf), { forceLatest: marketAuditState.followLive });
        renderOrUpdateMarketAuditModal();
        void loadMarketAudit({ background: Boolean(marketAuditState.payload), forceLatest: marketAuditState.followLive });
      }
      return;
    }
    const selectNode = event.target.closest("[data-market-audit-select]");
    if (selectNode) {
      const nextIndex = Number(selectNode.dataset.marketAuditSelect);
      if (Number.isFinite(nextIndex)) {
        if (Number(marketAuditState.activeIndex || 0) === nextIndex) return;
        stopMarketAuditPlayback();
        disableMarketAuditLiveFollow();
        marketAuditState.activeIndex = nextIndex;
        if (!updateMarketAuditSelectionOnly()) renderOrUpdateMarketAuditModal();
      }
      return;
    }
    const actionNode = event.target.closest("[data-market-audit-action]");
    if (!actionNode) return;
    const action = String(actionNode.dataset.marketAuditAction || "").trim();
    if (action === "reload") {
      stopMarketAuditPlayback();
      void loadMarketAudit();
      return;
    }
    if (action === "prev") {
      stopMarketAuditPlayback();
      stepMarketAudit(-1);
      return;
    }
    if (action === "next") {
      stopMarketAuditPlayback();
      stepMarketAudit(1);
      return;
    }
    if (action === "jump-end") {
      stopMarketAuditPlayback();
      const count = Number(marketAuditState?.payload?.bar_count || marketAuditState?.payload?.rows?.length || 0);
      marketAuditState.activeIndex = Math.max(0, count - 1);
      if (!updateMarketAuditSelectionOnly()) renderOrUpdateMarketAuditModal();
      return;
    }
    if (action === "follow-live") {
      stopMarketAuditPlayback();
      marketAuditState.followLive = !marketAuditState.followLive;
      if (marketAuditState.followLive) {
        void loadMarketAudit({ forceLatest: true });
      } else {
        stopMarketAuditLiveRefresh();
        renderOrUpdateMarketAuditModal();
      }
      return;
    }
    if (action === "play") {
      toggleMarketAuditPlayback();
    }
  }

  function handleMarketAuditModalInput(event) {
    const backdrop = byId("detail-modal-backdrop");
    if (backdrop?.dataset.mode !== "market-audit") return;
    if (!event.target.matches("[data-market-audit-bars]")) return;
    const nextBars = Math.max(1, Math.min(1200, Number(event.target.value || marketAuditState.bars || 300)));
    marketAuditState.bars = nextBars;
  }

  function handleMarketAuditModalChange(event) {
    const backdrop = byId("detail-modal-backdrop");
    if (backdrop?.dataset.mode !== "market-audit") return;
    if (!event.target.matches("[data-market-audit-bars]")) return;
    stopMarketAuditPlayback();
    stopMarketAuditLiveRefresh();
    void loadMarketAudit();
  }

  function handleMarketAuditModalScroll(event) {
    const backdrop = byId("detail-modal-backdrop");
    if (backdrop?.dataset.mode !== "market-audit") return;
    const shell = event.target;
    if (!(shell instanceof HTMLElement) || !shell.classList.contains("market-audit-chart-shell")) return;
    if (marketAuditState.syncingChartScroll) return;
    marketAuditState.chartScrollLeft = Number(shell.scrollLeft || 0);
  }

  function bindMarketAuditModal() {
    const body = byId("detail-modal-body");
    if (!body || body.dataset.boundMarketAudit === "1") return;
    body.dataset.boundMarketAudit = "1";
    body.addEventListener("click", handleMarketAuditModalClick);
    body.addEventListener("input", handleMarketAuditModalInput);
    body.addEventListener("change", handleMarketAuditModalChange);
    body.addEventListener("scroll", handleMarketAuditModalScroll, true);
    const backdrop = byId("detail-modal-backdrop");
    backdrop?.addEventListener("click", (event) => {
      if (backdrop.dataset.mode === "market-audit" && event.target?.id === "detail-modal-backdrop") {
        stopMarketAuditPlayback();
        stopMarketAuditLiveRefresh();
        stopMarketAuditFetch();
        marketAuditState.open = false;
      }
    });
  }

  function bindMarketAuditLauncher() {
    const button = byId("market-audit-button");
    if (!button || button.dataset.boundMarketAuditLauncher === "1") return;
    button.dataset.boundMarketAuditLauncher = "1";
    button.addEventListener("click", () => {
      marketAuditState.source = "live";
      openMarketAuditModal();
    });
  }

  function builderPreviewRequestPayload() {
    if (typeof builderSnapshotPayload === "function") {
      return builderSnapshotPayload();
    }
    if (!state?.builderPreset) return null;
    const name = String(state?.builderPresetName || state?.selectedPreset?.name || "").trim() || "draft_preview";
    return {
      name,
      preset: state.builderPreset,
    };
  }

  function snapshotTimeframes() {
    return snapshotRowsSafe().map((row) => row.timeframe);
  }

  function enabledPresetTimeframes() {
    const tfMap =
      state?.builderPreset?.settings?.timeframes ||
      state?.selectedPreset?.preset?.settings?.timeframes ||
      {};
    const enabled = TIMEFRAME_OPTIONS.filter((tf) => Boolean(tfMap?.[tf]));
    return enabled.length ? enabled : [];
  }

  function selectableTimeframes() {
    return TIMEFRAME_OPTIONS.slice();
  }

  function ensureActiveSnapshotTimeframe() {
    const rows = snapshotRowsSafe();
    const selectable = selectableTimeframes();
    if (!selectable.length) {
      activeSnapshotTimeframe = null;
      return null;
    }
    if (activeSnapshotTimeframe && selectable.includes(activeSnapshotTimeframe)) {
      return activeSnapshotTimeframe;
    }
    activeSnapshotTimeframe = selectable[0];
    return activeSnapshotTimeframe;
  }

  function activeSnapshotRow() {
    const tf = ensureActiveSnapshotTimeframe();
    return mergeIndicatorRefreshCarryRow(snapshotRowsSafe().find((row) => row.timeframe === tf) || null);
  }

  function hasAnySnapshotValues() {
    return snapshotRowsSafe().some((row) => row && row.values && Object.keys(row.values).length > 0);
  }

  function toneClass(value) {
    const text = String(value ?? "").toUpperCase();
    if (!text) return "";
    if (["BUY", "GREEN", "UP"].includes(text)) return "is-positive";
    if (["SELL", "RED", "DOWN"].includes(text)) return "is-negative";
    if (["NEUTRAL"].includes(text)) return "is-neutral";
    if (["1M", "5M", "15M", "30M", "1H"].includes(text)) return "is-accent";
    return "";
  }

  function numericToneClass(value) {
    const num = Number(value);
    if (!Number.isFinite(num)) return "";
    if (num > 0) return "is-positive";
    if (num < 0) return "is-negative";
    return "is-neutral";
  }

  function emaSpreadTrendToneClass(value) {
    const text = String(value ?? "").toUpperCase();
    if (text === "EXPANDING") return "is-accent";
    if (text === "CONTRACTING" || text === "FLAT") return "is-neutral";
    return "";
  }

  function emaPressureToneClass(value) {
    const text = String(value ?? "").toUpperCase();
    if (text === "BULL_ACCELERATING") return "is-positive";
    if (text === "BEAR_ACCELERATING") return "is-negative";
    if (["BULL_DECELERATING", "BEAR_DECELERATING", "NEUTRAL"].includes(text)) return "is-neutral";
    return "";
  }

  function spreadDeltaToneClass(value) {
    const num = Number(value);
    if (!Number.isFinite(num)) return "";
    if (num > 0) return "is-accent";
    if (num < 0) return "is-neutral";
    return "is-neutral";
  }

  function displayValue(value, digits = 2) {
    if (typeof fmtSnapshotValue === "function") return sanitizeText(fmtSnapshotValue(value, digits));
    const num = Number(value);
    if (Number.isFinite(num)) return num.toFixed(digits);
    return sanitizeText(value ?? "-");
  }

  function takerSellSharePct(value) {
    const buyShare = Number(value);
    if (!Number.isFinite(buyShare)) return null;
    return 100.0 - buyShare;
  }

  function takerBiasSummaryEntries(summary) {
    const payload = summary && typeof summary === "object" ? summary : {};
    const sellShare = takerSellSharePct(payload.taker_buy_share_pct);
    return [
      ["Buy Volume", displayValue(payload.taker_buy_volume, 4)],
      ["Sell Volume", displayValue(payload.taker_sell_volume, 4)],
      ["Buy %", displayValue(payload.taker_buy_share_pct, 2)],
      ["Sell %", displayValue(sellShare, 2)],
      ["Delta Volume", displayValue(payload.taker_delta_volume, 4)],
      ["Delta %", displayValue(payload.taker_delta_pct, 2)],
      ["Trades", displayValue(payload.taker_trade_count, 0)],
      ["State", sanitizeText(payload.taker_bias_state ?? "-")],
    ];
  }

  function renderTakerBiasDetailSection(title, summary, note = "") {
    return `
      <section>
        <div class="builder-row spread">
          <div class="eyebrow">${sanitizeText(title)}</div>
          ${note ? `<div class="verify-status-note">${sanitizeText(note)}</div>` : ""}
        </div>
        ${detailRows(takerBiasSummaryEntries(summary))}
      </section>
    `;
  }

  function renderTakerBiasWindowButtons(detail) {
    const current = String(takerBiasDetailState.window || 3);
    const available = Array.isArray(detail?.available_windows) && detail.available_windows.length
      ? detail.available_windows
      : [1, 3, 5, 10];
    return `
      <div class="builder-row spread">
        <div class="eyebrow">Closed Window</div>
        <div class="logic-truth-buttons">
          ${available.map((size) => {
            const key = String(size);
            const active = key === current ? "is-active" : "";
            return `<button class="mini-button ${active}" type="button" data-taker-bias-window="${sanitizeText(key)}">${sanitizeText(key)}</button>`;
          }).join("")}
        </div>
      </div>
    `;
  }

  function renderTakerBiasDetailHtml(row) {
    const values = row?.values || {};
    const detail = row?.taker_bias_detail && typeof row.taker_bias_detail === "object" ? row.taker_bias_detail : {};
    const liveSummary = {
      taker_bias_state: values.taker_bias_state,
      taker_trade_count: values.taker_trade_count,
      taker_delta_pct: values.taker_delta_pct,
      taker_buy_share_pct: values.taker_buy_share_pct,
      taker_buy_volume: values.taker_buy_volume,
      taker_sell_volume: values.taker_sell_volume,
      taker_delta_volume: values.taker_delta_volume,
    };
    const prevClosed = detail?.prev_closed && typeof detail.prev_closed === "object" ? detail.prev_closed : null;
    const windowKey = String(takerBiasDetailState.window || 3);
    const windowSummary = detail?.windows && typeof detail.windows === "object"
      ? (detail.windows[windowKey] || detail.windows[String((detail.available_windows || [1])[0])] || null)
      : null;
    const prevNote = prevClosed?.timestamp_utc ? `Closed ${prevClosed.timestamp_utc}` : "Latest confirmed candle";
    const requestedBars = Number(windowSummary?.requested_bars || windowKey || 0);
    const barsUsed = Number(windowSummary?.bars_used || 0);
    const windowNote = barsUsed > 0
      ? (requestedBars > 0 && requestedBars !== barsUsed
        ? `${barsUsed} of requested ${requestedBars} closed bar(s)`
        : `${barsUsed} closed bar(s)`)
      : "No closed bars yet";
    return `
      <div class="downloader-field-modal">
        ${renderTakerBiasDetailSection("Live Preview", liveSummary, row?.timestamp_utc ? `As of ${row.timestamp_utc}` : "")}
        ${renderTakerBiasDetailSection("Previous Closed Candle", prevClosed, prevNote)}
        ${renderTakerBiasWindowButtons(detail)}
        ${renderTakerBiasDetailSection(`Last ${sanitizeText(windowKey)} Closed`, windowSummary, windowNote)}
      </div>
    `;
  }

  function rerenderTakerBiasDetailModal() {
    const row = takerBiasDetailState.row;
    if (!row || typeof showDetailModal !== "function") return;
    showDetailModal(`Aggressive Pressure Details - ${row?.timeframe || "-"}`, renderTakerBiasDetailHtml(row), { mode: "taker-bias" });
    sanitizeTree(byId("detail-modal-backdrop"));
  }

  function handleTakerBiasDetailClick(event) {
    const backdrop = byId("detail-modal-backdrop");
    if (backdrop?.dataset.mode !== "taker-bias") return;
    const button = event.target?.closest?.("[data-taker-bias-window]");
    if (!button) return;
    const nextWindow = String(button.getAttribute("data-taker-bias-window") || "").trim();
    if (!nextWindow) return;
    takerBiasDetailState.window = nextWindow;
    rerenderTakerBiasDetailModal();
  }

  function bindTakerBiasDetailModal() {
    const body = byId("detail-modal-body");
    if (!body || body.dataset.boundTakerBias === "1") return;
    body.dataset.boundTakerBias = "1";
    body.addEventListener("click", handleTakerBiasDetailClick);
  }

  function metricValueText(value, options = {}) {
    if (value === null || value === undefined || value === "") return "--";
    return options.raw ? sanitizeText(value) : displayValue(value, options.digits ?? 2);
  }

  function renderMetricButton(field, label, value, options = {}) {
    const classes = ["verify-metric-main"];
    const tone = options.tone || "";
    if (tone) classes.push(tone);
    if (value === null || value === undefined || value === "") classes.push("is-empty");
    const disabled = options.disabled ? "disabled" : "";
    const hint = options.hint
      ? `<span class="verify-metric-hint">${sanitizeText(options.hint)}</span>`
      : "";
    return `
      <button class="${classes.join(" ")}" type="button" data-verify-field="${sanitizeText(field)}" ${disabled}>
        <span class="verify-metric-label">${sanitizeText(label)}</span>
        <strong class="verify-metric-value">${metricValueText(value, options)}</strong>
        ${hint}
      </button>
    `;
  }

  function renderMetricBadge(field, label, value, options = {}) {
    const classes = ["verify-metric-badge"];
    const tone = options.tone || "";
    if (tone) classes.push(tone);
    if (value === null || value === undefined || value === "") classes.push("is-empty");
    const disabled = options.disabled ? "disabled" : "";
    return `
      <button class="${classes.join(" ")}" type="button" data-verify-field="${sanitizeText(field)}" ${disabled}>
        <span class="verify-metric-badge-label">${sanitizeText(label)}</span>
        <strong>${metricValueText(value, options)}</strong>
      </button>
    `;
  }

  function renderPassiveBadge(label, value, options = {}) {
    const classes = ["verify-metric-badge", "is-passive"];
    const tone = options.tone || "";
    if (tone) classes.push(tone);
    if (value === null || value === undefined || value === "") classes.push("is-empty");
    return `
      <span class="${classes.join(" ")}">
        <span class="verify-metric-badge-label">${sanitizeText(label)}</span>
        <strong>${metricValueText(value, options)}</strong>
      </span>
    `;
  }

  function renderMetricRow(mainHtml, extras = []) {
    const extraHtml = Array.isArray(extras) ? extras.filter(Boolean).join("") : String(extras || "");
    return `
      <div class="verify-metric-row">
        ${mainHtml}
        ${extraHtml ? `<div class="verify-metric-side">${extraHtml}</div>` : ""}
      </div>
    `;
  }

  function renderMetricCombo(items = []) {
    const html = Array.isArray(items) ? items.filter(Boolean).join("") : String(items || "");
    return `<div class="verify-metric-combo">${html}</div>`;
  }

  function renderMetricBlock(title, rows, note = "") {
    return `
      <section class="verify-group-block">
        <div class="verify-group-head">
          <div class="verify-group-title">${sanitizeText(title)}</div>
          ${note ? `<div class="verify-group-note">${sanitizeText(note)}</div>` : ""}
        </div>
        <div class="verify-group-rows">${rows.join("")}</div>
      </section>
    `;
  }

  function renderChip(field, label, value, options = {}) {
    const tone = options.tone || toneClass(value);
    const rendered = options.raw ? sanitizeText(value ?? "-") : displayValue(value, options.digits ?? 2);
    return `
      <button class="verify-chip ${tone}" type="button" data-verify-field="${field}">
        <span class="verify-chip-label">${sanitizeText(label)}</span>
        <strong>${rendered}</strong>
      </button>
    `;
  }

  function renderTimeframeButtons() {
    const rows = snapshotRowsSafe();
    return selectableTimeframes().map((tf) => `
      <button
        class="verify-tf-button ${tf === activeSnapshotTimeframe ? "is-active" : ""} ${rows.some((row) => row.timeframe === tf) ? "" : "is-pending"}"
        type="button"
        data-verify-timeframe="${tf}"
      >${tf}</button>
    `).join("");
  }

  function friendlyLiveError(error) {
    const raw = sanitizeText(error?.message || String(error || "") || "Unknown live error.");
    if (raw.trim().toLowerCase() === "not found") {
      return "Live backend route not found. Restart run_app_v2.cmd so the new websocket live server is loaded.";
    }
    return raw;
  }

  function currentLivePhase() {
    return String(liveSnapshotPayload?.live_phase || "").trim().toLowerCase();
  }

  function liveStatusText() {
    if (livePaused) return "Paused";
    if (liveBootstrapError && !snapshotRowsSafe().length) return "Error";
    const phase = currentLivePhase();
    if (phase === "error") return "Error";
    if (phase === "prefilling" || phase === "starting" || phase === "warming") return "Loading";
    if (phase === "stale") return "Stale";
    if (phase === "live") return liveSocketConnected ? "Live" : "Live (reconnecting)";
    if (liveFetchInFlight || liveSnapshotLoading) return "Loading";
    return "Waiting";
  }

  function liveStatusHint(row) {
    if (livePaused) {
      return "Updates are paused. Resume to continue live refresh.";
    }
    if (liveBootstrapError && !snapshotRowsSafe().length) {
      return liveBootstrapError;
    }
    const phase = currentLivePhase();
    const message = sanitizeText(liveSnapshotPayload?.live_message || "");
    if (phase === "error") {
      return message || "Live engine failed to start.";
    }
    if (phase === "starting" || phase === "prefilling" || phase === "warming") {
      return message || "Preloading live market context...";
    }
    if (phase === "stale") {
      return message || "Feed is stale. Waiting for the next websocket update.";
    }
    if (!row) {
      return message || "Live values are loading. Pick a timeframe once data arrives.";
    }
    return message || "Live values are streaming from the market feed. Left click a tile for details. Right click for rule criteria and insert options.";
  }

  function renderPlaceholderChip(field, label) {
    return `
      <button class="verify-chip is-empty" type="button" data-verify-field="${field}" disabled>
        <span class="verify-chip-label">${sanitizeText(label)}</span>
        <strong>--</strong>
      </button>
    `;
  }

  function verifyPresetSource() {
    return liveContextPresetView();
  }

  function patchIndicatorSpec(kind) {
    const key = String(kind || "").trim();
    if (key === "ema") {
      return {
        key: "ema_indicators",
        label: "EMA",
        current: typeof currentEmaSettings === "function" ? currentEmaSettings : null,
        hasOverride: typeof hasPresetEmaOverride === "function" ? hasPresetEmaOverride : null,
        format: typeof formatEmaPair === "function" ? formatEmaPair : null,
      };
    }
    if (key === "rsi") {
      return {
        key: "rsi_indicators",
        label: "RSI",
        current: typeof currentRsiSettings === "function" ? currentRsiSettings : null,
        hasOverride: typeof hasPresetRsiOverride === "function" ? hasPresetRsiOverride : null,
        format: typeof formatRsiConfig === "function" ? formatRsiConfig : null,
      };
    }
    return null;
  }

  function currentVerifyIndicatorSettings(kind, timeframe = null) {
    const spec = patchIndicatorSpec(kind);
    if (!spec) return {};
    const preset = verifyPresetSource();
    if (typeof spec.current === "function") {
      return spec.current(preset, timeframe);
    }
    const normalized = normalizeLiveIndicatorSettingsValue(kind, preset?.settings?.[spec.key] || {});
    const defaults = normalized?.defaults || {};
    const tf = String(timeframe || "").trim();
    return tf && normalized?.overrides?.[tf] ? { ...normalized.overrides[tf] } : { ...defaults };
  }

  function verifyHasIndicatorOverride(kind, timeframe) {
    const spec = patchIndicatorSpec(kind);
    if (!spec) return false;
    const preset = verifyPresetSource();
    if (typeof spec.hasOverride === "function") {
      return spec.hasOverride(preset, timeframe);
    }
    const tf = String(timeframe || "").trim();
    return Boolean(tf && preset?.settings?.[spec.key]?.overrides?.[tf]);
  }

  function verifyIndicatorText(kind, config) {
    const spec = patchIndicatorSpec(kind);
    if (!spec) return "";
    if (typeof spec.format === "function") return spec.format(config);
    return Object.values(config || {}).join("/");
  }

  function currentVerifyEmaSettings(timeframe = null) {
    return currentVerifyIndicatorSettings("ema", timeframe);
  }

  function verifyHasEmaOverride(timeframe) {
    return verifyHasIndicatorOverride("ema", timeframe);
  }

  function verifyEmaPairText(pair) {
    return verifyIndicatorText("ema", pair);
  }

  function currentVerifyRsiSettings(timeframe = null) {
    return currentVerifyIndicatorSettings("rsi", timeframe);
  }

  function verifyHasRsiOverride(timeframe) {
    return verifyHasIndicatorOverride("rsi", timeframe);
  }

  function verifyRsiConfigText(config) {
    return verifyIndicatorText("rsi", config);
  }

  function verifyIndicatorControlSpec(kind) {
    const key = String(kind || "").trim();
    if (key === "ema") {
      return {
        kind: "ema",
        controlClass: "verify-ema-control",
        toggleClass: "verify-ema-toggle",
        popoverClass: "verify-ema-popover",
        gearClass: "verify-ema-toggle-gear",
        badgeClass: "verify-ema-toggle-badge",
        gridClass: "verify-ema-popover-grid",
        toggleDataAttr: "verify-ema-toggle",
        resetDataAttr: "verify-ema-reset",
        timeframeDataAttr: "verify-ema-timeframe",
        keyDataAttr: "verify-ema-key",
        label: "EMA",
        titleLabel: "EMA pair",
        panelTitle: "EMA Pair",
        currentSettings: currentVerifyEmaSettings,
        hasOverride: verifyHasEmaOverride,
        format: verifyEmaPairText,
        applyMutation: typeof setPresetEmaSettings === "function" ? setPresetEmaSettings : null,
        statusMessages(updatedText, tf, isOverride) {
          if (!tf) return `Updated live default EMA pair to ${updatedText}.`;
          if (isOverride) return `Updated live ${tf} EMA pair to ${updatedText}.`;
          return `Live ${tf} EMA pair now follows the default ${updatedText}.`;
        },
        fields: [
          { key: "ema_1_length", label: "Fast" },
          { key: "ema_2_length", label: "Slow" },
        ],
      };
    }
    if (key === "rsi") {
      return {
        kind: "rsi",
        controlClass: "verify-rsi-control",
        toggleClass: "verify-rsi-toggle",
        popoverClass: "verify-rsi-popover",
        gearClass: "verify-rsi-toggle-gear",
        badgeClass: "verify-rsi-toggle-badge",
        gridClass: "verify-rsi-popover-grid",
        toggleDataAttr: "verify-rsi-toggle",
        resetDataAttr: "verify-rsi-reset",
        timeframeDataAttr: "verify-rsi-timeframe",
        keyDataAttr: "verify-rsi-key",
        label: "RSI",
        titleLabel: "RSI config",
        panelTitle: "RSI Config",
        currentSettings: currentVerifyRsiSettings,
        hasOverride: verifyHasRsiOverride,
        format: verifyRsiConfigText,
        applyMutation: typeof setPresetRsiSettings === "function" ? setPresetRsiSettings : null,
        statusMessages(updatedText, tf, isOverride) {
          if (!tf) return `Updated live default RSI config to ${updatedText}.`;
          if (isOverride) return `Updated live ${tf} RSI config to ${updatedText}.`;
          return `Live ${tf} RSI now follows the default ${updatedText}.`;
        },
        fields: [
          { key: "length", label: "Length" },
          { key: "smoothing", label: "Smooth" },
        ],
      };
    }
    return null;
  }

  function isVerifyIndicatorEditorOpen(kind) {
    const key = String(kind || "").trim();
    if (key === "ema") return Boolean(verifyEmaEditorOpen);
    if (key === "rsi") return Boolean(verifyRsiEditorOpen);
    return false;
  }

  function setVerifyIndicatorEditorOpen(kind, nextOpen) {
    const key = String(kind || "").trim();
    const isOpen = Boolean(nextOpen);
    if (key === "ema") {
      verifyEmaEditorOpen = isOpen;
      if (isOpen) verifyRsiEditorOpen = false;
    } else if (key === "rsi") {
      verifyRsiEditorOpen = isOpen;
      if (isOpen) verifyEmaEditorOpen = false;
    }
  }

  function closeVerifyIndicatorEditors() {
    verifyEmaEditorOpen = false;
    verifyRsiEditorOpen = false;
  }

  function buildVerifyIndicatorControlHtml(kind, timeframe) {
    const spec = verifyIndicatorControlSpec(kind);
    const activeTf = String(timeframe || ensureActiveSnapshotTimeframe() || "").trim();
    if (!spec || !activeTf) return "";
    const currentConfig = spec.currentSettings(activeTf);
    const defaults = spec.currentSettings(null);
    const isOverride = spec.hasOverride(activeTf);
    const isOpen = isVerifyIndicatorEditorOpen(kind);
    return `
      <div class="${spec.controlClass} ${isOpen ? "is-open" : ""}">
        <button class="mini-button ${spec.toggleClass}" type="button" data-${spec.toggleDataAttr}="1" aria-expanded="${isOpen ? "true" : "false"}" title="Edit ${sanitizeText(activeTf)} ${spec.titleLabel}">
          <span class="${spec.gearClass}" aria-hidden="true">⚙</span>
          <span>${spec.label} ${sanitizeText(spec.format(currentConfig))}</span>
          ${isOverride ? `<span class="${spec.badgeClass}">TF</span>` : ""}
        </button>
        ${isOpen ? `
          <div class="${spec.popoverClass}" role="dialog" aria-label="${sanitizeText(activeTf)} ${spec.panelTitle}">
            <div class="${spec.popoverClass}-head">
              <div>
                <div class="eyebrow">${sanitizeText(activeTf)} ${spec.panelTitle}</div>
                <div class="verify-status-note">Live default ${sanitizeText(spec.format(defaults))}</div>
              </div>
              <button class="mini-button" type="button" data-${spec.resetDataAttr}="${sanitizeText(activeTf)}" ${isOverride ? "" : "disabled"}>Use Default</button>
            </div>
            <div class="${spec.gridClass}">
              ${spec.fields.map((field) => `
                <label>
                  <span>${sanitizeText(field.label)}</span>
                  <input type="number" min="1" step="1" data-${spec.timeframeDataAttr}="${sanitizeText(activeTf)}" data-${spec.keyDataAttr}="${sanitizeText(field.key)}" value="${sanitizeText(currentConfig[field.key])}" />
                </label>
              `).join("")}
            </div>
          </div>
        ` : ""}
      </div>
    `;
  }

  function applyVerifyIndicatorMutation(kind, timeframe, nextConfig) {
    const spec = verifyIndicatorControlSpec(kind);
    if (!spec || typeof spec.applyMutation !== "function") return;
    beginIndicatorRefreshCarry();
    const tf = String(timeframe || "").trim();
    const nextPresetView = liveContextPresetView();
    const updated = spec.applyMutation(nextPresetView, tf || "default", nextConfig);
    const syncPresetTarget = (presetLike) => {
      if (!presetLike || typeof presetLike !== "object") return;
      try {
        spec.applyMutation(presetLike, tf || "default", updated);
      } catch (error) {
        /* noop */
      }
    };
    syncPresetTarget(state?.builderPreset);
    syncPresetTarget(state?.selectedPreset?.preset);
    if (state?.builderPreset) {
      state.builderPresetDirty = true;
    }
    liveContextState = normalizeLiveContext({
      session_name: liveContextState?.session_name || "live_market",
      settings: nextPresetView.settings,
    });
    persistLiveContext();
    const isOverride = tf ? spec.hasOverride(tf) : false;
    if (typeof setBuilderStatus === "function") {
      setBuilderStatus(spec.statusMessages(spec.format(updated), tf, isOverride));
    }
    if (typeof renderSnapshotPanel === "function") renderSnapshotPanel();
    if (typeof syncJsonEditorFromBuilder === "function") syncJsonEditorFromBuilder();
    if (typeof syncBuilderHeader === "function") syncBuilderHeader();
    renderVerifyHost();
    void refreshSnapshotOnce(false);
  }

  function buildVerifyEmaControlHtml(timeframe) {
    return buildVerifyIndicatorControlHtml("ema", timeframe);
  }

  function buildVerifyRsiControlHtml(timeframe) {
    return buildVerifyIndicatorControlHtml("rsi", timeframe);
  }

  function applyVerifyEmaMutation(timeframe, nextPair) {
    applyVerifyIndicatorMutation("ema", timeframe, nextPair);
  }

  function applyVerifyRsiMutation(timeframe, nextConfig) {
    applyVerifyIndicatorMutation("rsi", timeframe, nextConfig);
  }

  function buildVerifyStripHtml(row) {
    const values = row?.values || {};
    const activeTf = ensureActiveSnapshotTimeframe() || row?.timeframe || "-";
    const emaPair = currentVerifyEmaSettings(activeTf);
    const rsiConfig = currentVerifyRsiSettings(activeTf);
    const livePresetView = liveContextPresetView();
    const chipLabel = (field, fallback) => {
      if (typeof compactFieldLabel === "function") return compactFieldLabel(field, activeTf, livePresetView);
      if (typeof fieldLabel === "function") return fieldLabel(field, activeTf, livePresetView);
      return fallback;
    };
    const framaAge = row ? values.frama_display_age : null;
    const rangeState = row ? (values.range_filter_display_state ?? values.range_filter_state) : null;
    const rangeAge = row ? values.range_filter_display_age : null;
    const diSpread = Number(values.di_spread);
    const emaRows = [
      renderMetricRow(
        renderMetricCombo([
          renderMetricButton("ema_1", chipLabel("ema_1", "EMA Fast"), row ? values.ema_1_angle_state : null, {
            raw: true,
            tone: toneClass(values.ema_1_angle_state),
            hint: "Angle",
            disabled: !row,
          }),
          renderMetricButton("ema_2", chipLabel("ema_2", "EMA Slow"), row ? values.ema_2_angle_state : null, {
            raw: true,
            tone: toneClass(values.ema_2_angle_state),
            hint: "Angle",
            disabled: !row,
          }),
        ])
      ),
      renderMetricRow(
        renderMetricCombo([
          renderMetricButton("ema_spread", "Spread", row ? values.ema_spread : null, {
            digits: 2,
            tone: "",
            hint: `Distance | EMA ${verifyEmaPairText(emaPair)}`,
            disabled: !row,
          }),
          renderMetricButton("ema_stack", "Stack", row ? values.ema_stack : null, {
            raw: true,
            tone: toneClass(values.ema_stack),
            disabled: !row,
          }),
        ])
      ),
      renderMetricRow(
        renderMetricCombo([
          renderMetricButton("ema_spread_delta", "Δ Spread", row ? values.ema_spread_delta : null, {
            digits: 3,
            tone: spreadDeltaToneClass(values.ema_spread_delta),
            hint: "Prev bar distance",
            disabled: !row,
          }),
          renderMetricButton("ema_spread_pct_delta", "Δ %", row ? values.ema_spread_pct_delta : null, {
            digits: 3,
            tone: spreadDeltaToneClass(values.ema_spread_pct_delta),
            disabled: !row,
          }),
        ])
      ),
    ];
    const strengthRows = [
      renderMetricRow(
        renderMetricButton("adx", "ADX", row ? values.adx : null, {
          digits: 1,
          tone: Number(values.adx) >= 25 ? "is-positive" : (Number(values.adx) >= 15 ? "is-neutral" : ""),
          disabled: !row,
        }),
        [
          renderMetricBadge("atr", "ATR", row ? values.atr : null, {
            digits: 1,
            disabled: !row,
          }),
        ]
      ),
      renderMetricRow(
        renderMetricButton("di_plus", "+DI", row ? values.di_plus : null, {
          digits: 1,
          tone: Number.isFinite(diSpread) && diSpread > 0 ? "is-positive" : "",
          disabled: !row,
        })
      ),
      renderMetricRow(
        renderMetricButton("di_minus", "-DI", row ? values.di_minus : null, {
          digits: 1,
          tone: Number.isFinite(diSpread) && diSpread < 0 ? "is-negative" : "",
          disabled: !row,
        })
      ),
    ];
    const takerTradeCount = row ? values.taker_trade_count : null;
    const takerBuyShareNum = Number(values.taker_buy_share_pct);
    const takerSellShareNum = takerSellSharePct(values.taker_buy_share_pct);
    const takerRows = [
      renderMetricRow(
        renderMetricButton("taker_buy_volume", "Buy Vol", row ? values.taker_buy_volume : null, {
          digits: 3,
          tone: numericToneClass(values.taker_delta_volume),
          disabled: !row,
        }),
        [
          renderMetricBadge("taker_sell_volume", "Sell Vol", row ? values.taker_sell_volume : null, {
            digits: 3,
            tone: numericToneClass(-(Number(values.taker_delta_volume) || 0)),
            disabled: !row,
          }),
        ]
      ),
      renderMetricRow(
        renderMetricButton("taker_buy_share_pct", "Buy %", row ? values.taker_buy_share_pct : null, {
          digits: 1,
          tone: Number.isFinite(takerBuyShareNum) ? numericToneClass(takerBuyShareNum - 50.0) : "",
          disabled: !row,
        }),
        [
          renderMetricBadge("taker_buy_share_pct", "Sell %", row ? takerSellShareNum : null, {
            digits: 1,
            tone: Number.isFinite(takerSellShareNum) ? numericToneClass(takerSellShareNum - 50.0) : "",
            disabled: !row,
          }),
        ]
      ),
      renderMetricRow(
        renderMetricButton("taker_delta_volume", "Delta Vol", row ? values.taker_delta_volume : null, {
          digits: 3,
          tone: numericToneClass(values.taker_delta_volume),
          disabled: !row,
        }),
        [
          renderMetricBadge("taker_delta_pct", "Delta %", row ? values.taker_delta_pct : null, {
            digits: 1,
            tone: numericToneClass(values.taker_delta_pct),
            disabled: !row,
          }),
        ]
      ),
    ];
    const regimeRows = [
      renderMetricRow(
        renderMetricButton("frama_state", "FRAMA", row ? values.frama_state : null, {
          raw: true,
          tone: toneClass(values.frama_state),
          disabled: !row,
        }),
        [renderPassiveBadge("Trigger Age", framaAge, { digits: 0 })]
      ),
      renderMetricRow(
        renderMetricButton("vidya_state", "VIDYA", row ? values.vidya_state : null, {
          raw: true,
          tone: toneClass(values.vidya_state),
          disabled: !row,
        }),
        [
          renderMetricBadge("vidya_state", "Delta %", row ? values.vidya_delta_pct : null, {
            digits: 2,
            tone: numericToneClass(values.vidya_delta_pct),
            disabled: !row,
          }),
        ]
      ),
      renderMetricRow(
        renderMetricButton("range_filter_state", "Range", row ? rangeState : null, {
          raw: true,
          tone: toneClass(rangeState),
          disabled: !row,
        }),
        [renderPassiveBadge("Trigger Age", rangeAge, { digits: 0 })]
      ),
    ];
    let rsiNum = values.rsi;
    rsiNum = rsiNum === null || rsiNum === undefined ? null : Number(rsiNum);
    if (!Number.isFinite(rsiNum)) rsiNum = null;
    let rsiSmoothNum = values.rsi_smooth;
    rsiSmoothNum = rsiSmoothNum === null || rsiSmoothNum === undefined ? null : Number(rsiSmoothNum);
    if (!Number.isFinite(rsiSmoothNum)) rsiSmoothNum = null;
    const rsiGap = rsiNum !== null && rsiSmoothNum !== null ? rsiNum - rsiSmoothNum : null;
    const rsiZone = rsiNum === null ? null : (rsiNum >= 70 ? "70+" : (rsiNum <= 30 ? "30-" : "Mid"));
    const rsiTone = rsiNum === null ? "" : (rsiNum >= 70 ? "is-positive" : (rsiNum <= 30 ? "is-negative" : ""));
    const rsiRows = [
      renderMetricRow(
        renderMetricButton("rsi", "RSI", row ? values.rsi : null, {
          digits: 2,
          tone: rsiTone,
          hint: `Length ${rsiConfig.length}`,
          disabled: !row,
        }),
        [
          renderMetricBadge("rsi", "Zone", row ? rsiZone : null, {
            raw: true,
            tone: rsiTone,
            disabled: !row,
          }),
        ]
      ),
      renderMetricRow(
        renderMetricButton("rsi_smooth", "RSI Smooth", row ? values.rsi_smooth : null, {
          digits: 2,
          tone: numericToneClass(values.rsi_smooth),
          hint: `Smooth ${rsiConfig.smoothing}`,
          disabled: !row,
        })
      ),
      renderMetricRow(
        renderMetricButton("rsi_state", "State", row ? values.rsi_state : null, {
          raw: true,
          tone: toneClass(values.rsi_state),
          disabled: !row,
        }),
        [
          renderMetricBadge("rsi_state", "Gap", row ? rsiGap : null, {
            digits: 2,
            tone: numericToneClass(rsiGap),
            disabled: !row,
          }),
        ]
      ),
    ];
    let stochKNum = values.stoch_rsi_k;
    stochKNum = stochKNum === null || stochKNum === undefined ? null : Number(stochKNum);
    if (!Number.isFinite(stochKNum)) stochKNum = null;
    let stochDNum = values.stoch_rsi_d;
    stochDNum = stochDNum === null || stochDNum === undefined ? null : Number(stochDNum);
    if (!Number.isFinite(stochDNum)) stochDNum = null;
    const stochGap = stochKNum !== null && stochDNum !== null ? stochKNum - stochDNum : null;
    const stochKZone = stochKNum === null ? null : (stochKNum >= 80 ? "80+" : (stochKNum <= 20 ? "20-" : "Mid"));
    const stochKTone = stochKNum === null ? "" : (stochKNum >= 80 ? "is-positive" : (stochKNum <= 20 ? "is-negative" : ""));
    const stochDTone = stochDNum === null ? "" : (stochDNum >= 80 ? "is-positive" : (stochDNum <= 20 ? "is-negative" : ""));
    const stochRows = [
      renderMetricRow(
        renderMetricButton("stoch_rsi_k", "Stoch K", row ? values.stoch_rsi_k : null, {
          digits: 2,
          tone: stochKTone,
          disabled: !row,
        }),
        [
          renderMetricBadge("stoch_rsi_k", "Zone", row ? stochKZone : null, {
            raw: true,
            tone: stochKTone,
            disabled: !row,
          }),
        ]
      ),
      renderMetricRow(
        renderMetricButton("stoch_rsi_d", "Stoch D", row ? values.stoch_rsi_d : null, {
          digits: 2,
          tone: stochDTone,
          disabled: !row,
        })
      ),
      renderMetricRow(
        renderMetricButton("stoch_rsi_kd", "K/D", row ? values.stoch_rsi_kd : null, {
          raw: true,
          tone: toneClass(values.stoch_rsi_kd),
          disabled: !row,
        }),
        [
          renderMetricBadge("stoch_rsi_kd", "Gap", row ? stochGap : null, {
            digits: 2,
            tone: numericToneClass(stochGap),
            disabled: !row,
          }),
        ]
      ),
    ];
    const macdRows = [
      renderMetricRow(
        renderMetricButton("macd", "MACD", row ? values.macd : null, {
          digits: 2,
          tone: numericToneClass(values.macd),
          disabled: !row,
        }),
        [
          renderMetricBadge("angle", "Angle", row ? values.angle : null, {
            raw: true,
            tone: toneClass(values.angle),
            disabled: !row,
          }),
        ]
      ),
      renderMetricRow(
        renderMetricButton("signal", "Signal", row ? values.signal : null, {
          digits: 2,
          tone: numericToneClass(values.signal),
          disabled: !row,
        }),
        [
          renderMetricBadge("sig_angle", "SigAngle", row ? values.sig_angle : null, {
            raw: true,
            tone: toneClass(values.sig_angle),
            disabled: !row,
          }),
        ]
      ),
      renderMetricRow(
        renderMetricButton("ms", "M/S", row ? values.ms : null, {
          raw: true,
          tone: toneClass(values.ms),
          disabled: !row,
        }),
        [
          renderMetricBadge("ppo", "PPO", row ? values.ppo : null, {
            raw: true,
            tone: toneClass(values.ppo),
            disabled: !row,
          }),
          renderMetricBadge("flip_age", "FlipAge", row ? values.flip_age : null, {
            digits: 0,
            disabled: !row,
          }),
        ]
      ),
    ];
    return `
      <section class="verify-strip" id="verify-strip">
        <div class="verify-strip-head">
          <div>
            <div class="eyebrow">Live Control Surface</div>
            <div class="verify-status-line">
              <span class="verify-selected-tf">TF ${sanitizeText(activeTf)}</span>
              <span class="verify-status-note">${sanitizeText(liveStatusHint(row))}</span>
            </div>
          </div>
          <div class="verify-head-actions">
            <div class="verify-tf-row">${renderTimeframeButtons()}</div>
            <div class="verify-live-controls">
              ${buildVerifyEmaControlHtml(activeTf)}
              ${buildVerifyRsiControlHtml(activeTf)}
              <span class="verify-live-pill ${livePaused ? "is-paused" : ""}">
                <span class="verify-live-dot" id="verify-live-dot"></span>
                <span id="verify-live-label">${liveStatusText()}</span>
              </span>
              <button class="mini-button" type="button" id="verify-pause-toggle">${livePaused ? "Resume" : "Pause"}</button>
            </div>
          </div>
        </div>
        <div class="verify-strip-groups">
          ${renderMetricBlock("EMA Pair", emaRows, verifyEmaPairText(emaPair))}
          ${renderMetricBlock("RSI", rsiRows, verifyRsiConfigText(rsiConfig))}
          ${renderMetricBlock("Strength", strengthRows, "ADX, ATR, directional flow")}
          ${renderMetricBlock("Aggressive Pressure", takerRows, "Candle taker-buy versus taker-sell pressure")}
          ${renderMetricBlock("Regime", regimeRows, "State summary with aging")}
          ${renderMetricBlock("Stoch RSI", stochRows, "TradingView %K / %D")}
          ${renderMetricBlock("MACD", macdRows, "Momentum, slope, PPO")}
        </div>
      </section>
    `;
  }

  function renderSnapshotCompactPanel() {
    if (typeof syncBuilderHeader === "function") syncBuilderHeader();
  }

  function showMacdDetails(row) {
    const values = row?.values || {};
    if (typeof showDetailModal !== "function" || typeof detailRows !== "function") return;
    showDetailModal(`MACD Details - ${row?.timeframe || "-"}`, detailRows([
      ["MACD", displayValue(values.macd, 4)],
      ["Signal", displayValue(values.signal, 4)],
      ["PPO", sanitizeText(values.ppo ?? "-")],
      ["FlipAge", displayValue(values.flip_age, 0)],
      ["M/S", sanitizeText(values.ms ?? "-")],
      ["Angle", sanitizeText(values.angle ?? "-")],
      ["SigAngle", sanitizeText(values.sig_angle ?? "-")],
      ["MACD x Signal", sanitizeText(values.ms_cross ?? "-")],
      ["MACD x 0", sanitizeText(values.macd0_cross ?? "-")],
    ]));
    sanitizeTree(byId("detail-modal-backdrop"));
  }

  function showStochRsiDetails(row) {
    const values = row?.values || {};
    let k = values.stoch_rsi_k;
    k = k === null || k === undefined ? null : Number(k);
    if (!Number.isFinite(k)) k = null;
    let d = values.stoch_rsi_d;
    d = d === null || d === undefined ? null : Number(d);
    if (!Number.isFinite(d)) d = null;
    const gap = k !== null && d !== null ? k - d : null;
    if (typeof showDetailModal !== "function" || typeof detailRows !== "function") return;
    showDetailModal(`Stoch RSI Details - ${row?.timeframe || "-"}`, detailRows([
      ["Stoch K", displayValue(values.stoch_rsi_k, 4)],
      ["Stoch D", displayValue(values.stoch_rsi_d, 4)],
      ["K/D", sanitizeText(values.stoch_rsi_kd ?? "-")],
      ["K-D Gap", displayValue(gap, 4)],
      ["K >= 80", k !== null && k >= 80 ? "YES" : "no"],
      ["K <= 20", k !== null && k <= 20 ? "YES" : "no"],
    ]));
    sanitizeTree(byId("detail-modal-backdrop"));
  }

  function showRsiDetails(row) {
    const values = row?.values || {};
    let rsi = values.rsi;
    rsi = rsi === null || rsi === undefined ? null : Number(rsi);
    if (!Number.isFinite(rsi)) rsi = null;
    let smooth = values.rsi_smooth;
    smooth = smooth === null || smooth === undefined ? null : Number(smooth);
    if (!Number.isFinite(smooth)) smooth = null;
    const gap = rsi !== null && smooth !== null ? rsi - smooth : null;
    if (typeof showDetailModal !== "function" || typeof detailRows !== "function") return;
    showDetailModal(`RSI Details - ${row?.timeframe || "-"}`, detailRows([
      ["RSI", displayValue(values.rsi, 4)],
      ["RSI Smooth", displayValue(values.rsi_smooth, 4)],
      ["RSI State", sanitizeText(values.rsi_state ?? "-")],
      ["RSI-Smooth Gap", displayValue(gap, 4)],
      ["RSI >= 70", rsi !== null && rsi >= 70 ? "YES" : "no"],
      ["RSI <= 30", rsi !== null && rsi <= 30 ? "YES" : "no"],
    ]));
    sanitizeTree(byId("detail-modal-backdrop"));
  }

  function showStrengthDetails(row) {
    const values = row?.values || {};
    if (typeof showDetailModal !== "function" || typeof detailRows !== "function") return;
    showDetailModal(`Trend Strength Details - ${row?.timeframe || "-"}`, detailRows([
      ["ADX", displayValue(values.adx, 2)],
      ["ADX Angle", sanitizeText(values.adx_angle ?? "-")],
      ["ATR", displayValue(values.atr, 2)],
      ["ATR Angle", sanitizeText(values.atr_angle ?? "-")],
      ["+DI", displayValue(values.di_plus, 2)],
      ["-DI", displayValue(values.di_minus, 2)],
      ["DI Spread", displayValue(values.di_spread, 2)],
      ["DI Cross", sanitizeText(values.di_cross ?? "-")],
    ]));
    sanitizeTree(byId("detail-modal-backdrop"));
  }

  function showTakerBiasDetails(row) {
    takerBiasDetailState.row = row || null;
    const available = Array.isArray(row?.taker_bias_detail?.available_windows) ? row.taker_bias_detail.available_windows : [];
    const current = String(takerBiasDetailState.window || "3");
    if (!available.map((item) => String(item)).includes(current)) {
      takerBiasDetailState.window = available.length ? String(available[0]) : "3";
    }
    rerenderTakerBiasDetailModal();
  }

  function showFramaDetails(row) {
    const values = row?.values || {};
    if (typeof showDetailModal !== "function" || typeof detailRows !== "function") return;
      showDetailModal(`FRAMA Details - ${row?.timeframe || "-"}`, detailRows([
      ["State", sanitizeText(values.frama_state ?? "-")],
      ["Last Trigger", sanitizeText(values.frama_trigger_side ?? "-")],
      ["Trigger Age", displayValue(values.frama_display_age, 0)],
      ["State Age", displayValue(values.frama_state_age, 0)],
      ["Break Up Event", boolish(values.frama_break_up) ? "YES" : "no"],
      ["Break Up Age", displayValue(values.frama_break_up_age, 0)],
      ["Break Down Event", boolish(values.frama_break_down) ? "YES" : "no"],
      ["Break Down Age", displayValue(values.frama_break_down_age, 0)],
      ["Mid Cross Event", boolish(values.frama_mid_cross) ? "YES" : "no"],
      ["Mid Cross Age", displayValue(values.frama_mid_cross_age, 0)],
    ]));
    sanitizeTree(byId("detail-modal-backdrop"));
  }

  function showVerifyFieldDetails(row, field) {
    if (!row || !field) return;
    if (["ema_1", "ema_2", "ema_stack", "ema_spread", "ema_spread_pct", "ema_spread_delta", "ema_spread_pct_delta", "ema_spread_trend", "ema_stack_pressure"].includes(field) && typeof showEmaDetails === "function") {
      showEmaDetails(row);
      return;
    }
    if (["rsi", "rsi_smooth", "rsi_state"].includes(field)) {
      showRsiDetails(row);
      return;
    }
    if (["taker_bias_state", "taker_trade_count", "taker_delta_pct", "taker_buy_share_pct", "taker_buy_volume", "taker_sell_volume", "taker_delta_volume"].includes(field)) {
      showTakerBiasDetails(row);
      return;
    }
    if (field === "vidya_state" && typeof showVidyaDetails === "function") {
      showVidyaDetails(row);
      return;
    }
    if (field === "range_filter_state" && typeof showRangeDetails === "function") {
      showRangeDetails(row);
      return;
    }
    if (field === "frama_state") {
      showFramaDetails(row);
      return;
    }
    if (["stoch_rsi_k", "stoch_rsi_d", "stoch_rsi_kd"].includes(field)) {
      showStochRsiDetails(row);
      return;
    }
    if (["macd", "signal", "ppo", "flip_age", "ms", "angle", "sig_angle"].includes(field)) {
      showMacdDetails(row);
      return;
    }
    if (["adx", "atr", "di_plus", "di_minus"].includes(field)) {
      showStrengthDetails(row);
    }
  }

  function verifyDetailLabel(row, field) {
    if (["ema_1", "ema_2", "ema_stack", "ema_spread", "ema_spread_pct", "ema_spread_delta", "ema_spread_pct_delta", "ema_spread_trend", "ema_stack_pressure"].includes(field)) return "View EMA Pair Details";
    if (["rsi", "rsi_smooth", "rsi_state"].includes(field)) return "View RSI Details";
    if (["taker_bias_state", "taker_trade_count", "taker_delta_pct", "taker_buy_share_pct", "taker_buy_volume", "taker_sell_volume", "taker_delta_volume"].includes(field)) return "View Taker Bias Details";
    if (field === "vidya_state") return "View VIDYA Details";
    if (field === "range_filter_state") return "View Range Details";
    if (field === "frama_state") return "View FRAMA Details";
    if (["stoch_rsi_k", "stoch_rsi_d", "stoch_rsi_kd"].includes(field)) return "View Stoch RSI Details";
    if (["macd", "signal", "ppo", "flip_age", "ms", "angle", "sig_angle"].includes(field)) return "View MACD Details";
    if (["adx", "atr", "di_plus", "di_minus"].includes(field)) return "View Trend Strength Details";
    return `View ${sanitizeText(fieldLabel(field, row?.timeframe))} Details`;
  }

  const originalBuildSnapshotMenuItems =
    typeof buildSnapshotMenuItems === "function" ? buildSnapshotMenuItems : null;

  if (originalBuildSnapshotMenuItems) {
    buildSnapshotMenuItems = function (row, columnKey) {
      if (!row || !["ema_1", "ema_2", "ema_stack", "ema_spread", "ema_spread_delta", "ema_spread_trend", "ema_stack_pressure"].includes(columnKey)) {
        return originalBuildSnapshotMenuItems(row, columnKey);
      }
      const timeframe = row.timeframe;
      const values = row.values || {};
      const livePresetView = liveContextPresetView();
      const emaLabel = (field) => fieldLabel(field, timeframe, livePresetView);
      const items = [
        { id: menuId("a"), label: "View EMA Details", onClick: () => showEmaDetails(row) },
      ];
      if (columnKey === "ema_1" || columnKey === "ema_2") {
        const angleField = columnKey === "ema_1" ? "ema_1_angle_state" : "ema_2_angle_state";
        const slopeField = columnKey === "ema_1" ? "ema_1_slope" : "ema_2_slope";
        const pretty = emaLabel(columnKey);
        items.push(
          { type: "separator" },
          { id: menuId("b"), label: `Add STATE ${emaLabel(angleField)} = UP`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", angleField, "=", "UP")) },
          { id: menuId("c"), label: `Add STATE ${emaLabel(angleField)} = DOWN`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", angleField, "=", "DOWN")) },
          { id: menuId("d"), label: `Add STATE ${pretty} (custom)...`, onClick: () => promptAndAddNumericRule(timeframe, columnKey, pretty, ">=", fmtSnapshotValue(values[columnKey], 2)) },
          { id: menuId("e"), label: `Add STATE ${emaLabel(slopeField)} (custom)...`, onClick: () => promptAndAddNumericRule(timeframe, slopeField, emaLabel(slopeField), ">=", fmtSnapshotValue(values[slopeField], 6)) },
        );
        return items;
      }
      items.push(
        { type: "separator" },
        { id: menuId("b"), label: `Add STATE ${emaLabel("ema_stack")} = BUY`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "ema_stack", "=", "BUY")) },
        { id: menuId("c"), label: `Add STATE ${emaLabel("ema_stack")} = SELL`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "ema_stack", "=", "SELL")) },
        { id: menuId("d"), label: `Add STATE ${emaLabel("ema_stack")} = NEUTRAL`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "ema_stack", "=", "NEUTRAL")) },
        { type: "separator" },
        { id: menuId("e"), label: `Add EVENT ${emaLabel("ema_cross_up")}`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "ema_cross_up", "EVENT", null)) },
        { id: menuId("f"), label: `Add EVENT ${emaLabel("ema_cross_down")}`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "ema_cross_down", "EVENT", null)) },
        { id: menuId("g"), label: `Add ★TRIGGER ${emaLabel("ema_cross_up")}`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "ema_cross_up", "EVENT", null, { is_trigger: true })) },
        { id: menuId("h"), label: `Add ★TRIGGER ${emaLabel("ema_cross_down")}`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "event", "ema_cross_down", "EVENT", null, { is_trigger: true })) },
        { type: "separator" },
        { id: menuId("i"), label: `Add STATE ${emaLabel("ema_1_angle_state")} = UP`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "ema_1_angle_state", "=", "UP")) },
        { id: menuId("j"), label: `Add STATE ${emaLabel("ema_1_angle_state")} = DOWN`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "ema_1_angle_state", "=", "DOWN")) },
        { id: menuId("k"), label: `Add STATE ${emaLabel("ema_2_angle_state")} = UP`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "ema_2_angle_state", "=", "UP")) },
        { id: menuId("l"), label: `Add STATE ${emaLabel("ema_2_angle_state")} = DOWN`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "ema_2_angle_state", "=", "DOWN")) },
        { type: "separator" },
        { id: menuId("m"), label: `Add STATE ${emaLabel("ema_spread")} (custom)...`, onClick: () => promptAndAddNumericRule(timeframe, "ema_spread", emaLabel("ema_spread"), ">=", fmtSnapshotValue(values.ema_spread, 6)) },
        { id: menuId("n"), label: `Add STATE ${emaLabel("ema_spread_pct")} (custom)...`, onClick: () => promptAndAddNumericRule(timeframe, "ema_spread_pct", emaLabel("ema_spread_pct"), ">=", fmtSnapshotValue(values.ema_spread_pct, 3)) },
        { id: menuId("o"), label: `Add STATE ${emaLabel("ema_spread_delta")} > 0`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "ema_spread_delta", ">", 0)) },
        { id: menuId("p"), label: `Add STATE ${emaLabel("ema_spread_delta")} < 0`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "ema_spread_delta", "<", 0)) },
        { id: menuId("q"), label: `Add STATE ${emaLabel("ema_spread_pct_delta")} (custom)...`, onClick: () => promptAndAddNumericRule(timeframe, "ema_spread_pct_delta", emaLabel("ema_spread_pct_delta"), ">=", fmtSnapshotValue(values.ema_spread_pct_delta, 3)) },
        { id: menuId("r"), label: `Add STATE ${emaLabel("ema_spread_trend")} = EXPANDING`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "ema_spread_trend", "=", "EXPANDING")) },
        { id: menuId("s"), label: `Add STATE ${emaLabel("ema_spread_trend")} = CONTRACTING`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "ema_spread_trend", "=", "CONTRACTING")) },
        { id: menuId("t"), label: `Add STATE ${emaLabel("ema_stack_pressure")} = BULL_ACCELERATING`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "ema_stack_pressure", "=", "BULL_ACCELERATING")) },
        { id: menuId("u"), label: `Add STATE ${emaLabel("ema_stack_pressure")} = BEAR_ACCELERATING`, onClick: () => insertRuleIntoTarget(makeRule(timeframe, "state", "ema_stack_pressure", "=", "BEAR_ACCELERATING")) },
      );
      return items;
    };
  }

  function menuWithDetails(row, field) {
    const items = typeof buildSnapshotMenuItems === "function" ? buildSnapshotMenuItems(row, field) : [];
    const detailItem = {
      id: `detail-${field}`,
      label: verifyDetailLabel(row, field),
      onClick: () => showVerifyFieldDetails(row, field),
    };
    return items?.length ? [detailItem, { type: "separator" }, ...items] : [detailItem];
  }

  function renderBuilderOutline() {
    const panel = byId("builder-outline");
    if (!panel) return;
    if (typeof state === "undefined" || !state.builderPreset) {
      panel.innerHTML = '<div class="empty-state">Load a preset to inspect tab and group targets.</div>';
      return;
    }
    const preset = state.builderPreset;
    const target = typeof ensureBuilderTarget === "function" ? ensureBuilderTarget() : state.builderTarget;
    panel.innerHTML = TAB_NAMES.map((tabName) => {
      const tab = preset.tabs?.[tabName] || { groups_join: "AND", groups: [] };
      const groups = Array.isArray(tab.groups) ? tab.groups : [];
      const totalRules = groups.reduce((sum, group) => sum + ((group.rules || []).length), 0);
      return `
        <section class="outline-section">
          <div class="outline-section-head">
            <div>
              <div class="outline-tab">${tabName}</div>
              <div class="outline-meta">${groups.length} groups | ${totalRules} rules | join ${tab.groups_join || "AND"}</div>
            </div>
            <button class="mini-button" type="button" data-builder-action="add-group" data-tab="${tabName}">Add Group</button>
          </div>
          <div class="outline-group-list">
            ${groups.length ? groups.map((group, groupIndex) => {
              const isTarget = target?.tabName === tabName && Number(target?.groupIndex) === Number(groupIndex);
              return `
                <button class="outline-group ${isTarget ? "is-target" : ""}" type="button" data-tab="${tabName}" data-group-index="${groupIndex}" data-builder-action="set-target-group">
                  <strong>Group ${groupIndex + 1}</strong>
                  <span class="outline-meta">${group.rules_join || "OR"} | ${(group.rules || []).length} rules</span>
                </button>
              `;
            }).join("") : '<div class="empty-state">No groups yet.</div>'}
          </div>
        </section>
      `;
    }).join("");
    sanitizeTree(panel);
  }

  function tuneAdvancedLogicButtons() {
    const surface = byId("preset-builder");
    if (!surface || typeof activeBuilderTabName !== "function" || !state?.builderPreset) return;
    const activeTab = activeBuilderTabName();
    const logic = state.builderPreset?.tabs?.[activeTab]?.advanced_logic || { enabled: false, routes: [] };
    surface.querySelectorAll(".builder-head .builder-row").forEach((row) => {
      const button = row.querySelector('[data-builder-action="open-group-logic"]');
      if (!button) return;
      row.querySelectorAll(".pill").forEach((pill) => pill.remove());
      const routeCount = Array.isArray(logic?.routes) ? logic.routes.length : 0;
      button.textContent = logic?.enabled
        ? `Advanced Logic | ${routeCount} route${routeCount === 1 ? "" : "s"}`
        : "Advanced Logic";
      button.classList.toggle("is-active", !!logic?.enabled);
    });
  }

  function renderVerifyHost(options = {}) {
    const host = byId("builder-verify-host");
    if (!host) return;
    const force = Boolean(options && options.force);
    if (!force && verifyEditorRenderLocked()) {
      sanitizeTree(host);
      return;
    }
    const row = activeSnapshotRow();
    host.innerHTML = buildVerifyStripHtml(row);
    sanitizeTree(host);
  }

  function pulseLiveDot() {
    const dot = byId("verify-live-dot");
    if (!dot) return;
    dot.classList.remove("is-pulse");
    void dot.offsetWidth;
    dot.classList.add("is-pulse");
    window.clearTimeout(livePulseTimer);
    livePulseTimer = window.setTimeout(() => dot.classList.remove("is-pulse"), 450);
  }

  function updateLiveControls() {
    const label = byId("verify-live-label");
    if (label) label.textContent = liveStatusText();
    const button = byId("verify-pause-toggle");
    if (button) {
      button.textContent = livePaused ? "Resume" : "Pause";
      button.disabled = false;
    }
    const pill = document.querySelector(".verify-live-pill");
    if (pill) {
      pill.classList.toggle("is-paused", livePaused);
      pill.classList.toggle("is-live", !livePaused && liveSocketConnected && currentLivePhase() === "live");
    }
  }

  function applyLivePayload(payload, options = {}) {
    if (!payload || typeof payload !== "object") return;
    liveSnapshotPayload = payload;
    ensureActiveSnapshotTimeframe();
    renderVerifyHost();
    updateLiveControls();
    if (options.pulse) pulseLiveDot();
  }

  function desiredLiveSocketUrl(name) {
    const base = liveSocketUrl || liveSnapshotPayload?.live_ws_url;
    if (!base) return null;
    try {
      const url = new URL(base, window.location.href);
      if (name) url.searchParams.set("preset", name);
      else url.searchParams.delete("preset");
      return url.toString();
    } catch (error) {
      return null;
    }
  }

  function clearReconnectTimer() {
    if (liveReconnectTimer) {
      window.clearTimeout(liveReconnectTimer);
      liveReconnectTimer = null;
    }
  }

  function closeLiveSocket() {
    clearReconnectTimer();
    if (liveSocket) {
      try {
        liveSocket.close();
      } catch (error) {
        /* noop */
      }
    }
    liveSocket = null;
    liveSocketConnected = false;
  }

  async function stopLiveSession(options = {}) {
    closeLiveSocket();
    pendingPausedSnapshot = null;
    liveBootstrapError = "";
    try {
      await api("/api/v2/live/session", {
        method: "DELETE",
        headers: { "Content-Type": "application/json" },
      });
    } catch (error) {
      if (options.reportError && typeof setSnapshotStatus === "function") {
        setSnapshotStatus(friendlyLiveError(error), true);
      }
    } finally {
      if (options.clearSnapshot === false) return;
      liveSnapshotPayload = null;
      liveSocketUrl = null;
      activeSnapshotTimeframe = null;
      clearIndicatorRefreshCarry();
      renderVerifyHost({ force: true });
      renderSnapshotCompactPanel();
      updateLiveControls();
    }
  }

  function stopLiveSessionOnUnload() {
    closeLiveSocket();
    pendingPausedSnapshot = null;
    try {
      window.fetch("/api/v2/live/session", {
        method: "DELETE",
        keepalive: true,
        headers: { "Content-Type": "application/json" },
      });
    } catch (error) {
      /* noop */
    }
  }

  function scheduleLiveReconnect() {
    clearReconnectTimer();
    const session = currentLiveSessionSpec();
    if (livePaused || !session || !liveSocketUrl) return;
    liveReconnectTimer = window.setTimeout(() => {
      connectLiveSocket(session.socketPresetName);
    }, LIVE_RECONNECT_MS);
  }

  function connectLiveSocket(presetName) {
    const url = desiredLiveSocketUrl(presetName);
    if (!url) return;
    if (liveSocket && (liveSocket.readyState === WebSocket.OPEN || liveSocket.readyState === WebSocket.CONNECTING) && liveSocket.datasetUrl === url) {
      return;
    }
    closeLiveSocket();
    const socket = new WebSocket(url);
    socket.datasetUrl = url;
    liveSocket = socket;
    liveSocketConnected = false;
    updateLiveControls();
    socket.addEventListener("open", () => {
      if (liveSocket !== socket) return;
      liveSocketConnected = true;
      clearReconnectTimer();
      updateLiveControls();
      renderVerifyHost();
    });
    socket.addEventListener("message", (event) => {
      if (liveSocket !== socket) return;
      try {
        const payload = JSON.parse(event.data);
        if (payload && payload.live_ws_url) liveSocketUrl = payload.live_ws_url;
        if (livePaused) {
          pendingPausedSnapshot = payload;
          updateLiveControls();
          return;
        }
        pendingPausedSnapshot = null;
        applyLivePayload(payload, { pulse: true });
      } catch (error) {
        if (typeof setSnapshotStatus === "function") setSnapshotStatus(error.message || String(error), true);
      }
    });
    socket.addEventListener("close", () => {
      if (liveSocket !== socket) return;
      liveSocketConnected = false;
      updateLiveControls();
      renderVerifyHost();
      scheduleLiveReconnect();
    });
    socket.addEventListener("error", () => {
      if (liveSocket !== socket) return;
      liveSocketConnected = false;
      updateLiveControls();
    });
  }

  async function refreshSnapshotOnce(manual = false) {
    const session = currentLiveSessionSpec();
    if (!session || liveFetchInFlight || !isBuilderViewActive()) {
      if (!session) closeLiveSocket();
      renderVerifyHost();
      return;
    }
    const hadSnapshot = snapshotRowsSafe().length > 0;
    liveSnapshotLoading = !hadSnapshot;
    liveFetchInFlight = true;
    renderVerifyHost();
    renderSnapshotCompactPanel();
    updateLiveControls();
    try {
      const snapshot = await api("/api/v2/live/session", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(session.requestBody),
      });
      liveBootstrapError = "";
      if (snapshot?.live_ws_url) {
        liveSocketUrl = snapshot.live_ws_url;
      }
      if (!isBuilderViewActive()) {
        closeLiveSocket();
        return;
      }
      applyLivePayload(snapshot, { pulse: hadSnapshot });
      connectLiveSocket(session.socketPresetName);
      if (typeof setSnapshotStatus === "function") {
        const sourceBits = [snapshot.snapshot_source, snapshot.snapshot_source_note].filter(Boolean);
        if (snapshot.transport) sourceBits.push(snapshot.transport);
        setSnapshotStatus(
          sourceBits.length
            ? `${manual ? "Manual" : "Live"} session ready (${sourceBits.join(", ")}).`
            : "Live session ready."
        );
      }
    } catch (error) {
      liveBootstrapError = friendlyLiveError(error);
      if (typeof setSnapshotStatus === "function") setSnapshotStatus(liveBootstrapError, true);
    } finally {
      liveSnapshotLoading = false;
      liveFetchInFlight = false;
      renderVerifyHost();
      renderSnapshotCompactPanel();
      updateLiveControls();
    }
  }

  function startLiveLoop() {
    clearReconnectTimer();
    if (!isBuilderViewActive()) return;
    if (!livePaused && !hasAnySnapshotValues() && !liveFetchInFlight) {
      void refreshSnapshotOnce(false);
    }
  }

  function handleVerifyHostClick(event) {
    const insideEmaControl = event.target.closest(".verify-ema-control");
    let editorStateChanged = false;
    if (!insideEmaControl && verifyEmaEditorOpen) {
      setVerifyIndicatorEditorOpen("ema", false);
      editorStateChanged = true;
    }
    const insideRsiControl = event.target.closest(".verify-rsi-control");
    if (!insideRsiControl && verifyRsiEditorOpen) {
      setVerifyIndicatorEditorOpen("rsi", false);
      editorStateChanged = true;
    }
    const flushVerifyEditorState = () => {
      if (!editorStateChanged) return;
      renderVerifyHost({ force: true });
      updateLiveControls();
      editorStateChanged = false;
    };
    if (editorStateChanged && !event.target.closest("[data-verify-field]")) {
      flushVerifyEditorState();
    }
    const emaReset = event.target.closest("[data-verify-ema-reset]");
    if (emaReset) {
      const timeframe = String(emaReset.dataset.verifyEmaReset || "").trim();
      applyVerifyEmaMutation(timeframe, currentVerifyEmaSettings(null));
      return;
    }
    const rsiReset = event.target.closest("[data-verify-rsi-reset]");
    if (rsiReset) {
      const timeframe = String(rsiReset.dataset.verifyRsiReset || "").trim();
      applyVerifyRsiMutation(timeframe, currentVerifyRsiSettings(null));
      return;
    }
    const emaToggle = event.target.closest("[data-verify-ema-toggle]");
    if (emaToggle) {
      setVerifyIndicatorEditorOpen("ema", !verifyEmaEditorOpen);
      renderVerifyHost({ force: true });
      updateLiveControls();
      return;
    }
    const rsiToggle = event.target.closest("[data-verify-rsi-toggle]");
    if (rsiToggle) {
      setVerifyIndicatorEditorOpen("rsi", !verifyRsiEditorOpen);
      renderVerifyHost({ force: true });
      updateLiveControls();
      return;
    }
    if (event.target.closest("[data-market-audit-open]")) {
      flushVerifyEditorState();
      openMarketAuditModal();
      return;
    }
    const tfButton = event.target.closest("[data-verify-timeframe]");
    if (tfButton) {
      activeSnapshotTimeframe = tfButton.dataset.verifyTimeframe;
      closeVerifyIndicatorEditors();
      renderVerifyHost({ force: true });
      renderSnapshotCompactPanel();
      return;
    }
    if (event.target.id === "verify-pause-toggle") {
      const session = currentLiveSessionSpec();
      livePaused = !livePaused;
      if (!livePaused) {
        if (pendingPausedSnapshot) {
          applyLivePayload(pendingPausedSnapshot, { pulse: false });
          pendingPausedSnapshot = null;
        } else if (session) {
          connectLiveSocket(session.socketPresetName);
        } else {
          void refreshSnapshotOnce(false);
        }
      }
      updateLiveControls();
      renderVerifyHost({ force: true });
      return;
    }
    const chip = event.target.closest("[data-verify-field]");
    if (!chip) {
      flushVerifyEditorState();
      return;
    }
    const row = activeSnapshotRow();
    if (!row) {
      flushVerifyEditorState();
      return;
    }
    flushVerifyEditorState();
    showVerifyFieldDetails(row, chip.dataset.verifyField);
  }

  function handleVerifyHostChange(event) {
    if (event.target.dataset.verifyEmaTimeframe && event.target.dataset.verifyEmaKey) {
      const timeframe = String(event.target.dataset.verifyEmaTimeframe || "").trim();
      const key = event.target.dataset.verifyEmaKey;
      const nextPair = currentVerifyEmaSettings(timeframe);
      nextPair[key] = Math.max(1, Number(event.target.value || 1));
      applyVerifyEmaMutation(timeframe, nextPair);
      return;
    }
    if (event.target.dataset.verifyRsiTimeframe && event.target.dataset.verifyRsiKey) {
      const timeframe = String(event.target.dataset.verifyRsiTimeframe || "").trim();
      const key = event.target.dataset.verifyRsiKey;
      const nextConfig = currentVerifyRsiSettings(timeframe);
      nextConfig[key] = Math.max(1, Number(event.target.value || 1));
      applyVerifyRsiMutation(timeframe, nextConfig);
    }
  }

  function handleVerifyHostContextMenu(event) {
    const chip = event.target.closest("[data-verify-field]");
    if (!chip) return;
    const row = activeSnapshotRow();
    if (!row || typeof buildSnapshotMenuItems !== "function" || typeof showContextMenu !== "function") return;
    event.preventDefault();
    const items = menuWithDetails(row, chip.dataset.verifyField);
    if (!items || !items.length) return;
    showContextMenu(items, event.clientX, event.clientY);
  }

  function bindVerifyHost() {
    const host = byId("builder-verify-host");
    if (!host || host.dataset.boundVerifyHost === "1") return;
    host.dataset.boundVerifyHost = "1";
    host.addEventListener("click", handleVerifyHostClick);
    host.addEventListener("change", handleVerifyHostChange);
    host.addEventListener("contextmenu", handleVerifyHostContextMenu);
  }

  function bindOutline() {
    const panel = byId("builder-outline");
    if (!panel || panel.dataset.boundOutline === "1") return;
    panel.dataset.boundOutline = "1";
    panel.addEventListener("click", (event) => {
      if (typeof handleBuilderClick === "function") handleBuilderClick(event);
    });
  }

  const originalRenderBuilder = typeof renderBuilder === "function" ? renderBuilder : null;
  if (originalRenderBuilder) {
    renderBuilder = function () {
      originalRenderBuilder();
      tuneAdvancedLogicButtons();
      renderBuilderOutline();
      sanitizeTree(byId("preset-builder"));
      renderVerifyHost();
      renderSnapshotCompactPanel();
    };
  }

  renderSnapshotPanel = function () {
    renderSnapshotCompactPanel();
  };

  const originalLoadSnapshotForSelectedPreset = typeof loadSnapshotForSelectedPreset === "function" ? loadSnapshotForSelectedPreset : null;
  if (originalLoadSnapshotForSelectedPreset) {
    loadSnapshotForSelectedPreset = async function () {
      await originalLoadSnapshotForSelectedPreset();
    };
  }

  const originalRequestBuilderLiveRefresh = typeof requestBuilderLiveRefresh === "function" ? requestBuilderLiveRefresh : null;
  if (originalRequestBuilderLiveRefresh) {
    requestBuilderLiveRefresh = function () {
      originalRequestBuilderLiveRefresh();
      syncLiveContextFromBuilderSettings({ refresh: true });
    };
  }

  const originalSetView = typeof setView === "function" ? setView : null;
  if (originalSetView) {
    setView = function (viewName) {
      const wasBuilderViewActive = isBuilderViewActive();
      originalSetView(viewName);
      if (viewName === "builder" && !livePaused && !hasAnySnapshotValues() && !liveFetchInFlight) {
        void refreshSnapshotOnce(false);
      } else if (wasBuilderViewActive && viewName !== "builder") {
        void stopLiveSession({ clearSnapshot: true });
      }
    };
  }

  const originalShowContextMenu = typeof showContextMenu === "function" ? showContextMenu : null;
  if (originalShowContextMenu) {
    showContextMenu = function (items, x, y) {
      const normalized = Array.isArray(items)
        ? items.map((item) => (item && item.type !== "separator" ? { ...item, label: sanitizeText(item.label) } : item))
        : items;
      originalShowContextMenu(normalized, x, y);
      sanitizeTree(byId("context-menu"));
    };
  }

  const originalShowDetailModal = typeof showDetailModal === "function" ? showDetailModal : null;
  if (originalShowDetailModal) {
    showDetailModal = function (title, html, options) {
      originalShowDetailModal(sanitizeText(title), html, options);
      sanitizeTree(byId("detail-modal-backdrop"));
    };
  }

  const originalHideDetailModal = typeof hideDetailModal === "function" ? hideDetailModal : null;
  if (originalHideDetailModal) {
    hideDetailModal = function () {
      const detailMode = String(byId("detail-modal-backdrop")?.dataset.mode || "");
      if (detailMode === "taker-bias") {
        takerBiasDetailState.row = null;
      }
      stopMarketAuditPlayback();
      stopMarketAuditLiveRefresh();
      stopMarketAuditFetch();
      marketAuditState.open = false;
      return originalHideDetailModal();
    };
  }

  const originalShowEmaDetails = typeof showEmaDetails === "function" ? showEmaDetails : null;
  if (originalShowEmaDetails) {
    showEmaDetails = function (row) {
      const values = row?.values || {};
      if (typeof showDetailModal !== "function" || typeof detailRows !== "function") {
        return originalShowEmaDetails(row);
      }
      const livePresetView = liveContextPresetView();
      const ema = typeof emaDisplayConfig === "function"
        ? emaDisplayConfig(livePresetView, row?.timeframe)
        : {
          fastLength: currentVerifyEmaSettings(row?.timeframe).ema_1_length,
          slowLength: currentVerifyEmaSettings(row?.timeframe).ema_2_length,
        };
      const price = Number(values.price);
      const ema1 = Number(values.ema_1);
      const ema2 = Number(values.ema_2);
      const priceVsFast = Number.isFinite(price) && Number.isFinite(ema1) ? price - ema1 : null;
      const priceVsSlow = Number.isFinite(price) && Number.isFinite(ema2) ? price - ema2 : null;
      const labelFor = (field, fallback) => {
        if (typeof fieldLabel === "function") return fieldLabel(field, row?.timeframe, livePresetView);
        return fallback;
      };
      showDetailModal(`EMA Pair Details · ${row?.timeframe || "-"}`, detailRows([
        ["Fast EMA Length", ema.fastLength],
        ["Slow EMA Length", ema.slowLength],
        [labelFor("ema_1", "Fast EMA"), displayValue(values.ema_1, 3)],
        [labelFor("ema_2", "Slow EMA"), displayValue(values.ema_2, 3)],
        [labelFor("ema_stack", "EMA Stack"), sanitizeText(values.ema_stack ?? "-")],
        [labelFor("ema_1_angle_state", "Fast EMA Angle"), sanitizeText(values.ema_1_angle_state ?? "-")],
        [labelFor("ema_2_angle_state", "Slow EMA Angle"), sanitizeText(values.ema_2_angle_state ?? "-")],
        [labelFor("ema_1_slope", "Fast EMA Slope"), displayValue(values.ema_1_slope, 6)],
        [labelFor("ema_2_slope", "Slow EMA Slope"), displayValue(values.ema_2_slope, 6)],
        [labelFor("ema_spread", "EMA Spread"), displayValue(values.ema_spread, 6)],
        [labelFor("ema_spread_pct", "EMA Spread %"), displayValue(values.ema_spread_pct, 3)],
        [labelFor("ema_spread_delta", "EMA Spread Δ"), displayValue(values.ema_spread_delta, 6)],
        [labelFor("ema_spread_pct_delta", "EMA Spread Δ%"), displayValue(values.ema_spread_pct_delta, 3)],
        [labelFor("ema_spread_trend", "EMA Spread Trend"), sanitizeText(values.ema_spread_trend ?? "-")],
        [labelFor("ema_stack_pressure", "EMA Pressure"), sanitizeText(values.ema_stack_pressure ?? "-")],
        [labelFor("ema_cross_up", "EMA Cross Up"), boolish(values.ema_cross_up) ? "YES" : "no"],
        [labelFor("ema_cross_down", "EMA Cross Down"), boolish(values.ema_cross_down) ? "YES" : "no"],
        ["Price", displayValue(values.price, 3)],
        ["Price vs Fast EMA", displayValue(priceVsFast, 3)],
        ["Price vs Slow EMA", displayValue(priceVsSlow, 3)],
      ]));
      sanitizeTree(byId("detail-modal-backdrop"));
    };
  }

  const originalShowRangeDetails = typeof showRangeDetails === "function" ? showRangeDetails : null;
  if (originalShowRangeDetails) {
    showRangeDetails = function (row) {
      const values = row?.values || {};
      if (typeof showDetailModal === "function" && typeof detailRows === "function") {
        showDetailModal(`Range Details - ${row?.timeframe || "-"}`, detailRows([
          ["State", values.range_filter_display_state ?? values.range_filter_state],
          ["Trigger Age", displayValue(values.range_filter_display_age, 0)],
          ["Raw State", values.range_filter_state],
          ["Phase", values.range_filter_phase],
          ["Line", displayValue(values.range_filter_line, 3)],
          ["Smooth Range", displayValue(values.range_filter_smooth_range, 3)],
          ["Upward Count", displayValue(values.range_filter_upward_count, 0)],
          ["Downward Count", displayValue(values.range_filter_downward_count, 0)],
          ["Buy Trigger Age", displayValue(values.range_filter_buy_age, 0)],
          ["Sell Trigger Age", displayValue(values.range_filter_sell_age, 0)],
          ["Range Buy Event", values.range_filter_buy ? "YES" : "no"],
          ["Range Sell Event", values.range_filter_sell ? "YES" : "no"],
        ]));
        sanitizeTree(byId("detail-modal-backdrop"));
        return;
      }
      originalShowRangeDetails(row);
    };
  }

  const originalFocusTargetGroup = typeof focusTargetGroup === "function" ? focusTargetGroup : null;
  if (originalFocusTargetGroup) {
    focusTargetGroup = function () {
      originalFocusTargetGroup();
      const target = typeof ensureBuilderTarget === "function" ? ensureBuilderTarget() : state?.builderTarget;
      if (!target) return;
      const outlineNode = document.querySelector(`.outline-group[data-tab="${CSS.escape(target.tabName)}"][data-group-index="${target.groupIndex}"]`);
      if (outlineNode) {
        outlineNode.scrollIntoView({ behavior: "smooth", block: "nearest" });
      }
    };
  }

  const DOWNLOADER_TIMEFRAMES = ["1m", "5m", "15m", "30m", "1h"];
  const DOWNLOADER_FAMILIES = [
    { key: "ohlcv", label: "OHLCV" },
    { key: "macd_lines", label: "MACD Lines" },
    { key: "macd_states", label: "MACD States" },
    { key: "rsi", label: "RSI" },
    { key: "stoch_rsi", label: "Stoch RSI" },
    { key: "adx", label: "ADX" },
    { key: "atr", label: "ATR" },
    { key: "taker_bias", label: "Taker Bias" },
    { key: "ema", label: "EMA" },
    { key: "frama", label: "FRAMA" },
    { key: "vidya", label: "VIDYA" },
    { key: "range_filter", label: "Range" },
    { key: "market_aug", label: "Market" },
  ];
  const DOWNLOADER_FAMILY_LABELS = Object.fromEntries(DOWNLOADER_FAMILIES.map((item) => [item.key, item.label]));
  const DOWNLOADER_FAMILY_DEPENDENCIES = {
    macd_states: ["macd_lines"],
    rsi: ["ohlcv"],
    taker_bias: ["ohlcv"],
    ema: ["ohlcv"],
    market_aug: ["ohlcv", "macd_lines", "atr", "frama", "vidya", "range_filter"],
  };
  const DOWNLOADER_FIELD_FAMILY = {
    macd: "macd_lines",
    signal: "macd_lines",
    rsi: "rsi",
    rsi_smooth: "rsi",
    rsi_state: "rsi",
    stoch_rsi_k: "stoch_rsi",
    stoch_rsi_d: "stoch_rsi",
    stoch_rsi_kd: "stoch_rsi",
    ms_cross: "macd_lines",
    macd0_cross: "macd_lines",
    ms: "macd_states",
    angle: "macd_states",
    sig_angle: "macd_states",
    ppo: "macd_states",
    flip_age: "macd_states",
    adx: "adx",
    adx_angle: "adx",
    di_plus: "adx",
    di_minus: "adx",
    di_spread: "adx",
    di_cross: "adx",
    taker_buy_volume: "taker_bias",
    taker_sell_volume: "taker_bias",
    taker_delta_volume: "taker_bias",
    taker_delta_pct: "taker_bias",
    taker_buy_share_pct: "taker_bias",
    taker_trade_count: "taker_bias",
    taker_bias_state: "taker_bias",
    atr: "atr",
    atr_angle: "atr",
    ema_1: "ema",
    ema_2: "ema",
    ema_stack: "ema",
    ema_cross_up: "ema",
    ema_cross_down: "ema",
    ema_1_angle_state: "ema",
    ema_2_angle_state: "ema",
    ema_1_slope: "ema",
    ema_2_slope: "ema",
    ema_spread: "ema",
    ema_spread_pct: "ema",
    frama_state: "frama",
    frama_break_up: "frama",
    frama_break_down: "frama",
    frama_mid_cross: "frama",
    vidya_state: "vidya",
    vidya_line: "vidya",
    vidya_raw: "vidya",
    vidya_upper: "vidya",
    vidya_lower: "vidya",
    vidya_trend_up: "vidya",
    vidya_trend_down: "vidya",
    vidya_up_volume: "vidya",
    vidya_down_volume: "vidya",
    vidya_delta_pct: "vidya",
    vidya_active_liq_low_count: "vidya",
    vidya_active_liq_high_count: "vidya",
    vidya_nearest_liq_low: "vidya",
    vidya_nearest_liq_high: "vidya",
    vidya_dist_to_liq_low_pct: "vidya",
    vidya_dist_to_liq_high_pct: "vidya",
    vidya_new_liq_low: "vidya",
    vidya_new_liq_high: "vidya",
    vidya_liq_low_taken: "vidya",
    vidya_liq_high_taken: "vidya",
    vidya_slope: "market_aug",
    vidya_angle_deg: "market_aug",
    vidya_angle_deg_norm: "market_aug",
    vidya_angle_state: "market_aug",
    vidya_angle_accel: "market_aug",
    range_filter_buy: "range_filter",
    range_filter_sell: "range_filter",
    range_filter_long_cond: "range_filter",
    range_filter_short_cond: "range_filter",
    range_filter_cond_ini: "range_filter",
    range_filter_state: "range_filter",
    range_filter_phase: "range_filter",
    range_filter_line: "range_filter",
    range_filter_upper: "range_filter",
    range_filter_lower: "range_filter",
    range_filter_smooth_range: "range_filter",
    range_filter_upward_count: "range_filter",
    range_filter_downward_count: "range_filter",
    market_state: "market_aug",
    market_bias: "market_aug",
    market_phase: "market_aug",
    market_confidence: "market_aug",
    market_state_age: "market_aug",
    market_direction_score: "market_aug",
    market_energy_score: "market_aug",
    market_alignment_bonus: "market_aug",
    market_macd_gap_norm: "market_aug",
  };
  const DOWNLOADER_INDICATOR_GROUPS = [
    {
      key: "macd_suite",
      label: "MACD / Signal",
      description: "MACD line, Signal line, color states, PPO dot, and MACD cross events live together here.",
      fields: ["macd", "signal", "ms", "angle", "sig_angle", "ppo", "flip_age", "ms_cross", "macd0_cross"],
    },
    {
      key: "taker_bias_suite",
      label: "Taker Bias",
      description: "Binance candle taker-buy versus total volume, trade count, and aggressor bias state.",
      fields: ["taker_bias_state", "taker_delta_pct", "taker_buy_share_pct", "taker_trade_count", "taker_buy_volume", "taker_sell_volume", "taker_delta_volume"],
    },
    {
      key: "rsi_suite",
      label: "RSI",
      description: "Classic RSI, optional smoothing line, and the RSI color state built from RSI versus its smooth companion.",
      fields: ["rsi", "rsi_smooth", "rsi_state"],
    },
    {
      key: "stoch_rsi_suite",
      label: "Stoch RSI",
      description: "TradingView-style Stoch RSI %K, %D, and the K/D state so momentum filters can stay modular.",
      fields: ["stoch_rsi_k", "stoch_rsi_d", "stoch_rsi_kd"],
    },
    {
      key: "adx_suite",
      label: "ADX / DMI",
      description: "ADX plus the full DMI trio so +DI, -DI, spread, and DI cross rules stay together.",
      fields: ["adx", "adx_angle", "di_plus", "di_minus", "di_spread", "di_cross"],
    },
    {
      key: "atr_suite",
      label: "ATR",
      description: "ATR value and ATR angle for volatility-aware rule checks.",
      fields: ["atr", "atr_angle"],
    },
    {
      key: "ema_suite",
      label: "EMA Pair",
      description: "Fast EMA, slow EMA, stack, cross events, angle states, slopes, and spread metrics for the configured EMA pair.",
      fields: [
        "ema_1", "ema_2", "ema_stack", "ema_cross_up", "ema_cross_down",
        "ema_1_angle_state", "ema_2_angle_state", "ema_1_slope", "ema_2_slope",
        "ema_spread", "ema_spread_pct",
      ],
    },
    {
      key: "frama_suite",
      label: "FRAMA",
      description: "FRAMA state and FRAMA event rules, grouped so they do not disappear into legacy core logic.",
      fields: ["frama_state", "frama_break_up", "frama_break_down", "frama_mid_cross"],
    },
    {
      key: "vidya_suite",
      label: "VIDYA",
      description: "VIDYA line, bands, trend events, liquidity signals, slope, and VIDYA angle states in one place.",
      fields: [
        "vidya_state", "vidya_line", "vidya_raw", "vidya_upper", "vidya_lower",
        "vidya_trend_up", "vidya_trend_down", "vidya_up_volume", "vidya_down_volume",
        "vidya_delta_pct", "vidya_slope", "vidya_angle_deg", "vidya_angle_deg_norm",
        "vidya_angle_state", "vidya_angle_accel", "vidya_active_liq_low_count",
        "vidya_active_liq_high_count", "vidya_nearest_liq_low", "vidya_nearest_liq_high",
        "vidya_dist_to_liq_low_pct", "vidya_dist_to_liq_high_pct", "vidya_new_liq_low",
        "vidya_new_liq_high", "vidya_liq_low_taken", "vidya_liq_high_taken",
      ],
    },
    {
      key: "range_suite",
      label: "Range Filter",
      description: "Range state, phase, line, bands, counts, and the long or short trigger conditions that feed event rules.",
      fields: [
        "range_filter_buy", "range_filter_sell", "range_filter_long_cond", "range_filter_short_cond",
        "range_filter_cond_ini", "range_filter_state", "range_filter_phase",
        "range_filter_line", "range_filter_upper", "range_filter_lower",
        "range_filter_smooth_range", "range_filter_upward_count", "range_filter_downward_count",
      ],
    },
    {
      key: "market_suite",
      label: "Market State",
      description: "Higher-level market labels, confidence, and composite scores. Leave this off if you are not using market-state logic.",
      fields: [
        "market_state", "market_bias", "market_phase", "market_confidence", "market_state_age",
        "market_direction_score", "market_energy_score", "market_alignment_bonus", "market_macd_gap_norm",
      ],
    },
  ];
  const DOWNLOADER_INDICATOR_FIELD_ORDER = DOWNLOADER_INDICATOR_GROUPS.flatMap((group) => group.fields);
  const DOWNLOADER_ACTION_LABELS = {
    tick_verify_range: "Verify + Hash",
    tick_download_range: "Download Missing Ticks",
    tick_update_open: "Update OPEN",
    tick_repair_range: "Continue / Repair",
    candle_download_window: "Download Missing Candles",
    indicator_build_window: "Build Indicator Store",
  };

  state.downloader = Object.assign({
    initialized: false,
    summary: null,
    currentJobId: null,
    jobTimer: null,
    currentJobState: null,
    currentJobCanCancel: false,
    indicatorFields: null,
    activeFieldGroupKey: null,
  }, state.downloader || {});

  function setDownloaderStatus(message, isError = false) {
    const node = byId("downloader-status");
    if (!node) return;
    node.textContent = sanitizeText(message || "Ready");
    node.style.color = isError ? "var(--danger)" : "";
  }

  function downloaderActionLabel(action) {
    const key = String(action || "").trim().toLowerCase();
    return DOWNLOADER_ACTION_LABELS[key] || sanitizeText(String(action || "Downloader action"));
  }

  function toDownloaderUtcInputValue(value) {
    const raw = String(value ?? "").trim();
    if (!raw) return "";
    if (typeof toUtcDateTimeLocalValue === "function") {
      return toUtcDateTimeLocalValue(raw);
    }
    return raw.replace(" ", "T").slice(0, 16);
  }

  function fromDownloaderUtcInputValue(value) {
    const raw = String(value ?? "").trim();
    if (!raw) return "";
    if (typeof fromUtcDateTimeLocalValue === "function") {
      return fromUtcDateTimeLocalValue(raw);
    }
    return raw.replace("T", " ");
  }

  function currentClosedUtcMinuteInputValue() {
    const now = new Date();
    now.setUTCSeconds(0, 0);
    now.setUTCMinutes(now.getUTCMinutes() - 1);
    const year = String(now.getUTCFullYear()).padStart(4, "0");
    const month = String(now.getUTCMonth() + 1).padStart(2, "0");
    const day = String(now.getUTCDate()).padStart(2, "0");
    const hour = String(now.getUTCHours()).padStart(2, "0");
    const minute = String(now.getUTCMinutes()).padStart(2, "0");
    return `${year}-${month}-${day}T${hour}:${minute}`;
  }

  function syncDownloaderDateLimits() {
    const maxValue = currentClosedUtcMinuteInputValue();
    ["downloader-start-input", "downloader-end-input"].forEach((id) => {
      const node = byId(id);
      if (!node) return;
      node.max = maxValue;
    });
  }

  function setDownloaderSectionLoading(hostId, message) {
    const host = byId(hostId);
    if (!host) return;
    host.innerHTML = `<div class="empty-state is-loading">${escapeHtml(message)}</div>`;
  }

  function candleInspectionStatus(summary) {
    if (!summary) return "Candle coverage refreshed.";
    if (summary.note) return String(summary.note);
    const coverage = summary.coverage_pct ?? 0;
    const missing = Array.isArray(summary.missing_ranges) ? summary.missing_ranges.length : 0;
    const exactReady = Boolean(summary.exact_cache?.exists);
    return `Candle coverage refreshed: ${coverage}% local, ${missing} missing segment${missing === 1 ? "" : "s"}, ${exactReady ? "exact cache present" : "exact cache not built yet"}.`;
  }

  function indicatorInspectionStatus(summary) {
    if (!summary) return "Indicator coverage refreshed.";
    if (summary.note) return String(summary.note);
    const coverageDays = Number(summary.coverage_days ?? 0);
    const missingDays = Number(summary.missing_days ?? 0);
    const profile = String(summary.profile_hash || "-").slice(0, 12);
    return `Indicator coverage refreshed: ${coverageDays} day${coverageDays === 1 ? "" : "s"} ready, ${missingDays} missing, profile ${sanitizeText(profile)}.`;
  }

  function renderChoicePills(hostId, items, checkedValues) {
    const host = byId(hostId);
    if (!host) return;
    const selected = new Set(Array.isArray(checkedValues) ? checkedValues : []);
    host.innerHTML = items.map((item) => `
      <label class="choice-pill">
        <input type="checkbox" value="${escapeHtml(item.key)}" ${selected.has(item.key) ? "checked" : ""} />
        <span>${escapeHtml(item.label)}</span>
      </label>
    `).join("");
  }

  function getCheckedValues(hostId) {
    return Array.from(document.querySelectorAll(`#${hostId} input[type="checkbox"]:checked`)).map((node) => node.value);
  }

  function setCheckedValues(hostId, values) {
    const selected = new Set(Array.isArray(values) ? values : []);
    document.querySelectorAll(`#${hostId} input[type="checkbox"]`).forEach((node) => {
      node.checked = selected.has(node.value);
    });
  }

  function downloaderFieldKnownSet() {
    return new Set(DOWNLOADER_INDICATOR_FIELD_ORDER);
  }

  function sanitizeDownloaderIndicatorFields(values) {
    const known = downloaderFieldKnownSet();
    const incoming = Array.isArray(values) ? values : [];
    const selected = new Set(incoming.map((value) => String(value || "").trim()).filter(Boolean));
    return DOWNLOADER_INDICATOR_FIELD_ORDER.filter((field) => known.has(field) && selected.has(field));
  }

  function allDownloaderIndicatorFields() {
    return DOWNLOADER_INDICATOR_FIELD_ORDER.slice();
  }

  function defaultDownloaderIndicatorFields() {
    return DOWNLOADER_INDICATOR_FIELD_ORDER.filter((field) => DOWNLOADER_FIELD_FAMILY[field] !== "market_aug");
  }

  function deriveDownloaderFamilies(fields) {
    const selected = sanitizeDownloaderIndicatorFields(fields);
    const families = new Set();
    const stack = [];
    selected.forEach((field) => {
      const family = DOWNLOADER_FIELD_FAMILY[field];
      if (family) stack.push(family);
    });
    while (stack.length) {
      const family = stack.pop();
      if (!family || families.has(family)) continue;
      families.add(family);
      (DOWNLOADER_FAMILY_DEPENDENCIES[family] || []).forEach((dependency) => stack.push(dependency));
    }
    return DOWNLOADER_FAMILIES.map((item) => item.key).filter((key) => families.has(key));
  }

  function getDownloaderIndicatorFields() {
    if (!Array.isArray(state?.downloader?.indicatorFields)) {
      return defaultDownloaderIndicatorFields();
    }
    return sanitizeDownloaderIndicatorFields(state.downloader.indicatorFields);
  }

  function setDownloaderIndicatorFields(values, options = {}) {
    state.downloader.indicatorFields = sanitizeDownloaderIndicatorFields(values);
    if (options.render !== false) {
      renderDownloaderIndicatorGroups();
      renderDownloaderPills();
    }
  }

  function formatFieldPreview(fields, maxCount = 3) {
    const preview = fields.slice(0, maxCount).map((field) => sanitizeText(fieldLabel(field)));
    if (fields.length > maxCount) preview.push(`+${fields.length - maxCount} more`);
    return preview;
  }

  function groupByKey(groupKey) {
    return DOWNLOADER_INDICATOR_GROUPS.find((group) => group.key === groupKey) || null;
  }

  function renderDownloaderIndicatorGroups() {
    const host = byId("downloader-indicator-groups");
    if (!host) return;
    const selected = new Set(getDownloaderIndicatorFields());
    host.innerHTML = DOWNLOADER_INDICATOR_GROUPS.map((group) => {
      const picked = group.fields.filter((field) => selected.has(field));
      const preview = picked.length
        ? formatFieldPreview(picked).map((label, index) => `<span class="downloader-suite-field ${index === 3 ? "is-more" : ""}">${escapeHtml(label)}</span>`).join("")
        : `<span class="downloader-suite-field is-empty">Nothing selected yet</span>`;
      return `
        <article class="downloader-suite-card ${picked.length ? "" : "is-empty"}">
          <div class="downloader-suite-head">
            <div>
              <strong>${escapeHtml(group.label)}</strong>
              <div class="downloader-suite-copy">${escapeHtml(group.description)}</div>
            </div>
            <span class="downloader-suite-count">${picked.length}/${group.fields.length}</span>
          </div>
          <div class="downloader-suite-preview">${preview}</div>
          <button class="mini-button downloader-suite-open" type="button" data-downloader-field-group="${escapeHtml(group.key)}">Choose Fields</button>
        </article>
      `;
    }).join("");
  }

  function downloaderFieldModalHtml(group) {
    const selected = new Set(getDownloaderIndicatorFields());
    return `
      <div class="downloader-field-modal">
        <div class="downloader-field-modal-note">
          ${escapeHtml(group.description)}
          Select the ruleable fields you care about here. The backend still computes the required dependency families automatically.
        </div>
        <div class="downloader-field-modal-actions">
          <button class="secondary-button" type="button" data-downloader-field-action="select-group" data-group-key="${escapeHtml(group.key)}">Select Entire Suite</button>
          <button class="secondary-button" type="button" data-downloader-field-action="clear-group" data-group-key="${escapeHtml(group.key)}">Clear Suite</button>
        </div>
        <div class="downloader-field-list">
          ${group.fields.map((field) => `
            <label class="downloader-field-row">
              <input
                type="checkbox"
                data-downloader-field-checkbox="1"
                data-group-key="${escapeHtml(group.key)}"
                data-field-key="${escapeHtml(field)}"
                ${selected.has(field) ? "checked" : ""}
              />
              <span>
                <strong>${escapeHtml(sanitizeText(fieldLabel(field)))}</strong>
                <small>${escapeHtml(sanitizeText(typeof fieldDescription === "function" ? fieldDescription(field) : field))}</small>
              </span>
            </label>
          `).join("")}
        </div>
      </div>
    `;
  }

  function openDownloaderIndicatorGroup(groupKey) {
    const group = groupByKey(groupKey);
    if (!group || typeof showDetailModal !== "function") return;
    state.downloader.activeFieldGroupKey = group.key;
    showDetailModal(`${group.label} Field Selector`, downloaderFieldModalHtml(group), { mode: "downloader-fields", wide: true });
    sanitizeTree(byId("detail-modal-backdrop"));
  }

  function presetRuleFields(preset) {
    const known = downloaderFieldKnownSet();
    const fields = [];
    const seen = new Set();
    TAB_NAMES.forEach((tabName) => {
      const groups = preset?.tabs?.[tabName]?.groups || [];
      groups.forEach((group) => {
        const rules = Array.isArray(group?.rules) ? group.rules : [];
        rules.forEach((rule) => {
          const field = String(rule?.field || "").trim();
          if (!field || !known.has(field) || seen.has(field)) return;
          seen.add(field);
          fields.push(field);
        });
      });
    });
    return fields;
  }

  function selectedPresetSettings() {
    return state?.selectedPreset?.preset?.settings || null;
  }

  function selectedDownloaderPreset() {
    return state?.selectedPreset?.preset || state?.builderPreset || null;
  }

  function downloaderIndicatorSettings(kind) {
    const spec = patchIndicatorSpec(kind);
    if (!spec) return { defaults: {}, overrides: {} };
    const raw = selectedDownloaderPreset()?.settings?.[spec.key] || null;
    return normalizeLiveIndicatorSettingsValue(kind, raw);
  }

  function downloaderEmaSettings() {
    return downloaderIndicatorSettings("ema");
  }

  function downloaderRsiSettings() {
    return downloaderIndicatorSettings("rsi");
  }

  function summarizeDownloaderIndicatorSettings(kind, indicatorSettings, timeframes) {
    const spec = patchIndicatorSpec(kind);
    if (!spec) return "";
    const normalized = normalizeLiveIndicatorSettingsValue(kind, indicatorSettings);
    const defaults = normalized?.defaults || {};
    const activeTimeframes = Array.isArray(timeframes) && timeframes.length ? timeframes : DOWNLOADER_TIMEFRAMES;
    const overrideCount = activeTimeframes.filter((tf) => Boolean(normalized?.overrides?.[tf])).length;
    return overrideCount
      ? `${spec.label} ${verifyIndicatorText(kind, defaults)} + ${overrideCount} TF override${overrideCount === 1 ? "" : "s"}`
      : `${spec.label} ${verifyIndicatorText(kind, defaults)}`;
  }

  function summarizeDownloaderEmaSettings(emaSettings, timeframes) {
    return summarizeDownloaderIndicatorSettings("ema", emaSettings, timeframes);
  }

  function summarizeDownloaderRsiSettings(rsiSettings, timeframes) {
    return summarizeDownloaderIndicatorSettings("rsi", rsiSettings, timeframes);
  }

  function downloaderPresetDefaults() {
    const settings = selectedPresetSettings() || {};
    const preset = state?.selectedPreset?.preset || null;
    const timeframes = Object.entries(settings.timeframes || {})
      .filter(([, enabled]) => Boolean(enabled))
      .map(([tf]) => tf);
    const ruleFields = presetRuleFields(preset);
    return {
      symbol: settings.symbol || "BTCUSDT",
      cache_dir: settings.cache_dir || "data_cache",
      start: settings.start || "",
      end: settings.end || "",
      warmup_minutes: Number(settings.warmup_min || settings.warmup || 0) || 0,
      price_source: settings.price_source || "LAST",
      timeframes: timeframes.length ? timeframes : DOWNLOADER_TIMEFRAMES.slice(),
      indicator_fields: ruleFields.length ? ruleFields : defaultDownloaderIndicatorFields(),
      req_per_min: 20,
      include_no_meta: false,
      prefer_tail_resume: true,
    };
  }

  function applyDownloaderPreset(force = false) {
    const defaults = downloaderPresetDefaults();
    const symbolNode = byId("downloader-symbol-input");
    const cacheNode = byId("downloader-cache-input");
    const startNode = byId("downloader-start-input");
    const endNode = byId("downloader-end-input");
    const warmupNode = byId("downloader-warmup-input");
    const priceNode = byId("downloader-price-source");
    const rpmNode = byId("downloader-req-per-min-input");
    const noMetaNode = byId("downloader-include-no-meta-input");
    const resumeNode = byId("downloader-prefer-tail-resume-input");
    if (!symbolNode || !cacheNode || !startNode || !endNode || !warmupNode || !priceNode || !rpmNode || !noMetaNode || !resumeNode) return;

    if (force || !symbolNode.value.trim()) symbolNode.value = defaults.symbol;
    if (force || !cacheNode.value.trim()) cacheNode.value = defaults.cache_dir;
    if (force || !startNode.value.trim()) startNode.value = toDownloaderUtcInputValue(defaults.start);
    if (force || !endNode.value.trim()) endNode.value = toDownloaderUtcInputValue(defaults.end);
    if (force || !String(warmupNode.value || "").trim()) warmupNode.value = String(defaults.warmup_minutes);
    if (force || !priceNode.value) priceNode.value = defaults.price_source;
    if (force || !String(rpmNode.value || "").trim()) rpmNode.value = String(defaults.req_per_min);
    if (force) noMetaNode.checked = Boolean(defaults.include_no_meta);
    if (force) resumeNode.checked = Boolean(defaults.prefer_tail_resume);
    setCheckedValues("downloader-timeframes", defaults.timeframes);
    if (force || !sanitizeDownloaderIndicatorFields(state.downloader.indicatorFields).length) {
      setDownloaderIndicatorFields(defaults.indicator_fields, { render: false });
    }
    renderDownloaderIndicatorGroups();
    renderDownloaderPills();
  }

  function renderDownloaderPills() {
    const host = byId("downloader-pill-row");
    if (!host) return;
    const symbol = byId("downloader-symbol-input")?.value?.trim() || "BTCUSDT";
    const startRaw = byId("downloader-start-input")?.value?.trim() || "";
    const endRaw = byId("downloader-end-input")?.value?.trim() || "";
    const start = startRaw ? fromDownloaderUtcInputValue(startRaw) : "Window not set";
    const end = endRaw ? fromDownloaderUtcInputValue(endRaw) : "Window not set";
    const price = byId("downloader-price-source")?.value || "LAST";
    const tfs = getCheckedValues("downloader-timeframes");
    const indicatorFields = getDownloaderIndicatorFields();
    const families = deriveDownloaderFamilies(indicatorFields);
    const noMeta = Boolean(byId("downloader-include-no-meta-input")?.checked);
    const preferResume = Boolean(byId("downloader-prefer-tail-resume-input")?.checked);
    const activeTimeframes = tfs.length ? tfs : DOWNLOADER_TIMEFRAMES;
    const pills = [
      `<span class="pill">${escapeHtml(symbol)}</span>`,
      `<span class="pill">${escapeHtml(start)} -> ${escapeHtml(end)}</span>`,
      `<span class="pill">Source ${escapeHtml(price)}</span>`,
      `<span class="pill">TF ${escapeHtml(activeTimeframes.join(", "))}</span>`,
      `<span class="pill">Fields ${escapeHtml(indicatorFields.length ? formatFieldPreview(indicatorFields, 4).join(", ") : "none")}</span>`,
      `<span class="pill">Families ${escapeHtml((families.length ? families : ["none"]).join(", "))}</span>`,
    ];
    if (families.includes("ema")) {
      pills.push(`<span class="pill">${escapeHtml(summarizeDownloaderEmaSettings(downloaderEmaSettings(), activeTimeframes))}</span>`);
    }
    if (families.includes("rsi")) {
      pills.push(`<span class="pill">${escapeHtml(summarizeDownloaderRsiSettings(downloaderRsiSettings(), activeTimeframes))}</span>`);
    }
    pills.push(
      `<span class="pill">Tail Resume ${preferResume ? "ON" : "OFF"}</span>`,
      `<span class="pill">NO_META ${noMeta ? "ON" : "OFF"}</span>`,
    );
    host.innerHTML = pills.join("");
  }

  function collectDownloaderScope(options = {}) {
    const requireWindow = Boolean(options.requireWindow);
    const startRaw = String(byId("downloader-start-input")?.value || "").trim();
    const endRaw = String(byId("downloader-end-input")?.value || "").trim();
    const payload = {
      symbol: String(byId("downloader-symbol-input")?.value || "").trim().toUpperCase(),
      cache_dir: String(byId("downloader-cache-input")?.value || "").trim(),
      start: startRaw ? fromDownloaderUtcInputValue(startRaw) : "",
      end: endRaw ? fromDownloaderUtcInputValue(endRaw) : "",
      warmup_minutes: Number(byId("downloader-warmup-input")?.value || 0),
      price_source: String(byId("downloader-price-source")?.value || "LAST").trim().toUpperCase(),
      req_per_min: Number(byId("downloader-req-per-min-input")?.value || 20),
      include_no_meta: Boolean(byId("downloader-include-no-meta-input")?.checked),
      prefer_tail_resume: Boolean(byId("downloader-prefer-tail-resume-input")?.checked),
      timeframes: getCheckedValues("downloader-timeframes"),
      indicator_fields: getDownloaderIndicatorFields(),
      ema_settings: downloaderEmaSettings(),
      rsi_settings: downloaderRsiSettings(),
    };
    if (!payload.symbol) {
      throw new Error("Symbol is required.");
    }
    if (!payload.cache_dir) {
      payload.cache_dir = "data_cache";
    }
    if (!payload.timeframes.length) {
      payload.timeframes = DOWNLOADER_TIMEFRAMES.slice();
    }
    payload.indicator_families = deriveDownloaderFamilies(payload.indicator_fields);
    if (requireWindow && (!payload.start || !payload.end)) {
      throw new Error("Start UTC and End UTC are required for this action.");
    }
    return payload;
  }

  function queryStringFromPayload(payload) {
    const params = new URLSearchParams();
    Object.entries(payload || {}).forEach(([key, value]) => {
      if (Array.isArray(value)) {
        if (value.length) params.set(key, value.join(","));
        return;
      }
      if (value === null || value === undefined) return;
      if (typeof value === "object") {
        try {
          params.set(key, JSON.stringify(value));
        } catch (_error) {
          return;
        }
        return;
      }
      const text = String(value).trim();
      if (!text) return;
      params.set(key, text);
    });
    return params.toString();
  }

  function renderStatusChip(label, category) {
    return `<span class="status-chip is-${escapeHtml(category || "repairable")}">${escapeHtml(label || "-")}</span>`;
  }

  function renderTickSummary(summary) {
    const cardHost = byId("tick-summary-cards");
    const body = byId("tick-table-body");
    if (!cardHost || !body) return;
    if (!summary) {
      cardHost.innerHTML = `<div class="empty-state">Run an inspection to see tick day coverage, integrity, and reports.</div>`;
      body.innerHTML = `<tr><td colspan="7" class="muted">No tick rows yet.</td></tr>`;
      return;
    }
    const totals = summary.totals || {};
    cardHost.innerHTML = [
      ["Days", totals.days || 0],
      ["Final", totals.final || 0],
      ["Open", totals.open || 0],
      ["Repairable", totals.repairable || 0],
      ["Corrupt", totals.corrupt || 0],
      ["Missing", totals.missing || 0],
    ].map(([label, value]) => `<article class="summary-card"><div class="eyebrow">${label}</div><strong>${value}</strong></article>`).join("");
    const rows = summary.rows || [];
    if (!rows.length) {
      body.innerHTML = `<tr><td colspan="7" class="muted">No tick rows in the current scope.</td></tr>`;
      return;
    }
    body.innerHTML = rows.map((row) => `
      <tr>
        <td>${escapeHtml(row.day)}</td>
        <td>${renderStatusChip(row.status, row.category)}</td>
        <td>${escapeHtml(row.integrity || "-")}</td>
        <td>${row.rows ?? "-"}</td>
        <td>${escapeHtml(row.last_ts_utc || "-")}</td>
        <td>${row.size_mb ?? "-"}</td>
        <td><button class="mini-button report-button" type="button" data-downloader-report-day="${escapeHtml(row.day)}">Report</button></td>
      </tr>
    `).join("");
  }

  function renderSummaryLines(items, emptyText, options = {}) {
    const values = Array.isArray(items) ? items.filter((item) => String(item || "").trim()) : [];
    const extraClass = options.extraClass ? ` ${escapeHtml(options.extraClass)}` : "";
    if (!values.length) {
      return `<div class="summary-scroll${extraClass}"><div class="summary-line muted">${escapeHtml(emptyText)}</div></div>`;
    }
    return `
      <div class="summary-scroll${extraClass}">
        ${values.map((item) => `<div class="summary-line">${escapeHtml(item)}</div>`).join("")}
      </div>
    `;
  }

  function renderCandleSummary(summary) {
    const host = byId("candle-summary");
    if (!host) return;
    if (!summary) {
      host.innerHTML = `<div class="empty-state">Inspect a window to see candle coverage, missing segments, and window cache reuse.</div>`;
      return;
    }
    const exact = summary.exact_cache || {};
    const covering = summary.covering_cache || {};
    const missing = summary.missing_ranges || [];
    const localDays = summary.local_days || [];
    const missingLines = missing.map((item) => `${item.start_utc} -> ${item.end_utc} (${item.minutes}m)`);
    const localDayLines = localDays.map((item) => `${item.day} | ${item.rows ?? 0} rows | ${item.has_store ? "present" : "missing"}`);
    host.innerHTML = `
      <div class="summary-grid">
        <div class="summary-card-grid">
          <article class="summary-card"><div class="eyebrow">Local Rows</div><strong>${summary.local_row_count ?? 0}</strong></article>
          <article class="summary-card"><div class="eyebrow">Coverage %</div><strong>${summary.coverage_pct ?? "-"}</strong></article>
          <article class="summary-card"><div class="eyebrow">Missing Segments</div><strong>${missing.length}</strong></article>
          <article class="summary-card"><div class="eyebrow">Exact Cache</div><strong>${exact.exists ? "YES" : "no"}</strong></article>
        </div>
        ${summary.note ? `<div class="empty-state">${escapeHtml(summary.note)}</div>` : ""}
        <div class="summary-list">
          <div class="summary-item"><strong>Exact Window Cache</strong><div class="muted">${exact.path ? escapeHtml(exact.path) : "Not materialized yet."}</div></div>
          <div class="summary-item"><strong>Covering Cache</strong><div class="muted">${covering.exists ? `${escapeHtml(covering.path)} (${escapeHtml(covering.start_utc || "-")} -> ${escapeHtml(covering.end_utc || "-")})` : "No covering cache file for this window."}</div></div>
          <div class="summary-item"><strong>Missing Ranges</strong>${renderSummaryLines(missingLines, "None. The local candle store covers the requested window.")}</div>
          <div class="summary-item"><strong>Local Day Files</strong>${renderSummaryLines(localDayLines, "No local day files in scope yet.", { extraClass: "is-tall" })}</div>
        </div>
      </div>
    `;
  }

  function renderIndicatorSummary(summary) {
    const host = byId("indicator-summary");
    if (!host) return;
    if (!summary) {
      host.innerHTML = `<div class="empty-state">Inspect or build the indicator library to see reusable cache coverage for the selected profile.</div>`;
      return;
    }
    const exact = summary.exact_cache || {};
    const covering = summary.covering_cache || {};
    const localDays = summary.local_days || [];
    const localDayLines = localDays.map((item) => `${item.day} | ${item.has_store ? "present" : "missing"}${item.created_at_utc ? ` | ${item.created_at_utc}` : ""}`);
    const selectedFieldLines = (summary.selected_fields || []).map((field) => sanitizeText(fieldLabel(field)));
    const requiredFamilyLines = (summary.required_families || []).map((family) => sanitizeText(DOWNLOADER_FAMILY_LABELS[family] || family));
    const emaProfileLines = Array.isArray(summary.ema_profile_signature)
      ? summary.ema_profile_signature.map((item) => {
        const label = sanitizeText(String(item?.[0] ?? "default"));
        const fast = Number(item?.[1] ?? 9);
        const slow = Number(item?.[2] ?? 21);
        return `${label} | ${fast}/${slow}`;
      })
      : [];
    const rsiProfileLines = Array.isArray(summary.rsi_profile_signature)
      ? summary.rsi_profile_signature.map((item) => {
        const label = sanitizeText(String(item?.[0] ?? "default"));
        const length = Number(item?.[1] ?? 14);
        const smoothing = Number(item?.[2] ?? 3);
        return `${label} | ${length}/${smoothing}`;
      })
      : [];
    host.innerHTML = `
      <div class="summary-grid">
        <div class="summary-card-grid">
          <article class="summary-card"><div class="eyebrow">Profile Hash</div><strong>${escapeHtml(summary.profile_hash || "-")}</strong></article>
          <article class="summary-card"><div class="eyebrow">Coverage Days</div><strong>${summary.coverage_days ?? 0}</strong></article>
          <article class="summary-card"><div class="eyebrow">Missing Days</div><strong>${summary.missing_days ?? 0}</strong></article>
          <article class="summary-card"><div class="eyebrow">Exact Cache</div><strong>${exact.exists ? "YES" : "no"}</strong></article>
        </div>
        ${summary.note ? `<div class="empty-state">${escapeHtml(summary.note)}</div>` : ""}
        <div class="summary-list">
          <div class="summary-item"><strong>Selected Rule Fields</strong>${renderSummaryLines(selectedFieldLines, "All ruleable indicator fields are included for this profile.")}</div>
          <div class="summary-item"><strong>Required Families</strong>${renderSummaryLines(requiredFamilyLines, "No indicator families selected for this profile.")}</div>
          <div class="summary-item"><strong>EMA Profile</strong>${renderSummaryLines(emaProfileLines, "No EMA pair requested for this profile.")}</div>
          <div class="summary-item"><strong>RSI Profile</strong>${renderSummaryLines(rsiProfileLines, "No RSI config requested for this profile.")}</div>
          <div class="summary-item"><strong>Exact Window Cache</strong><div class="muted">${exact.path ? escapeHtml(exact.path) : "Not materialized yet."}</div></div>
          <div class="summary-item"><strong>Covering Cache</strong><div class="muted">${covering.exists ? `${escapeHtml(covering.path)} (${escapeHtml(covering.start_utc || "-")} -> ${escapeHtml(covering.end_utc || "-")})` : "No covering indicator cache for this window."}</div></div>
          <div class="summary-item"><strong>Local Indicator Store</strong>${renderSummaryLines(localDayLines, "No local indicator day files in scope yet.", { extraClass: "is-tall" })}</div>
        </div>
      </div>
    `;
  }

  function renderDownloaderSummary(summary) {
    state.downloader.summary = summary || null;
    renderTickSummary(summary?.tick || null);
    renderCandleSummary(summary?.candles || null);
    renderIndicatorSummary(summary?.indicators || null);
  }

  function renderDownloaderJob(job) {
    const statusNode = byId("downloader-job-status-text");
    const progressNode = byId("downloader-job-progress-text");
    const fillNode = byId("downloader-job-progress-fill");
    const msgNode = byId("downloader-job-message");
    const logNode = byId("downloader-job-log");
    const stopButton = byId("downloader-stop-job-button");
    if (!statusNode || !fillNode || !msgNode || !logNode) return;
    if (!job) {
      state.downloader.currentJobState = null;
      state.downloader.currentJobCanCancel = false;
      statusNode.textContent = "Idle";
      if (progressNode) progressNode.textContent = "0%";
      fillNode.style.width = "0%";
      msgNode.textContent = "No downloader job running.";
      logNode.textContent = "No logs yet.";
      if (stopButton) {
        stopButton.disabled = true;
        stopButton.textContent = "Stop Job";
      }
      return;
    }
    state.downloader.currentJobState = sanitizeText(job.state || "");
    state.downloader.currentJobCanCancel = Boolean(job.can_cancel);
    const progressPct = Math.max(0, Math.min(100, job.progress_pct || 0));
    statusNode.textContent = sanitizeText(job.state || "running");
    if (progressNode) progressNode.textContent = `${Math.round(progressPct)}%`;
    fillNode.style.width = `${progressPct}%`;
    msgNode.textContent = sanitizeText(job.error || job.message || "Working...");
    const logs = Array.isArray(job.logs) && job.logs.length ? job.logs.join("\n") : "No logs yet.";
    logNode.textContent = sanitizeText(logs);
    if (stopButton) {
      const busy = ["queued", "running", "cancelling"].includes(String(job.state || ""));
      stopButton.disabled = !job.can_cancel || !busy || Boolean(job.stop_requested) || String(job.state || "") === "cancelled";
      stopButton.textContent = String(job.state || "") === "cancelling" || Boolean(job.stop_requested) ? "Stopping..." : "Stop Job";
    }
  }

  async function inspectDownloaderCaches(targetLabel = "caches", options = {}) {
    try {
      const focus = String(options.focus || "").trim().toLowerCase();
      if (focus === "candles") {
        setDownloaderSectionLoading("candle-summary", "Inspecting candle coverage for the selected window...");
      } else if (focus === "indicators") {
        setDownloaderSectionLoading("indicator-summary", "Inspecting indicator coverage for the selected profile...");
      }
      setDownloaderStatus(`Inspecting ${targetLabel}...`);
      const payload = collectDownloaderScope({ requireWindow: Boolean(options.requireWindow) });
      if (options.requireIndicatorFields && !payload.indicator_fields.length) {
        throw new Error("Select at least one indicator rule field first.");
      }
      const query = queryStringFromPayload(payload);
      const summary = await api(`/api/v2/data/summary${query ? `?${query}` : ""}`);
      renderDownloaderSummary(summary);
      if (focus === "candles") {
        setDownloaderStatus(candleInspectionStatus(summary?.candles || null));
        return;
      }
      if (focus === "indicators") {
        setDownloaderStatus(indicatorInspectionStatus(summary?.indicators || null));
        return;
      }
      setDownloaderStatus(`Inspection complete for ${targetLabel}.`);
    } catch (error) {
      setDownloaderStatus(error.message, true);
    }
  }

  async function openTickReport(day) {
    try {
      setDownloaderStatus(`Building verification report for ${day}...`);
      const payload = collectDownloaderScope({ requireWindow: false });
      payload.day = day;
      const query = queryStringFromPayload({ symbol: payload.symbol, cache_dir: payload.cache_dir, day });
      const data = await api(`/api/v2/data/tick-report?${query}`);
      if (typeof showDetailModal === "function") {
        showDetailModal(`Tick Report - ${day}`, `<pre class="report-pre">${escapeHtml(data.report_text || "No report available.")}</pre>`, { mode: "tick-report", wide: true });
        sanitizeTree(byId("detail-modal-backdrop"));
      }
      setDownloaderStatus(`Verification report ready for ${day}.`);
    } catch (error) {
      setDownloaderStatus(error.message, true);
    }
  }

  async function startDownloaderJob(action, options = {}) {
    try {
      if (["queued", "running", "cancelling"].includes(String(state.downloader.currentJobState || ""))) {
        throw new Error("A downloader job is already running. Stop it first or wait for it to finish.");
      }
      clearTimeout(state.downloader.jobTimer);
      state.downloader.jobTimer = null;
      const payload = collectDownloaderScope({ requireWindow: Boolean(options.requireWindow) });
      if (options.requireIndicatorFields && !payload.indicator_fields.length) {
        throw new Error("Select at least one indicator rule field first.");
      }
      const response = await api("/api/v2/data/jobs", {
        method: "POST",
        body: JSON.stringify({ action, params: payload }),
      });
      state.downloader.currentJobId = response.job_id;
      renderDownloaderJob({
        state: response.state,
        progress_pct: response.progress_pct,
        message: response.message,
        logs: [],
        can_cancel: response.can_cancel,
        stop_requested: response.stop_requested,
      });
      setDownloaderStatus(`Started ${downloaderActionLabel(action)}.`);
      if (typeof setView === "function") setView("downloader");
      pollDownloaderJob();
    } catch (error) {
      clearTimeout(state.downloader.jobTimer);
      state.downloader.jobTimer = null;
      setDownloaderStatus(error.message, true);
    }
  }

  async function stopDownloaderJob() {
    try {
      if (!state.downloader.currentJobId) {
        throw new Error("There is no active downloader job to stop.");
      }
      clearTimeout(state.downloader.jobTimer);
      state.downloader.jobTimer = null;
      const data = await api(`/api/v2/data/jobs/${state.downloader.currentJobId}/cancel`, {
        method: "POST",
      });
      renderDownloaderJob(data);
      setDownloaderStatus(data.message || "Stop requested. Waiting for the downloader to finish safely...");
      pollDownloaderJob();
    } catch (error) {
      setDownloaderStatus(error.message, true);
    }
  }

  async function pollDownloaderJob() {
    if (!state.downloader.currentJobId) return;
    try {
      const data = await api(`/api/v2/data/jobs/${state.downloader.currentJobId}`);
      renderDownloaderJob(data);
      if (data.state === "cancelling") {
        setDownloaderStatus(data.message || "Stop requested. Waiting for downloader shutdown...");
      }
      if (data.state === "completed") {
        if (data.result?.summary) {
          renderDownloaderSummary(data.result.summary);
        }
        const clampNote = data.result?.window_end_clamped
          ? ` End auto-capped to ${data.result?.window_end || "the last closed 1m candle"}.`
          : "";
        setDownloaderStatus(`${downloaderActionLabel(data.result?.action || data.action)} complete.${clampNote}`);
        clearTimeout(state.downloader.jobTimer);
        state.downloader.jobTimer = null;
        return;
      }
      if (data.state === "cancelled") {
        if (data.result?.summary) {
          renderDownloaderSummary(data.result.summary);
        }
        setDownloaderStatus(data.message || `${downloaderActionLabel(data.result?.action || data.action)} stopped.`);
        clearTimeout(state.downloader.jobTimer);
        state.downloader.jobTimer = null;
        return;
      }
      if (data.state === "failed") {
        setDownloaderStatus(data.error || data.message || "Downloader action failed.", true);
        clearTimeout(state.downloader.jobTimer);
        state.downloader.jobTimer = null;
        return;
      }
    } catch (error) {
      clearTimeout(state.downloader.jobTimer);
      state.downloader.jobTimer = null;
      setDownloaderStatus(error.message, true);
      return;
    }
    state.downloader.jobTimer = setTimeout(pollDownloaderJob, 1200);
  }

  function updateGroupFieldSelection(groupKey, mode) {
    const group = groupByKey(groupKey);
    if (!group) return;
    const selected = new Set(getDownloaderIndicatorFields());
    if (mode === "select") {
      group.fields.forEach((field) => selected.add(field));
    } else if (mode === "clear") {
      group.fields.forEach((field) => selected.delete(field));
    }
    setDownloaderIndicatorFields(Array.from(selected));
    if (state.downloader.activeFieldGroupKey === group.key) {
      openDownloaderIndicatorGroup(group.key);
    }
  }

  function handleDownloaderFieldGroupClick(event) {
    const trigger = event.target.closest("[data-downloader-field-group]");
    if (!trigger) return;
    openDownloaderIndicatorGroup(trigger.dataset.downloaderFieldGroup);
  }

  function handleDownloaderFieldModalClick(event) {
    const backdrop = byId("detail-modal-backdrop");
    if (backdrop?.dataset.mode !== "downloader-fields") return;
    const actionNode = event.target.closest("[data-downloader-field-action]");
    if (!actionNode) return;
    const groupKey = actionNode.dataset.groupKey;
    if (actionNode.dataset.downloaderFieldAction === "select-group") {
      updateGroupFieldSelection(groupKey, "select");
    }
    if (actionNode.dataset.downloaderFieldAction === "clear-group") {
      updateGroupFieldSelection(groupKey, "clear");
    }
  }

  function handleDownloaderFieldModalChange(event) {
    const backdrop = byId("detail-modal-backdrop");
    if (backdrop?.dataset.mode !== "downloader-fields") return;
    const checkbox = event.target.closest("[data-downloader-field-checkbox='1']");
    if (!checkbox) return;
    const field = String(checkbox.dataset.fieldKey || "").trim();
    if (!field) return;
    const selected = new Set(getDownloaderIndicatorFields());
    if (checkbox.checked) {
      selected.add(field);
    } else {
      selected.delete(field);
    }
    setDownloaderIndicatorFields(Array.from(selected));
  }

  function bindDownloader() {
    if (state.downloader.initialized) return;
    state.downloader.initialized = true;
    renderChoicePills("downloader-timeframes", DOWNLOADER_TIMEFRAMES.map((tf) => ({ key: tf, label: tf })), DOWNLOADER_TIMEFRAMES);
    syncDownloaderDateLimits();
    applyDownloaderPreset(false);
    renderDownloaderSummary(null);
    renderDownloaderJob(null);
    setDownloaderStatus("Ready");

    byId("downloader-use-preset-button")?.addEventListener("click", () => {
      applyDownloaderPreset(true);
      setDownloaderStatus("Loaded scope from the selected preset.");
    });
    byId("downloader-inspect-button")?.addEventListener("click", () => void inspectDownloaderCaches("all caches"));
    byId("tick-verify-button")?.addEventListener("click", () => void startDownloaderJob("tick_verify_range", { requireWindow: true }));
    byId("tick-download-button")?.addEventListener("click", () => void startDownloaderJob("tick_download_range", { requireWindow: true }));
    byId("tick-update-open-button")?.addEventListener("click", () => void startDownloaderJob("tick_update_open", { requireWindow: false }));
    byId("tick-repair-button")?.addEventListener("click", () => void startDownloaderJob("tick_repair_range", { requireWindow: false }));
    byId("candle-inspect-button")?.addEventListener("click", () => void inspectDownloaderCaches("candle coverage", { focus: "candles", requireWindow: true }));
    byId("candle-download-button")?.addEventListener("click", () => void startDownloaderJob("candle_download_window", { requireWindow: true }));
    byId("indicator-inspect-button")?.addEventListener("click", () => void inspectDownloaderCaches("indicator coverage", { focus: "indicators", requireWindow: true, requireIndicatorFields: true }));
    byId("indicator-build-button")?.addEventListener("click", () => void startDownloaderJob("indicator_build_window", { requireWindow: true, requireIndicatorFields: true }));
    byId("downloader-stop-job-button")?.addEventListener("click", () => void stopDownloaderJob());
    byId("downloader-fields-select-all-button")?.addEventListener("click", () => setDownloaderIndicatorFields(allDownloaderIndicatorFields()));
    byId("downloader-fields-clear-button")?.addEventListener("click", () => setDownloaderIndicatorFields([]));
    byId("downloader-form")?.addEventListener("input", renderDownloaderPills);
    byId("downloader-form")?.addEventListener("change", renderDownloaderPills);
    byId("downloader-form")?.addEventListener("focusin", syncDownloaderDateLimits);
    byId("downloader-timeframes")?.addEventListener("change", renderDownloaderPills);
    byId("downloader-indicator-groups")?.addEventListener("click", handleDownloaderFieldGroupClick);
    byId("downloader-include-no-meta-input")?.addEventListener("change", renderDownloaderPills);
    byId("downloader-prefer-tail-resume-input")?.addEventListener("change", renderDownloaderPills);
    byId("detail-modal-body")?.addEventListener("click", handleDownloaderFieldModalClick);
    byId("detail-modal-body")?.addEventListener("change", handleDownloaderFieldModalChange);
    byId("tick-table-body")?.addEventListener("click", (event) => {
      const button = event.target.closest("[data-downloader-report-day]");
      if (!button) return;
      void openTickReport(button.dataset.downloaderReportDay);
    });
    document.querySelector('[data-view="downloader"]')?.addEventListener("click", () => {
      if (!state.downloader.summary) {
        setTimeout(() => { void inspectDownloaderCaches("all caches"); }, 0);
      }
    });
  }

  function ensureBacktestSweepState() {
    state.backtestSweep = state.backtestSweep || {
      runId: null,
      rows: [],
      selectedIndex: 0,
      payload: null,
      pendingAutoRunPayload: null,
    };
    return state.backtestSweep;
  }

  function setExitSweepStatus(message, isError = false) {
    const node = byId("exit-sweep-status");
    if (!node) return;
    node.textContent = sanitizeText(message || "");
    node.style.color = isError ? "var(--danger)" : "";
  }

  function sweepNumber(id) {
    return Number(String(byId(id)?.value || "").trim() || 0);
  }

  function sweepAxisPayload(startId, endId, stepId, offId) {
    return {
      start: sweepNumber(startId),
      end: sweepNumber(endId),
      step: sweepNumber(stepId),
      include_off: Boolean(byId(offId)?.checked),
    };
  }

  function expandSweepAxis(axis, label, allowEmpty = false) {
    const values = [];
    let start = Math.max(0, Number(axis?.start || 0));
    let end = Math.max(0, Number(axis?.end || 0));
    const step = Math.max(0, Number(axis?.step || 0));
    const includeOff = Boolean(axis?.include_off);

    if (start > 0 || end > 0) {
      if (end <= 0) end = start;
      if (start <= 0) start = end;
      if (end < start) throw new Error(`${label}: end must be greater than or equal to start.`);
      if (step <= 0) {
        if (Math.abs(end - start) > 1e-9) throw new Error(`${label}: step must be positive when From and To differ.`);
        values.push(Number(start.toFixed(8)));
      } else {
        let current = start;
        let guard = 0;
        while (current <= end + (step * 0.5)) {
          values.push(Number(current.toFixed(8)));
          current += step;
          guard += 1;
          if (guard > 250) throw new Error(`${label}: too many values in this range.`);
        }
      }
    }

    if (includeOff) values.unshift(0);
    const deduped = [];
    const seen = new Set();
    values.forEach((value) => {
      const rounded = Number(Number(value).toFixed(8));
      if (seen.has(rounded)) return;
      seen.add(rounded);
      deduped.push(rounded);
    });
    if (!deduped.length && !allowEmpty) throw new Error(`${label}: add a range or enable OFF.`);
    return deduped;
  }

  function exitSweepMaxCombinations() {
    return Math.max(1, Number(byId("exit-sweep-max-combos-input")?.value || 5000));
  }

  function collectExitSweepPayload(options = {}) {
    const maxCombos = exitSweepMaxCombinations();
    const payload = {
      auto_run: Boolean(byId("exit-sweep-auto-run-input")?.checked),
      sort_by: String(byId("exit-sweep-sort-by")?.value || "net_profit"),
      top_n: maxCombos,
      max_combinations: maxCombos,
      stop_loss: sweepAxisPayload("sweep-stop-loss-start", "sweep-stop-loss-end", "sweep-stop-loss-step", "sweep-stop-loss-off"),
      take_profit: sweepAxisPayload("sweep-take-profit-start", "sweep-take-profit-end", "sweep-take-profit-step", "sweep-take-profit-off"),
      break_even: sweepAxisPayload("sweep-break-even-start", "sweep-break-even-end", "sweep-break-even-step", "sweep-break-even-off"),
      trailing_stop: sweepAxisPayload("sweep-trailing-stop-start", "sweep-trailing-stop-end", "sweep-trailing-stop-step", "sweep-trailing-stop-off"),
    };
    if (options.validate !== false) {
      expandSweepAxis(payload.stop_loss, "Stop Loss");
      expandSweepAxis(payload.take_profit, "Take Profit");
      expandSweepAxis(payload.break_even, "Break Even");
      expandSweepAxis(payload.trailing_stop, "Trailing Stop");
    }
    return payload;
  }

  function renderExitSweepEstimate() {
    const node = byId("exit-sweep-estimate");
    if (!node) return;
    try {
      const payload = collectExitSweepPayload({ validate: false });
      const sl = expandSweepAxis(payload.stop_loss, "Stop Loss", true);
      const tp = expandSweepAxis(payload.take_profit, "Take Profit", true);
      const be = expandSweepAxis(payload.break_even, "Break Even", true);
      const tr = expandSweepAxis(payload.trailing_stop, "Trailing Stop", true);
      if (!sl.length || !tp.length || !be.length || !tr.length) {
        node.textContent = "Define From/To/Step or enable OFF for all four exit axes.";
        node.style.color = "";
        return;
      }
      const combos = sl.length * tp.length * be.length * tr.length;
      const limit = Math.max(1, Number(payload.max_combinations || 5000));
      node.textContent = `Estimated combinations: ${combos} (${sl.length} x ${tp.length} x ${be.length} x ${tr.length}, limit ${limit})`;
      node.style.color = combos > limit ? "var(--danger)" : "";
    } catch (error) {
      node.textContent = sanitizeText(error.message || "Sweep range is invalid.");
      node.style.color = "var(--danger)";
    }
  }

  function syncExitSweepButtons() {
    const sweepState = ensureBacktestSweepState();
    const hasRows = Array.isArray(sweepState.rows) && sweepState.rows.length > 0;
    const selected = hasRows ? sweepState.rows[Math.max(0, Math.min(sweepState.selectedIndex || 0, sweepState.rows.length - 1))] : null;
    if (byId("exit-sweep-apply-best-button")) byId("exit-sweep-apply-best-button").disabled = !hasRows;
    if (byId("exit-sweep-apply-selected-button")) byId("exit-sweep-apply-selected-button").disabled = !selected;
    if (byId("exit-sweep-run-button")) byId("exit-sweep-run-button").disabled = !sweepState.runId;
  }

  function selectExitSweepRow(index) {
    const sweepState = ensureBacktestSweepState();
    sweepState.selectedIndex = Number(index) || 0;
    renderExitSweepResults(sweepState.payload);
  }

  function renderExitSweepResults(sweepPayload) {
    const cards = byId("exit-sweep-best-cards");
    const body = byId("exit-sweep-table-body");
    const sweepState = ensureBacktestSweepState();
    sweepState.payload = sweepPayload || null;
    sweepState.rows = Array.isArray(sweepPayload?.rows) ? sweepPayload.rows : [];
    if (typeof sweepState.selectedIndex !== "number") sweepState.selectedIndex = 0;

    if (!cards || !body) return;
    if (!sweepPayload || !Array.isArray(sweepPayload.rows) || !sweepPayload.rows.length) {
      cards.innerHTML = `<div class="empty-state">No exit sweep results yet. Run a backtest, then launch the sweep to rank SL / TP / BE / TR combinations.</div>`;
      body.innerHTML = `<tr><td colspan="15" class="muted">No exit sweep results yet.</td></tr>`;
      syncExitSweepButtons();
      return;
    }

    const best = sweepPayload.best_combo || sweepPayload.rows[0] || null;
    cards.innerHTML = best ? [
      ["Best Combo", `SL ${best.stop_loss_label} | TP ${best.take_profit_label} | BE ${best.break_even_label} | TR ${best.trailing_stop_label}`],
      ["Combos", sweepPayload.combo_count ?? sweepPayload.rows.length],
      ["Best Net", fmt(best.net_profit)],
      ["Best Return %", fmt(best.return_pct)],
      ["Best PF", fmt(best.profit_factor)],
      ["Best Max DD %", fmt(best.max_drawdown_pct)],
    ].map(([label, value]) => `<article class="metric-card"><div class="eyebrow">${escapeHtml(label)}</div><strong>${escapeHtml(value)}</strong></article>`).join("") : "";

    body.innerHTML = sweepPayload.rows.map((row, index) => `
      <tr class="${index === sweepState.selectedIndex ? "is-selected" : ""}" data-exit-sweep-index="${index}">
        <td>${row.rank ?? (index + 1)}</td>
        <td>${escapeHtml(row.stop_loss_label)}</td>
        <td>${escapeHtml(row.take_profit_label)}</td>
        <td>${escapeHtml(row.break_even_label)}</td>
        <td>${escapeHtml(row.trailing_stop_label)}</td>
        <td>${fmt(row.net_profit)}</td>
        <td>${fmt(row.return_pct)}</td>
        <td>${fmt(row.profit_factor)}</td>
        <td>${fmt(row.win_rate_pct)}</td>
        <td>${fmt(row.max_drawdown_pct)}</td>
        <td>${row.num_trades ?? "-"}</td>
        <td>${row.stop_loss_count ?? 0}</td>
        <td>${row.take_profit_count ?? 0}</td>
        <td>${row.break_even_count ?? 0}</td>
        <td>${row.trailing_stop_count ?? 0}</td>
      </tr>
    `).join("");
    setExitSweepStatus(`Sweep ranked ${sweepPayload.combo_count || sweepPayload.rows.length} combinations by ${sanitizeText(String(sweepPayload.sort_by || "net_profit"))}.`);
    syncExitSweepButtons();
  }

  function applyExitSweepCombo(combo) {
    if (!combo) return;
    if (byId("stop-loss-input")) byId("stop-loss-input").value = Number(combo.stop_loss_pct || 0);
    if (byId("take-profit-input")) byId("take-profit-input").value = Number(combo.take_profit_pct || 0);
    if (byId("break-even-input")) byId("break-even-input").value = Number(combo.break_even_after_mfe_pct || 0);
    if (byId("trailing-stop-input")) byId("trailing-stop-input").value = Number(combo.trailing_stop_pct || 0);
    setExitSweepStatus(`Applied combo to settings: SL ${combo.stop_loss_label} | TP ${combo.take_profit_label} | BE ${combo.break_even_label} | TR ${combo.trailing_stop_label}.`);
  }

  async function startExitSweepJob(options = {}) {
    try {
      const sweepState = ensureBacktestSweepState();
      const presetName = String(options.presetName || byId("preset-select")?.value || state?.selectedPreset?.name || "").trim();
      if (!presetName) throw new Error("Select a preset first.");
      if (!sweepState.runId) throw new Error("Run a backtest first so the sweep can reuse that replay context.");
      const exitSweep = options.payload ? JSON.parse(JSON.stringify(options.payload)) : collectExitSweepPayload({ validate: true });
      expandSweepAxis(exitSweep.stop_loss, "Stop Loss");
      expandSweepAxis(exitSweep.take_profit, "Take Profit");
      expandSweepAxis(exitSweep.break_even, "Break Even");
      expandSweepAxis(exitSweep.trailing_stop, "Trailing Stop");
      exitSweep.auto_run = false;
      const payload = await api("/api/v2/backtests/exit-sweep", {
        method: "POST",
        body: JSON.stringify({ preset_name: presetName, run_id: sweepState.runId, exit_sweep: exitSweep }),
      });
      state.currentJobId = payload.job_id;
      $("job-pill").textContent = `Job ${payload.job_id.slice(0, 8)} running`;
      setExitSweepStatus(options.autoTriggered ? "Base backtest finished. Exit sweep started in a separate job." : "Exit sweep started.");
      pollJob();
      if (typeof setView === "function") setView("backtest");
    } catch (error) {
      const prefix = options.autoTriggered ? "Base backtest is ready, but auto exit sweep was not launched" : null;
      setExitSweepStatus(prefix ? `${prefix}: ${error.message}` : error.message, true);
    }
  }

  const originalSubmitBacktest = typeof submitBacktest === "function" ? submitBacktest : null;
  submitBacktest = async function (event) {
    event.preventDefault();
    const presetName = $("preset-select").value;
    if (!presetName) return;
    const formState = typeof collectBacktestFormState === "function"
      ? collectBacktestFormState()
      : {
        start: (typeof fromUtcDateTimeLocalValue === "function" ? fromUtcDateTimeLocalValue($("start-input").value) : $("start-input").value.trim()) || "",
        end: (typeof fromUtcDateTimeLocalValue === "function" ? fromUtcDateTimeLocalValue($("end-input").value) : $("end-input").value.trim()) || "",
        stop_loss_value: Number($("stop-loss-input").value || 0),
        take_profit_value: Number($("take-profit-input").value || 0),
        break_even_after_mfe_pct: Number($("break-even-input").value || 0),
        trailing_stop_pct: Number($("trailing-stop-input").value || 0),
        order_notional: Number($("position-size-input").value || 100),
        fee_pct: Number($("commission-input").value || 0.04),
        equity_curve_stride: Number($("equity-stride-input").value || 15),
        allow_reverse: $("allow-reverse-input").checked,
        capture_trade_details: $("capture-details-input").checked,
      };
    const overrides = {
      start: formState.start || null,
      end: formState.end || null,
      stop_loss_mode: "PERCENT",
      stop_loss_value: Number(formState.stop_loss_value || 0),
      take_profit_mode: "PERCENT",
      take_profit_value: Number(formState.take_profit_value || 0),
      break_even_after_mfe_pct: Number(formState.break_even_after_mfe_pct || 0),
      trailing_stop_pct: Number(formState.trailing_stop_pct || 0),
      order_notional: Number(formState.order_notional || 100),
      fee_pct: Number(formState.fee_pct || 0.04),
      equity_curve_stride: Number(formState.equity_curve_stride || 15),
      allow_reverse: Boolean(formState.allow_reverse),
      capture_trade_details: Boolean(formState.capture_trade_details),
    };
    const sweepState = ensureBacktestSweepState();
    sweepState.pendingAutoRunPayload = null;
    if (Boolean(byId("exit-sweep-auto-run-input")?.checked)) {
      sweepState.pendingAutoRunPayload = collectExitSweepPayload({ validate: false });
      setExitSweepStatus("Auto-run enabled. Base backtest will render first, then the exit sweep will start in a separate job.");
    } else {
      setExitSweepStatus("Backtest running. Exit sweep stays idle until you launch it.");
    }
    if (typeof setBacktestFormStatus === "function") {
      setBacktestFormStatus(`Running backtest for ${presetName}...`);
    }
    const payload = await api("/api/v2/backtests", {
      method: "POST",
      body: JSON.stringify({ preset_name: presetName, overrides }),
    });
    state.currentJobId = payload.job_id;
    $("job-pill").textContent = `Job ${payload.job_id.slice(0, 8)} running`;
    pollJob();
    if (typeof setView === "function") setView("backtest");
  };

  const originalPollJob = typeof pollJob === "function" ? pollJob : null;
  pollJob = async function () {
    if (!state.currentJobId) return;
    try {
      const data = await api(`/api/v2/jobs/${state.currentJobId}`);
      $("job-status-text").textContent = data.state;
      $("job-message").textContent = data.error || data.message || "Working...";
      $("job-progress-fill").style.width = `${Math.max(0, Math.min(100, data.progress_pct || 0))}%`;
      if (data.state === "completed" && data.result) {
        const resultKind = String(data.result.result_kind || data.kind || "backtest");
        $("job-pill").textContent = `Last job ${data.job_id.slice(0, 8)} complete`;
        if (resultKind === "backtest") {
          const sweepState = ensureBacktestSweepState();
          sweepState.runId = data.result.run_id || null;
          state.currentRunId = data.result.run_id || null;
          state.currentResult = data.result || null;
          $("result-bundle-path").textContent = data.result.verification_bundle || "Result ready";
          renderMetrics(data.result.summary || {});
          renderTrades(data.result.sample_trades || []);
          if (typeof clearTradeInspector === "function") {
            clearTradeInspector("Loading full trade list...");
          }
          renderActivity(data.result.event_activity || {});
          drawEquity(data.result.equity_curve_preview || []);
          renderExitSweepResults(data.result.exit_sweep || null);
          if (typeof hydrateRunTrades === "function" && data.result.run_id) {
            setTimeout(() => { void hydrateRunTrades(data.result.run_id); }, 0);
          }
          const pendingAutoSweep = sweepState.pendingAutoRunPayload ? JSON.parse(JSON.stringify(sweepState.pendingAutoRunPayload)) : null;
          sweepState.pendingAutoRunPayload = null;
          if (data.result.exit_sweep_error) {
            setExitSweepStatus(`Auto sweep failed: ${data.result.exit_sweep_error}`, true);
          } else if (!data.result.exit_sweep) {
            setExitSweepStatus("Backtest is ready. You can run the exit sweep now.");
          }
          clearTimeout(state.jobTimer);
          state.jobTimer = null;
          syncExitSweepButtons();
          if (pendingAutoSweep) {
            setTimeout(() => { void startExitSweepJob({ payload: pendingAutoSweep, autoTriggered: true }); }, 0);
          }
          return;
        } else if (resultKind === "exit_sweep") {
          ensureBacktestSweepState().runId = data.result.run_id || ensureBacktestSweepState().runId;
          if (data.result.verification_bundle) $("result-bundle-path").textContent = data.result.verification_bundle;
          renderExitSweepResults(data.result.exit_sweep || null);
        }
        clearTimeout(state.jobTimer);
        state.jobTimer = null;
        syncExitSweepButtons();
        return;
      }
      if (data.state === "failed") {
        $("job-pill").textContent = `Job ${data.job_id.slice(0, 8)} failed`;
        if (String(data.kind || "") === "exit_sweep") {
          setExitSweepStatus(data.error || data.message || "Exit sweep failed.", true);
        }
        clearTimeout(state.jobTimer);
        state.jobTimer = null;
        return;
      }
    } catch (error) {
      $("job-message").textContent = error.message;
    }
    state.jobTimer = setTimeout(pollJob, 1200);
  };

  function bindBacktestSweep() {
    const sweepState = ensureBacktestSweepState();
    if (sweepState.initialized) return;
    sweepState.initialized = true;
    sweepState.runId = sweepState.runId || null;
    renderExitSweepEstimate();
    renderExitSweepResults(null);

    [
      "exit-sweep-auto-run-input",
      "exit-sweep-sort-by",
      "exit-sweep-max-combos-input",
      "sweep-stop-loss-start",
      "sweep-stop-loss-end",
      "sweep-stop-loss-step",
      "sweep-stop-loss-off",
      "sweep-take-profit-start",
      "sweep-take-profit-end",
      "sweep-take-profit-step",
      "sweep-take-profit-off",
      "sweep-break-even-start",
      "sweep-break-even-end",
      "sweep-break-even-step",
      "sweep-break-even-off",
      "sweep-trailing-stop-start",
      "sweep-trailing-stop-end",
      "sweep-trailing-stop-step",
      "sweep-trailing-stop-off",
    ].forEach((id) => {
      byId(id)?.addEventListener("input", renderExitSweepEstimate);
      byId(id)?.addEventListener("change", renderExitSweepEstimate);
    });

    byId("exit-sweep-run-button")?.addEventListener("click", () => void startExitSweepJob());
    byId("exit-sweep-apply-best-button")?.addEventListener("click", () => {
      const payload = ensureBacktestSweepState().payload;
      if (payload?.best_combo) applyExitSweepCombo(payload.best_combo);
    });
    byId("exit-sweep-apply-selected-button")?.addEventListener("click", () => {
      const sweep = ensureBacktestSweepState();
      const combo = Array.isArray(sweep.rows) ? sweep.rows[sweep.selectedIndex || 0] : null;
      if (combo) applyExitSweepCombo(combo);
    });
    byId("exit-sweep-table-body")?.addEventListener("click", (event) => {
      const row = event.target.closest("[data-exit-sweep-index]");
      if (!row) return;
      selectExitSweepRow(Number(row.dataset.exitSweepIndex || 0));
    });
    syncExitSweepButtons();
  }

  const originalRenderPresetDetail = typeof renderPresetDetail === "function" ? renderPresetDetail : null;
  if (originalRenderPresetDetail) {
    renderPresetDetail = function (detail) {
      originalRenderPresetDetail(detail);
      applyDownloaderPreset(false);
    };
  }

  function bootstrapPatch() {
    if (!readStoredLiveContext()) {
      liveContextState = normalizeLiveContext({});
      persistLiveContext();
    }
    bindOutline();
    bindVerifyHost();
    bindMarketAuditModal();
    bindTakerBiasDetailModal();
    bindMarketAuditLauncher();
    bindDownloader();
    bindBacktestSweep();
    ensureActiveSnapshotTimeframe();
    renderBuilderOutline();
    renderVerifyHost();
    renderSnapshotCompactPanel();
    updateLiveControls();
    startLiveLoop();
    sanitizeTree(document.body);
  }

  document.addEventListener("DOMContentLoaded", bootstrapPatch);
  window.addEventListener("load", bootstrapPatch);
  window.addEventListener("beforeunload", stopLiveSessionOnUnload);
})();
