from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

from ...prepared_dataset import (
    PreparedDataset,
    build_prepared_dataset,
    find_covering_prepared_dataset,
    find_covering_prepared_dataset_on_disk,
    prepared_dataset_identity_key,
    save_prepared_dataset_to_disk,
)


class PreparedDatasetMixin:
    """In-memory + on-disk prepared dataset cache helpers for backtest/history."""

    def _remember_prepared_dataset(self, *,
                                   symbol: str,
                                   start: "pd.Timestamp",
                                   end: "pd.Timestamp",
                                   timeframes: List[str],
                                   price_source: str,
                                   macd_impl: str,
                                   adx_impl: str,
                                   required_fields: Any,
                                   market_state_thresholds: Any,
                                   df_1m_full: Optional[pd.DataFrame],
                                   streams_full: Optional[Dict[str, pd.DataFrame]]) -> None:
        if not isinstance(df_1m_full, pd.DataFrame) or df_1m_full.empty:
            return
        if not isinstance(streams_full, dict) or not streams_full:
            return
        try:
            entry = build_prepared_dataset(
                symbol=symbol,
                start=start,
                end=end,
                timeframes=timeframes,
                price_source=price_source,
                macd_impl=macd_impl,
                adx_impl=adx_impl,
                required_fields=required_fields,
                market_state_thresholds=market_state_thresholds,
                df_1m_full=df_1m_full,
                streams_full=streams_full,
            )
        except Exception:
            return

        key = prepared_dataset_identity_key(entry)
        kept: List[PreparedDataset] = []
        for existing in getattr(self, "_prepared_datasets", []) or []:
            try:
                if prepared_dataset_identity_key(existing) != key:
                    kept.append(existing)
            except Exception:
                continue
        kept.append(entry)
        limit = max(1, int(getattr(self, "_prepared_dataset_limit", 4) or 4))
        self._prepared_datasets = kept[-limit:]

    def _find_prepared_dataset(self, *,
                               symbol: str,
                               start: "pd.Timestamp",
                               end: "pd.Timestamp",
                               timeframes: List[str],
                               price_source: str,
                               macd_impl: str,
                               adx_impl: str,
                               required_fields: Any,
                               market_state_thresholds: Any) -> Optional[PreparedDataset]:
        try:
            return find_covering_prepared_dataset(
                getattr(self, "_prepared_datasets", []) or [],
                symbol=symbol,
                start=start,
                end=end,
                timeframes=timeframes,
                price_source=price_source,
                macd_impl=macd_impl,
                adx_impl=adx_impl,
                required_fields=required_fields,
                market_state_thresholds=market_state_thresholds,
            )
        except Exception:
            return None

    def _find_prepared_dataset_on_disk(self, *,
                                       cache_dir: str,
                                       symbol: str,
                                       start: "pd.Timestamp",
                                       end: "pd.Timestamp",
                                       timeframes: List[str],
                                       price_source: str,
                                       macd_impl: str,
                                       adx_impl: str,
                                       required_fields: Any,
                                       market_state_thresholds: Any) -> Optional[PreparedDataset]:
        try:
            return find_covering_prepared_dataset_on_disk(
                cache_dir,
                symbol=symbol,
                start=start,
                end=end,
                timeframes=timeframes,
                price_source=price_source,
                macd_impl=macd_impl,
                adx_impl=adx_impl,
                required_fields=required_fields,
                market_state_thresholds=market_state_thresholds,
            )
        except Exception:
            return None

    def _persist_prepared_dataset(self, *,
                                  cache_dir: str,
                                  symbol: str,
                                  start: "pd.Timestamp",
                                  end: "pd.Timestamp",
                                  timeframes: List[str],
                                  price_source: str,
                                  macd_impl: str,
                                  adx_impl: str,
                                  required_fields: Any,
                                  market_state_thresholds: Any,
                                  df_1m_full: Optional[pd.DataFrame],
                                  streams_full: Optional[Dict[str, pd.DataFrame]]) -> Optional[str]:
        if not cache_dir:
            cache_dir = "data_cache"
        try:
            entry = build_prepared_dataset(
                symbol=symbol,
                start=start,
                end=end,
                timeframes=timeframes,
                price_source=price_source,
                macd_impl=macd_impl,
                adx_impl=adx_impl,
                required_fields=required_fields,
                market_state_thresholds=market_state_thresholds,
                df_1m_full=df_1m_full,
                streams_full=streams_full,
            )
        except Exception:
            return None
        try:
            return save_prepared_dataset_to_disk(cache_dir, entry)
        except Exception:
            return None
