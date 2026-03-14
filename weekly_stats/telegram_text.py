from typing import Any, Dict, List, Optional, Tuple


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_pct(value: Any, digits: int = 1) -> str:
    parsed = _safe_float(value)
    if parsed is None:
        return "n/a"
    return f"{parsed:.{digits}f}%"


def _top_symbols_line(risk_stats: Dict[str, Any]) -> str:
    symbols_with_share = risk_stats.get("top_symbols_by_risk_ge_3_share_pct") or []
    symbols_by_avg = risk_stats.get("top_symbols_by_avg_risk") or []
    selected: List[Tuple[Any, Any]] = symbols_with_share or symbols_by_avg
    names = [str(item[0]).replace("USDT", "") for item in selected[:3] if isinstance(item, (list, tuple)) and item]
    return ", ".join(names) if names else "n/a"


def _directional_regimes_pct(bybit_stats: Dict[str, Any]) -> float:
    explicit = _safe_float(bybit_stats.get("regime_directional_total_pct"))
    if explicit is not None:
        return explicit

    regime_share = bybit_stats.get("regime_share_pct") or {}
    directional_up = _safe_float(regime_share.get("directional_up")) or 0.0
    directional_down = _safe_float(regime_share.get("directional_down")) or 0.0
    return round(directional_up + directional_down, 1)


def _warm_hot_share_pct(deribit_stats: Dict[str, Any], symbol: str) -> Optional[float]:
    prefetched = _safe_float(deribit_stats.get(f"{symbol.lower()}_warm_hot_pct"))
    if prefetched is not None:
        return prefetched

    state_share = ((deribit_stats.get("symbols") or {}).get(symbol) or {}).get("state_share_pct") or {}
    warm = _safe_float(state_share.get("WARM")) or 0.0
    hot = _safe_float(state_share.get("HOT")) or 0.0
    return round(warm + hot, 1)


def _cross_layer_state(futures_pressure: str, options_state: str, vol_state: str) -> Tuple[str, str]:
    active = 0
    if futures_pressure in {"persistent", "visible"}:
        active += 1
    if options_state == "active":
        active += 1
    if vol_state == "firm":
        active += 1

    if active >= 3:
        regime = "broad alignment"
        sentence = "Pressure appeared across all three layers, with synchronized confirmation rather than isolated pockets."
    elif futures_pressure in {"persistent", "visible"} and active == 1:
        regime = "futures-led pressure"
        sentence = "Futures carried the clearest stress signal, while broader confirmation remained partial."
    elif options_state == "active" and active == 1:
        regime = "options-led pressure"
        sentence = "Options carried the strongest directional pressure, while futures and volatility stayed less aligned."
    elif vol_state == "firm" and active == 1:
        regime = "vol-led backdrop"
        sentence = "Volatility ran firmer than positioning alone would imply, without full futures/options alignment."
    elif active == 0:
        regime = "contained structure"
        sentence = "Cross-layer structure remained contained, with no persistent multi-layer stress regime."
    else:
        regime = "mixed structure"
        sentence = "Pressure appeared across more than one layer, but alignment remained partial rather than fully synchronized."

    return regime, sentence


def build_weekly_telegram_interpretation(stats: Dict[str, Any]) -> str:
    risk_stats = stats.get("risk") or {}
    bybit_stats = stats.get("bybit") or {}
    okx_stats = stats.get("okx") or {}
    deribit_stats = stats.get("deribit") or {}

    avg_risk = _safe_float(risk_stats.get("avg_risk")) or 0.0
    high_risk_3 = _safe_float((risk_stats.get("market_high_risk_hours") or {}).get("risk_ge_3")) or 0.0
    high_risk_5 = _safe_float((risk_stats.get("market_high_risk_hours") or {}).get("risk_ge_5")) or 0.0
    top_symbols = _top_symbols_line(risk_stats)

    calm_pct = _safe_float(bybit_stats.get("regime_calm_pct")) or 0.0
    directional_pct = _directional_regimes_pct(bybit_stats)
    compression_pct = _safe_float(bybit_stats.get("mci_gt_06_share_pct")) or 0.0
    avg_mci = _safe_float(bybit_stats.get("avg_mci")) or 0.0
    avg_olsi = _safe_float(okx_stats.get("avg_olsi")) or 0.0

    btc_warm_hot = _warm_hot_share_pct(deribit_stats, "BTC")
    eth_warm_hot = _warm_hot_share_pct(deribit_stats, "ETH")
    overlap = _safe_float(deribit_stats.get("both_hot_or_warm_share_pct")) or 0.0
    btc_vbi = _safe_float(deribit_stats.get("btc_vbi")) or 0.0
    eth_vbi = _safe_float(deribit_stats.get("eth_vbi")) or 0.0

    if avg_risk >= 0.60 or high_risk_3 >= 20:
        futures_pressure = "persistent"
    elif avg_risk >= 0.35 or high_risk_3 >= 10:
        futures_pressure = "visible"
    elif avg_risk >= 0.20:
        futures_pressure = "localized"
    else:
        futures_pressure = "contained"

    if high_risk_5 >= 10:
        stress_state = "active"
    elif high_risk_5 >= 4:
        stress_state = "visible"
    else:
        stress_state = "contained"

    futures_sentence = (
        f"Futures pressure was {futures_pressure} through the week, while extreme stress remained {stress_state}; "
        f"main stress leaders were {top_symbols}."
    )

    if calm_pct > 60 and compression_pct < 5:
        options_state = "neutral"
    elif directional_pct > 25 or compression_pct > 10:
        options_state = "active"
    else:
        options_state = "mixed"
    options_sentence = (
        f"Options structure stayed {options_state}, with CALM at {_format_pct(calm_pct)} and directional regimes at {_format_pct(directional_pct)}; "
        f"avg MCI/OLSI held near {avg_mci:.2f}/{avg_olsi:.2f}, with compression windows (>0.6) at {_format_pct(compression_pct)}."
    )

    avg_vbi = (btc_vbi + eth_vbi) / 2.0
    if overlap >= 25 or avg_vbi >= 20:
        vol_state = "firm"
    elif overlap >= 12 or avg_vbi >= 15:
        vol_state = "mixed"
    else:
        vol_state = "contained"
    vol_sentence = (
        f"Volatility background was {vol_state}, with BTC warm/hot at {_format_pct(btc_warm_hot)} and ETH warm/hot at {_format_pct(eth_warm_hot)}; "
        f"joint elevated overlap reached {_format_pct(overlap)} and average BTC/ETH VBI was {avg_vbi:.1f}."
    )

    regime_label, cross_layer_sentence = _cross_layer_state(futures_pressure, options_state, vol_state)
    regime_sentence = f"{regime_label.capitalize()} with futures at {futures_pressure}, options {options_state}, and volatility {vol_state}."

    lines = [
        "Livermore weekly structural read (7d)",
        "",
        "Futures structure",
        futures_sentence,
        "",
        "Options structure",
        options_sentence,
        "",
        "Volatility structure",
        vol_sentence,
        "",
        "Cross-layer read",
        cross_layer_sentence,
        "",
        "Weekly regime",
        regime_sentence,
    ]
    return "\n".join(lines).strip()
