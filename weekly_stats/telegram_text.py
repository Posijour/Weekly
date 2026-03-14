from typing import Any, Dict, List, Optional, Tuple


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _format_int(value: Any) -> str:
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return "n/a"


def _format_pct(value: Any) -> str:
    parsed = _safe_float(value)
    if parsed is None:
        return "n/a"
    return f"{parsed:.1f}%"


def _top_symbols_line(risk_stats: Dict[str, Any]) -> str:
    symbols_with_share = risk_stats.get("top_symbols_by_risk_ge_3_share_pct") or []
    symbols_by_avg = risk_stats.get("top_symbols_by_avg_risk") or []
    selected: List[Tuple[Any, Any]] = symbols_with_share or symbols_by_avg
    names = [str(item[0]) for item in selected[:3] if isinstance(item, (list, tuple)) and item]
    return ", ".join(names) if names else "n/a"


def _directional_regimes_pct(bybit_stats: Dict[str, Any]) -> Optional[float]:
    explicit = _safe_float(bybit_stats.get("regime_directional_total_pct"))
    if explicit is not None:
        return explicit

    regime_share = bybit_stats.get("regime_share_pct") or {}
    directional_up = _safe_float(regime_share.get("directional_up")) or 0.0
    directional_down = _safe_float(regime_share.get("directional_down")) or 0.0
    total = directional_up + directional_down
    return round(total, 1)


def _warm_hot_share_pct(deribit_stats: Dict[str, Any], symbol: str) -> Optional[float]:
    prefetched = _safe_float(deribit_stats.get(f"{symbol.lower()}_warm_hot_pct"))
    if prefetched is not None:
        return prefetched

    state_share = ((deribit_stats.get("symbols") or {}).get(symbol) or {}).get("state_share_pct") or {}
    warm = _safe_float(state_share.get("WARM")) or 0.0
    hot = _safe_float(state_share.get("HOT")) or 0.0
    return round(warm + hot, 1)


def build_weekly_telegram_extension_interpretation(stats: Dict[str, Any]) -> str:
    risk_stats = stats.get("risk") or {}
    bybit_stats = stats.get("bybit") or {}
    deribit_stats = stats.get("deribit") or {}

    high_risk_3 = risk_stats.get("market_high_risk_hours", {}).get("risk_ge_3")
    high_risk_5 = risk_stats.get("market_high_risk_hours", {}).get("risk_ge_5")
    calm_pct = _safe_float(bybit_stats.get("regime_calm_pct")) or 0.0
    directional_pct = _directional_regimes_pct(bybit_stats) or 0.0
    overlap_pct = _safe_float(deribit_stats.get("both_hot_or_warm_share_pct")) or 0.0

    pressure_shape = "persistent" if (high_risk_3 or 0) >= 24 else "localized"
    stress_shape = "contained" if (high_risk_5 or 0) <= 6 else "active"
    options_confirmation = "limited" if calm_pct >= 45 else "visible"
    vol_alignment = "firmer" if overlap_pct >= 25 else "selective"

    lines = [
    (
        "Futures pressure was "
        f"{pressure_shape}, while extreme stress stayed {stress_shape} through the week."
    ),
    (
        "Options confirmation remained "
        f"{options_confirmation}: CALM share held at {_format_pct(calm_pct)} versus directional regimes at {_format_pct(directional_pct)}."
    ),
    (
        "The volatility backdrop was "
        f"{'firmer' if vol_alignment == 'firmer' else 'more selective'} than futures alone would imply, with BTC/ETH warm alignment at {_format_pct(overlap_pct)}."
    ),
    "The broader structure stayed mixed: pressure was visible, but cross-market alignment remained partial.",
]
    return "\n".join(lines)


def build_weekly_telegram_post(stats: Dict[str, Any], tweets: List[str]) -> str:
    twitter_block = "\n\n".join(tweet.strip() for tweet in tweets if isinstance(tweet, str) and tweet.strip())

    risk_stats = stats.get("risk") or {}
    bybit_stats = stats.get("bybit") or {}
    deribit_stats = stats.get("deribit") or {}

    high_risk_3 = risk_stats.get("market_high_risk_hours", {}).get("risk_ge_3")
    high_risk_5 = risk_stats.get("market_high_risk_hours", {}).get("risk_ge_5")
    top_symbols = _top_symbols_line(risk_stats)

    calm_pct = bybit_stats.get("regime_calm_pct")
    directional_pct = _directional_regimes_pct(bybit_stats)
    mci_gt_06 = bybit_stats.get("mci_gt_06_share_pct")

    btc_warm_hot = _warm_hot_share_pct(deribit_stats, "BTC")
    eth_warm_hot = _warm_hot_share_pct(deribit_stats, "ETH")
    overlap = deribit_stats.get("both_hot_or_warm_share_pct")

    additional_read = build_weekly_telegram_extension_interpretation(stats)

    blocks = [
        "Livermore weekly snapshot (7d)",
        twitter_block,
        "— Telegram extension —",
        "Futures structure",
        f"• High-risk hours (risk ≥3): {_format_int(high_risk_3)}",
        f"• Extreme stress hours (risk ≥5): {_format_int(high_risk_5)}",
        f"• Most persistent pressure: {top_symbols}",
        "",
        "Options structure",
        f"• CALM regime share: {_format_pct(calm_pct)}",
        f"• Directional regimes: {_format_pct(directional_pct)}",
        f"• High-compression windows (>0.6): {_format_pct(mci_gt_06)}",
        "",
        "Volatility structure",
        f"• BTC warm/hot share: {_format_pct(btc_warm_hot)}",
        f"• ETH warm/hot share: {_format_pct(eth_warm_hot)}",
        f"• BTC/ETH warm overlap: {_format_pct(overlap)}",
        "",
        "Additional read",
        additional_read,
    ]

    return "\n".join(block for block in blocks if block is not None).strip()
