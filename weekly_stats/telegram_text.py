from typing import Any, Dict, List

from tweet_interpretations import (
    build_short_futures_interpretation,
    build_short_options_interpretation,
    build_short_vol_interpretation,
    build_synthesis_text,
)


def _split_synthesis_sections(synthesis: str) -> List[str]:
    lines = [line.strip() for line in synthesis.splitlines() if line.strip()]
    parts = [line for line in lines if line.lower() != "structural takeaway:"]
    if not parts:
        return ["Market structure stayed readable, but evidence remained partial this week."]
    return parts


def build_weekly_telegram_interpretation(stats: Dict[str, Any]) -> str:
    window = stats.get("window_days", 7)
    risk = stats.get("risk") or {}
    bybit = stats.get("bybit") or {}
    okx = stats.get("okx") or {}
    deribit = stats.get("deribit") or {}

    futures_text = build_short_futures_interpretation(risk.get("avg_risk"), risk.get("max_risk"))
    options_text = build_short_options_interpretation(
        bybit.get("avg_mci"),
        okx.get("avg_olsi"),
        bybit.get("mci_gt_06_share_pct"),
    )
    vol_text = build_short_vol_interpretation(
        deribit.get("both_hot_or_warm_share_pct"),
        (deribit.get("symbols", {}).get("BTC", {}) or {}).get("avg_vbi_score"),
        (deribit.get("symbols", {}).get("ETH", {}) or {}).get("avg_vbi_score"),
    )

    synthesis_parts = _split_synthesis_sections(build_synthesis_text(stats))
    cross_layer_text = synthesis_parts[0]
    weekly_regime_text = synthesis_parts[1] if len(synthesis_parts) > 1 else synthesis_parts[0]

    lines = [
        f"<b>Livermore weekly structural read ({window}d)</b>",
        "",
        "<u>Futures structure</u>",
        futures_text,
        "",
        "<u>Options structure</u>",
        options_text,
        "",
        "<u>Volatility structure</u>",
        vol_text,
        "",
        "<u>Cross-layer read</u>",
        cross_layer_text,
        "",
        "<u>Weekly regime</u>",
        weekly_regime_text,
    ]
    return "\n".join(lines).strip()
