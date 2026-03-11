import re
from typing import Any, Dict, List, Optional, Tuple


def trim_tweet(text: str, max_len: int = 280) -> str:
    text = re.sub(r"[ \t]+", " ", text).strip()
    text = re.sub(r"\n{3,}", "\n\n", text)
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"


def clean_symbol(sym: str) -> str:
    return str(sym).replace("USDT", "")


def top_symbol_names(items: List[Tuple[str, Any]], limit: int = 3) -> str:
    if not items:
        return "none"
    return ", ".join(clean_symbol(k) for k, _ in items[:limit])


def rounded_str(x: Any, digits: int = 2) -> str:
    if x is None:
        return "n/a"
    try:
        return str(round(float(x), digits))
    except Exception:
        return str(x)


def build_short_futures_interpretation(avg_risk: Optional[float], peak_risk: Optional[float]) -> str:
    avg_risk = avg_risk or 0.0
    peak_risk = peak_risk or 0.0

    if avg_risk >= 0.90 or peak_risk >= 8:
        return "Futures positioning stayed under broad pressure, pointing to persistent crowding rather than a few isolated spikes."

    if avg_risk >= 0.60 or peak_risk >= 6:
        return "Crowding resurfaced repeatedly through the week, suggesting stress had more continuity than in a typical short-lived flare-up."

    if avg_risk >= 0.35 or peak_risk >= 5:
        return "Pressure emerged in visible bursts, though it never became a fully sustained market-wide build."

    if avg_risk >= 0.20 or peak_risk >= 3:
        return "Positioning stayed active, but the pressure looked localized rather than broad enough to define the week."

    return "Futures positioning remained relatively contained, with no broad crowding signal taking hold."


def build_short_options_interpretation(
    avg_mci: Optional[float],
    avg_olsi: Optional[float],
    mci_gt_06_share_pct: Optional[float] = None,
) -> str:
    avg_mci = avg_mci or 0.0
    avg_olsi = avg_olsi or 0.0
    compression_share = mci_gt_06_share_pct or 0.0
    composite = max(avg_mci, avg_olsi)

    if composite >= 0.40 or compression_share >= 18:
        return "Options positioning carried clear structural tension, with compression appearing often enough to matter at the weekly horizon."

    if composite >= 0.30 or compression_share >= 10:
        return "Options markets began to lean away from neutral, with pressure building in a way that deserves attention."

    if composite >= 0.20:
        return "Options stayed cautious overall, but without strong enough conviction to frame the week as decisively directional."

    if composite >= 0.10:
        return "Options positioning remained mostly balanced, with only light and occasional signs of compression."

    return "Options stayed broadly neutral, with little evidence of meaningful structural pressure."


def build_short_vol_interpretation(
    overlap: Optional[float],
    btc_vbi: Optional[float] = None,
    eth_vbi: Optional[float] = None,
) -> str:
    overlap = overlap or 0.0
    btc_vbi = btc_vbi or 0.0
    eth_vbi = eth_vbi or 0.0
    avg_vbi = (btc_vbi + eth_vbi) / 2 if (btc_vbi or eth_vbi) else 0.0

    if overlap >= 40 or avg_vbi >= 26:
        return "Volatility term structure stayed elevated for a meaningful part of the week, which is hard to dismiss as background noise."

    if overlap >= 20 or avg_vbi >= 20:
        return "Volatility firmed up from time to time, though it never developed into a sustained expansion regime."

    if overlap >= 10 or avg_vbi >= 15:
        return "The volatility backdrop showed brief pockets of firmness, but not the kind of persistence usually seen in broader stress phases."

    return "The volatility backdrop stayed relatively calm, with little sign of sustained repricing pressure."


def build_synthesis_text(stats: Dict[str, Any]) -> str:
    risk = stats["risk"]
    bybit = stats["bybit"]
    okx = stats["okx"]
    deribit = stats["deribit"]

    avg_risk = risk.get("avg_risk") or 0.0
    peak_risk = risk.get("max_risk") or 0.0
    avg_mci = bybit.get("avg_mci") or 0.0
    mci_gt_06 = bybit.get("mci_gt_06_share_pct") or 0.0
    avg_olsi = okx.get("avg_olsi") or 0.0
    overlap = deribit.get("both_hot_or_warm_share_pct") or 0.0
    btc_vbi = deribit.get("symbols", {}).get("BTC", {}).get("avg_vbi_score") or 0.0
    eth_vbi = deribit.get("symbols", {}).get("ETH", {}).get("avg_vbi_score") or 0.0
    avg_vbi = (btc_vbi + eth_vbi) / 2

    if (avg_risk >= 0.55 and overlap >= 20) or (peak_risk >= 6 and avg_vbi >= 20):
        return (
            "Structural takeaway:\n\n"
            "Pressure was not confined to one layer.\n\n"
            "Futures crowding and the volatility backdrop lined up often enough to suggest unstable conditions rather than random short-term noise."
        )

    if (avg_risk >= 0.35 or peak_risk >= 5) and max(avg_mci, avg_olsi) < 0.22 and overlap < 15:
        return (
            "Structural takeaway:\n\n"
            "The pressure was more visible in futures than elsewhere.\n\n"
            "Options and volatility did not confirm a broader unstable regime, which keeps this closer to selective crowding than to system-wide stress."
        )

    if avg_risk < 0.30 and (avg_mci >= 0.28 or mci_gt_06 >= 10) and overlap < 20:
        return (
            "Structural takeaway:\n\n"
            "Options carried more of the structural signal than futures this week.\n\n"
            "Compression appeared without broad crowding, which fits latent pressure better than an already expanded move."
        )

    if avg_risk < 0.30 and overlap >= 20:
        return (
            "Structural takeaway:\n\n"
            "The volatility backdrop stayed firmer than futures positioning.\n\n"
            "That points to repricing in the background, without broad crowding spreading across the market."
        )

    if avg_risk < 0.25 and max(avg_mci, avg_olsi) < 0.18 and overlap < 12:
        return (
            "Structural takeaway:\n\n"
            "The week stayed relatively contained.\n\n"
            "Some local pressure appeared, but not in a way that suggests a broader regime shift."
        )

    return (
        "Structural takeaway:\n\n"
        "The picture remained fragmented.\n\n"
        "Pressure appeared in places, but the broader system never aligned strongly enough to confirm a clear market-wide shift."
    )


def build_thread_tweets(stats: Dict[str, Any]) -> List[str]:
    window = stats["window_days"]
    risk = stats["risk"]
    bybit = stats["bybit"]
    okx = stats["okx"]
    deribit = stats["deribit"]

    avg_risk = risk.get("avg_risk")
    peak_risk = risk.get("max_risk")
    top_stress = top_symbol_names(risk.get("top_symbols_by_avg_risk", []), limit=3)
    fut_text = build_short_futures_interpretation(avg_risk, peak_risk)

    avg_mci = bybit.get("avg_mci")
    mci_gt_06 = bybit.get("mci_gt_06_share_pct")
    avg_olsi = okx.get("avg_olsi")
    opt_text = build_short_options_interpretation(avg_mci, avg_olsi, mci_gt_06)

    btc_vbi = deribit.get("symbols", {}).get("BTC", {}).get("avg_vbi_score")
    eth_vbi = deribit.get("symbols", {}).get("ETH", {}).get("avg_vbi_score")
    overlap = deribit.get("both_hot_or_warm_share_pct")
    vol_text = build_short_vol_interpretation(overlap, btc_vbi, eth_vbi)

    tweets = [
        (
            f"Livermore weekly snapshot ({window}d)\n\n"
            f"7-day structural view across:\n\n"
            f"• futures positioning\n"
            f"• options expectations\n"
            f"• volatility background"
        ),
        (
            f"Futures layer (Binance)\n\n"
            f"Avg risk: {rounded_str(avg_risk, 2)}\n"
            f"Peak risk: {rounded_str(peak_risk, 1)}\n\n"
            f"Main stress leaders:\n"
            f"{top_stress}.\n\n"
            f"{fut_text}"
        ),
        (
            f"Options expectations (Bybit / OKX)\n\n"
            f"Bybit MCI avg: {rounded_str(avg_mci, 2)}\n"
            f"High-compression windows (>0.6): {rounded_str(mci_gt_06, 0)}%\n\n"
            f"OKX avg OLSI: {rounded_str(avg_olsi, 2)}\n\n"
            f"{opt_text}"
        ),
        (
            f"Volatility background (Deribit)\n\n"
            f"BTC VBI avg: {rounded_str(btc_vbi, 1)}\n"
            f"ETH VBI avg: {rounded_str(eth_vbi, 1)}\n\n"
            f"BTC/ETH warm overlap: {rounded_str(overlap, 0)}% of windows.\n\n"
            f"{vol_text}"
        ),
        build_synthesis_text(stats),
    ]
    return [trim_tweet(t, max_len=260) for t in tweets]
