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
        return "Positioning pressure stayed broad enough to suggest persistent crowding rather than isolated bursts."

    if avg_risk >= 0.60 or peak_risk >= 6:
        return "Positioning pressure built repeatedly across the week, with stress showing more continuity than usual."

    if avg_risk >= 0.35 or peak_risk >= 5:
        return "Positioning pressure appeared in bursts rather than sustained build-up."

    if avg_risk >= 0.20 or peak_risk >= 3:
        return "Positioning stayed active, but crowding remained localized rather than regime-like."

    return "Positioning pressure remained mostly contained."


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
        return "Options positioning showed structural pressure, with compression appearing often enough to matter."

    if composite >= 0.30 or compression_share >= 10:
        return "Options markets showed building pressure, with directional expectations becoming less neutral."

    if composite >= 0.20:
        return "Options markets stayed cautious with limited directional conviction."

    if composite >= 0.10:
        return "Options positioning remained mostly balanced, with only light signs of compression."

    return "Options positioning remained broadly neutral."


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
        return "Volatility term structure stayed elevated through a meaningful part of the week."

    if overlap >= 20 or avg_vbi >= 20:
        return "Volatility showed intermittent elevation rather than a sustained expansion."

    if overlap >= 10 or avg_vbi >= 15:
        return "Volatility background showed brief pockets of firmness, but not a persistent stress regime."

    return "Volatility background stayed relatively calm."


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
        return "Structural takeaway:\n\nPressure was not isolated.\n\nFutures crowding and volatility background aligned often enough to suggest unstable conditions rather than random noise."

    if (avg_risk >= 0.35 or peak_risk >= 5) and max(avg_mci, avg_olsi) < 0.22 and overlap < 15:
        return "Structural takeaway:\n\nFutures pressure appeared locally, but options expectations and volatility background did not confirm a broader unstable regime.\n\nThis looked more like selective crowding than system-wide stress."

    if avg_risk < 0.30 and (avg_mci >= 0.28 or mci_gt_06 >= 10) and overlap < 20:
        return "Structural takeaway:\n\nOptions markets carried more of the structural signal than futures positioning.\n\nCompression appeared without broad crowding, which is more consistent with latent pressure than with an already expanded move."

    if avg_risk < 0.30 and overlap >= 20:
        return "Structural takeaway:\n\nVolatility background stayed firmer than futures positioning.\n\nThis suggests repricing in the background, without broad crowding across the market."

    if avg_risk < 0.25 and max(avg_mci, avg_olsi) < 0.18 and overlap < 12:
        return "Structural takeaway:\n\nSignals stayed relatively contained.\n\nPressure appeared selectively, but not as a clear regime shift across the system."

    return "Structural takeaway:\n\nSignals stayed mixed.\n\nPressure appeared selectively, not as a clear system-wide regime shift."



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
        f"Livermore weekly snapshot ({window}d)\n\nSignals aggregated across:\n\n• futures positioning\n• options expectations\n• volatility background",
        f"Futures layer (Binance)\n\nAvg risk: {rounded_str(avg_risk, 2)}\nPeak risk: {rounded_str(peak_risk, 1)}\n\nMain stress leaders:\n{top_stress}.\n\n{fut_text}",
        f"Options expectations (Bybit / OKX)\n\nBybit MCI avg: {rounded_str(avg_mci, 2)}\nHigh-compression windows (>0.6): {rounded_str(mci_gt_06, 0)}%\n\nOKX avg OLSI: {rounded_str(avg_olsi, 2)}\n\n{opt_text}",
        f"Volatility background (Deribit)\n\nBTC VBI avg: {rounded_str(btc_vbi, 1)}\nETH VBI avg: {rounded_str(eth_vbi, 1)}\n\nBTC/ETH warm overlap: {rounded_str(overlap, 0)}% of windows.\n\n{vol_text}",
        build_synthesis_text(stats),
    ]
    return [trim_tweet(t, max_len=260) for t in tweets]
