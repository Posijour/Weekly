import re
import random
from typing import Any, Dict, List, Optional, Tuple


INTRO_VARIANTS = [
    "Livermore weekly snapshot ({window}d)\n\n7-day structural view across:\n\n• futures positioning\n• options expectations\n• volatility background",
    "Livermore weekly snapshot ({window}d)\n\nThis week's structural read across:\n\n• futures positioning\n• options expectations\n• volatility background",
    "Livermore weekly snapshot ({window}d)\n\nWeekly market structure view across:\n\n• futures positioning\n• options expectations\n• volatility background",
]

FUTURES_VARIANTS = {
    "extreme": [
        "Futures positioning stayed under broad pressure, pointing to persistent crowding rather than a few isolated spikes.",
        "Crowding remained broad enough to suggest a persistent stress regime rather than brief flare-ups.",
        "Pressure held across enough of the week to look like sustained crowding, not random bursts.",
    ],
    "high": [
        "Crowding resurfaced repeatedly through the week, suggesting stress had more continuity than in a typical short-lived flare-up.",
        "Pressure returned often enough through the week to suggest something more persistent than isolated bursts.",
        "Futures stress appeared again and again, pointing to recurring crowding rather than one-off spikes.",
    ],
    "medium": [
        "Pressure emerged in visible bursts, though it never became a fully sustained market-wide build.",
        "Stress came in waves, but it did not develop into a broader market-wide pattern.",
        "Crowding showed up in bursts rather than as a continuous build through the week.",
    ],
    "light": [
        "Positioning stayed active, but the pressure looked localized rather than broad enough to define the week.",
        "Futures remained active, though the pressure stayed mostly local rather than systemic.",
        "Some crowding appeared, but not broadly enough to shape the full weekly picture.",
    ],
    "calm": [
        "Futures positioning remained relatively contained, with no broad crowding signal taking hold.",
        "Positioning stayed mostly contained, without a meaningful broad crowding regime.",
        "Futures remained comparatively quiet, with little sign of broad pressure building.",
    ],
}

OPTIONS_VARIANTS = {
    "extreme": [
        "Options positioning carried clear structural tension, with compression appearing often enough to matter at the weekly horizon.",
        "Compression showed up often enough to make options one of the clearest sources of structural pressure this week.",
        "Options markets reflected persistent tension, not just scattered pockets of compression.",
    ],
    "high": [
        "Options markets began to lean away from neutral, with pressure building in a way that deserves attention.",
        "The options layer showed building tension, with directional expectations becoming less neutral.",
        "Options stopped looking fully balanced, with compression showing up often enough to matter.",
    ],
    "medium": [
        "Options stayed cautious overall, but without strong enough conviction to frame the week as decisively directional.",
        "The options layer looked watchful rather than aggressive, with limited conviction behind directional pricing.",
        "Options pricing hinted at caution, though not strongly enough to define the broader weekly picture.",
    ],
    "light": [
        "Options positioning remained mostly balanced, with only light and occasional signs of compression.",
        "The options layer stayed largely balanced, with only mild hints of structural pressure.",
        "Compression signals appeared lightly, but not in a way that meaningfully shaped the week.",
    ],
    "calm": [
        "Options stayed broadly neutral, with little evidence of meaningful structural pressure.",
        "The options layer remained quiet overall, without a strong compression signal.",
        "Options pricing stayed close to neutral, with little sign of persistent tension.",
    ],
}

VOL_VARIANTS = {
    "extreme": [
        "Volatility term structure stayed elevated for a meaningful part of the week, which is hard to dismiss as background noise.",
        "The volatility backdrop remained firm long enough to look like a real condition, not a passing fluctuation.",
        "Volatility held at elevated levels often enough to matter beyond isolated episodes.",
    ],
    "high": [
        "Volatility firmed up from time to time, though it never developed into a sustained expansion regime.",
        "The volatility backdrop strengthened intermittently, but without turning into a full expansion phase.",
        "Volatility rose often enough to register, though not as a continuous expansion.",
    ],
    "medium": [
        "The volatility backdrop showed brief pockets of firmness, but not the kind of persistence usually seen in broader stress phases.",
        "Volatility looked firmer in places, though the move lacked the persistence associated with broader stress.",
        "Some firmness appeared in the vol backdrop, but not enough to frame the week as a sustained stress period.",
    ],
    "calm": [
        "The volatility backdrop stayed relatively calm, with little sign of sustained repricing pressure.",
        "Volatility conditions remained mostly calm, without a durable expansion signal.",
        "The vol backdrop stayed quiet overall, with only limited signs of repricing pressure.",
    ],
}

SYNTHESIS_VARIANTS = {
    "broad_alignment": [
        "Structural takeaway:\n\nPressure was not confined to one layer.\n\nFutures crowding and the volatility backdrop lined up often enough to suggest unstable conditions rather than random short-term noise.",
        "Structural takeaway:\n\nThis was not just an isolated futures story.\n\nCrowding and the volatility backdrop aligned often enough to point to broader instability.",
        "Structural takeaway:\n\nMore than one layer leaned in the same direction.\n\nThat makes the week's pressure harder to dismiss as local noise.",
    ],
    "futures_only": [
        "Structural takeaway:\n\nThe pressure was more visible in futures than elsewhere.\n\nOptions and volatility did not confirm a broader unstable regime, which keeps this closer to selective crowding than to system-wide stress.",
        "Structural takeaway:\n\nMost of the pressure sat in futures.\n\nWithout confirmation from options or volatility, this looks more selective than fully systemic.",
        "Structural takeaway:\n\nFutures carried the clearest sign of stress.\n\nThe broader market structure did not align strongly enough to confirm a market-wide unstable regime.",
    ],
    "options_led": [
        "Structural takeaway:\n\nOptions carried more of the structural signal than futures this week.\n\nCompression appeared without broad crowding, which fits latent pressure better than an already expanded move.",
        "Structural takeaway:\n\nThe options layer spoke louder than futures.\n\nCompression appeared before any broad crowding regime took shape.",
        "Structural takeaway:\n\nThis week leaned more toward latent pressure than overt futures stress.\n\nThat signal came mainly from the options layer.",
    ],
    "vol_led": [
        "Structural takeaway:\n\nThe volatility backdrop stayed firmer than futures positioning.\n\nThat points to repricing in the background, without broad crowding spreading across the market.",
        "Structural takeaway:\n\nVolatility looked firmer than futures positioning would suggest.\n\nThat usually points to repricing beneath the surface rather than broad speculative crowding.",
        "Structural takeaway:\n\nThe vol backdrop carried more tension than futures.\n\nThat suggests background repricing without full market-wide crowding.",
    ],
    "contained": [
        "Structural takeaway:\n\nThe week stayed relatively contained.\n\nSome local pressure appeared, but not in a way that suggests a broader regime shift.",
        "Structural takeaway:\n\nThe broader structure remained fairly contained.\n\nPressure appeared locally, but never spread widely enough to redefine the week.",
        "Structural takeaway:\n\nSome stress showed up in pockets, but the overall picture stayed relatively contained.",
    ],
    "mixed": [
        "Structural takeaway:\n\nThe picture remained fragmented.\n\nPressure appeared in places, but the broader system never aligned strongly enough to confirm a clear market-wide shift.",
        "Structural takeaway:\n\nThe week never resolved into one dominant structure.\n\nPressure appeared selectively, without broader confirmation across the system.",
        "Structural takeaway:\n\nThe market structure stayed mixed.\n\nLocal pressure appeared, but the broader regime never aligned cleanly enough to confirm a wider shift.",
    ],
}

FALLBACK_VARIANT = "Market structure stayed readable, but evidence remained partial this week."


def pick_variant(options: Any, fallback: str = FALLBACK_VARIANT) -> str:
    try:
        if not isinstance(options, list) or not options:
            return fallback
        normalized = [str(item).strip() for item in options if str(item).strip()]
        if not normalized:
            return fallback
        return random.choice(normalized)
    except Exception:
        return fallback


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
        return "n/a"
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
        return pick_variant(FUTURES_VARIANTS.get("extreme"))

    if avg_risk >= 0.60 or peak_risk >= 6:
        return pick_variant(FUTURES_VARIANTS.get("high"))

    if avg_risk >= 0.35 or peak_risk >= 5:
        return pick_variant(FUTURES_VARIANTS.get("medium"))

    if avg_risk >= 0.20 or peak_risk >= 3:
        return pick_variant(FUTURES_VARIANTS.get("calm"))

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
        return pick_variant(OPTIONS_VARIANTS.get("extreme"))

    if composite >= 0.30 or compression_share >= 10:
        return pick_variant(OPTIONS_VARIANTS.get("high"))

    if composite >= 0.20:
        return pick_variant(OPTIONS_VARIANTS.get("medium"))

    if composite >= 0.10:
        return pick_variant(OPTIONS_VARIANTS.get("light"))

    return pick_variant(OPTIONS_VARIANTS.get("calm"))


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
        return pick_variant(VOL_VARIANTS.get("extreme"))

    if overlap >= 20 or avg_vbi >= 20:
        return pick_variant(VOL_VARIANTS.get("high"))

    if overlap >= 10 or avg_vbi >= 15:
        return pick_variant(VOL_VARIANTS.get("medium"))

    return pick_variant(VOL_VARIANTS.get("calm"))


def build_synthesis_text(stats: Dict[str, Any]) -> str:
    risk = stats.get("risk") or {}
    bybit = stats.get("bybit") or {}
    okx = stats.get("okx") or {}
    deribit = stats.get("deribit") or {}

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
        return pick_variant(SYNTHESIS_VARIANTS.get("broad_alignment"))

    if (avg_risk >= 0.35 or peak_risk >= 5) and max(avg_mci, avg_olsi) < 0.22 and overlap < 15:
        return pick_variant(SYNTHESIS_VARIANTS.get("futures_only"))

    if avg_risk < 0.30 and (avg_mci >= 0.28 or mci_gt_06 >= 10) and overlap < 20:
        return pick_variant(SYNTHESIS_VARIANTS.get("options_led"))

    if avg_risk < 0.30 and overlap >= 20:
        return pick_variant(SYNTHESIS_VARIANTS.get("vol_led"))

    if avg_risk < 0.25 and max(avg_mci, avg_olsi) < 0.18 and overlap < 12:
        return pick_variant(SYNTHESIS_VARIANTS.get("contained"))

    return pick_variant(SYNTHESIS_VARIANTS.get("mixed"))


def build_thread_tweets(stats: Dict[str, Any]) -> List[str]:
    window = stats.get("window_days", 7)
    risk = stats.get("risk") or {}
    bybit = stats.get("bybit") or {}
    okx = stats.get("okx") or {}
    deribit = stats.get("deribit") or {}

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
            pick_variant(INTRO_VARIANTS).format(window=window)
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
