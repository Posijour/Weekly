from typing import Optional


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
