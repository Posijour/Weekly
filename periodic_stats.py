import argparse
import json
import math
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests


# ============================================================
# SUPABASE CONFIG
# ВПИШИ СВОИ КЛЮЧИ ПРЯМО СЮДА
# ============================================================

SUPABASE_URL = "https://qcusrlmueapuqbjwuwvh.supabase.co"
SUPABASE_KEY = "sb_publishable_VsMaZGz98nm5lSQZJ-g-kQ_bUOfSO_r"

TABLE_NAME = "logs"
PAGE_SIZE = 1000
REQUEST_TIMEOUT = 30

DERIBIT_EVENT_NAMES = {"deribit_vbi_snapshot", "deribit_vbi_snasphot"}


# ============================================================
# HELPERS
# ============================================================

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def dt_to_unix_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def unix_ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def safe_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    if x is None:
        return default
    try:
        if isinstance(x, str) and x.strip() == "":
            return default
        return float(x)
    except Exception:
        return default


def safe_int(x: Any, default: Optional[int] = None) -> Optional[int]:
    if x is None:
        return default
    try:
        if isinstance(x, str) and x.strip() == "":
            return default
        return int(x)
    except Exception:
        return default


def compact_pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(100.0 * numerator / denominator, 1)


def mean(nums: List[float]) -> Optional[float]:
    vals = [x for x in nums if x is not None and not math.isnan(x)]
    if not vals:
        return None
    return sum(vals) / len(vals)


def median(nums: List[float]) -> Optional[float]:
    vals = sorted(x for x in nums if x is not None and not math.isnan(x))
    if not vals:
        return None
    n = len(vals)
    mid = n // 2
    if n % 2 == 1:
        return vals[mid]
    return (vals[mid - 1] + vals[mid]) / 2


def top_items(counter_like: Dict[str, Any], n: int = 5, round_digits: int = 2) -> List[Tuple[str, Any]]:
    items = list(counter_like.items())
    items.sort(key=lambda x: x[1], reverse=True)
    out = []
    for k, v in items[:n]:
        if isinstance(v, float):
            out.append((k, round(v, round_digits)))
        else:
            out.append((k, v))
    return out


def hour_bucket_from_ms(ts_ms: int) -> str:
    dt = unix_ms_to_dt(ts_ms)
    dt = dt.replace(minute=0, second=0, microsecond=0)
    return dt.isoformat()


def ten_min_bucket_from_ms(ts_ms: int) -> str:
    dt = unix_ms_to_dt(ts_ms)
    minute = (dt.minute // 10) * 10
    dt = dt.replace(minute=minute, second=0, microsecond=0)
    return dt.isoformat()


def trim_tweet(text: str, max_len: int = 280) -> str:
    text = re.sub(r"[ \t]+", " ", text).strip()
    text = re.sub(r"\n{3,}", "\n\n", text)
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"


def pct_or_na(x: Any, digits: int = 1) -> str:
    if x is None:
        return "n/a"
    try:
        return f"{round(float(x), digits)}%"
    except Exception:
        return "n/a"


def val_or_na(x: Any, digits: int = 2) -> str:
    if x is None:
        return "n/a"
    try:
        return str(round(float(x), digits))
    except Exception:
        return str(x)


def format_top_symbols(items: List[Tuple[str, Any]], limit: int = 3) -> str:
    if not items:
        return "none"
    cleaned = []
    for k, _v in items[:limit]:
        cleaned.append(str(k).replace("USDT", ""))
    return ", ".join(cleaned)


def format_top_types(items: List[Tuple[str, Any]], limit: int = 2) -> str:
    if not items:
        return "none"
    out = []
    for k, v in items[:limit]:
        out.append(f"{k} ({v})")
    return ", ".join(out)


def next_full_hour(dt: datetime) -> datetime:
    floored = dt.replace(minute=0, second=0, microsecond=0)
    if dt == floored:
        return floored
    return floored + timedelta(hours=1)


def current_full_hour(dt: datetime) -> datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def iso_hour_str(dt: datetime) -> str:
    return dt.replace(minute=0, second=0, microsecond=0).isoformat()


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


# ============================================================
# SUPABASE FETCH WITH PAGINATION
# ============================================================

def fetch_logs_paginated(
    supabase_url: str,
    supabase_key: str,
    start_ts_ms: int,
    end_ts_ms: int,
    page_size: int = 1000,
) -> List[Dict[str, Any]]:
    base_url = f"{supabase_url}/rest/v1/{TABLE_NAME}"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Accept": "application/json",
    }

    all_rows: List[Dict[str, Any]] = []
    offset = 0

    while True:
        range_from = offset
        range_to = offset + page_size - 1

        params = {
            "select": "id,ts,event,symbol,data",
            "ts": f"gte.{start_ts_ms}",
            "and": f"(ts.lte.{end_ts_ms})",
            "order": "ts.asc,id.asc",
        }

        page_headers = dict(headers)
        page_headers["Range"] = f"{range_from}-{range_to}"
        page_headers["Prefer"] = "count=exact"

        resp = requests.get(
            base_url,
            headers=page_headers,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )

        if resp.status_code not in (200, 206):
            raise RuntimeError(
                f"Supabase fetch failed: HTTP {resp.status_code} | {resp.text}"
            )

        rows = resp.json()
        if not isinstance(rows, list):
            raise RuntimeError(f"Unexpected response payload: {rows}")

        all_rows.extend(rows)

        print(
            f"[fetch] got {len(rows)} rows | total={len(all_rows)} | range={range_from}-{range_to}",
            flush=True,
        )

        if len(rows) < page_size:
            break

        offset += page_size

    return all_rows


# ============================================================
# EVENT PARSING
# ============================================================

def get_data_field(row: Dict[str, Any], key: str, default: Any = None) -> Any:
    data = row.get("data") or {}
    if isinstance(data, dict):
        return data.get(key, default)
    return default


def normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    ts_ms = safe_int(row.get("ts"))
    if ts_ms is None:
        ts_ms = safe_int(get_data_field(row, "ts_unix_ms"))

    data = row.get("data")
    if not isinstance(data, dict):
        data = {}

    symbol = row.get("symbol") or data.get("symbol") or "UNKNOWN"
    event = row.get("event") or "UNKNOWN"

    return {
        "id": row.get("id"),
        "ts_ms": ts_ms,
        "event": event,
        "symbol": symbol,
        "data": data,
    }


# ============================================================
# RISK STATS
# ============================================================

def compute_risk_stats(
    rows: List[Dict[str, Any]],
    start_dt: datetime,
    end_dt: datetime,
) -> Dict[str, Any]:
    risk_rows = [r for r in rows if r["event"] == "risk_eval"]

    total_rows = len(risk_rows)
    symbols = sorted({r["symbol"] for r in risk_rows if r["symbol"]})

    risk_values: List[float] = []
    max_risk = None

    per_symbol_risks: Dict[str, List[float]] = defaultdict(list)
    per_symbol_obs: Counter = Counter()
    per_symbol_ge_2: Counter = Counter()
    per_symbol_ge_3: Counter = Counter()

    for r in risk_rows:
        data = r["data"]
        risk = safe_float(data.get("risk"))
        if risk is None:
            continue

        sym = r["symbol"]

        risk_values.append(risk)
        per_symbol_risks[sym].append(risk)
        per_symbol_obs[sym] += 1

        if risk >= 2:
            per_symbol_ge_2[sym] += 1
        if risk >= 3:
            per_symbol_ge_3[sym] += 1

        if max_risk is None or risk > max_risk:
            max_risk = risk

    avg_risk = mean(risk_values)
    med_risk = median(risk_values)

    per_symbol_avg = {
        sym: mean(vals) for sym, vals in per_symbol_risks.items() if vals
    }
    per_symbol_max = {
        sym: max(vals) for sym, vals in per_symbol_risks.items() if vals
    }
    per_symbol_ge_2_share = {
        sym: round(100.0 * per_symbol_ge_2[sym] / per_symbol_obs[sym], 1)
        for sym in per_symbol_obs
        if per_symbol_obs[sym] > 0
    }
    per_symbol_ge_3_share = {
        sym: round(100.0 * per_symbol_ge_3[sym] / per_symbol_obs[sym], 1)
        for sym in per_symbol_obs
        if per_symbol_obs[sym] > 0
    }

    return {
        "rows": total_rows,
        "symbols_count": len(symbols),
        "symbols": symbols,
        "avg_risk": round(avg_risk, 3) if avg_risk is not None else None,
        "median_risk": round(med_risk, 3) if med_risk is not None else None,
        "max_risk": round(max_risk, 3) if max_risk is not None else None,
        "top_symbols_by_avg_risk": top_items(per_symbol_avg, n=5, round_digits=3),
        "top_symbols_by_max_risk": top_items(per_symbol_max, n=5, round_digits=3),
        "top_symbols_by_risk_ge_2_share_pct": top_items(per_symbol_ge_2_share, n=5, round_digits=1),
        "top_symbols_by_risk_ge_3_share_pct": top_items(per_symbol_ge_3_share, n=5, round_digits=1),
    }


# ============================================================
# INTERPRETATION HELPERS
# ============================================================

def build_short_futures_interpretation(avg_risk: Optional[float], peak_risk: Optional[float]) -> str:
    avg_risk = avg_risk or 0.0
    peak_risk = peak_risk or 0.0

    if avg_risk >= 0.90 or peak_risk >= 8:
        return (
            "Positioning pressure stayed broad enough\n"
            "to suggest persistent crowding\n"
            "rather than isolated bursts."
        )

    if avg_risk >= 0.60 or peak_risk >= 6:
        return (
            "Positioning pressure built repeatedly\n"
            "across the week,\n"
            "with stress showing more continuity\n"
            "than usual."
        )

    if avg_risk >= 0.35 or peak_risk >= 5:
        return (
            "Positioning pressure appeared in bursts\n"
            "rather than sustained build-up."
        )

    if avg_risk >= 0.20 or peak_risk >= 3:
        return (
            "Positioning stayed active,\n"
            "but crowding remained localized\n"
            "rather than regime-like."
        )

    return (
        "Positioning pressure\n"
        "remained mostly contained."
    )


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
        return (
            "Options positioning showed structural pressure,\n"
            "with compression appearing often enough\n"
            "to matter."
        )

    if composite >= 0.30 or compression_share >= 10:
        return (
            "Options markets showed building pressure,\n"
            "with directional expectations\n"
            "becoming less neutral."
        )

    if composite >= 0.20:
        return (
            "Options markets stayed cautious\n"
            "with limited directional conviction."
        )

    if composite >= 0.10:
        return (
            "Options positioning remained mostly balanced,\n"
            "with only light signs of compression."
        )

    return (
        "Options positioning\n"
        "remained broadly neutral."
    )


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
        return (
            "Volatility term structure stayed elevated\n"
            "through a meaningful part of the week."
        )

    if overlap >= 20 or avg_vbi >= 20:
        return (
            "Volatility showed intermittent elevation\n"
            "rather than a sustained expansion."
        )

    if overlap >= 10 or avg_vbi >= 15:
        return (
            "Volatility background showed brief pockets\n"
            "of firmness,\n"
            "but not a persistent stress regime."
        )

    return (
        "Volatility background\n"
        "stayed relatively calm."
    )


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
    avg_vbi = mean([btc_vbi, eth_vbi]) or 0.0

    # 1) Confirmed unstable regime
    if (avg_risk >= 0.55 and overlap >= 20) or (peak_risk >= 6 and avg_vbi >= 20):
        return (
            "Structural takeaway:\n\n"
            "Pressure was not isolated.\n\n"
            "Futures crowding and volatility background\n"
            "aligned often enough to suggest\n"
            "unstable conditions\n"
            "rather than random noise."
        )

    # 2) Futures pressure not confirmed by other layers
    if (avg_risk >= 0.35 or peak_risk >= 5) and max(avg_mci, avg_olsi) < 0.22 and overlap < 15:
        return (
            "Structural takeaway:\n\n"
            "Futures pressure appeared locally,\n"
            "but options expectations and volatility background\n"
            "did not confirm a broader unstable regime.\n\n"
            "This looked more like selective crowding\n"
            "than system-wide stress."
        )

    # 3) Options compression ahead of futures
    if avg_risk < 0.30 and (avg_mci >= 0.28 or mci_gt_06 >= 10) and overlap < 20:
        return (
            "Structural takeaway:\n\n"
            "Options markets carried more of the structural signal\n"
            "than futures positioning.\n\n"
            "Compression appeared without broad crowding,\n"
            "which is more consistent with latent pressure\n"
            "than with an already expanded move."
        )

    # 4) Volatility elevated without crowding
    if avg_risk < 0.30 and overlap >= 20:
        return (
            "Structural takeaway:\n\n"
            "Volatility background stayed firmer\n"
            "than futures positioning.\n\n"
            "This suggests repricing in the background,\n"
            "without broad crowding across the market."
        )

    # 5) Calm / mixed
    if avg_risk < 0.25 and max(avg_mci, avg_olsi) < 0.18 and overlap < 12:
        return (
            "Structural takeaway:\n\n"
            "Signals stayed relatively contained.\n\n"
            "Pressure appeared selectively,\n"
            "but not as a clear regime shift\n"
            "across the system."
        )

    return (
        "Structural takeaway:\n\n"
        "Signals stayed mixed.\n\n"
        "Pressure appeared selectively,\n"
        "not as a clear system-wide regime shift."
    )


# ============================================================
# ALERT / BINANCE EVENT STATS
# ============================================================

def extract_alert_type(data: Dict[str, Any]) -> str:
    candidates = [
        data.get("alert_type"),
        data.get("type"),
        data.get("event_type"),
        data.get("divergence_type"),
        data.get("alert"),
        data.get("name"),
        data.get("signal"),
        data.get("kind"),
    ]
    for c in candidates:
        if c is not None and str(c).strip():
            return str(c).strip()

    if data.get("divergence"):
        return f"divergence:{data.get('divergence')}"
    if data.get("buildup_type"):
        return f"buildup:{data.get('buildup_type')}"

    return "unknown_alert"


def compute_alert_stats(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    alert_rows = [r for r in rows if r["event"] == "alert_sent"]

    total_rows = len(alert_rows)
    type_counter = Counter()
    symbol_counter = Counter()
    dedup_hour_counter = Counter()

    for r in alert_rows:
        data = r["data"]
        alert_type = extract_alert_type(data)
        type_counter[alert_type] += 1

        symbol = data.get("symbol") or r.get("symbol") or "UNKNOWN"
        symbol = str(symbol)
        symbol_counter[symbol] += 1

        hb = hour_bucket_from_ms(r["ts_ms"])
        dedup_key = (symbol, alert_type, hb)
        dedup_hour_counter[dedup_key] += 1

    dedup_type_counter = Counter()
    dedup_symbol_counter = Counter()
    for (symbol, alert_type, _hb), _cnt in dedup_hour_counter.items():
        dedup_type_counter[alert_type] += 1
        dedup_symbol_counter[symbol] += 1

    return {
        "rows": total_rows,
        "top_alert_types_raw": top_items(dict(type_counter), n=10),
        "top_symbols_raw": top_items(dict(symbol_counter), n=10),
        "top_alert_types_dedup_1h": top_items(dict(dedup_type_counter), n=10),
        "top_symbols_dedup_1h": top_items(dict(dedup_symbol_counter), n=10),
    }


# ============================================================
# BYBIT STATS
# ============================================================

def compute_bybit_stats(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    bybit_rows = [r for r in rows if r["event"] == "bybit_market_state"]

    mci_vals = []
    slope_vals = []
    conf_vals = []
    regime_counter = Counter()
    phase_counter = Counter()

    above_06 = 0
    valid_mci_count = 0

    prev_regime = None
    regime_transitions = Counter()

    for r in bybit_rows:
        d = r["data"]

        mci = safe_float(d.get("mci"))
        slope = safe_float(d.get("mci_slope"))
        conf = safe_float(d.get("confidence"))
        regime = d.get("regime")
        phase = d.get("mci_phase")

        if mci is not None:
            mci_vals.append(mci)
            valid_mci_count += 1
            if mci > 0.6:
                above_06 += 1

        if slope is not None:
            slope_vals.append(slope)

        if conf is not None:
            conf_vals.append(conf)

        if regime:
            regime = str(regime)
            regime_counter[regime] += 1
            if prev_regime is not None and prev_regime != regime:
                regime_transitions[f"{prev_regime}->{regime}"] += 1
            prev_regime = regime

        if phase:
            phase_counter[str(phase)] += 1

    total_regimes = sum(regime_counter.values())
    total_phases = sum(phase_counter.values())

    return {
        "rows": len(bybit_rows),
        "avg_mci": round(mean(mci_vals), 4) if mci_vals else None,
        "median_mci": round(median(mci_vals), 4) if mci_vals else None,
        "max_mci": round(max(mci_vals), 4) if mci_vals else None,
        "avg_mci_slope": round(mean(slope_vals), 4) if slope_vals else None,
        "avg_confidence": round(mean(conf_vals), 4) if conf_vals else None,
        "regime_share_pct": {
            k: compact_pct(v, total_regimes) for k, v in regime_counter.items()
        },
        "phase_share_pct": {
            k: compact_pct(v, total_phases) for k, v in phase_counter.items()
        },
        "mci_gt_06_share_pct": compact_pct(above_06, valid_mci_count),
        "top_regime_transitions": top_items(dict(regime_transitions), n=10),
    }


# ============================================================
# OKX STATS
# ============================================================

def compute_okx_stats(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    okx_rows = [r for r in rows if r["event"] == "okx_market_state"]

    olsi_vals = []
    slope_vals = []
    diff_vals = []

    divergence_counter = Counter()
    phase_div_counter = Counter()
    dedup_divergence_1h = Counter()

    for r in okx_rows:
        d = r["data"]

        olsi = (
            safe_float(d.get("okx_olsi_avg"))
            if d.get("okx_olsi_avg") is not None
            else safe_float(d.get("olsi"))
        )
        slope = (
            safe_float(d.get("okx_olsi_slope"))
            if d.get("okx_olsi_slope") is not None
            else safe_float(d.get("olsi_slope"))
        )
        div_diff = safe_float(d.get("divergence_diff"))

        div_type = d.get("divergence_type")
        phase_div = d.get("phase_divergence")

        if olsi is not None:
            olsi_vals.append(olsi)
        if slope is not None:
            slope_vals.append(slope)
        if div_diff is not None:
            diff_vals.append(div_diff)

        if div_type:
            div_type = str(div_type)
            divergence_counter[div_type] += 1

            hb = hour_bucket_from_ms(r["ts_ms"])
            dedup_divergence_1h[(div_type, hb)] += 1

        if phase_div:
            phase_div_counter[str(phase_div)] += 1

    dedup_type_counter = Counter()
    for (div_type, _hb), _cnt in dedup_divergence_1h.items():
        dedup_type_counter[div_type] += 1

    return {
        "rows": len(okx_rows),
        "avg_olsi": round(mean(olsi_vals), 4) if olsi_vals else None,
        "median_olsi": round(median(olsi_vals), 4) if olsi_vals else None,
        "max_olsi": round(max(olsi_vals), 4) if olsi_vals else None,
        "avg_olsi_slope": round(mean(slope_vals), 4) if slope_vals else None,
        "avg_divergence_diff": round(mean(diff_vals), 4) if diff_vals else None,
        "divergence_types_raw": top_items(dict(divergence_counter), n=10),
        "divergence_types_dedup_1h": top_items(dict(dedup_type_counter), n=10),
        "phase_divergence_counts": top_items(dict(phase_div_counter), n=10),
    }


# ============================================================
# DERIBIT STATS
# ============================================================

def compute_deribit_stats(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    deribit_rows = [r for r in rows if r["event"] in DERIBIT_EVENT_NAMES]

    by_symbol = defaultdict(list)
    for r in deribit_rows:
        by_symbol[r["symbol"]].append(r)

    result: Dict[str, Any] = {
        "rows": len(deribit_rows),
        "symbols": {},
        "both_hot_or_warm_share_pct": 0.0,
    }

    bucket_state_map: Dict[str, Dict[str, str]] = defaultdict(dict)

    for sym, sym_rows in by_symbol.items():
        vbi_scores = []
        iv_slopes = []
        skews = []
        curvatures = []
        state_counter = Counter()

        for r in sym_rows:
            d = r["data"]

            vbi = safe_float(d.get("vbi_score"))
            iv_slope = safe_float(d.get("iv_slope"))
            skew = safe_float(d.get("skew"))
            curvature = safe_float(d.get("curvature"))
            state = d.get("vbi_state")

            if vbi is not None:
                vbi_scores.append(vbi)
            if iv_slope is not None:
                iv_slopes.append(iv_slope)
            if skew is not None:
                skews.append(skew)
            if curvature is not None:
                curvatures.append(curvature)
            if state:
                state = str(state)
                state_counter[state] += 1
                bucket = ten_min_bucket_from_ms(r["ts_ms"])
                bucket_state_map[bucket][sym] = state

        total_states = sum(state_counter.values())

        result["symbols"][sym] = {
            "rows": len(sym_rows),
            "avg_vbi_score": round(mean(vbi_scores), 4) if vbi_scores else None,
            "max_vbi_score": round(max(vbi_scores), 4) if vbi_scores else None,
            "avg_iv_slope": round(mean(iv_slopes), 4) if iv_slopes else None,
            "avg_skew": round(mean(skews), 4) if skews else None,
            "avg_curvature": round(mean(curvatures), 4) if curvatures else None,
            "state_share_pct": {
                k: compact_pct(v, total_states) for k, v in state_counter.items()
            },
        }

    joint_buckets = 0
    elevated_buckets = 0
    elevated_states = {"HOT", "WARM"}

    for _bucket, sym_map in bucket_state_map.items():
        if "BTC" in sym_map and "ETH" in sym_map:
            joint_buckets += 1
            if sym_map["BTC"] in elevated_states and sym_map["ETH"] in elevated_states:
                elevated_buckets += 1

    result["both_hot_or_warm_share_pct"] = compact_pct(elevated_buckets, joint_buckets)
    return result


# ============================================================
# MASTER STATS
# ============================================================

def compute_all_stats(
    rows: List[Dict[str, Any]],
    window_days: int,
    start_dt: datetime,
    end_dt: datetime,
) -> Dict[str, Any]:
    event_counts = Counter(r["event"] for r in rows)

    return {
        "window_days": window_days,
        "from_utc": start_dt.isoformat(),
        "to_utc": end_dt.isoformat(),
        "rows_total": len(rows),
        "event_counts": dict(event_counts),
        "risk": compute_risk_stats(rows, start_dt=start_dt, end_dt=end_dt),
        "alerts": compute_alert_stats(rows),
        "bybit": compute_bybit_stats(rows),
        "okx": compute_okx_stats(rows),
        "deribit": compute_deribit_stats(rows),
    }


# ============================================================
# SYNTHESIS
# ============================================================

def infer_market_takeaway(stats: Dict[str, Any]) -> str:
    return build_synthesis_text(stats)


# ============================================================
# THREAD GENERATION
# ============================================================

def build_thread_tweets(stats: Dict[str, Any]) -> List[str]:
    window = stats["window_days"]
    risk = stats["risk"]
    bybit = stats["bybit"]
    okx = stats["okx"]
    deribit = stats["deribit"]

    # 1) Intro
    tweet1 = (
        f"Livermore weekly snapshot ({window}d)\n\n"
        f"Signals aggregated across:\n\n"
        f"• futures positioning\n"
        f"• options expectations\n"
        f"• volatility background"
    )

    # 2) Futures
    avg_risk = risk.get("avg_risk")
    peak_risk = risk.get("max_risk")
    top_stress = top_symbol_names(risk.get("top_symbols_by_avg_risk", []), limit=3)
    fut_text = build_short_futures_interpretation(avg_risk, peak_risk)

    tweet2 = (
        f"Futures layer (Binance)\n\n"
        f"Avg risk: {rounded_str(avg_risk, 2)}\n"
        f"Peak risk: {rounded_str(peak_risk, 1)}\n\n"
        f"Main stress leaders:\n"
        f"{top_stress}.\n\n"
        f"{fut_text}"
    )

    # 3) Options
    avg_mci = bybit.get("avg_mci")
    mci_gt_06 = bybit.get("mci_gt_06_share_pct")
    avg_olsi = okx.get("avg_olsi")
    opt_text = build_short_options_interpretation(avg_mci, avg_olsi, mci_gt_06)

    tweet3 = (
        f"Options expectations (Bybit / OKX)\n\n"
        f"Bybit MCI avg: {rounded_str(avg_mci, 2)}\n"
        f"High-compression windows (>0.6): {rounded_str(mci_gt_06, 0)}%\n\n"
        f"OKX avg OLSI: {rounded_str(avg_olsi, 2)}\n\n"
        f"{opt_text}"
    )

    # 4) Volatility
    btc_vbi = deribit.get("symbols", {}).get("BTC", {}).get("avg_vbi_score")
    eth_vbi = deribit.get("symbols", {}).get("ETH", {}).get("avg_vbi_score")
    overlap = deribit.get("both_hot_or_warm_share_pct")
    vol_text = build_short_vol_interpretation(overlap, btc_vbi, eth_vbi)

    tweet4 = (
        f"Volatility background (Deribit)\n\n"
        f"BTC VBI avg: {rounded_str(btc_vbi, 1)}\n"
        f"ETH VBI avg: {rounded_str(eth_vbi, 1)}\n\n"
        f"BTC/ETH warm overlap:\n"
        f"{rounded_str(overlap, 0)}% of windows.\n\n"
        f"{vol_text}"
    )

    # 5) Synthesis
    tweet5 = build_synthesis_text(stats)

    tweets = [tweet1, tweet2, tweet3, tweet4, tweet5]
    tweets = [trim_tweet(t, max_len=260) for t in tweets]
    return tweets


# ============================================================
# FILE OUTPUT
# ============================================================

def save_outputs(stats: Dict[str, Any], tweets: List[str], prefix: str) -> None:
    json_path = f"{prefix}.json"
    thread_path = f"{prefix}_thread.txt"

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    with open(thread_path, "w", encoding="utf-8") as f:
        for i, tw in enumerate(tweets, start=1):
            f.write(f"----- TWEET {i} | {len(tw)} chars -----\n")
            f.write(tw + "\n\n")

    for i, tw in enumerate(tweets, start=1):
        path = f"{prefix}_tweet_{i}.txt"
        with open(path, "w", encoding="utf-8") as f:
            f.write(tw + "\n")
        print(f"[saved] {path} | {len(tw)} chars")

    print(f"[saved] {json_path}")
    print(f"[saved] {thread_path}")


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Weekly / Monthly stats from Supabase logs")
    parser.add_argument("--window", type=int, choices=[7, 30], required=True, help="Days window: 7 or 30")
    parser.add_argument("--page-size", type=int, default=PAGE_SIZE, help="Pagination page size")
    parser.add_argument("--save-prefix", type=str, default=None, help="Prefix for output files")
    args = parser.parse_args()

    end_dt = now_utc()
    start_dt = end_dt - timedelta(days=args.window)

    start_ts_ms = dt_to_unix_ms(start_dt)
    end_ts_ms = dt_to_unix_ms(end_dt)

    print(f"[window] {args.window}d")
    print(f"[from]   {start_dt.isoformat()}")
    print(f"[to]     {end_dt.isoformat()}")

    raw_rows = fetch_logs_paginated(
        supabase_url=SUPABASE_URL,
        supabase_key=SUPABASE_KEY,
        start_ts_ms=start_ts_ms,
        end_ts_ms=end_ts_ms,
        page_size=args.page_size,
    )

    rows = [normalize_row(r) for r in raw_rows]
    rows = [r for r in rows if r["ts_ms"] is not None]

    print(f"[rows normalized] {len(rows)}")

    stats = compute_all_stats(
        rows=rows,
        window_days=args.window,
        start_dt=start_dt,
        end_dt=end_dt,
    )

    tweets = build_thread_tweets(stats)

    print("\n" + "=" * 80)
    print("JSON STATS")
    print("=" * 80)
    print(json.dumps(stats, ensure_ascii=False, indent=2))

    print("\n" + "=" * 80)
    print("THREAD DRAFT")
    print("=" * 80)
    for i, tw in enumerate(tweets, start=1):
        print(f"\n----- TWEET {i} | len={len(tw)} -----\n")
        print(tw)

    prefix = args.save_prefix or f"livermore_{args.window}d_stats"
    save_outputs(stats, tweets, prefix)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\nFATAL: {e}")
        sys.exit(1)