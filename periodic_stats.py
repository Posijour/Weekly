import math
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests

from tweet_interpretations import build_thread_tweets


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
# SUPABASE SAVE (WEEKLY STATS)
# ============================================================

def save_weekly_stats_row(
    stats: Dict[str, Any],
    supabase_url: str,
    supabase_key: str,
    tweet_ids: Optional[List[str]] = None,
) -> Dict[str, Any]:
    period_start_iso = stats.get("from_utc")
    period_end_iso = stats.get("to_utc")
    period_label = f"{period_start_iso} -> {period_end_iso}" if period_start_iso and period_end_iso else None
    tweet_ids = tweet_ids or []

    payload = {
        "period_start": period_start_iso,
        "period_end": period_end_iso,
        "period_label": period_label,
        "run_status": "success",
        "source_job": "render-cron-weekly-stats",
    
        "avg_risk": stats.get("avg_risk"),
        "median_risk": stats.get("median_risk"),
        "max_risk": stats.get("max_risk"),
    
        "market_high_risk_ge2": stats.get("market_high_risk_hours", {}).get("risk_ge_2"),
        "market_high_risk_ge3": stats.get("market_high_risk_hours", {}).get("risk_ge_3"),
        "market_high_risk_ge4": stats.get("market_high_risk_hours", {}).get("risk_ge_4"),
        "market_high_risk_ge5": stats.get("market_high_risk_hours", {}).get("risk_ge_5"),
    
        "symbol_high_risk_ge2": stats.get("symbol_high_risk_hours", {}).get("risk_ge_2"),
        "symbol_high_risk_ge3": stats.get("symbol_high_risk_hours", {}).get("risk_ge_3"),
        "symbol_high_risk_ge4": stats.get("symbol_high_risk_hours", {}).get("risk_ge_4"),
        "symbol_high_risk_ge5": stats.get("symbol_high_risk_hours", {}).get("risk_ge_5"),
    
        "alerts_rows": stats.get("alerts_rows"),
    
        "bybit_avg_mci": stats.get("bybit_avg_mci"),
        "bybit_regime_calm_pct": stats.get("bybit_regime_calm_pct"),
        "bybit_regime_uncertain_pct": stats.get("bybit_regime_uncertain_pct"),
    
        "okx_avg_olsi": stats.get("okx_avg_olsi"),
        "okx_divergence_calm_dominant": stats.get("okx_divergence_calm_dominant"),
    
        "deribit_btc_vbi": stats.get("deribit_btc_vbi"),
        "deribit_eth_vbi": stats.get("deribit_eth_vbi"),
    
        "tweet_count": len(tweet_ids),
        "root_tweet_id": tweet_ids[0] if tweet_ids else None,
        "tweet_ids": tweet_ids,
        "raw_json": stats,
    }

    response = requests.post(
        f"{supabase_url}/rest/v1/weekly_stats",
        headers={
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        },
        json=payload,
        timeout=REQUEST_TIMEOUT,
    )

    if response.status_code not in (200, 201):
        raise RuntimeError(f"Supabase weekly_stats insert failed: HTTP {response.status_code} | {response.text}")

    rows = response.json()
    if not isinstance(rows, list) or not rows:
        return {}
    return rows[0]


# ============================================================
# TWITTER THREAD POSTING
# ============================================================

TWITTER_POST_URL = "https://api.twitter.com/2/tweets"


def _percent_encode(value: Any) -> str:
    from urllib.parse import quote
    return quote(str(value), safe="~")


def _build_oauth_header(method: str, url: str, api_key: str, api_secret: str, access_token: str, access_token_secret: str) -> str:
    import base64
    import hashlib
    import hmac
    import secrets
    import time

    oauth_params = {
        "oauth_consumer_key": api_key,
        "oauth_nonce": secrets.token_hex(16),
        "oauth_signature_method": "HMAC-SHA1",
        "oauth_timestamp": str(int(time.time())),
        "oauth_token": access_token,
        "oauth_version": "1.0",
    }

    param_string = "&".join(
        f"{_percent_encode(key)}={_percent_encode(value)}"
        for key, value in sorted(oauth_params.items())
    )

    signature_base = "&".join([
        method.upper(),
        _percent_encode(url),
        _percent_encode(param_string),
    ])

    signing_key = f"{_percent_encode(api_secret)}&{_percent_encode(access_token_secret)}"
    digest = hmac.new(signing_key.encode(), signature_base.encode(), hashlib.sha1).digest()
    oauth_params["oauth_signature"] = base64.b64encode(digest).decode()

    header = ", ".join(
        f'{_percent_encode(key)}="{_percent_encode(value)}"'
        for key, value in sorted(oauth_params.items())
    )
    return f"OAuth {header}"


def post_tweet(text: str, api_key: str, api_secret: str, access_token: str, access_token_secret: str, reply_to_tweet_id: Optional[str] = None) -> Dict[str, Any]:
    auth_header = _build_oauth_header("POST", TWITTER_POST_URL, api_key, api_secret, access_token, access_token_secret)

    payload: Dict[str, Any] = {"text": text}
    if reply_to_tweet_id:
        payload["reply"] = {"in_reply_to_tweet_id": reply_to_tweet_id}

    response = requests.post(
        TWITTER_POST_URL,
        headers={"Authorization": auth_header, "Content-Type": "application/json"},
        json=payload,
        timeout=REQUEST_TIMEOUT,
    )
    if response.status_code >= 400:
        raise RuntimeError(f"Twitter post failed: HTTP {response.status_code} | {response.text}")
    return response.json()


def get_required_twitter_credentials() -> Dict[str, str]:
    creds = {
        "TWITTER_API_KEY": os.getenv("TWITTER_API_KEY"),
        "TWITTER_API_SECRET": os.getenv("TWITTER_API_SECRET"),
        "TWITTER_ACCESS_TOKEN": os.getenv("TWITTER_ACCESS_TOKEN"),
        "TWITTER_ACCESS_TOKEN_SECRET": os.getenv("TWITTER_ACCESS_TOKEN_SECRET"),
    }
    missing = [name for name, value in creds.items() if not value]
    if missing:
        raise RuntimeError("Missing required Twitter credentials: " + ", ".join(missing))
    return creds  # type: ignore[return-value]


def post_thread_tweets(texts: List[str]) -> List[str]:
    if not texts:
        return []

    creds = get_required_twitter_credentials()
    tweet_ids: List[str] = []
    previous_id: Optional[str] = None

    for idx, text in enumerate(texts, start=1):
        result = post_tweet(
            text=text,
            api_key=creds["TWITTER_API_KEY"],
            api_secret=creds["TWITTER_API_SECRET"],
            access_token=creds["TWITTER_ACCESS_TOKEN"],
            access_token_secret=creds["TWITTER_ACCESS_TOKEN_SECRET"],
            reply_to_tweet_id=previous_id,
        )
        tweet_id = str(result.get("data", {}).get("id", ""))
        if not tweet_id:
            raise RuntimeError(f"Twitter response has no tweet id for item {idx}: {result}")
        tweet_ids.append(tweet_id)
        previous_id = tweet_id
        print(f"[twitter] tweet_{idx}_id={tweet_id}", flush=True)

    return tweet_ids


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    window_days = 7

    end_dt = now_utc()
    start_dt = end_dt - timedelta(days=window_days)

    start_ts_ms = dt_to_unix_ms(start_dt)
    end_ts_ms = dt_to_unix_ms(end_dt)

    print(f"[window] {window_days}d")
    print(f"[from]   {start_dt.isoformat()}")
    print(f"[to]     {end_dt.isoformat()}")

    raw_rows = fetch_logs_paginated(
        supabase_url=SUPABASE_URL,
        supabase_key=SUPABASE_KEY,
        start_ts_ms=start_ts_ms,
        end_ts_ms=end_ts_ms,
        page_size=PAGE_SIZE,
    )

    rows = [normalize_row(r) for r in raw_rows]
    rows = [r for r in rows if r["ts_ms"] is not None]

    print(f"[rows normalized] {len(rows)}")

    stats = compute_all_stats(
        rows=rows,
        window_days=window_days,
        start_dt=start_dt,
        end_dt=end_dt,
    )

    tweets = build_thread_tweets(stats)

    print(f"[metrics] rows_total={stats['rows_total']}")
    print(f"[metrics] avg_risk={stats['risk'].get('avg_risk')} peak_risk={stats['risk'].get('max_risk')}")
    print(f"[metrics] bybit_avg_mci={stats['bybit'].get('avg_mci')} okx_avg_olsi={stats['okx'].get('avg_olsi')}")
    print(f"[metrics] deribit_overlap_pct={stats['deribit'].get('both_hot_or_warm_share_pct')}")

    print("\n" + "=" * 80)
    print("THREAD")
    print("=" * 80)
    for i, tw in enumerate(tweets, start=1):
        print(f"\n----- TWEET {i} | len={len(tw)} -----\n")
        print(tw)

    tweet_ids = post_thread_tweets(tweets)
    print(f"[twitter] posted_thread_ids={tweet_ids}")

    saved = save_weekly_stats_row(
        stats=stats,
        supabase_url=SUPABASE_URL,
        supabase_key=SUPABASE_KEY,
        tweet_ids=tweet_ids,
    )
    print(f"[supabase] weekly_stats row saved id={saved.get('id', 'n/a')}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\nFATAL: {e}")
        sys.exit(1)
