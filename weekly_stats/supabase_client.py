from typing import Any, Dict, List, Optional

import requests

from weekly_stats.config import REQUEST_TIMEOUT, TABLE_NAME


def bool_to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, str):
        normalized = value.strip().lower().strip('"')
        if normalized in {"true", "t", "1", "yes"}:
            return 1
        if normalized in {"false", "f", "0", "no"}:
            return 0
    return None


def fetch_existing_weekly_row(supabase_url: str, supabase_key: str, period_start_iso: str, period_end_iso: str) -> Optional[Dict[str, Any]]:
    response = requests.get(
        f"{supabase_url}/rest/v1/weekly_stats",
        headers={
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
            "Accept": "application/json",
        },
        params={
            "select": "id,period_start,period_end,root_tweet_id,tweet_count",
            "period_start": f"eq.{period_start_iso}",
            "period_end": f"eq.{period_end_iso}",
            "order": "id.desc",
            "limit": 1,
        },
        timeout=REQUEST_TIMEOUT,
    )
    if response.status_code != 200:
        raise RuntimeError(f"Supabase weekly_stats precheck failed: HTTP {response.status_code} | {response.text}")

    rows = response.json()
    if isinstance(rows, list) and rows:
        return rows[0]
    return None


def fetch_logs_paginated(supabase_url: str, supabase_key: str, start_ts_ms: int, end_ts_ms: int, page_size: int = 1000) -> List[Dict[str, Any]]:
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

        resp = requests.get(base_url, headers=page_headers, params=params, timeout=REQUEST_TIMEOUT)

        if resp.status_code not in (200, 206):
            raise RuntimeError(f"Supabase fetch failed: HTTP {resp.status_code} | {resp.text}")

        rows = resp.json()
        if not isinstance(rows, list):
            raise RuntimeError(f"Unexpected response payload: {rows}")

        all_rows.extend(rows)

        print(f"[fetch] got {len(rows)} rows | total={len(all_rows)} | range={range_from}-{range_to}", flush=True)

        if len(rows) < page_size:
            break

        offset += page_size

    return all_rows


def save_weekly_stats_row(stats: Dict[str, Any], supabase_url: str, supabase_key: str, tweet_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    period_start_iso = stats.get("from_utc")
    period_end_iso = stats.get("to_utc")
    period_label = f"{period_start_iso} -> {period_end_iso}" if period_start_iso and period_end_iso else None
    tweet_ids = tweet_ids or []
    risk_stats = stats.get("risk") or {}
    alert_stats = stats.get("alerts") or {}
    bybit_stats = stats.get("bybit") or {}
    okx_stats = stats.get("okx") or {}
    deribit_stats = stats.get("deribit") or {}

    payload = {
        "period_start": period_start_iso,
        "period_end": period_end_iso,
        "period_label": period_label,
        "run_status": "success",
        "source_job": "render-cron-weekly-stats",
        "avg_risk": risk_stats.get("avg_risk"),
        "median_risk": risk_stats.get("median_risk"),
        "max_risk": risk_stats.get("max_risk"),
        "market_high_risk_ge2": risk_stats.get("market_high_risk_hours", {}).get("risk_ge_2"),
        "market_high_risk_ge3": risk_stats.get("market_high_risk_hours", {}).get("risk_ge_3"),
        "market_high_risk_ge4": risk_stats.get("market_high_risk_hours", {}).get("risk_ge_4"),
        "market_high_risk_ge5": risk_stats.get("market_high_risk_hours", {}).get("risk_ge_5"),
        "symbol_high_risk_ge2": risk_stats.get("symbol_high_risk_hours", {}).get("risk_ge_2"),
        "symbol_high_risk_ge3": risk_stats.get("symbol_high_risk_hours", {}).get("risk_ge_3"),
        "symbol_high_risk_ge4": risk_stats.get("symbol_high_risk_hours", {}).get("risk_ge_4"),
        "symbol_high_risk_ge5": risk_stats.get("symbol_high_risk_hours", {}).get("risk_ge_5"),
        "alerts_rows": alert_stats.get("rows"),
        "bybit_avg_mci": bybit_stats.get("avg_mci"),
        "bybit_regime_calm_pct": bybit_stats.get("regime_calm_pct"),
        "bybit_regime_uncertain_pct": bybit_stats.get("regime_uncertain_pct"),
        "okx_avg_olsi": okx_stats.get("avg_olsi"),
        "okx_divergence_calm_dominant": bool_to_int(okx_stats.get("divergence_calm_dominant")),
        "deribit_btc_vbi": deribit_stats.get("btc_vbi"),
        "deribit_eth_vbi": deribit_stats.get("eth_vbi"),
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


def update_weekly_stats_twitter_fields(row_id: Any, tweet_ids: List[str], supabase_url: str, supabase_key: str) -> Dict[str, Any]:
    response = requests.patch(
        f"{supabase_url}/rest/v1/weekly_stats",
        headers={
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        },
        params={"id": f"eq.{row_id}"},
        json={
            "tweet_count": len(tweet_ids),
            "root_tweet_id": tweet_ids[0] if tweet_ids else None,
            "tweet_ids": tweet_ids,
        },
        timeout=REQUEST_TIMEOUT,
    )

    if response.status_code not in (200, 204):
        raise RuntimeError(f"Supabase weekly_stats patch failed: HTTP {response.status_code} | {response.text}")

    rows = response.json() if response.text else []
    if isinstance(rows, list) and rows:
        return rows[0]
    return {}
