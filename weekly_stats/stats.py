from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, List

from weekly_stats.config import DERIBIT_EVENT_NAMES
from weekly_stats.utils import compact_pct, hour_bucket_from_ms, mean, median, safe_float, ten_min_bucket_from_ms, top_items


def compute_risk_stats(rows: List[Dict[str, Any]], start_dt: datetime, end_dt: datetime) -> Dict[str, Any]:
    del start_dt, end_dt
    risk_rows = [r for r in rows if r["event"] == "risk_eval"]

    total_rows = len(risk_rows)
    symbols = sorted({r["symbol"] for r in risk_rows if r["symbol"]})

    risk_values: List[float] = []
    max_risk = None

    per_symbol_risks: Dict[str, List[float]] = defaultdict(list)
    per_symbol_obs: Counter = Counter()
    per_symbol_ge_2: Counter = Counter()
    per_symbol_ge_3: Counter = Counter()

    market_high_risk_hours: Dict[str, set] = {
        "risk_ge_2": set(),
        "risk_ge_3": set(),
        "risk_ge_4": set(),
        "risk_ge_5": set(),
    }
    symbol_high_risk_hours: Dict[str, set] = {
        "risk_ge_2": set(),
        "risk_ge_3": set(),
        "risk_ge_4": set(),
        "risk_ge_5": set(),
    }

    for r in risk_rows:
        data = r["data"]
        risk = safe_float(data.get("risk"))
        if risk is None:
            continue

        sym = str(r["symbol"])
        hour_bucket = hour_bucket_from_ms(r["ts_ms"])

        risk_values.append(risk)
        per_symbol_risks[sym].append(risk)
        per_symbol_obs[sym] += 1

        if risk >= 2:
            per_symbol_ge_2[sym] += 1
            market_high_risk_hours["risk_ge_2"].add(hour_bucket)
            symbol_high_risk_hours["risk_ge_2"].add((sym, hour_bucket))
        if risk >= 3:
            per_symbol_ge_3[sym] += 1
            market_high_risk_hours["risk_ge_3"].add(hour_bucket)
            symbol_high_risk_hours["risk_ge_3"].add((sym, hour_bucket))
        if risk >= 4:
            market_high_risk_hours["risk_ge_4"].add(hour_bucket)
            symbol_high_risk_hours["risk_ge_4"].add((sym, hour_bucket))
        if risk >= 5:
            market_high_risk_hours["risk_ge_5"].add(hour_bucket)
            symbol_high_risk_hours["risk_ge_5"].add((sym, hour_bucket))

        if max_risk is None or risk > max_risk:
            max_risk = risk

    avg_risk = mean(risk_values)
    med_risk = median(risk_values)

    per_symbol_avg = {sym: mean(vals) for sym, vals in per_symbol_risks.items() if vals}
    per_symbol_max = {sym: max(vals) for sym, vals in per_symbol_risks.items() if vals}
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
        "market_high_risk_hours": {k: len(v) for k, v in market_high_risk_hours.items()},
        "symbol_high_risk_hours": {k: len(v) for k, v in symbol_high_risk_hours.items()},
    }


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
    regime_share = {k: compact_pct(v, total_regimes) for k, v in regime_counter.items()}

    return {
        "rows": len(bybit_rows),
        "avg_mci": round(mean(mci_vals), 4) if mci_vals else None,
        "median_mci": round(median(mci_vals), 4) if mci_vals else None,
        "max_mci": round(max(mci_vals), 4) if mci_vals else None,
        "avg_mci_slope": round(mean(slope_vals), 4) if slope_vals else None,
        "avg_confidence": round(mean(conf_vals), 4) if conf_vals else None,
        "regime_share_pct": regime_share,
        "phase_share_pct": {k: compact_pct(v, total_phases) for k, v in phase_counter.items()},
        "mci_gt_06_share_pct": compact_pct(above_06, valid_mci_count),
        "top_regime_transitions": top_items(dict(regime_transitions), n=10),
        "regime_calm_pct": regime_share.get("calm", 0.0),
        "regime_uncertain_pct": regime_share.get("uncertain", 0.0),
    }


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

        olsi = safe_float(d.get("okx_olsi_avg")) if d.get("okx_olsi_avg") is not None else safe_float(d.get("olsi"))
        slope = safe_float(d.get("okx_olsi_slope")) if d.get("okx_olsi_slope") is not None else safe_float(d.get("olsi_slope"))
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

    dominant_div = top_items(dict(dedup_type_counter), n=1)
    dominant_div_name = dominant_div[0][0] if dominant_div else None

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
        "divergence_calm_dominant": dominant_div_name == "calm",
    }


def compute_deribit_stats(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    deribit_rows = [r for r in rows if r["event"] in DERIBIT_EVENT_NAMES]

    by_symbol = defaultdict(list)
    for r in deribit_rows:
        by_symbol[r["symbol"]].append(r)

    result: Dict[str, Any] = {
        "rows": len(deribit_rows),
        "symbols": {},
        "both_hot_or_warm_share_pct": 0.0,
        "btc_vbi": None,
        "eth_vbi": None,
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
        avg_vbi = round(mean(vbi_scores), 4) if vbi_scores else None

        result["symbols"][sym] = {
            "rows": len(sym_rows),
            "avg_vbi_score": avg_vbi,
            "max_vbi_score": round(max(vbi_scores), 4) if vbi_scores else None,
            "avg_iv_slope": round(mean(iv_slopes), 4) if iv_slopes else None,
            "avg_skew": round(mean(skews), 4) if skews else None,
            "avg_curvature": round(mean(curvatures), 4) if curvatures else None,
            "state_share_pct": {k: compact_pct(v, total_states) for k, v in state_counter.items()},
        }

        if sym == "BTC":
            result["btc_vbi"] = avg_vbi
        if sym == "ETH":
            result["eth_vbi"] = avg_vbi

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


def compute_all_stats(rows: List[Dict[str, Any]], window_days: int, start_dt: datetime, end_dt: datetime) -> Dict[str, Any]:
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
