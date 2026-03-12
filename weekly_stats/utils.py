import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


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
