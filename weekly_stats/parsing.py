from typing import Any, Dict

from weekly_stats.utils import safe_int


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
