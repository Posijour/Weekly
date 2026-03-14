import os
from typing import Any, Dict

import requests

from weekly_stats.config import REQUEST_TIMEOUT


TELEGRAM_API_BASE = "https://api.telegram.org"


def get_required_telegram_credentials() -> Dict[str, str]:
    creds = {
        "TELEGRAM_BOT_TOKEN": os.getenv("TELEGRAM_BOT_TOKEN"),
        "TELEGRAM_CHAT_ID": os.getenv("TELEGRAM_CHAT_ID"),
    }
    missing = [name for name, value in creds.items() if not value]
    if missing:
        raise RuntimeError("Missing required Telegram credentials: " + ", ".join(missing))
    return creds  # type: ignore[return-value]


def send_telegram_message(text: str) -> Dict[str, Any]:
    creds = get_required_telegram_credentials()
    endpoint = f"{TELEGRAM_API_BASE}/bot{creds['TELEGRAM_BOT_TOKEN']}/sendMessage"

    response = requests.post(
        endpoint,
        json={
            "chat_id": creds["TELEGRAM_CHAT_ID"],
            "text": text,
            "disable_web_page_preview": True,
            "parse_mode": "HTML",
        },
        timeout=REQUEST_TIMEOUT,
    )

    if response.status_code >= 400:
        raise RuntimeError(f"Telegram post failed: HTTP {response.status_code} | {response.text}")

    payload = response.json()
    if not payload.get("ok"):
        raise RuntimeError(f"Telegram post failed: {payload}")
    return payload
