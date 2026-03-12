import os
from typing import Any, Dict, List, Optional, Tuple

import requests

from weekly_stats.config import EXPECTED_TWEET_COUNT, MAX_NA_PER_TWEET, REQUEST_TIMEOUT, TWEET_MAX_LEN, TWITTER_POST_URL


def validate_thread_tweets(texts: List[str], expected_count: int = EXPECTED_TWEET_COUNT, max_len: int = TWEET_MAX_LEN) -> Tuple[bool, List[str]]:
    errors: List[str] = []

    if len(texts) != expected_count:
        errors.append(f"unexpected_tweet_count={len(texts)} expected={expected_count}")

    for idx, tweet in enumerate(texts, start=1):
        if not isinstance(tweet, str):
            errors.append(f"tweet_{idx}_not_string")
            continue

        stripped = tweet.strip()
        if not stripped:
            errors.append(f"tweet_{idx}_empty")
            continue

        if len(tweet) > max_len:
            errors.append(f"tweet_{idx}_too_long={len(tweet)}")

        lowered = stripped.lower()
        if "none." in lowered or lowered == "none":
            errors.append(f"tweet_{idx}_contains_none")

        na_count = lowered.count("n/a")
        if na_count > MAX_NA_PER_TWEET:
            errors.append(f"tweet_{idx}_too_many_na={na_count}")

    return (len(errors) == 0, errors)


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

    param_string = "&".join(f"{_percent_encode(key)}={_percent_encode(value)}" for key, value in sorted(oauth_params.items()))

    signature_base = "&".join([
        method.upper(),
        _percent_encode(url),
        _percent_encode(param_string),
    ])

    signing_key = f"{_percent_encode(api_secret)}&{_percent_encode(access_token_secret)}"
    digest = hmac.new(signing_key.encode(), signature_base.encode(), hashlib.sha1).digest()
    oauth_params["oauth_signature"] = base64.b64encode(digest).decode()

    header = ", ".join(f'{_percent_encode(key)}="{_percent_encode(value)}"' for key, value in sorted(oauth_params.items()))
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
