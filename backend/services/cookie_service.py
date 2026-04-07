"""
services/cookie_service.py
==========================
Cookie file management - read, write, validate per source.
"""
from __future__ import annotations

import base64
import math
import json
import logging
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote

from config.sources import SOURCES
from services.session_service import clear_session

logger = logging.getLogger("maaike.cookies")
BASE_DIR = Path(__file__).parent.parent


def _expired_message(days: int | None, label: str = "JWT") -> str | None:
    if days is None:
        return None
    if days < 0:
        return f"{label} expired {-int(days)} days ago"
    return None


def get_all_statuses() -> dict:
    """Return cookie health for every source that needs_cookies."""
    return {
        key: _status_for(key, cfg)
        for key, cfg in SOURCES.items()
        if cfg.get("needs_cookies")
    }


def save_cookies(source: str, data: list) -> dict:
    """Save cookie JSON for a source, clear cached session."""
    cfg = SOURCES.get(source)
    if not cfg:
        raise ValueError(f"Unknown source: {source}")

    path = BASE_DIR / cfg["cookie_file"]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2))

    if source == "jancisrobinson":
        (BASE_DIR / "real_cookies.json").write_text(json.dumps(data, indent=2))

    clear_session(source)
    logger.info("[%s] cookies saved (%d cookies)", source, len(data))
    return {"ok": True, "source": source, "count": len(data)}


def _status_for(source: str, cfg: dict) -> dict:
    path = BASE_DIR / cfg["cookie_file"]
    if not path.exists():
        fallback = BASE_DIR / "real_cookies.json"
        if source == "jancisrobinson" and fallback.exists():
            path = fallback
        else:
            return {"ok": False, "message": "Cookie file not found"}

    try:
        cookies = json.loads(path.read_text())
        if source == "jancisrobinson":
            return _status_jr(cookies)
        if source == "robertparker":
            return _status_rp(cookies)
        if source == "jamessuckling":
            return _status_js(cookies)
        if source == "decanter":
            return _status_dc(cookies)
        return {"ok": True, "cookie_count": len(cookies)}
    except Exception as e:
        return {"ok": False, "message": str(e)}


def _status_jr(cookies: list) -> dict:
    jr = next((c for c in cookies if c.get("name") == "jrAccessRole"), None)
    if not jr:
        return {"ok": False, "message": "jrAccessRole cookie missing"}
    try:
        payload = _decode_jwt(jr["value"])
        days = _days_remaining(payload)
        has_sess = any(c.get("name", "").upper().startswith(("SESS", "SSESS")) for c in cookies)
        has_cf_clearance = any((c.get("name") or "").lower() == "cf_clearance" for c in cookies)
        message = _expired_message(days, "JWT")
        return {
            "ok": message is None,
            "message": message,
            "days_remaining": days,
            "is_member": payload.get("isMember", False),
            "tasting_access": payload.get("canAccessTastingNotes", False),
            "has_session": has_sess,
            "has_cf_clearance": has_cf_clearance,
            "cookie_count": len(cookies),
        }
    except Exception as e:
        return {"ok": False, "message": str(e)}


def _status_rp(cookies: list) -> dict:
    rp = next((c for c in cookies if c.get("name") == "RPWA_AUTH"), None)
    if not rp:
        return {"ok": False, "message": "RPWA_AUTH cookie missing"}
    try:
        auth = json.loads(unquote(rp["value"]))
        payload = _decode_jwt(auth["token"])
        days = _days_remaining(payload)
        message = _expired_message(days, "JWT")
        return {
            "ok": message is None,
            "message": message,
            "days_remaining": days,
            "user_id": auth.get("userId"),
            "cookie_count": len(cookies),
        }
    except Exception as e:
        return {"ok": False, "message": str(e)}


def _status_js(cookies: list) -> dict:
    token = next((c for c in cookies if c.get("name") == "__Secure-next-auth.session-token"), None)
    if not token:
        return {"ok": False, "message": "__Secure-next-auth.session-token cookie missing"}

    exp = token.get("expirationDate")
    days = None
    if exp:
        try:
            days = int((float(exp) - datetime.utcnow().timestamp()) // 86400)
        except Exception:
            days = None

    message = _expired_message(days, "Session token")
    return {
        "ok": message is None,
        "message": message,
        "days_remaining": days,
        "cookie_count": len(cookies),
        "has_csrf": any(c.get("name") == "__Host-next-auth.csrf-token" for c in cookies),
        "has_policy_cookie": any(c.get("name") == "accepted_policy_cc" for c in cookies),
    }


def _status_dc(cookies: list) -> dict:
    oauth = next((c for c in cookies if c.get("name") == "wine_api_oauth_tokens"), None)
    if not oauth:
        return {"ok": False, "message": "wine_api_oauth_tokens cookie missing"}

    days = None
    hours = None
    expires_at = None
    try:
        payload = json.loads(unquote(oauth["value"]))
        exp_ms = payload.get("expires_at")
        if exp_ms:
            exp_ts = float(exp_ms) / 1000.0
            remaining_sec = exp_ts - datetime.utcnow().timestamp()
            days = int(remaining_sec // 86400)
            hours = max(0, int(math.ceil(remaining_sec / 3600.0))) if remaining_sec > 0 else 0
            expires_at = datetime.utcfromtimestamp(exp_ts).isoformat() + "Z"
    except Exception as e:
        return {"ok": False, "message": f"Invalid wine_api_oauth_tokens cookie: {e}"}

    message = _expired_message(days, "JWT")
    return {
        "ok": message is None,
        "message": message,
        "days_remaining": days,
        "hours_remaining": hours,
        "expires_at": expires_at,
        "cookie_count": len(cookies),
        "has_session_cookie": any(c.get("name") == "FTR_Vanilla_Session_ID" for c in cookies),
        "has_incapsula": any(c.get("name", "").startswith("visid_incap_") for c in cookies),
    }


def _decode_jwt(token: str) -> dict:
    part = token.split(".")[1]
    pad = part + "=" * (-len(part) % 4)
    return json.loads(base64.urlsafe_b64decode(pad).decode())


def _days_remaining(payload: dict) -> int:
    exp = payload.get("exp", 0)
    return int((exp - datetime.utcnow().timestamp()) // 86400)
