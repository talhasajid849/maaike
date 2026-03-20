"""
services/session_service.py
============================
Lazy-load and cache one authenticated session per source.
Think of this like a connection pool manager in Express.

Usage:
    session = get_session("robertparker")
    if not session:
        print("RP cookies missing or invalid")
"""
from __future__ import annotations

import importlib
import logging
from pathlib import Path

from config.sources import SOURCES

logger  = logging.getLogger("maaike.sessions")
_cache: dict[str, any] = {}     # { source_key: requests.Session }

BASE_DIR = Path(__file__).parent.parent


def get_session(source: str):
    """Return cached session, loading it if needed. Returns None on failure."""
    if source in _cache:
        return _cache[source]
    return _load_session(source)


def clear_session(source: str):
    """Force reload on next get_session() call — call after cookie upload."""
    _cache.pop(source, None)
    logger.info("[%s] session cleared", source)


def get_all_active_sessions() -> dict:
    """
    Load sessions for all enabled sources.
    Returns { source_key: session_or_None }
    """
    return {
        key: get_session(key)
        for key, cfg in SOURCES.items()
        if cfg.get("enabled")
    }


def _load_session(source: str):
    cfg = SOURCES.get(source)
    if not cfg or not cfg.get("enabled"):
        return None

    cookie_path = BASE_DIR / cfg["cookie_file"]

    # JancisRobinson is still in maaike_phase1.py (legacy)
    if source == "jancisrobinson":
        return _load_jr_session(cookie_path)

    # All other sources: sources/<key>.py
    return _load_source_session(source, cookie_path)


def _load_jr_session(cookie_path: Path):
    """Load JR session from maaike_phase1.py (legacy compatibility)."""
    # Also check old cookie path
    fallback = BASE_DIR / "real_cookies.json"
    path = cookie_path if cookie_path.exists() else fallback

    if not path.exists():
        logger.warning("[jancisrobinson] Cookie file not found: %s", path)
        return None
    try:
        import sys
        sys.path.insert(0, str(BASE_DIR))
        from maaike_phase1 import load_session
        session = load_session(str(path))
        _cache["jancisrobinson"] = session
        logger.info("[jancisrobinson] session loaded")
        return session
    except Exception as e:
        logger.error("[jancisrobinson] session failed: %s", e)
        return None


def _load_source_session(source: str, cookie_path: Path):
    """Dynamically load sources/<source>.py and call load_session()."""
    if not cookie_path.exists():
        logger.warning("[%s] Cookie file not found: %s", source, cookie_path)
        return None
    try:
        mod = importlib.import_module(f"sources.{source}")
        session = mod.load_session(str(cookie_path))
        _cache[source] = session
        logger.info("[%s] session loaded", source)
        return session
    except Exception as e:
        logger.error("[%s] session failed: %s", source, e)
        return None