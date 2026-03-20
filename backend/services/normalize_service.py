"""
services/normalize_service.py
==============================
Score normalization + note validation service.

KEY BUG FIXED HERE:
  JancisRobinson publishes "batch tasting" articles where one critic
  tastes 10-20 wines in a session. The scraper sometimes returns the
  FIRST wine's note for EVERY wine in that session.

  We detect this by tracking (note_fingerprint, date, reviewer) tuples
  within a single enrichment call. If the same note appears for a
  different wine, we strip the note (keep the score — which IS correct).
"""

from config.sources import SOURCES


# ─── Per-enrichment-run note deduplication ────────────────────────────────────
# This is a module-level set, cleared at the start of each enrich_one() call.
# It catches session-level note pollution within a single wine's enrichment.
_seen_note_fingerprints: dict = {}  # fingerprint → (wine_name, date)


def reset_note_tracking():
    """Call at the start of a full batch run (not per-wine)."""
    global _seen_note_fingerprints
    _seen_note_fingerprints = {}


def _note_fingerprint(note: str) -> str | None:
    """Return a short fingerprint of a note for dedup. None if note is empty."""
    if not note or len(note.strip()) < 40:
        return None
    # Use first 120 chars normalized
    return note.strip()[:120].lower().replace(" ", "").replace(",", "")


# ─── Public API ───────────────────────────────────────────────────────────────

def normalize_review(source: str, raw: dict, wine_name: str = "",
                     flag_duplicate_notes: bool = True) -> dict:
    """
    Take a raw scraper result and return a normalized review dict
    ready to be saved to the DB.

    Input keys (scrapers may use different names — we handle both):
      score_native / score_20   - raw score from source
      note / tasting_note       - tasting note text
      reviewer                  - critic name
      drink_from / drink_to     - years
      date_tasted               - string
      review_url                - URL
      colour                    - wine colour
      wine_name_src             - name as source spells it
      score_label               - band label (RP Extraordinary etc.)
      jr_lwin                   - JR's LWIN (JR only)
    """
    cfg   = SOURCES.get(source, {})
    scale = cfg.get("scale", 100)

    # Resolve raw score
    raw_score = raw.get("score_native") or raw.get("score_20")
    score_native, score_20, score_100 = _normalize_scores(scale, raw_score)

    # Resolve note
    note = raw.get("note") or raw.get("tasting_note") or ""

    # ── Note deduplication guard ───────────────────────────────────────────
    # JR batch-tasting bug: same note appears for multiple wines in a session.
    # We check: if this exact note was already seen for a DIFFERENT wine
    # recently, it's a session-level note, not a wine-specific note → strip it.
    note = _validate_note(note, wine_name, raw.get("date_tasted", ""),
                          raw.get("reviewer", ""), flag_duplicate_notes)

    return {
        "score_native": score_native,
        "score_20":     score_20,
        "score_100":    score_100,
        "score_label":  raw.get("score_label"),
        "reviewer":     raw.get("reviewer"),
        "note":         note,
        "drink_from":   raw.get("drink_from"),
        "drink_to":     raw.get("drink_to"),
        "date_tasted":  raw.get("date_tasted"),
        "review_url":   raw.get("review_url"),
        "colour":       raw.get("colour") or raw.get("maaike_colour"),
        "wine_name_src": raw.get("wine_name_src") or raw.get("wine_name_jr"),
        "jr_lwin":      raw.get("jr_lwin"),
    }


def normalize_reviews(source: str, raws: list[dict], wine_name: str = "") -> list[dict]:
    return [normalize_review(source, r, wine_name=wine_name) for r in raws]


# ─── Note validation ──────────────────────────────────────────────────────────

# Paywall / non-note strings to reject at scrape time
_PAYWALL_PREFIXES = (
    "become a member",
    "subscribe to",
    "sign in to",
    "log in to",
    "please log",
    "login to read",
    "members only",
    "upgrade to",
)

def _is_paywall_note(note: str) -> bool:
    """Return True if this is a scraper hitting a login wall, not a real note."""
    low = note.strip().lower()
    return any(low.startswith(p) for p in _PAYWALL_PREFIXES) or len(note.strip()) < 25


def _validate_note(note: str, wine_name: str, date: str,
                   reviewer: str, flag_duplicate: bool) -> str:
    """
    Detect and strip bad notes:

    1. PAYWALL notes — scraper hit a login wall ("Become a member to read more")
    2. DUPLICATE notes — same note assigned to 2+ different wines in same session
       (JR batch tasting bug). When duplicate detected, clear ALL wines' notes
       from that session.

    Score is always kept. Only the bad note text is returned as empty string.
    """
    if not note:
        return note

    # Reject paywall/stub notes immediately
    if _is_paywall_note(note):
        import logging
        logging.getLogger("maaike.normalize").warning(
            "PAYWALL NOTE rejected for %r: %r", wine_name, note[:60]
        )
        return ""

    fp = _note_fingerprint(note)
    if not fp or not flag_duplicate:
        return note

    # Key: fingerprint + date + reviewer (notes from same session share date+reviewer)
    key = f"{fp}|{date}|{reviewer}"

    if key in _seen_note_fingerprints:
        prev_wine = _seen_note_fingerprints[key]
        if prev_wine and prev_wine.strip().lower() != wine_name.strip().lower():
            import logging
            logging.getLogger("maaike.normalize").warning(
                "DUPLICATE NOTE stripped — first seen for %r, now for %r (date=%s)",
                prev_wine, wine_name, date
            )
            return ""   # strip — keep score, clear wrong note
    else:
        _seen_note_fingerprints[key] = wine_name

    return note


# ─── Score normalization ──────────────────────────────────────────────────────

def _normalize_scores(scale: int, raw_score) -> tuple:
    """
    Returns (score_native, score_20, score_100).

    /20 sources  → multiply by 5 to get /100
    /100 sources → divide by 5 to get /20
    """
    if raw_score is None:
        return None, None, None
    s = float(raw_score)
    if scale == 20:
        return s, s, round(s * 5.0, 1)
    else:
        return s, round(s / 5.0, 2), s


def display_score(source: str, review: dict) -> str:
    """Human-readable score string: 'JR 18.5/20' or 'RP 96/100'"""
    cfg   = SOURCES.get(source, {})
    short = cfg.get("short", source.upper())
    scale = cfg.get("scale", 100)
    score = review.get("score_native")
    if score is None:
        return "—"
    return f"{short} {score}/{scale}"