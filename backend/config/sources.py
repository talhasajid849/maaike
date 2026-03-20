"""
config/sources.py
=================
Single source of truth for all review sources.
Think of this like your Express config/sources.js

To add a new source:
  1. Add entry here
  2. Drop sources/<key>.py with load_session() + search_wine()
  3. That's it — everything else picks it up automatically

Each source must have these fields:
  key          - internal ID (matches filename in sources/)
  name         - display name
  short        - badge label (JR, RP, JS, DC)
  scale        - native score scale (20 or 100)
  color        - hex color for UI badge
  icon         - emoji
  url          - website
  enabled      - bool
  needs_cookies- bool
  cookie_file  - path relative to backend/
  sleep_sec    - default delay between requests (polite scraping)
"""

SOURCES = {
    "jancisrobinson": {
        "key":          "jancisrobinson",
        "name":         "Jancis Robinson",
        "short":        "JR",
        "scale":        20,
        "color":        "#00bfa5",
        "icon":         "🍷",
        "url":          "https://www.jancisrobinson.com",
        "enabled":      True,
        "needs_cookies": True,
        "cookie_file":  "cookies/jancisrobinson.json",
        "sleep_sec":    3.0,
    },
    "robertparker": {
        "key":          "robertparker",
        "name":         "Robert Parker Wine Advocate",
        "short":        "RP",
        "scale":        100,
        "color":        "#A0843A",
        "icon":         "⭐",
        "url":          "https://www.robertparker.com",
        "enabled":      True,
        "needs_cookies": True,
        "cookie_file":  "cookies/robertparker.json",
        "sleep_sec":    4.0,
    },
    "jamessuckling": {
        "key":          "jamessuckling",
        "name":         "James Suckling",
        "short":        "JS",
        "scale":        100,
        "color":        "#C0392B",
        "icon":         "ðŸ†",
        "url":          "https://www.jamessuckling.com",
        "enabled":      True,
        "needs_cookies": True,
        "cookie_file":  "cookies/jamessuckling.json",
        "sleep_sec":    3.0,
    },
    # Uncomment when ready:
    # "jamessuckling": {
    #     "key":          "jamessuckling",
    #     "name":         "James Suckling",
    #     "short":        "JS",
    #     "scale":        100,
    #     "color":        "#C0392B",
    #     "icon":         "🏆",
    #     "url":          "https://www.jamessuckling.com",
    #     "enabled":      False,
    #     "needs_cookies": True,
    #     "cookie_file":  "cookies/jamessuckling.json",
    #     "sleep_sec":    3.0,
    # },
    # "decanter": {
    #     "key":          "decanter",
    #     "name":         "Decanter",
    #     "short":        "DC",
    #     "scale":        100,
    #     "color":        "#1B1B1B",
    #     "icon":         "📰",
    #     "url":          "https://www.decanter.com",
    #     "enabled":      False,
    #     "needs_cookies": True,
    #     "cookie_file":  "cookies/decanter.json",
    #     "sleep_sec":    5.0,
    # },
}
