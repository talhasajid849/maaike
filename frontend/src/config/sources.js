/**
 * config/sources.js
 * ==================
 * Frontend mirror of backend/config/sources.py
 *
 * The real source of truth is the backend.
 * This file provides FALLBACK display data (icons, colors, labels)
 * so the UI renders correctly even before the API responds.
 *
 * To add a new source: add it here AND in backend/config/sources.py
 */

export const SOURCE_META = {
  jancisrobinson: {
    label: 'Jancis Robinson',
    short: 'JR',
    icon:  '🍷',
    color: '#00bfa5',
    scale: 20,
  },
  robertparker: {
    label: 'Robert Parker',
    short: 'RP',
    icon:  '⭐',
    color: '#A0843A',
    scale: 100,
  },
  jamessuckling: {
    label: 'James Suckling',
    short: 'JS',
    icon:  '🏆',
    color: '#C0392B',
    scale: 100,
  },
  decanter: {
    label: 'Decanter',
    short: 'DC',
    icon:  '📰',
    color: '#1B4F72',
    scale: 100,
  },
};

/** Get display label for a source key */
export function sourceLabel(key) {
  return SOURCE_META[key]?.label ?? key;
}

/** Get short badge text */
export function sourceShort(key) {
  return SOURCE_META[key]?.short ?? key?.toUpperCase?.() ?? '?';
}

/** Get accent color */
export function sourceColor(key) {
  return SOURCE_META[key]?.color ?? '#8b949e';
}

/** Score → /20 display  (backend already normalizes, but useful for display) */
export function formatScore(score20) {
  if (score20 == null) return '—';
  return score20.toFixed(1);
}

/** Score → Tailwind color class */
export function scoreColorClass(s) {
  if (s >= 19) return 'text-teal border-teal';
  if (s >= 18) return 'text-green border-green';
  if (s >= 17) return 'text-blue border-blue';
  if (s >= 16) return 'text-yellow border-yellow';
  if (s >= 15) return 'text-orange border-orange';
  return 'text-text2 border-border';
}

/** Alias so Dashboard can import as either SOURCE_META or SOURCES */
export const SOURCES = SOURCE_META;