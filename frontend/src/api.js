export const API = '/api';

export async function ap(path, opts = {}, apiKey) {
  const headers = {
    'X-API-Key': apiKey,
    'Content-Type': 'application/json',
    ...(opts.headers || {}),
  };
  delete opts.headers;
  const r = await fetch(API + path, { ...opts, headers });
  if (r.status === 401) throw new Error('UNAUTHORIZED');
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}

export function esc(s) {
  if (s == null) return '';
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

export function scls(s) {
  if (s >= 19) return 's19';
  if (s >= 18) return 's18';
  if (s >= 17) return 's17';
  if (s >= 16) return 's16';
  if (s >= 15) return 's15';
  return '';
}
