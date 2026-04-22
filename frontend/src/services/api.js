/**
 * services/api.js
 * ================
 * All API calls go through here.
 * Think of this like your Express axios service layer.
 *
 * ap(path, opts, apiKey)  — generic authenticated fetch
 * Named functions below   — one per domain
 */

export const API = '/api';

// ── Core fetch wrapper ────────────────────────────────────────────────────────

export async function ap(path, opts = {}, apiKey) {
  const headers = {
    'X-API-Key': apiKey,
    'Content-Type': 'application/json',
    ...(opts.headers || {}),
  };
  delete opts.headers;
  const r = await fetch(API + path, { ...opts, headers });
  let payload = null;
  const contentType = r.headers.get('content-type') || '';
  if (contentType.includes('application/json')) {
    try {
      payload = await r.json();
    } catch {
      payload = null;
    }
  }
  if (r.status === 401) {
    const err = new Error('UNAUTHORIZED');
    err.status = 401;
    throw err;
  }
  if (!r.ok) {
    const msg = payload?.error || payload?.message || `HTTP ${r.status}`;
    const err = new Error(msg);
    err.status = r.status;
    err.payload = payload;
    throw err;
  }
  return payload ?? r.json();
}

// ── Auth ─────────────────────────────────────────────────────────────────────

export const authApi = {
  verify: (apiKey) =>
    fetch(`${API}/auth`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ api_key: apiKey }),
    }).then(r => r.json()),
};

// ── Stats ─────────────────────────────────────────────────────────────────────

export const statsApi = {
  get:     (key) => ap('/stats', {}, key),
  filters: (key, source = '') => {
    const url = '/filter-options' + (source ? `?source=${encodeURIComponent(source)}` : '');
    return ap(url, {}, key);
  },
  sources: (key) => ap('/sources', {}, key),
};

// ── Wines ─────────────────────────────────────────────────────────────────────

export const winesApi = {
  list:    (params, key) => ap(`/wines?${new URLSearchParams(params)}`, {}, key),
  get:     (id, key)     => ap(`/wines/${id}`, {}, key),
  add:     (data, key)   => ap('/wines/add', { method: 'POST', body: JSON.stringify(data) }, key),
  update:  (id, data, key) => ap(`/wines/${id}`, { method: 'PATCH', body: JSON.stringify(data) }, key),
  delete:  (id, key)     => ap(`/wines/${id}`, { method: 'DELETE' }, key),
  reviews: (id, key, source = '') => ap(`/wines/${id}/reviews${source ? `?source=${source}` : ''}`, {}, key),
  enrich:  (id, key)     => ap(`/wines/${id}/enrich`, { method: 'POST' }, key),
};

// ── Enrichment ────────────────────────────────────────────────────────────────

export const enrichApi = {
  start:        (data, key) => ap('/enrich/start',         { method: 'POST', body: JSON.stringify(data) }, key),
  stop:         (key)       => ap('/enrich/stop',          { method: 'POST' }, key),
  testSearch:   (data, key) => ap('/enrich/test-search',   { method: 'POST', body: JSON.stringify(data) }, key),
  status:       (key)       => ap('/enrich/status',        {}, key),
  sourceStatus: (key)       => ap('/enrich/source-status', {}, key),
};

// ── Cookies ───────────────────────────────────────────────────────────────────

export const cookiesApi = {
  status: (key) => ap('/cookies/status', {}, key),

  upload: (file, source, apiKey) => {
    const form = new FormData();
    form.append('file', file);
    return fetch(`${API}/cookies?source=${source}`, {
      method: 'POST',
      headers: { 'X-API-Key': apiKey },
      body: form,
    }).then(r => r.json());
  },
};

// ── CSV ───────────────────────────────────────────────────────────────────────

export const csvApi = {
  upload: (file, apiKey) => {
    const form = new FormData();
    form.append('file', file);
    return fetch(`${API}/upload`, {
      method: 'POST',
      headers: { 'X-API-Key': apiKey },
      body: form,
    }).then(r => r.json());
  },

  uploadReviews: (file, apiKey) => {
    const form = new FormData();
    form.append('file', file);
    return fetch(`${API}/upload-reviews`, {
      method: 'POST',
      headers: { 'X-API-Key': apiKey },
      body: form,
    }).then(r => r.json());
  },

  downloadUrl: (params, apiKey) => {
    const p = new URLSearchParams({ api_key: apiKey, ...params });
    return `${API}/download?${p}`;
  },
};

// ── XLSX review export ────────────────────────────────────────────────────────

export const xlsxApi = {
  upload: (file, apiKey, source = 'jancisrobinson', sleepSec = 2.5, startItem = 1, lwinFilter = '') => {
    const form = new FormData();
    form.append('file', file);
    form.append('source', source);
    form.append('sleep_sec', String(sleepSec));
    form.append('start_item', String(startItem || 1));
    if (String(lwinFilter || '').trim()) {
      form.append('lwin_filter', String(lwinFilter || '').trim());
    }
    return fetch(`${API}/xlsx/upload`, {
      method: 'POST',
      headers: { 'X-API-Key': apiKey },
      body: form,
    }).then(r => r.json());
  },

  status: (jobId, apiKey) => ap(`/xlsx/status/${jobId}`, {}, apiKey),
  stop:   (jobId, apiKey) => ap(`/xlsx/stop/${jobId}`, { method: 'POST' }, apiKey),
  resume: (jobId, apiKey) => ap(`/xlsx/resume/${jobId}`, { method: 'POST' }, apiKey),
  files:  (apiKey) => ap('/xlsx/files', {}, apiKey),
  file:   (fileId, apiKey, opts = {}) => {
    const preview = opts.preview === true ? '1' : '0';
    return ap(`/xlsx/files/${fileId}?preview=${preview}`, {}, apiKey);
  },
  restartFile: (fileId, data, apiKey) => ap(`/xlsx/files/${fileId}/restart`, {
    method: 'POST',
    body: JSON.stringify(data || {}),
  }, apiKey),
  deleteFile: (fileId, apiKey) => ap(`/xlsx/files/${fileId}`, { method: 'DELETE' }, apiKey),

  downloadUrl: (jobId, apiKey) => `${API}/xlsx/download/${jobId}?api_key=${apiKey}`,
  fileDownloadUrl: (fileId, apiKey, kind = 'original') =>
    `${API}/xlsx/files/${fileId}/download?api_key=${apiKey}&kind=${encodeURIComponent(kind)}`,
};

// ── Admin ─────────────────────────────────────────────────────────────────────

export const adminApi = {
  resetNotFound: (key) => ap('/admin/reset-not-found', { method: 'POST' }, key),
  resetFound:    (key) => ap('/admin/reset-found',     { method: 'POST' }, key),
  fixNotes:      (key) => ap('/admin/fix-notes',       { method: 'POST' }, key),
  wipeWines:     (key) => ap('/admin/wipe-wines',      { method: 'POST' }, key),
};
