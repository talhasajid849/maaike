/**
 * services/api.js
 * ================
 * All API calls go through here.
 * Think of this like your Express axios service layer.
 *
 * ap(path, opts, apiKey)  — generic authenticated fetch
 * Named functions below   — one per domain
 */

export const API = "/api";

// ── Core fetch wrapper ────────────────────────────────────────────────────────

export async function ap(path, opts = {}, apiKey) {
  const headers = {
    "X-API-Key": apiKey,
    "Content-Type": "application/json",
    ...(opts.headers || {}),
  };
  delete opts.headers;
  const r = await fetch(API + path, { ...opts, headers });
  if (r.status === 401) throw new Error("UNAUTHORIZED");
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}

// ── Auth ─────────────────────────────────────────────────────────────────────

export const authApi = {
  verify: async (apiKey) => {
    const res = await fetch(`${API}/auth`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-API-Key": apiKey, // ✅ IMPORTANT
      },
      body: JSON.stringify({ api_key: apiKey }),
    });

    const text = await res.text();

    try {
      return JSON.parse(text);
    } catch {
      throw new Error(text); // shows real HTML error
    }
  },
};

// ── Stats ─────────────────────────────────────────────────────────────────────

export const statsApi = {
  get: (key) => ap("/stats", {}, key),
  filters: (key) => ap("/filter-options", {}, key),
  sources: (key) => ap("/sources", {}, key),
};

// ── Wines ─────────────────────────────────────────────────────────────────────

export const winesApi = {
  list: (params, key) => ap(`/wines?${new URLSearchParams(params)}`, {}, key),
  get: (id, key) => ap(`/wines/${id}`, {}, key),
  add: (data, key) =>
    ap("/wines/add", { method: "POST", body: JSON.stringify(data) }, key),
  update: (id, data, key) =>
    ap(`/wines/${id}`, { method: "PATCH", body: JSON.stringify(data) }, key),
  delete: (id, key) => ap(`/wines/${id}`, { method: "DELETE" }, key),
  reviews: (id, key, source = "") =>
    ap(`/wines/${id}/reviews${source ? `?source=${source}` : ""}`, {}, key),
  enrich: (id, key) => ap(`/wines/${id}/enrich`, { method: "POST" }, key),
};

// ── Enrichment ────────────────────────────────────────────────────────────────

export const enrichApi = {
  sourceStatus: (key) => ap("/enrich/source-status", {}, key),
  start: (data, key) =>
    ap("/enrich/start", { method: "POST", body: JSON.stringify(data) }, key),
  stop: (key) => ap("/enrich/stop", { method: "POST" }, key),
  status: (key) => ap("/enrich/status", {}, key),
};

// ── Cookies ───────────────────────────────────────────────────────────────────

export const cookiesApi = {
  status: (key) => ap("/cookies/status", {}, key),

  upload: (file, source, apiKey) => {
    const form = new FormData();
    form.append("file", file);
    return fetch(`${API}/cookies?source=${source}`, {
      method: "POST",
      headers: { "X-API-Key": apiKey },
      body: form,
    }).then((r) => r.json());
  },
};

// ── CSV ───────────────────────────────────────────────────────────────────────

export const csvApi = {
  upload: (file, apiKey) => {
    const form = new FormData();
    form.append("file", file);
    return fetch(`${API}/upload`, {
      method: "POST",
      headers: { "X-API-Key": apiKey },
      body: form,
    }).then((r) => r.json());
  },

  downloadUrl: (params, apiKey) => {
    const p = new URLSearchParams({ api_key: apiKey, ...params });
    return `${API}/download?${p}`;
  },
};

// ── Admin ─────────────────────────────────────────────────────────────────────

export const adminApi = {
  resetNotFound: (key) => ap("/admin/reset-not-found", { method: "POST" }, key),
  wipeWines: (key) => ap("/admin/wipe-wines", { method: "POST" }, key),
  fixNotes: (key) => ap("/admin/fix-notes", { method: "POST" }, key),
};
