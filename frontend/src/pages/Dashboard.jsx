/**
 * pages/Dashboard.jsx
 * ====================
 * Premium dashboard — per-source enrichment engines + analytics.
 */

import { useState, useEffect, useRef, useCallback } from 'react';
import { useApp } from '../context/AppContext.jsx';
import { statsApi, enrichApi } from '../services/api.js';
import { getSocket } from '../services/socket.js';
import { SOURCE_META as SOURCES_META } from '../config/sources.js';
import { FormField, inputCls, selectCls } from '../components/ui/index.jsx';

// ─── Log colours ──────────────────────────────────────────────────────────────
const LOG_CLS = {
  success: 'text-emerald-400',
  warn:    'text-amber-400',
  error:   'text-red-400',
  info:    'text-slate-300',
};

// ─── Source palette ───────────────────────────────────────────────────────────
const SRC = {
  jancisrobinson: { border: '#3b82f6', text: '#93c5fd' },
  robertparker:   { border: '#ef4444', text: '#fca5a5' },
  jamessuckling:  { border: '#f59e0b', text: '#fcd34d' },
  decanter:       { border: '#a855f7', text: '#d8b4fe' },
};
const srcColor = k => SRC[k] || { border: '#00bfa5', text: '#00bfa5' };

const extractYear = (value) => {
  const m = String(value || '').match(/\b(19|20)\d{2}\b/);
  return m ? m[0] : '';
};

// ─── Stat Card ────────────────────────────────────────────────────────────────
function StatCard({ label, value, color, sub, highlight }) {
  return (
    <div
      className="relative rounded-xl p-4 overflow-hidden cursor-default select-none
                 transition-transform duration-200 hover:-translate-y-0.5"
      style={{
        background: '#161b22',
        border: `1px solid ${color}22`,
        boxShadow: highlight ? `0 0 24px ${color}18` : 'none',
      }}
    >
      {/* top line accent */}
      <div className="absolute inset-x-0 top-0 h-px"
        style={{ background: `linear-gradient(90deg, transparent, ${color}70, transparent)` }} />

      <div className="text-2xs font-bold uppercase tracking-widest text-slate-600 mb-2">
        {label}
      </div>
      <div className="text-2xl font-bold tabular-nums leading-none" style={{ color }}>
        {value ?? <span className="text-slate-700">—</span>}
      </div>
      {sub && <div className="text-2xs text-slate-700 mt-1.5">{sub}</div>}
    </div>
  );
}

// ─── Source Engine Card ───────────────────────────────────────────────────────
function SourceEngine({ sourceKey, cfg, apiKey, addToast, globalRunning, onStart, onStop }) {
  const [status,    setStatus]    = useState(undefined);
  const [running,   setRunning]   = useState(false);
  const [logs,      setLogs]      = useState([]);
  const [progress,  setProgress]  = useState({ pct: 0, done: 0, total: 0, found: 0, errors: 0, last_id: 0 });
  const [limit,     setLimit]     = useState(50);
  const [sleep,     setSleep]     = useState(cfg?.sleep_sec ?? 3.0);
  const [scope,     setScope]     = useState('pending');
  const [idFrom,    setIdFrom]    = useState('');
  const [idTo,      setIdTo]      = useState('');
  const [collapsed, setCollapsed] = useState(false);
  const [probeName, setProbeName] = useState('');
  const [probeVintage, setProbeVintage] = useState('');
  const [probeLwin, setProbeLwin] = useState('');
  const [probeJsUrl, setProbeJsUrl] = useState('');
  const [probeJsId, setProbeJsId] = useState('');
  const [probeLoading, setProbeLoading] = useState(false);
  const [probeResults, setProbeResults] = useState([]);
  const [probeError, setProbeError] = useState('');
  const [probeSearched, setProbeSearched] = useState(false);
  const logRef = useRef(null);
  const probeNameRef = useRef(null);
  const probeVintageRef = useRef(null);
  const probeLwinRef = useRef(null);
  const probeJsUrlRef = useRef(null);
  const probeJsIdRef = useRef(null);

  const c = srcColor(sourceKey);

  // ── Status ─────────────────────────────────────────────────────────────────
  const loadStatus = useCallback(async () => {
    try {
      const all = await enrichApi.sourceStatus(apiKey);
      const entry = (all || {})[sourceKey];
      setStatus(entry || { has_session: false, found: 0, errors: 0, skipped: 0, no_session: 0 });
    } catch {
      setStatus({ has_session: false, found: 0, errors: 0, skipped: 0, no_session: 0 });
    }
  }, [apiKey, sourceKey]);

  useEffect(() => { loadStatus(); }, [loadStatus]);

  // ── Socket ─────────────────────────────────────────────────────────────────
  useEffect(() => {
    const sock = getSocket();

    sock.on('enrich_log', d => {
      const src = d.source;
      const msg = d.msg || '';
      const mine = src === sourceKey
        || (src == null && running)
        || (src == null && msg.toLowerCase().includes(`[${sourceKey.slice(0, 2).toUpperCase()}]`));
      if (mine)
        setLogs(prev => [...prev.slice(-299), { msg, level: d.level || 'info', ts: d.ts }]);
    });

    sock.on('enrich_progress', d => {
      const mine = d.source === sourceKey || (d.source == null && running);
      if (!mine) return;
      setProgress({ pct: d.pct || 0, done: d.done || 0, total: d.total || 0,
        found: d.found || 0, errors: d.errors || 0, last_id: d.last_id || 0 });
      if (!d.running && d.done > 0) {
        setRunning(false); setIdFrom(''); setIdTo('');
        addToast(`[${cfg?.short}] Done: ${d.found}/${d.total} found`, 'success');
        loadStatus(); onStop?.();
      }
    });

    return () => { sock.off('enrich_log'); sock.off('enrich_progress'); };
  }, [sourceKey, running, cfg, addToast, loadStatus, onStop]);

  useEffect(() => {
    if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight;
  }, [logs]);

  // ── Actions ────────────────────────────────────────────────────────────────
  async function start() {
    try {
      await enrichApi.start({
        limit: parseInt(limit) || 0, sleep: parseFloat(sleep), scope,
        source: sourceKey,
        start_from_id: parseInt(idFrom) || 0,
        end_at_id:     parseInt(idTo)   || 0,
      }, apiKey);
      setRunning(true);
      setLogs([]);
      setProgress({ pct: 0, done: 0, total: 0, found: 0, errors: 0, last_id: 0 });
      onStart?.();
      addToast(`[${cfg?.short}] Starting enrichment…`, 'success');
    } catch (e) { addToast('Failed: ' + e.message, 'error'); }
  }

  async function stop() {
    try {
      await enrichApi.stop(apiKey);
      addToast(`[${cfg?.short}] Stop signal sent`, 'info');
    } catch (e) { addToast('Error: ' + e.message, 'error'); }
  }

  async function runProbeSearch() {
    const needsJsHint = sourceKey === 'jamessuckling';
    const liveProbeName = (probeNameRef.current?.value ?? probeName).trim();
    const liveProbeVintage = (probeVintageRef.current?.value ?? probeVintage).trim();
    const liveProbeLwin = (probeLwinRef.current?.value ?? probeLwin).trim();
    const liveProbeJsUrl = (probeJsUrlRef.current?.value ?? probeJsUrl).trim();
    const liveProbeJsId = (probeJsIdRef.current?.value ?? probeJsId).trim();

    if (
      liveProbeName !== probeName
      || liveProbeVintage !== probeVintage
      || liveProbeLwin !== probeLwin
      || liveProbeJsUrl !== probeJsUrl
      || liveProbeJsId !== probeJsId
    ) {
      setProbeName(liveProbeName);
      setProbeVintage(liveProbeVintage);
      setProbeLwin(liveProbeLwin);
      setProbeJsUrl(liveProbeJsUrl);
      setProbeJsId(liveProbeJsId);
    }

    if (!liveProbeName && !liveProbeJsUrl && !liveProbeJsId) {
      addToast(`[${cfg?.short}] Enter wine name or a James Suckling URL/ID`, 'warn');
      return;
    }
    const nameYear = extractYear(liveProbeName);
    const vintageYear = extractYear(liveProbeVintage);
    if (nameYear && vintageYear && nameYear !== vintageYear) {
      const msg = `Wine name says ${nameYear}, but Vintage says ${vintageYear}. Please make them match before testing.`;
      setProbeError(msg);
      setProbeSearched(true);
      addToast(`[${cfg?.short}] ${msg}`, 'warn');
      return;
    }
    setProbeLoading(true);
    setProbeSearched(false);
    setProbeError('');
    setProbeResults([]);
    try {
      const search_hints = needsJsHint ? {
        jamessuckling_url: liveProbeJsUrl,
        js_tasting_note_id: liveProbeJsId,
      } : undefined;
      const d = await enrichApi.testSearch({
        source: sourceKey,
        name: liveProbeName,
        vintage: liveProbeVintage,
        lwin: liveProbeLwin,
        sleep: Math.max(1.0, Number(sleep) || 1.5),
        limit: 5,
        search_hints,
      }, apiKey);
      if (d?.ok) {
        setProbeResults(d.results || []);
        addToast(`[${cfg?.short}] Test search: ${d.count || 0} result(s)`, 'success');
      } else {
        setProbeError(d?.error || 'Search failed');
      }
    } catch (e) {
      setProbeError(e.message || 'Search failed');
    } finally {
      setProbeLoading(false);
      setProbeSearched(true);
    }
  }

  // ── Derived ────────────────────────────────────────────────────────────────
  const hasSession = status?.has_session;
  const hitRate    = progress.done > 0 ? Math.round(progress.found / progress.done * 100) : null;
  const isBlocked  = globalRunning && !running;

  return (
    <div
      className="rounded-xl overflow-hidden"
      style={{
        background: '#161b22',
        border: `1px solid ${c.border}30`,
        boxShadow: running ? `0 0 32px ${c.border}18, 0 0 0 1px ${c.border}20` : 'none',
        transition: 'box-shadow .4s ease',
      }}
    >
      {/* top accent */}
      <div className="h-[3px]"
        style={{ background: `linear-gradient(90deg, ${c.border}dd, ${c.border}40, transparent)` }} />

      {/* header */}
      <div
        className="flex items-center gap-3 px-4 py-2.5 cursor-pointer select-none"
        style={{ borderBottom: collapsed ? 'none' : `1px solid ${c.border}18` }}
        onClick={() => setCollapsed(v => !v)}
      >
        <span className="text-base leading-none">{cfg?.icon ?? '🔍'}</span>
        <span className="text-sm font-bold tracking-wide" style={{ color: c.text }}>
          {cfg?.name ?? sourceKey}
        </span>

        {/* badges */}
        <div className="flex items-center gap-2.5 ml-1">
          {status === undefined
            ? <span className="text-2xs text-slate-700">checking…</span>
            : hasSession
              ? <span className="flex items-center gap-1 text-xs font-semibold text-emerald-400">
                  <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 inline-block" />
                  Session OK
                </span>
              : <span className="flex items-center gap-1 text-xs font-semibold text-red-400">
                  <span className="w-1.5 h-1.5 rounded-full bg-red-400 inline-block" />
                  No cookies
                </span>
          }

          {running && (
            <span className="flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-xs font-semibold"
              style={{ background: c.border + '1a', color: c.text, border: `1px solid ${c.border}35` }}>
              <span className="w-1.5 h-1.5 rounded-full animate-pulse-dot inline-block"
                style={{ background: c.border }} />
              {progress.total > 0 ? `${progress.done} / ${progress.total}` : 'Starting…'}
            </span>
          )}

          {status && !running && (status.found > 0 || status.errors > 0) && (
            <div className="flex items-center gap-2 text-xs">
              {status.found   > 0 && <span className="text-emerald-400">✓ {status.found}</span>}
              {status.errors  > 0 && <span className="text-red-400">✗ {status.errors}</span>}
              {status.skipped > 0 && <span className="text-slate-600">— {status.skipped}</span>}
            </div>
          )}
        </div>

        <button
          className="ml-auto bg-transparent border-none text-slate-600 hover:text-slate-300 transition-colors cursor-pointer text-sm leading-none"
          onClick={e => { e.stopPropagation(); setCollapsed(v => !v); }}
        >
          {collapsed ? '▸' : '▾'}
        </button>
      </div>

      {!collapsed && (
        <div className="p-4">

          {/* Controls */}
          <div className="flex flex-wrap gap-2.5 items-end mb-3">
            <FormField label="Limit (0 = all)">
              <input type="number" value={limit} min="0"
                onChange={e => setLimit(e.target.value)}
                className={inputCls} style={{ width: 72 }} />
            </FormField>

            <FormField label={`Delay — ${parseFloat(sleep).toFixed(1)} s`}
              className="flex-1" style={{ minWidth: 130, maxWidth: 210 }}>
              <div className="flex items-center gap-2 mt-0.5">
                <input type="range" min="0.5" max="10" step="0.5" value={sleep}
                  onChange={e => setSleep(parseFloat(e.target.value))} className="flex-1" />
                <span className="text-xs font-bold min-w-[32px] text-right" style={{ color: c.text }}>
                  {parseFloat(sleep).toFixed(1)}s
                </span>
              </div>
            </FormField>

            <FormField label="Scope">
              <select value={scope} onChange={e => setScope(e.target.value)}
                className={selectCls} style={{ width: 205 }}>
                <option value="pending">Pending only</option>
                <option value="all">Pending + Not found</option>
                <option value="found">Score only — re-fetch notes</option>
              </select>
            </FormField>

            <FormField label="ID from">
              <input type="number" value={idFrom} min="0" placeholder="1"
                onChange={e => setIdFrom(e.target.value)}
                className={inputCls} style={{ width: 74 }} />
            </FormField>

            <FormField label="ID to">
              <input type="number" value={idTo} min="0" placeholder="∞"
                onChange={e => setIdTo(e.target.value)}
                className={inputCls} style={{ width: 74 }} />
            </FormField>

            <FormField label="&nbsp;">
              {!running
                ? <button onClick={start} disabled={isBlocked}
                    className="inline-flex items-center gap-1.5 px-4 py-1.5 rounded text-sm font-bold
                               cursor-pointer border-none transition-all duration-150
                               disabled:opacity-40 disabled:cursor-not-allowed"
                    style={{
                      background: isBlocked
                        ? '#21262d'
                        : `linear-gradient(135deg, ${c.border}, ${c.border}90)`,
                      color: 'white',
                      boxShadow: isBlocked ? 'none' : `0 2px 14px ${c.border}40`,
                    }}
                  >
                    ▶ Run {cfg?.short}
                  </button>
                : <button onClick={stop}
                    className="inline-flex items-center gap-1.5 px-4 py-1.5 rounded text-sm font-bold
                               cursor-pointer border transition-all duration-150"
                    style={{ background: '#f8514912', color: '#f85149', borderColor: '#f8514940' }}
                  >
                    ⏹ Stop
                  </button>
              }
            </FormField>
          </div>

          {/* Progress block */}
          <div className="rounded-lg px-3 pt-2.5 pb-2 mb-3"
            style={{ background: '#0d1117', border: `1px solid ${c.border}14` }}>
            {/* bar */}
            <div className="h-1.5 rounded-full overflow-hidden mb-2" style={{ background: '#1e293b' }}>
              <div className="h-full rounded-full transition-all duration-500"
                style={{
                  width: progress.pct + '%',
                  background: `linear-gradient(90deg, ${c.border}, ${c.text})`,
                  boxShadow: progress.pct > 0 ? `0 0 8px ${c.border}80` : 'none',
                }} />
            </div>
            {/* row */}
            <div className="flex items-center justify-between text-xs">
              <span className="font-bold tabular-nums"
                style={{ color: progress.pct > 0 ? c.text : '#475569' }}>
                {progress.pct > 0 ? `${progress.pct}%` : '0%'}
              </span>

              <div className="flex items-center gap-3 text-slate-600">
                {progress.total > 0 && <>
                  <span className="tabular-nums">{progress.done}/{progress.total}</span>
                  <span className="text-emerald-400 tabular-nums">✓ {progress.found}</span>
                  {progress.errors > 0 && <span className="text-red-400 tabular-nums">✗ {progress.errors}</span>}
                  {hitRate !== null && <span className="text-slate-500">{hitRate}% hit</span>}
                  {progress.last_id > 0 && <span className="text-slate-700">ID #{progress.last_id}</span>}
                </>}
              </div>

              <button
                className="text-slate-700 hover:text-slate-400 transition-colors bg-transparent border-none cursor-pointer text-xs"
                onClick={() => setLogs([])}
              >
                Clear
              </button>
            </div>
          </div>

          {/* Single-product probe search */}
          <div className="rounded-lg px-3 py-3 mb-3"
            style={{ background: '#0d1117', border: `1px solid ${c.border}18` }}>
            <div className="text-2xs font-bold uppercase tracking-widest text-slate-600 mb-2">
              Search Quality Check
            </div>
            <div className="text-2xs text-slate-600 mb-2">
              Source: <span style={{ color: c.text }}>{cfg?.name} ({cfg?.short})</span>
            </div>
            {sourceKey === 'jamessuckling' && (
              <div className="text-2xs text-amber-300 mb-2">
                James Suckling can now search by wine name and will choose the latest matching tasting date. A direct tasting-note URL or note ID is still the most precise option, and you can paste multiple URLs or IDs separated by commas.
              </div>
            )}
            <div className="flex flex-wrap gap-2 items-end mb-2">
              <FormField label="Wine name" className="flex-1" style={{ minWidth: 250 }}>
                <input
                  type="text"
                  ref={probeNameRef}
                  value={probeName}
                  onChange={e => setProbeName(e.target.value)}
                  placeholder="Chateau La Fleur de Bouard, Lalande de Pomerol"
                  className={inputCls}
                />
              </FormField>
              <FormField label="Vintage">
                <input
                  type="text"
                  ref={probeVintageRef}
                  value={probeVintage}
                  onChange={e => setProbeVintage(e.target.value)}
                  placeholder="2020"
                  className={inputCls}
                  style={{ width: 88 }}
                />
              </FormField>
              <FormField label="LWIN (optional)">
                <input
                  type="text"
                  ref={probeLwinRef}
                  value={probeLwin}
                  onChange={e => setProbeLwin(e.target.value)}
                  placeholder="10098312020"
                  className={inputCls}
                  style={{ width: 130 }}
                />
              </FormField>
              {sourceKey === 'jamessuckling' && (
                <FormField label="JS URL" className="flex-1" style={{ minWidth: 280 }}>
                  <input
                    type="text"
                    ref={probeJsUrlRef}
                    value={probeJsUrl}
                    onChange={e => setProbeJsUrl(e.target.value)}
                    placeholder="https://www.jamessuckling.com/tasting-notes/20113/..."
                    className={inputCls}
                  />
                </FormField>
              )}
              {sourceKey === 'jamessuckling' && (
                <FormField label="JS Note ID">
                  <input
                    type="text"
                    ref={probeJsIdRef}
                    value={probeJsId}
                    onChange={e => setProbeJsId(e.target.value)}
                    placeholder="20113"
                    className={inputCls}
                    style={{ width: 110 }}
                  />
                </FormField>
              )}
              <FormField label="&nbsp;">
                <button
                  onClick={runProbeSearch}
                  disabled={probeLoading}
                  className="inline-flex items-center gap-1.5 px-4 py-1.5 rounded text-sm font-bold cursor-pointer border-none disabled:opacity-50"
                  style={{ background: `linear-gradient(135deg, ${c.border}, ${c.border}90)`, color: '#fff' }}
                >
                  {probeLoading ? 'Searching…' : `Test ${cfg?.short}`}
                </button>
              </FormField>
            </div>

            {probeError && (
              <div className="text-xs text-red-400 mb-2">{probeError}</div>
            )}
            {probeSearched && !probeError && probeResults.length === 0 && (
              <div className="text-xs text-amber-400 mb-2">
                No results found on {cfg?.name}. Try the same query on another source card.
              </div>
            )}

            {probeResults.length > 0 && (
              <div className="flex flex-col gap-2">
                {probeResults.map((r, i) => (
                  <div key={i} className="rounded p-2.5 border"
                    style={{ background: '#0a0f16', borderColor: `${c.border}28` }}>
                    <div className="flex flex-wrap items-center gap-2 text-xs mb-1">
                      <span className="text-slate-300 font-semibold">{r.score_native ?? '—'}/{cfg?.scale || 100}</span>
                      <span className="text-slate-500">{r.reviewer || 'Unknown reviewer'}</span>
                      <span className="text-slate-600">{r.date_tasted || 'No date'}</span>
                    </div>
                    <div className="text-xs text-slate-400">
                      {(r.wine_name_src || '').slice(0, 180) || 'No source wine name'}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Log terminal */}
          <div ref={logRef}
            className="rounded-lg p-3 h-36 overflow-y-auto font-mono text-xs leading-relaxed"
            style={{ background: '#080b10', border: `1px solid ${c.border}15` }}
          >
            {logs.length === 0
              ? <span className="text-slate-700 italic">Waiting for enrichment to start…</span>
              : logs.map((l, i) => (
                <div key={i} className={`py-px ${LOG_CLS[l.level] || 'text-slate-400'}`}>
                  {l.ts && <span className="text-slate-700 mr-2 select-none">{l.ts}</span>}
                  {l.msg}
                </div>
              ))
            }
          </div>
        </div>
      )}
    </div>
  );
}


// ─── Dashboard ────────────────────────────────────────────────────────────────
export default function Dashboard() {
  const { apiKey, addToast, setCurrentPage, setUploadTab } = useApp();
  const [stats,         setStats]         = useState(null);
  const [anyRunning,    setAnyRunning]    = useState(false);
  const [activeSources, setActiveSources] = useState([]);

  const loadStats = useCallback(async () => {
    try {
      const d = await statsApi.get(apiKey);
      setStats(d);
      if (d.enrichment_running) setAnyRunning(true);
    } catch {}
  }, [apiKey]);

  const loadSources = useCallback(async () => {
    const fallback = Object.entries(SOURCES_META).map(([k, m]) => [
      k, { ...m, has_session: false, found: 0, errors: 0, skipped: 0, no_session: 0 },
    ]);
    try {
      const d = await enrichApi.sourceStatus(apiKey);
      const entries = Object.entries(d || {});
      setActiveSources(entries.length > 0 ? entries : fallback);
    } catch { setActiveSources(fallback); }
  }, [apiKey]);

  useEffect(() => {
    loadStats(); loadSources();
    const sock = getSocket();
    sock.on('enrich_progress', d => {
      if (!d.running && d.done > 0) { setAnyRunning(false); loadStats(); }
    });
    return () => sock.off('enrich_progress');
  }, [loadStats, loadSources]);

  // ── Helpers ──────────────────────────────────────────────────────────────
  const bandColor = b => {
    if (b === '19-20') return '#34d399';
    if (b === '18-19') return '#06b6d4';
    if (b === '17-18') return '#60a5fa';
    if (b === '16-17') return '#a78bfa';
    return '#94a3b8';
  };

  const totalScored   = (stats?.score_dist || []).reduce((s, x) => s + x.count, 0);
  const maxDist       = Math.max(1, ...(stats?.score_dist || []).map(x => x.count));
  const maxReviewers  = stats?.reviewers?.[0]?.count || 1;

  return (
    <>
      {/* ── Stats ─────────────────────────────────────────────────────── */}
      <div className="grid gap-2.5 mb-6"
        style={{ gridTemplateColumns: 'repeat(auto-fill, minmax(138px, 1fr))' }}>
        <StatCard label="Total Wines"   value={stats?.total?.toLocaleString()}                       color="#94a3b8" />
        <StatCard label="Downloaded"    value={stats?.downloaded?.toLocaleString()}                  color="#06b6d4" highlight />
        <StatCard label="Found"         value={stats?.found?.toLocaleString()}                       color="#34d399" />
        <StatCard label="Pending"       value={stats?.pending?.toLocaleString()}                     color="#fbbf24" />
        <StatCard label="Not Found"     value={stats?.not_found?.toLocaleString()}                   color="#f87171" />
        <StatCard label="Coverage"      value={stats ? stats.coverage + '%' : '—'}                   color="#a78bfa" highlight sub="enriched" />
        <StatCard label="Avg Score"     value={stats?.avg_score ? stats.avg_score.toFixed(2) : '—'} color="#60a5fa" sub="out of 20" />
        <StatCard label="Total Reviews" value={stats?.total_reviews?.toLocaleString()}               color="#c084fc" highlight sub="all sources" />
      </div>

      {/* ── Engines section header ────────────────────────────────────── */}
      <div className="flex items-center justify-between mb-2.5">
        <span className="text-2xs font-bold uppercase tracking-widest text-slate-600">
          Enrichment Engines
        </span>
        <div className="flex items-center gap-2">
          <button
            onClick={() => {
              setUploadTab('xlsx');
              setCurrentPage('upload');
            }}
            className="px-3 py-1.5 rounded text-xs font-bold border-none cursor-pointer"
            style={{
              background: 'linear-gradient(135deg, #0d9488, #0891b2)',
              color: '#fff',
            }}
          >
            Upload + Fill XLSX
          </button>
          {anyRunning && (
            <span className="flex items-center gap-1.5 text-xs text-emerald-400">
              <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse-dot inline-block" />
              Running
            </span>
          )}
        </div>
      </div>

      {/* ── Engines ───────────────────────────────────────────────────── */}
      <div className="flex flex-col gap-3 mb-6">
        {activeSources.length === 0
          ? <div className="text-slate-700 text-sm py-4">Loading sources…</div>
          : activeSources.map(([key, cfg]) => (
            <SourceEngine key={key} sourceKey={key}
              cfg={SOURCES_META[key] || cfg} apiKey={apiKey} addToast={addToast}
              globalRunning={anyRunning}
              onStart={() => setAnyRunning(true)}
              onStop={() => setAnyRunning(false)}
            />
          ))
        }
      </div>

      {/* ── Analytics ─────────────────────────────────────────────────── */}
      <div className="grid gap-3" style={{ gridTemplateColumns: 'repeat(auto-fill, minmax(320px, 1fr))' }}>

        {/* Top Reviewers */}
        <div className="rounded-xl overflow-hidden" style={{ background: '#161b22', border: '1px solid #30363d' }}>
          <div className="flex items-center justify-between px-4 py-3 border-b border-border">
            <span className="text-2xs font-bold uppercase tracking-widest text-slate-600">🏆 Top Reviewers</span>
            {stats?.reviewers?.length > 0 && (
              <span className="text-2xs text-slate-700">{stats.reviewers.length} reviewers</span>
            )}
          </div>
          <div className="p-4 flex flex-col gap-2.5">
            {stats?.reviewers?.length
              ? stats.reviewers.map((r, i) => (
                <div key={r.name} className="flex items-center gap-3">
                  <span className="text-xs tabular-nums text-slate-700 w-4 text-right select-none">{i + 1}</span>
                  <div className="flex-1 min-w-0">
                    <div className="flex justify-between items-baseline mb-1">
                      <span className="text-sm text-slate-300 truncate pr-2">{r.name}</span>
                      <span className="text-xs font-bold tabular-nums text-emerald-400 shrink-0">
                        {r.count.toLocaleString()}
                      </span>
                    </div>
                    <div className="h-1 rounded-full overflow-hidden" style={{ background: '#21262d' }}>
                      <div className="h-full rounded-full transition-all duration-700"
                        style={{
                          width: Math.round(r.count / maxReviewers * 100) + '%',
                          background: ['#34d399', '#06b6d4', '#60a5fa', '#a78bfa'][i] || '#475569',
                        }} />
                    </div>
                  </div>
                </div>
              ))
              : <div className="text-slate-700 text-sm">No data yet</div>
            }
          </div>
        </div>

        {/* Score Distribution */}
        <div className="rounded-xl overflow-hidden" style={{ background: '#161b22', border: '1px solid #30363d' }}>
          <div className="flex items-center justify-between px-4 py-3 border-b border-border">
            <span className="text-2xs font-bold uppercase tracking-widest text-slate-600">📊 Score Distribution</span>
            {totalScored > 0 && (
              <span className="text-2xs text-slate-700">{totalScored.toLocaleString()} scored</span>
            )}
          </div>
          <div className="p-4 flex flex-col gap-3">
            {stats?.score_dist?.length
              ? stats.score_dist.map(x => {
                  const barPct   = Math.round(x.count / maxDist * 100);
                  const sharePct = totalScored ? (x.count / totalScored * 100).toFixed(1) : '0.0';
                  return (
                    <div key={x.band} className="flex items-center gap-2.5">
                      <span className="text-xs font-mono w-9 text-right tabular-nums shrink-0"
                        style={{ color: bandColor(x.band) }}>{x.band}
                      </span>
                      <div className="flex-1 h-2 rounded-full overflow-hidden" style={{ background: '#1e293b' }}>
                        <div className="h-full rounded-full transition-all duration-700"
                          style={{
                            width: barPct + '%',
                            background: bandColor(x.band),
                            boxShadow: `0 0 6px ${bandColor(x.band)}60`,
                          }} />
                      </div>
                      <span className="text-xs tabular-nums text-slate-500 w-12 text-right shrink-0">
                        {x.count.toLocaleString()}
                      </span>
                      <span className="text-2xs tabular-nums text-slate-700 w-9 text-right shrink-0">
                        {sharePct}%
                      </span>
                    </div>
                  );
                })
              : <div className="text-slate-700 text-sm">No scored wines yet</div>
            }
          </div>
        </div>

        {/* Reviews by Source — full width row */}
        {stats?.by_source?.length > 0 && (
          <div
            className="rounded-xl overflow-hidden"
            style={{ gridColumn: '1 / -1', background: '#161b22', border: '1px solid #30363d' }}
          >
            <div className="px-4 py-3 border-b border-border">
              <span className="text-2xs font-bold uppercase tracking-widest text-slate-600">📡 Reviews by Source</span>
            </div>
            <div className="p-4 grid gap-3"
              style={{ gridTemplateColumns: 'repeat(auto-fill, minmax(176px, 1fr))' }}>
              {stats.by_source.map(s => {
                const sc  = SRC[s.source] || { border: '#888', text: '#aaa' };
                const pct = stats.total_reviews > 0
                  ? (s.count / stats.total_reviews * 100).toFixed(1) : '0.0';
                return (
                  <div key={s.source} className="rounded-lg p-3.5"
                    style={{ background: sc.border + '10', border: `1px solid ${sc.border}22` }}>
                    <div className="flex items-center justify-between mb-2">
                      <span className="text-xs font-bold uppercase tracking-wider" style={{ color: sc.text }}>
                        {SOURCES_META[s.source]?.short || s.source}
                      </span>
                      <span className="text-2xs text-slate-700">{pct}%</span>
                    </div>
                    <div className="text-xl font-bold tabular-nums leading-none mb-0.5" style={{ color: sc.text }}>
                      {s.count.toLocaleString()}
                    </div>
                    <div className="text-2xs text-slate-700 mb-2">
                      {SOURCES_META[s.source]?.name || s.source}
                    </div>
                    <div className="h-1 rounded-full overflow-hidden" style={{ background: '#1e293b' }}>
                      <div className="h-full rounded-full"
                        style={{ width: pct + '%', background: sc.border }} />
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </div>
    </>
  );
}
