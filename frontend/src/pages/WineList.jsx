/**
 * pages/WineList.jsx
 * ==================
 * Wine review table — source-first design.
 *
 * CORE BEHAVIOUR:
 *   - Source selector is mandatory. Defaults to 'jancisrobinson'.
 *   - Inventory wines (data_origin='inventory') are ALWAYS shown — fixed base rows.
 *   - When you change source: only the source's review data changes.
 *     Inventory rows stay put.
 *   - Filters (vintage, score, reviewer, date) apply to the source's review data.
 *     Inventory rows always pass through filters so they never disappear.
 *   - Row # = actual DB id (w.id) — gaps are normal (same as screenshot).
 *
 * TABLE COLUMNS (match export CSV exactly):
 *   # | Publisher | LWIN11 | Product Name | Vintage | Critic Name |
 *   Score | Drink From | Drink To | Review Date | Review | Status
 *
 * CSV EXPORT: 100% frontend — no backend /download call needed.
 *   Format: Publisher,LWIN11,Product_Name,Vintage,Critic_Name,Score,
 *           Drink_From,Drink_To,Review_Date,Review
 */

import { useState, useCallback, useEffect, useRef, useMemo, memo } from 'react';
import { useApp } from '../context/AppContext';
import { winesApi, statsApi } from '../services/api';
import { useDebounce } from '../hooks/useDebounce';
import { Panel, FormField } from '../components/ui/index';

// ─── Source config ────────────────────────────────────────────────────────────
const SOURCES = {
  jancisrobinson: { label: 'JR',  full: 'Jancis Robinson',             badgeCls: 'bg-blue-600 text-white' },
  robertparker:   { label: 'RP',  full: 'Robert Parker Wine Advocate', badgeCls: 'bg-red-600 text-white' },
  jamessuckling:  { label: 'JS',  full: 'James Suckling',              badgeCls: 'bg-amber-500 text-black' },
  decanter:       { label: 'DC',  full: 'Decanter',                    badgeCls: 'bg-violet-600 text-white' },
};
const SOURCE_KEYS    = Object.keys(SOURCES);
const DEFAULT_SOURCE = 'jancisrobinson';

// ─── Status config ────────────────────────────────────────────────────────────
const STATUS = {
  downloaded: { label: 'downloaded', cls: 'bg-emerald-500/25 text-emerald-300 border border-emerald-400/40' },
  found:      { label: 'found',      cls: 'bg-sky-500/25 text-sky-300 border border-sky-400/40' },
  pending:    { label: 'pending',    cls: 'bg-zinc-700/50 text-zinc-400 border border-zinc-500/40' },
  not_found:  { label: 'not found',  cls: 'bg-red-500/25 text-red-400 border border-red-400/40' },
};

// ─── Styles ───────────────────────────────────────────────────────────────────
const inputCls  = 'w-full bg-[#0f0f0f] border border-[#252525] rounded text-xs text-[#ccc] px-2.5 py-1.5 placeholder-[#444] focus:outline-none focus:border-[#484848] transition-colors';
const selectCls = inputCls + ' cursor-pointer';

const INIT_FILTERS = {
  search: '', reviewer: '', vintage: '',
  minScore: '', maxScore: '', status: '',
  hasNote: '', dateFrom: '', dateTo: '',
};

// Current year cached at module level — no need to call new Date() per render
const CURRENT_YEAR = new Date().getFullYear();

// ─── Helpers ──────────────────────────────────────────────────────────────────

function extractLwin11(wine) {
  const raw = (wine.lwin11 || wine.lwin || '').toString();
  const digits = raw.replace(/^LWIN/i, '').replace(/\D/g, '');
  return digits.slice(0, 11) || '';
}

function fmtScore(v) {
  if (v == null || v === '') return '';
  const n = parseFloat(v);
  return isNaN(n) ? '' : n.toFixed(1);
}

function fmtDate(s) {
  if (!s) return '';
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) {
    try { return new Date(s + 'T00:00:00').toLocaleDateString('en-GB', { day:'2-digit', month:'short', year:'numeric' }); }
    catch { /**/ }
  }
  return s;
}

function publisherLabel(source) {
  return SOURCES[source]?.full || source || '';
}

// ─── Query params builder (pure function, not a hook) ─────────────────────────
// Called inside loadWines and handleExport using queryRef.current values.
function buildQueryParams({ page, perPage, sortField, sortDir, source, filters }) {
  const params = { page, per_page: perPage, sort: sortField, dir: sortDir, source };
  if (filters.search)   params.search    = filters.search;
  if (filters.status)   params.status    = filters.status;
  if (filters.vintage)  params.vintage   = filters.vintage;
  if (filters.reviewer) params.reviewer  = filters.reviewer;
  if (filters.minScore) params.min_score = filters.minScore;
  if (filters.maxScore) params.max_score = filters.maxScore;
  if (filters.hasNote)  params.has_note  = filters.hasNote;
  if (filters.dateFrom) params.date_from = filters.dateFrom;
  if (filters.dateTo)   params.date_to   = filters.dateTo;
  return params;
}

// ─── Frontend CSV export ──────────────────────────────────────────────────────

function winesToCSV(wines, source) {
  const header = ['Publisher','LWIN11','Product_Name','Vintage',
                  'Critic_Name','Score','Drink_From','Drink_To','Review_Date','Review'];
  const esc = v => {
    const s = String(v ?? '');
    return /[",\n\r]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
  };
  const rows = wines.map(w => {
    const score     = w.src_score_20   ?? w.maaike_score_20;
    const reviewer  = w.src_reviewer   || w.maaike_reviewer;
    const dateTasted= w.src_date_tasted|| w.maaike_date_tasted;
    const drinkFrom = w.src_drink_from ?? w.maaike_drink_from;
    const drinkTo   = w.src_drink_to   ?? w.maaike_drink_to;
    const note      = w.src_note       || w.maaike_short_quote;
    const srcKey    = w.src_source     || w.maaike_best_source || source;
    return [
      publisherLabel(srcKey),
      extractLwin11(w),
      w.name || '',
      w.vintage || 'NV',
      reviewer || '',
      fmtScore(score),
      (drinkFrom && drinkFrom !== 1900) ? drinkFrom : '',
      (drinkTo   && drinkTo   !== 1900) ? drinkTo   : '',
      fmtDate(dateTasted),
      (note || '').trim(),
    ].map(esc).join(',');
  });
  return [header.join(','), ...rows].join('\r\n');
}

function downloadBlob(content, filename) {
  const blob = new Blob([content], { type: 'text/csv;charset=utf-8;' });
  const url  = URL.createObjectURL(blob);
  const a    = Object.assign(document.createElement('a'), { href: url, download: filename });
  document.body.appendChild(a);
  a.click();
  setTimeout(() => { document.body.removeChild(a); URL.revokeObjectURL(url); }, 200);
}

// ─── Sub-components ───────────────────────────────────────────────────────────

function SortArrow({ active, dir }) {
  if (!active) return <span className="ml-1 text-[#333]">⇅</span>;
  return <span className="ml-1 text-cyan-400">{dir === 'desc' ? '↓' : '↑'}</span>;
}

function SourceBadge({ source }) {
  const cfg = SOURCES[source];
  if (!cfg) return <span className="text-[#444] text-xs">{source || '—'}</span>;
  return (
    <span className={`text-2xs font-bold px-1.5 py-0.5 rounded ${cfg.badgeCls}`} title={cfg.full}>
      {cfg.label}
    </span>
  );
}

function StatusBadge({ status }) {
  const cfg = STATUS[status] || STATUS.pending;
  return (
    <span className={`text-2xs font-medium px-2 py-0.5 rounded-full whitespace-nowrap ${cfg.cls}`}>
      {cfg.label}
    </span>
  );
}

function ScorePill({ score }) {
  if (score == null) return <span className="text-[#2a2a2a]">—</span>;
  const s = parseFloat(score);
  const style =
    s >= 19.5 ? { color: '#fbbf24', borderColor: '#f59e0b88', background: '#f59e0b14', boxShadow: '0 0 8px #f59e0b33' } :
    s >= 19   ? { color: '#34d399', borderColor: '#10b98166', background: '#10b98112', boxShadow: '0 0 6px #10b98122' } :
    s >= 18   ? { color: '#38bdf8', borderColor: '#0ea5e966', background: '#0ea5e912' } :
    s >= 17   ? { color: '#a78bfa', borderColor: '#8b5cf666', background: '#8b5cf610' } :
    s >= 16   ? { color: '#fb923c', borderColor: '#f9731666', background: '#f9731610' } :
               { color: '#6b7280', borderColor: '#4b556344' };
  return (
    <span className="border rounded-full px-2 py-0.5 text-xs font-bold tabular-nums" style={style}>
      {s.toFixed(1)}
    </span>
  );
}

function VintagePill({ vintage }) {
  if (!vintage) return <span className="text-[#2a2a2a]">—</span>;
  const y = parseInt(vintage);
  const style =
    y <= 1990 ? { color: '#fbbf24', background: '#78350f30', border: '1px solid #78350f60' } :
    y <= 2000 ? { color: '#fb923c', background: '#7c2d1230', border: '1px solid #7c2d1250' } :
    y <= 2005 ? { color: '#a78bfa', background: '#4c1d9530', border: '1px solid #4c1d9550' } :
    y <= 2010 ? { color: '#38bdf8', background: '#0c4a6e30', border: '1px solid #0c4a6e50' } :
    y <= 2015 ? { color: '#34d399', background: '#06402030', border: '1px solid #06402050' } :
               { color: '#94a3b8', background: '#1e293b30', border: '1px solid #1e293b60' };
  return (
    <span className="font-mono text-xs font-semibold px-1.5 py-0.5 rounded" style={style}>
      {vintage}
    </span>
  );
}

function DrinkWindow({ from, to }) {
  const f = from && from !== 1900 ? from : null;
  const t = to   && to   !== 1900 ? to   : null;
  if (!f && !t) return <span className="text-[#2a2a2a]">—</span>;
  const inWindow = f && t ? (CURRENT_YEAR >= f && CURRENT_YEAR <= t) : false;
  const upcoming = f ? CURRENT_YEAR < f : false;
  const past     = t ? CURRENT_YEAR > t : false;
  const color = inWindow ? '#34d399' : upcoming ? '#fbbf24' : past ? '#ef4444' : '#64748b';
  return (
    <span className="font-mono text-xs tabular-nums" style={{ color }}>
      {f || '?'}–{t || '?'}
    </span>
  );
}

function ReviewerName({ name }) {
  if (!name) return <span className="text-[#2a2a2a]">—</span>;
  let h = 0;
  for (let i = 0; i < name.length; i++) h = (h * 31 + name.charCodeAt(i)) & 0xffffff;
  const palette = ['#93c5fd','#86efac','#fca5a5','#fde68a','#c4b5fd','#6ee7b7','#f9a8d4','#67e8f9'];
  const color = palette[Math.abs(h) % palette.length];
  return <span className="text-xs" style={{ color }}>{name}</span>;
}

function InventoryTag() {
  return (
    <span className="text-2xs font-semibold px-1.5 py-0.5 rounded"
      style={{ color: '#64748b', background: '#1e293b', border: '1px solid #334155' }}>
      inv
    </span>
  );
}

function LwinChip({ lwin11 }) {
  if (!lwin11) return <span className="text-[#2a2a2a]">—</span>;
  return (
    <span className="font-mono text-xs tabular-nums px-1.5 py-0.5 rounded"
      style={{ color: '#94a3b8', background: '#0f172a', border: '1px solid #1e293b' }}>
      {lwin11}
    </span>
  );
}

function ReviewSnippet({ text }) {
  if (!text) return <span className="text-[#222] italic text-xs">—</span>;
  return (
    <span className="text-xs leading-relaxed line-clamp-1" style={{ color: '#475569' }}>
      {text.slice(0, 110)}{text.length > 110 ? '…' : ''}
    </span>
  );
}

// ─── Column definitions ───────────────────────────────────────────────────────
const COLS = [
  { key: 'id',          label: '#',            sort: null },
  { key: 'publisher',   label: 'Publisher',    sort: null },
  { key: 'lwin11',      label: 'LWIN11',       sort: 'lwin11' },
  { key: 'name',        label: 'Product Name', sort: 'name' },
  { key: 'vintage',     label: 'Vintage',      sort: 'vintage' },
  { key: 'critic',      label: 'Critic Name',  sort: null },
  { key: 'score',       label: 'Score',        sort: 'maaike_score_20' },
  { key: 'drink',       label: 'Drink Window', sort: null },
  { key: 'review_date', label: 'Review Date',  sort: 'maaike_date_tasted' },
  { key: 'review',      label: 'Review',       sort: null },
  { key: 'status',      label: 'Status',       sort: null },
];

// ─── Main ─────────────────────────────────────────────────────────────────────

export default function WineList() {
  const { apiKey, addToast, setWineModalId } = useApp();

  const [source,      setSource]      = useState(DEFAULT_SOURCE);
  const [filters,     setFilters]     = useState(INIT_FILTERS);
  const [sortField,   setSortField]   = useState('maaike_score_20');
  const [sortDir,     setSortDir]     = useState('desc');
  const [page,        setPage]        = useState(1);
  const [perPage,     setPerPage]     = useState(50);
  const [wines,       setWines]       = useState([]);
  const [total,       setTotal]       = useState(0);
  const [pages,       setPages]       = useState(1);
  const [filterOpts,  setFilterOpts]  = useState({ vintages: [], reviewers: [] });
  const [loading,       setLoading]       = useState(false);
  const [exporting,     setExporting]     = useState(false);
  const [filtersOpen,   setFiltersOpen]   = useState(true);
  const [exportIdFrom,  setExportIdFrom]  = useState('');
  const [exportIdTo,    setExportIdTo]    = useState('');

  // ── Query ref: always holds the latest query params without making
  //    loadWines depend on them, breaking the buildParams dep chain.
  const queryRef = useRef({ source, filters, sortField, sortDir, perPage, exportIdFrom, exportIdTo });
  useEffect(() => {
    queryRef.current = { source, filters, sortField, sortDir, perPage, exportIdFrom, exportIdTo };
  }); // no deps — syncs after every render (cheap assignment)

  // ── Load data ─────────────────────────────────────────────────────────────
  // Stable reference — only recreated when apiKey/addToast change (rare).
  // filtersOverride: pass fresh values to avoid stale state on immediate calls.
  const loadWines = useCallback(async (p = 1, filtersOverride = null) => {
    const q = queryRef.current;
    const params = buildQueryParams({
      page:      p,
      perPage:   q.perPage,
      sortField: q.sortField,
      sortDir:   q.sortDir,
      source:    q.source,
      filters:   filtersOverride ?? q.filters,
    });
    setLoading(true);
    try {
      const d = await winesApi.list(params, apiKey);
      setWines(d.wines || []);
      setTotal(d.total || 0);
      setPages(d.pages || 1);
    } catch (e) {
      addToast('Error loading wines: ' + e.message, 'error');
    } finally {
      setLoading(false);
    }
  }, [apiKey, addToast]);

  const loadFilterOpts = useCallback(async () => {
    try {
      const d = await statsApi.filters(apiKey, source);
      setFilterOpts(d);
    } catch { /**/ }
  }, [apiKey, source]);

  const debounceLoad = useDebounce(() => { setPage(1); loadWines(1); }, 300);

  // Initial load
  useEffect(() => {
    loadFilterOpts();
    loadWines(1);
  }, []); // eslint-disable-line

  // Reload when source changes (reset filters too)
  useEffect(() => {
    setFilters(INIT_FILTERS);
    setPage(1);
    loadFilterOpts();
    loadWines(1, INIT_FILTERS);
  }, [source]); // eslint-disable-line

  // Reload when sort/perPage changes
  useEffect(() => {
    setPage(1);
    loadWines(1);
  }, [sortField, sortDir, perPage]); // eslint-disable-line

  // ── Handlers ──────────────────────────────────────────────────────────────
  const onFilterChange = useCallback((key, val, immediate = false) => {
    const next = { ...queryRef.current.filters, [key]: val };
    setFilters(next);
    if (immediate) {
      setPage(1);
      loadWines(1, next);
    } else {
      debounceLoad();
    }
  }, [loadWines, debounceLoad]);

  const resetFilters = useCallback(() => {
    setFilters(INIT_FILTERS);
    setPage(1);
    loadWines(1, INIT_FILTERS);
  }, [loadWines]);

  const handleSort = useCallback((field) => {
    if (!field) return;
    if (queryRef.current.sortField === field) {
      setSortDir(d => d === 'desc' ? 'asc' : 'desc');
    } else {
      setSortField(field);
      setSortDir('desc');
    }
  }, []);

  const onPageChange = useCallback((p) => {
    setPage(p);
    loadWines(p);
  }, [loadWines]);

  // ── Export (100% frontend) ────────────────────────────────────────────────
  const handleExport = useCallback(async () => {
    setExporting(true);
    try {
      const q = queryRef.current;
      const params = buildQueryParams({
        page: 1, perPage: q.perPage,
        sortField: q.sortField, sortDir: q.sortDir,
        source: q.source, filters: q.filters,
      });
      const rangeParams = {};
      if (q.exportIdFrom) rangeParams.id_from = parseInt(q.exportIdFrom) || undefined;
      if (q.exportIdTo)   rangeParams.id_to   = parseInt(q.exportIdTo)   || undefined;
      const all = await winesApi.list({ ...params, ...rangeParams, per_page: 10000, page: 1, export: '1' }, apiKey);
      if (!all.wines?.length) { addToast('No wines to export', 'warning'); return; }
      const csv = winesToCSV(all.wines, q.source);
      const ts  = new Date().toISOString().slice(0, 19).replace(/[T:]/g, '-');
      downloadBlob(csv, `maaike_${q.source}_${ts}.csv`);
      addToast(`Exported ${all.wines.length} wines`, 'success');
    } catch (e) {
      addToast('Export failed: ' + e.message, 'error');
    } finally {
      setExporting(false);
    }
  }, [apiKey, addToast]);

  // ── Derived (memoized — not recomputed on every render) ───────────────────
  const activeFilters = useMemo(
    () => Object.values(filters).filter(Boolean).length,
    [filters],
  );

  // Year options built once — list never changes within a session
  const years = useMemo(
    () => Array.from({ length: CURRENT_YEAR - 1999 }, (_, i) => String(CURRENT_YEAR - i)),
    [],
  );

  // ── Render ────────────────────────────────────────────────────────────────
  return (
    <div className="flex flex-col gap-3 p-4 min-h-screen">

      {/* ── Source selector bar ────────────────────────────────────────── */}
      <div className="flex items-center gap-2">
        <span style={{ fontSize: '10px', fontWeight: 700, letterSpacing: '0.12em', textTransform: 'uppercase', color: '#1e293b', marginRight: 4 }}>Source</span>
        {SOURCE_KEYS.map(key => {
          const cfg = SOURCES[key];
          const active = source === key;
          return (
            <button
              key={key}
              onClick={() => setSource(key)}
              title={cfg.full}
              className={`px-3 py-1.5 rounded text-xs font-semibold transition-all border ${
                active
                  ? cfg.badgeCls + ' shadow-lg shadow-current/20 scale-105 border-transparent'
                  : 'bg-[#111] border-[#222] text-[#555] hover:text-[#bbb] hover:border-[#444]'
              }`}
            >
              {cfg.label}
              <span className="ml-1 font-normal opacity-70">{cfg.full.split(' ').slice(-1)[0]}</span>
            </button>
          );
        })}
        <span className="ml-auto text-xs text-[#444] italic">
          Inventory wines always visible · Source review data changes per selection
        </span>
      </div>

      {/* ── Filter panel ───────────────────────────────────────────────── */}
      <Panel>
        <div
          className="flex items-center gap-2 px-4 py-2.5 cursor-pointer"
          style={{ borderBottom: '1px solid #0f172a' }}
          onClick={() => setFiltersOpen(o => !o)}
        >
          <span style={{ fontSize: '10px', fontWeight: 700, letterSpacing: '0.1em', textTransform: 'uppercase', color: '#334155' }}>
            Filters
          </span>
          {activeFilters > 0 && (
            <span style={{ background: '#0ea5e9', color: 'white', fontSize: '10px', fontWeight: 700, padding: '1px 7px', borderRadius: 999 }}>
              {activeFilters}
            </span>
          )}
          <div className="ml-auto flex items-center gap-3">
            <span style={{ fontSize: '11px', color: '#1e293b' }}>{total.toLocaleString()} wines</span>
            {activeFilters > 0 && (
              <button
                style={{ fontSize: '10px', color: '#475569', border: '1px solid #1e293b', borderRadius: 4, padding: '2px 8px' }}
                className="hover:text-slate-300 transition-colors"
                onClick={e => { e.stopPropagation(); resetFilters(); }}
              >
                Reset
              </button>
            )}
            <span style={{ color: '#334155', fontSize: '13px' }}>{filtersOpen ? '▾' : '▸'}</span>
          </div>
        </div>

        {filtersOpen && (
          <div className="p-4 grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-2.5">

            <div className="col-span-2">
              <FormField label="Search name / LWIN11">
                <input
                  type="text"
                  placeholder="Wine name or LWIN11…"
                  value={filters.search}
                  onChange={e => onFilterChange('search', e.target.value)}
                  className={inputCls}
                />
              </FormField>
            </div>

            <FormField label={`Reviewer (${SOURCES[source]?.label})`}>
              <select value={filters.reviewer} onChange={e => onFilterChange('reviewer', e.target.value, true)} className={selectCls}>
                <option value="">All reviewers</option>
                {filterOpts.reviewers?.map(r => <option key={r} value={r}>{r}</option>)}
              </select>
            </FormField>

            <FormField label="Vintage">
              <select value={filters.vintage} onChange={e => onFilterChange('vintage', e.target.value, true)} className={selectCls}>
                <option value="">Any year</option>
                {years.map(y => <option key={y} value={y}>{y}</option>)}
              </select>
            </FormField>

            <FormField label="Status">
              <select value={filters.status} onChange={e => onFilterChange('status', e.target.value, true)} className={selectCls}>
                <option value="">All statuses</option>
                <option value="downloaded">Downloaded (score + note)</option>
                <option value="found">Found (score only)</option>
                <option value="pending">Pending</option>
                <option value="not_found">Not found</option>
              </select>
            </FormField>

            <FormField label="Min score /20">
              <input type="number" min="0" max="20" step="0.5" placeholder="e.g. 17"
                value={filters.minScore} onChange={e => onFilterChange('minScore', e.target.value)}
                className={inputCls} />
            </FormField>

            <FormField label="Max score /20">
              <input type="number" min="0" max="20" step="0.5" placeholder="e.g. 20"
                value={filters.maxScore} onChange={e => onFilterChange('maxScore', e.target.value)}
                className={inputCls} />
            </FormField>

            <FormField label="Review from">
              <input type="date" value={filters.dateFrom}
                onChange={e => onFilterChange('dateFrom', e.target.value, true)} className={inputCls} />
            </FormField>

            <FormField label="Review to">
              <input type="date" value={filters.dateTo}
                onChange={e => onFilterChange('dateTo', e.target.value, true)} className={inputCls} />
            </FormField>

            <FormField label="Review text">
              <select value={filters.hasNote} onChange={e => onFilterChange('hasNote', e.target.value, true)} className={selectCls}>
                <option value="">Any</option>
                <option value="1">Has review text</option>
              </select>
            </FormField>

          </div>
        )}
      </Panel>

      {/* ── Toolbar ────────────────────────────────────────────────────── */}
      <div className="flex items-center gap-3">
        <span style={{ fontSize: '11px', color: '#334155' }}>
          <span style={{ color: '#475569', fontWeight: 600 }}>{total.toLocaleString()}</span> wines
          {activeFilters > 0 && (
            <span style={{ color: '#0ea5e9', marginLeft: 6 }}>
              · {activeFilters} filter{activeFilters !== 1 ? 's' : ''}
            </span>
          )}
        </span>
        <div className="ml-auto flex items-center gap-2">
          <select
            value={perPage}
            onChange={e => setPerPage(Number(e.target.value))}
            style={{ background: '#0a0f1a', border: '1px solid #1e293b', color: '#475569', fontSize: '11px', borderRadius: 4, padding: '3px 8px', outline: 'none' }}
          >
            {[25, 50, 100, 200].map(n => <option key={n} value={n}>{n}/page</option>)}
          </select>

          {/* Export ID range */}
          <div className="flex items-center gap-1" title="Limit export to an ID range (leave blank = all)">
            <span style={{ fontSize: '10px', color: '#334155', whiteSpace: 'nowrap' }}>ID</span>
            <input type="number" min="1" placeholder="from"
              value={exportIdFrom} onChange={e => setExportIdFrom(e.target.value)}
              style={{ width: 58, background: '#0a0f1a', border: '1px solid #1e293b', color: '#64748b', fontSize: '11px', borderRadius: 4, padding: '3px 6px', outline: 'none' }}
            />
            <span style={{ fontSize: '10px', color: '#1e293b' }}>–</span>
            <input type="number" min="1" placeholder="to"
              value={exportIdTo} onChange={e => setExportIdTo(e.target.value)}
              style={{ width: 58, background: '#0a0f1a', border: '1px solid #1e293b', color: '#64748b', fontSize: '11px', borderRadius: 4, padding: '3px 6px', outline: 'none' }}
            />
          </div>

          <button
            onClick={handleExport}
            disabled={exporting || total === 0}
            style={{
              background: exporting || total === 0 ? '#0f172a' : 'linear-gradient(135deg, #0ea5e9, #06b6d4)',
              color: exporting || total === 0 ? '#1e293b' : 'white',
              fontSize: '11px', fontWeight: 600,
              padding: '5px 14px', borderRadius: 5, border: 'none',
              display: 'flex', alignItems: 'center', gap: 6, cursor: 'pointer',
              boxShadow: exporting || total === 0 ? 'none' : '0 0 12px #0ea5e930',
            }}
          >
            {exporting ? '…' : '↓'} Export CSV
            {activeFilters > 0 && <span style={{ opacity: 0.7 }}>(filtered)</span>}
            {(exportIdFrom || exportIdTo) && <span style={{ opacity: 0.7 }}>(range)</span>}
          </button>
        </div>
      </div>

      {/* ── Table ──────────────────────────────────────────────────────── */}
      <Panel className="overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-left border-collapse text-xs">
            <thead>
              <tr style={{ borderBottom: '1px solid #0f172a', background: 'linear-gradient(180deg, #060a10 0%, #080c14 100%)' }}>
                {COLS.map(col => (
                  <th
                    key={col.key}
                    onClick={() => handleSort(col.sort)}
                    className="px-3 py-3 whitespace-nowrap select-none"
                    style={{
                      fontSize: '10px',
                      fontWeight: 700,
                      letterSpacing: '0.08em',
                      textTransform: 'uppercase',
                      color: sortField === col.sort ? '#64748b' : '#1e293b',
                      cursor: col.sort ? 'pointer' : 'default',
                    }}
                  >
                    {col.label}
                    {col.sort && <SortArrow active={sortField === col.sort} dir={sortDir} />}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr>
                  <td colSpan={COLS.length} className="text-center py-20">
                    <span className="animate-pulse text-sm" style={{ color: '#1e293b' }}>Loading…</span>
                  </td>
                </tr>
              ) : wines.length === 0 ? (
                <tr>
                  <td colSpan={COLS.length} className="text-center py-20 text-sm" style={{ color: '#1e293b' }}>
                    No wines found
                    {activeFilters > 0 && (
                      <button
                        style={{ marginLeft: 8, color: '#0ea5e9', textDecoration: 'underline', fontSize: '11px', background: 'none', border: 'none', cursor: 'pointer' }}
                        onClick={resetFilters}
                      >
                        clear filters
                      </button>
                    )}
                  </td>
                </tr>
              ) : (
                wines.map(wine => (
                  <WineRow
                    key={wine.id}
                    wine={wine}
                    source={source}
                    onOpen={setWineModalId}
                  />
                ))
              )}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        {pages > 1 && (
          <div className="flex items-center justify-between px-4 py-3" style={{ borderTop: '1px solid #0f172a' }}>
            <span style={{ fontSize: '11px', color: '#1e293b' }}>
              Page <span style={{ color: '#334155' }}>{page}</span> / {pages} · {total.toLocaleString()} total
            </span>
            <div className="flex items-center gap-1">
              <PageBtn disabled={page <= 1} onClick={() => onPageChange(page - 1)}>← Prev</PageBtn>
              {pageNums(page, pages).map((p, i) =>
                p === '…'
                  ? <span key={`e${i}`} className="px-1.5 text-[#2a2a2a] text-xs">…</span>
                  : <button
                      key={p}
                      onClick={() => onPageChange(p)}
                      style={{
                        minWidth: 28, fontSize: '11px', padding: '1px 8px', borderRadius: 4,
                        border: p === page ? '1px solid #0ea5e966' : '1px solid #1e293b',
                        background: p === page ? '#0ea5e912' : 'transparent',
                        color: p === page ? '#38bdf8' : '#334155',
                        cursor: 'pointer',
                      }}
                    >{p}</button>
              )}
              <PageBtn disabled={page >= pages} onClick={() => onPageChange(page + 1)}>Next →</PageBtn>
            </div>
          </div>
        )}
      </Panel>

    </div>
  );
}

// ─── Wine row ─────────────────────────────────────────────────────────────────
// memo() prevents re-renders when parent state changes (loading, filtersOpen,
// total, exporting, etc.) unless wine data or source actually changes.
// onOpen receives setWineModalId which is a React state setter — always stable,
// so it never triggers a spurious re-render through memo's prop comparison.

const WineRow = memo(function WineRow({ wine, source, onOpen }) {
  const isInventory = wine.data_origin === 'inventory';
  const lwin11 = extractLwin11(wine);

  const score      = wine.src_score_20    ?? wine.maaike_score_20;
  const reviewer   = wine.src_reviewer    || wine.maaike_reviewer;
  const dateTasted = wine.src_date_tasted || wine.maaike_date_tasted;
  const drinkFrom  = wine.src_drink_from  ?? wine.maaike_drink_from;
  const drinkTo    = wine.src_drink_to    ?? wine.maaike_drink_to;
  const noteText   = wine.src_note        || wine.maaike_short_quote || '';
  const srcKey     = wine.src_source      || source;
  // Only dim wines that have NO enrichment data at all (never enriched for any source).
  // Wines enriched for another source (maaike_score_20 set) appear at full brightness.
  const noReview   = !wine.src_source && !wine.maaike_score_20 && isInventory;

  const s = score != null ? parseFloat(score) : null;
  const accentColor =
    s == null   ? null :
    s >= 19.5   ? '#f59e0b' :
    s >= 19     ? '#10b981' :
    s >= 18     ? '#0ea5e9' :
    s >= 17     ? '#8b5cf6' :
    s >= 16     ? '#f97316' : '#374151';

  const rowStyle = accentColor && !noReview
    ? { borderLeft: `3px solid ${accentColor}22` }
    : {};

  return (
    <tr
      className={`border-b border-[#0d0d0d] transition-all group cursor-pointer
        ${noReview ? 'opacity-35 hover:opacity-65' : 'hover:bg-[#0c1120]'}`}
      style={rowStyle}
      onClick={() => onOpen(wine.id)}
    >
      {/* # */}
      <td className="px-3 py-2.5 w-10">
        <span className="font-mono tabular-nums text-xs" style={{ color: '#2d3748' }}>{wine.id}</span>
      </td>

      {/* Publisher */}
      <td className="px-3 py-2.5 w-28">
        <div className="flex items-center gap-1.5">
          <SourceBadge source={srcKey} />
          {isInventory && <InventoryTag />}
        </div>
      </td>

      {/* LWIN11 */}
      <td className="px-3 py-2.5 w-36">
        <LwinChip lwin11={lwin11} />
      </td>

      {/* Product Name */}
      <td className="px-3 py-2.5 min-w-[220px]">
        <span className="font-medium text-sm line-clamp-1 transition-colors"
          style={{ color: noReview ? '#4a5568' : '#cbd5e1' }}>
          {wine.name}
        </span>
      </td>

      {/* Vintage */}
      <td className="px-3 py-2.5 w-20 text-center">
        <VintagePill vintage={wine.vintage} />
      </td>

      {/* Critic Name */}
      <td className="px-3 py-2.5 w-40">
        {noReview
          ? <span className="text-xs italic" style={{ color: '#1e293b' }}>—</span>
          : <ReviewerName name={reviewer} />
        }
      </td>

      {/* Score */}
      <td className="px-3 py-2.5 w-16 text-center">
        <ScorePill score={score} />
      </td>

      {/* Drink window */}
      <td className="px-3 py-2.5 w-28 text-center" colSpan={2}>
        <DrinkWindow from={drinkFrom} to={drinkTo} />
      </td>

      {/* Review Date */}
      <td className="px-3 py-2.5 w-28">
        <span className="font-mono text-xs tabular-nums" style={{ color: '#475569' }}>
          {fmtDate(dateTasted) || '—'}
        </span>
      </td>

      {/* Review snippet */}
      <td className="px-3 py-2.5 min-w-[180px] max-w-xs">
        <ReviewSnippet text={noteText} />
      </td>

      {/* Status */}
      <td className="px-3 py-2.5 w-28 text-center">
        <StatusBadge status={wine.enrichment_status} />
      </td>
    </tr>
  );
});

// ─── Pagination helpers ───────────────────────────────────────────────────────

function PageBtn({ disabled, onClick, children }) {
  return (
    <button
      disabled={disabled} onClick={onClick}
      style={{
        fontSize: '11px', padding: '2px 10px', borderRadius: 4,
        border: '1px solid #1e293b',
        background: 'transparent',
        color: disabled ? '#1e293b' : '#334155',
        cursor: disabled ? 'not-allowed' : 'pointer',
        opacity: disabled ? 0.3 : 1,
      }}
    >{children}</button>
  );
}

function pageNums(current, total) {
  if (total <= 7) return Array.from({ length: total }, (_, i) => i + 1);
  const p = [];
  p.push(1);
  if (current > 3) p.push('…');
  for (let n = Math.max(2, current - 1); n <= Math.min(total - 1, current + 1); n++) p.push(n);
  if (current < total - 2) p.push('…');
  p.push(total);
  return p;
}
