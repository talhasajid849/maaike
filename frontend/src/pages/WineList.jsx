import { useState, useEffect, useCallback, useRef } from 'react';
import { useApp } from '../context/AppContext.jsx';
import { ap, scls, API } from '../api.js';

const INIT_FILTERS = {
  search: '', status: '', vintage: '', colour: '', region: '',
  reviewer: '', source: '', minScore: '', maxScore: '', lwin: '',
  hasNote: '', minReviews: '', minNoteLen: '', dateFrom: '', dateTo: '', reviewYear: '',
};

export default function WineList() {
  const { apiKey, addToast, setWineModalId } = useApp();
  const [filters, setFilters] = useState(INIT_FILTERS);
  const [sortField, setSortField] = useState('maaike_score_20');
  const [sortDir, setSortDir] = useState('desc');
  const [page, setPage] = useState(1);
  const [perPage, setPerPage] = useState(50);
  const [wines, setWines] = useState([]);
  const [total, setTotal] = useState(0);
  const [pages, setPages] = useState(1);
  const [filterOpts, setFilterOpts] = useState({ vintages: [], colours: [], reviewers: [], sources: [] });
  const [collapsed, setCollapsed] = useState(false);
  const debRef = useRef(null);

  const loadFilterOptions = useCallback(async () => {
    try {
      const d = await ap('/filter-options', {}, apiKey);
      setFilterOpts(d);
    } catch {}
  }, [apiKey]);

  const loadWines = useCallback(async (p = page) => {
    const yr = new Date().getFullYear();
    const years = Array.from({ length: yr - 1999 }, (_, i) => yr - i);
    const params = new URLSearchParams({
      page: p, per_page: perPage,
      search: filters.search, status: filters.status, vintage: filters.vintage,
      colour: filters.colour, region: filters.region, reviewer: filters.reviewer,
      source: filters.source, min_score: filters.minScore, max_score: filters.maxScore,
      lwin: filters.lwin, has_note: filters.hasNote, min_reviews: filters.minReviews,
      min_note_len: filters.minNoteLen, date_from: filters.dateFrom,
      date_to: filters.dateTo, review_year: filters.reviewYear,
      sort: sortField, dir: sortDir,
    });
    try {
      const d = await ap(`/wines?${params}`, {}, apiKey);
      setWines(d.wines);
      setTotal(d.total);
      setPages(d.pages);
    } catch (e) { addToast('Error loading wines: ' + e.message, 'error'); }
  }, [filters, sortField, sortDir, page, perPage, apiKey, addToast]);

  useEffect(() => { loadFilterOptions(); }, [loadFilterOptions]);
  useEffect(() => { loadWines(page); }, [sortField, sortDir, perPage]); // eslint-disable-line
  useEffect(() => { loadWines(1); setPage(1); }, []); // eslint-disable-line

  function debounce() {
    clearTimeout(debRef.current);
    debRef.current = setTimeout(() => { setPage(1); loadWines(1); }, 320);
  }

  function setFilter(key, val) {
    setFilters(prev => ({ ...prev, [key]: val }));
  }

  function handleSort(field) {
    if (sortField === field) setSortDir(d => d === 'desc' ? 'asc' : 'desc');
    else { setSortField(field); setSortDir('desc'); }
    setPage(1);
  }

  function resetFilters() {
    setFilters(INIT_FILTERS);
    setPage(1);
  }

  // withFilters=true → apply all active filter panel values
  // withFilters=false → download all found wines, no filters
  function downloadCSV(withFilters = false) {
    const p = new URLSearchParams({ api_key: apiKey });
    if (withFilters) {
      if (filters.search)     p.set('search',      filters.search);
      if (filters.status)     p.set('status',      filters.status);
      if (filters.vintage)    p.set('vintage',     filters.vintage);
      if (filters.colour)     p.set('colour',      filters.colour);
      if (filters.region)     p.set('region',      filters.region);
      if (filters.reviewer)   p.set('reviewer',    filters.reviewer);
      if (filters.source)     p.set('source',      filters.source);
      if (filters.minScore)   p.set('min_score',   filters.minScore);
      if (filters.maxScore)   p.set('max_score',   filters.maxScore);
      if (filters.lwin)       p.set('lwin',        filters.lwin);
      if (filters.hasNote)    p.set('has_note',    filters.hasNote);
      if (filters.minReviews) p.set('min_reviews', filters.minReviews);
      if (filters.minNoteLen) p.set('min_note_len',filters.minNoteLen);
      if (filters.dateFrom)   p.set('date_from',   filters.dateFrom);
      if (filters.dateTo)     p.set('date_to',     filters.dateTo);
      if (filters.reviewYear) p.set('review_year', filters.reviewYear);
      // If no status filter is set, include all (not just found) when filters are active
      if (!filters.status)    p.set('include_all', '1');
    }
    window.open(`${API}/download?${p}`, '_blank');
  }

  const activeFilterCount = Object.values(filters).filter(v => v).length;

  const yr = new Date().getFullYear();
  const years = Array.from({ length: yr - 1999 }, (_, i) => yr - i);

  function SortTh({ field, label }) {
    const active = sortField === field;
    return (
      <th onClick={() => handleSort(field)} data-sort={field} className={active ? 'sorted' : ''}>
        {label} <span className="si">{active ? (sortDir === 'desc' ? '↓' : '↑') : '↕'}</span>
      </th>
    );
  }

  return (
    <>
      {/* Filters */}
      <div className="panel" style={{ marginBottom: 10 }}>
        <div className="ph">
          <span className="pt">🔍 Filters</span>
          <div style={{ marginLeft: 'auto', display: 'flex', gap: 6, alignItems: 'center' }}>
            {activeFilterCount > 0 && <span className="af-badge">{activeFilterCount} active</span>}
            <button className="btn btn-outline" style={{ fontSize: 10, padding: '3px 9px' }} onClick={resetFilters}>Reset</button>
            <button className="ptoggle" onClick={() => setCollapsed(c => !c)}>{collapsed ? '▸' : '▾'}</button>
          </div>
        </div>
        <div className="pcollapse" style={{ maxHeight: collapsed ? 0 : undefined }}>
          <div className="pb">
            <div style={{ display: 'grid', gridTemplateColumns: '1fr auto', gap: 10, marginBottom: 10 }}>
              <input type="text" className="search-bar" style={{ marginBottom: 0 }} placeholder="Search wine name, region, reviewer…"
                value={filters.search} onChange={e => { setFilter('search', e.target.value); debounce(); }} />
              <div className="fg">
                <label>🔑 LWIN Search</label>
                <input type="text" style={{ width: 190 }} placeholder="e.g. 1994896"
                  value={filters.lwin} onChange={e => { setFilter('lwin', e.target.value); debounce(); }} />
              </div>
            </div>
            <div className="fgrid" style={{ marginBottom: 10 }}>
              <div className="fg"><label>Status</label>
                <select value={filters.status} onChange={e => { setFilter('status', e.target.value); loadWines(1); setPage(1); }}>
                  <option value="">All statuses</option>
                  <option value="found">✓ Found</option>
                  <option value="pending">⏳ Pending</option>
                  <option value="not_found">✗ Not Found</option>
                </select>
              </div>
              <div className="fg"><label>Vintage</label>
                <select value={filters.vintage} onChange={e => { setFilter('vintage', e.target.value); loadWines(1); setPage(1); }}>
                  <option value="">All vintages</option>
                  {filterOpts.vintages?.map(v => <option key={v} value={v}>{v}</option>)}
                </select>
              </div>
              <div className="fg"><label>Colour</label>
                <select value={filters.colour} onChange={e => { setFilter('colour', e.target.value); loadWines(1); setPage(1); }}>
                  <option value="">All colours</option>
                  {filterOpts.colours?.map(v => <option key={v} value={v}>{v}</option>)}
                </select>
              </div>
              <div className="fg"><label>Region</label>
                <input type="text" placeholder="e.g. Burgundy" value={filters.region}
                  onChange={e => { setFilter('region', e.target.value); debounce(); }} />
              </div>
              <div className="fg"><label>Reviewer</label>
                <select value={filters.reviewer} onChange={e => { setFilter('reviewer', e.target.value); loadWines(1); setPage(1); }}>
                  <option value="">All reviewers</option>
                  {filterOpts.reviewers?.map(v => <option key={v} value={v}>{v}</option>)}
                </select>
              </div>
              <div className="fg"><label>Source</label>
                <select value={filters.source} onChange={e => { setFilter('source', e.target.value); loadWines(1); setPage(1); }}>
                  <option value="">All sources</option>
                  {filterOpts.sources?.map(v => <option key={v} value={v}>{v}</option>)}
                </select>
              </div>
              <div className="fg"><label>Min Score /20</label>
                <input type="number" step="0.5" min="0" max="20" placeholder="0"
                  value={filters.minScore} onChange={e => { setFilter('minScore', e.target.value); debounce(); }} />
              </div>
              <div className="fg"><label>Max Score /20</label>
                <input type="number" step="0.5" min="0" max="20" placeholder="20"
                  value={filters.maxScore} onChange={e => { setFilter('maxScore', e.target.value); debounce(); }} />
              </div>
              <div className="fg"><label>Has Tasting Note</label>
                <select value={filters.hasNote} onChange={e => { setFilter('hasNote', e.target.value); loadWines(1); setPage(1); }}>
                  <option value="">All wines</option>
                  <option value="1">With note only</option>
                </select>
              </div>
            </div>
            <div style={{ borderTop: '1px solid var(--border)', paddingTop: 9 }}>
              <div style={{ fontSize: 10, fontWeight: 700, textTransform: 'uppercase', letterSpacing: 1, color: 'var(--text3)', marginBottom: 7 }}>▸ Advanced</div>
              <div className="fgrid">
                <div className="fg"><label>Min Review Count</label>
                  <input type="number" min="0" placeholder="e.g. 2"
                    value={filters.minReviews} onChange={e => { setFilter('minReviews', e.target.value); debounce(); }} />
                </div>
                <div className="fg"><label>Min Note Length (chars)</label>
                  <input type="number" min="0" step="50" placeholder="e.g. 200"
                    value={filters.minNoteLen} onChange={e => { setFilter('minNoteLen', e.target.value); debounce(); }} />
                </div>
                <div className="fg"><label>📅 Review Date From</label>
                  <input type="date" value={filters.dateFrom} onChange={e => { setFilter('dateFrom', e.target.value); debounce(); }} />
                </div>
                <div className="fg"><label>📅 Review Date To</label>
                  <input type="date" value={filters.dateTo} onChange={e => { setFilter('dateTo', e.target.value); debounce(); }} />
                </div>
                <div className="fg"><label>Review Year</label>
                  <select value={filters.reviewYear} onChange={e => { setFilter('reviewYear', e.target.value); loadWines(1); setPage(1); }}>
                    <option value="">Any year</option>
                    {years.map(y => <option key={y} value={y}>{y}</option>)}
                  </select>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Table controls */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 9, flexWrap: 'wrap' }}>
        <span style={{ color: 'var(--text2)', fontSize: 12 }}>
          {total.toLocaleString()} wine{total !== 1 ? 's' : ''} found
        </span>
        <div style={{ marginLeft: 'auto', display: 'flex', gap: 7, flexWrap: 'wrap' }}>
          <select value={perPage} onChange={e => { setPerPage(parseInt(e.target.value)); setPage(1); }} style={{ width: 85 }}>
            <option value="25">25/page</option>
            <option value="50">50/page</option>
            <option value="100">100/page</option>
          </select>
          <button className="btn btn-outline" onClick={() => downloadCSV(false)}>⬇ Export All Found</button>
          <button className="btn btn-teal" onClick={() => downloadCSV(true)}
            title={activeFilterCount > 0 ? `Export with ${activeFilterCount} active filter(s)` : 'Export all wines'}>
            ⬇ Export{activeFilterCount > 0 ? ` (${activeFilterCount} filters)` : ' With Filters'}
          </button>
        </div>
      </div>

      {/* Table */}
      <div className="panel" style={{ padding: 0, overflow: 'hidden' }}>
        <div className="tw">
          <table>
            <thead>
              <tr>
                <SortTh field="name" label="Wine Name" />
                <SortTh field="vintage" label="Vintage" />
                <SortTh field="lwin7" label="LWIN7" />
                <SortTh field="maaike_score_20" label="Score /20" />
                <SortTh field="maaike_review_count" label="Reviews" />
                <th>Reviewer</th>
                <SortTh field="maaike_date_tasted" label="Review Date" />
                <SortTh field="maaike_note_length" label="Note" />
                <th>Region</th>
                <SortTh field="price_eur" label="Price EUR" />
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {wines.length === 0 ? (
                <tr><td colSpan="11"><div className="empty-state"><div className="ei">🍷</div><div>No wines match your filters</div></div></td></tr>
              ) : wines.map(w => (
                <WineRow key={w.id} w={w} onLwinClick={lwin => { setFilter('lwin', lwin); loadWines(1); setPage(1); }}
                  onClick={() => setWineModalId(w.id)} />
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Pagination */}
      {pages > 1 && (
        <div className="pag">
          <button className="pb2" disabled={page <= 1} onClick={() => { setPage(p => p - 1); loadWines(page - 1); }}>‹</button>
          {buildPageNums(page, pages).map((n, i) =>
            n === '…' ? <span key={i} style={{ color: 'var(--text3)', padding: '0 3px' }}>…</span>
              : <button key={n} className={`pb2 ${n === page ? 'cur' : ''}`} onClick={() => { setPage(n); loadWines(n); }}>{n}</button>
          )}
          <button className="pb2" disabled={page >= pages} onClick={() => { setPage(p => p + 1); loadWines(page + 1); }}>›</button>
          <span style={{ color: 'var(--text2)', fontSize: 11, marginLeft: 4 }}>
            Page {page}/{pages} · {total.toLocaleString()} wines
          </span>
        </div>
      )}
    </>
  );
}

function WineRow({ w, onClick, onLwinClick }) {
  const s = w.maaike_score_20;
  const rc = w.maaike_review_count || 0;
  const nl = w.maaike_note_length || 0;

  return (
    <tr onClick={onClick}>
      <td>
        <div className="nm" title={w.name}>{w.name}
          {w.added_manually && <span className="manual-badge">MANUAL</span>}
        </div>
        <div className="ns">{w.region || w.appellation || ''}</div>
      </td>
      <td>{w.vintage || 'NV'}</td>
      <td>
        {w.lwin7
          ? <span className="lwin-chip" onClick={e => { e.stopPropagation(); onLwinClick(w.lwin7); }} title="Click to filter by LWIN7">{w.lwin7}</span>
          : <span style={{ color: 'var(--text3)' }}>—</span>
        }
      </td>
      <td>
        {s
          ? <span className={`sbadge ${scls(s)}`}>{s.toFixed(1)}</span>
          : <span className="sbadge snone">—</span>
        }
      </td>
      <td>
        {rc > 0
          ? <span style={{ background: rc >= 3 ? 'var(--teal3)' : 'var(--bg4)', border: `1px solid ${rc >= 3 ? 'var(--teal)' : 'var(--border)'}`, color: rc >= 3 ? 'var(--teal)' : 'var(--text2)', borderRadius: 20, padding: '1px 8px', fontSize: 10, fontWeight: 700 }}>{rc}</span>
          : <span style={{ color: 'var(--text3)' }}>—</span>
        }
      </td>
      <td style={{ color: 'var(--text2)', whiteSpace: 'nowrap' }}>{w.maaike_reviewer || '—'}</td>
      <td>
        {w.maaike_date_tasted
          ? <span style={{ fontSize: 11, color: 'var(--text2)' }}>{w.maaike_date_tasted}</span>
          : <span style={{ color: 'var(--text3)' }}>—</span>
        }
      </td>
      <td>
        {nl > 0
          ? <div style={{ display: 'flex', alignItems: 'center', gap: 4 }} title={`${nl} chars`}>
              <div style={{ width: 36, height: 3, background: 'var(--bg4)', borderRadius: 2 }}>
                <div style={{ width: Math.min(100, Math.round(nl / 600 * 100)) + '%', height: '100%', background: nl > 400 ? 'var(--teal)' : nl > 200 ? 'var(--blue)' : 'var(--text3)', borderRadius: 2 }} />
              </div>
              <span style={{ fontSize: 10, color: 'var(--text3)' }}>{nl}</span>
            </div>
          : <span style={{ color: 'var(--text3)' }}>—</span>
        }
      </td>
      <td style={{ color: 'var(--text2)', fontSize: 11 }}>{w.region || w.appellation || '—'}</td>
      <td style={{ fontSize: 11, whiteSpace: 'nowrap' }}>{w.price_eur || '—'}</td>
      <td><span className={`spill ${w.enrichment_status}`}>{w.enrichment_status}</span></td>
    </tr>
  );
}

function buildPageNums(page, pages) {
  const d = 2, s = Math.max(1, page - d), e = Math.min(pages, page + d);
  const nums = [];
  if (s > 1) { nums.push(1); if (s > 2) nums.push('…'); }
  for (let i = s; i <= e; i++) nums.push(i);
  if (e < pages) { if (e < pages - 1) nums.push('…'); nums.push(pages); }
  return nums;
}
