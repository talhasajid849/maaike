/**
 * components/WineModal.jsx
 * =========================
 * Full-detail overlay for a single wine.
 * Opens when wineModalId is set in AppContext (clicking any row in WineList).
 * Shows all metadata + every review across all sources, grouped/sorted.
 */
import { useEffect, useState, useCallback } from 'react';
import { useApp } from '../context/AppContext.jsx';
import { winesApi } from '../services/api.js';

const SRC_LABELS = {
  jancisrobinson: 'JR',
  robertparker:   'RP',
  jamessuckling:  'JS',
  decanter:       'DC',
};
const SRC_FULL = {
  jancisrobinson: 'Jancis Robinson',
  robertparker:   'Robert Parker Wine Advocate',
  jamessuckling:  'James Suckling',
  decanter:       'Decanter',
};
const SRC_COLOR = {
  jancisrobinson: '#2563eb',
  robertparker:   '#dc2626',
  jamessuckling:  '#d97706',
  decanter:       '#7c3aed',
};

export default function WineModal() {
  const { wineModalId, setWineModalId, apiKey, addToast } = useApp();
  const [wine,    setWine]    = useState(null);
  const [reviews, setReviews] = useState([]);
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    if (!wineModalId) return;
    setLoading(true);
    setWine(null);
    setReviews([]);
    try {
      const [w, rv] = await Promise.all([
        winesApi.get(wineModalId, apiKey),
        winesApi.reviews(wineModalId, apiKey),
      ]);
      setWine(w);
      setReviews(rv.reviews || []);
    } catch (e) {
      addToast('Failed to load wine: ' + e.message, 'error');
    } finally {
      setLoading(false);
    }
  }, [wineModalId, apiKey, addToast]);

  useEffect(() => { load(); }, [load]);

  useEffect(() => {
    function onKey(e) { if (e.key === 'Escape') setWineModalId(null); }
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [setWineModalId]);

  function close() { setWineModalId(null); }

  async function enrichSingle() {
    try {
      await winesApi.enrich(wineModalId, apiKey);
      addToast('Searching reviews… check dashboard for progress', 'info');
    } catch (e) { addToast('Error: ' + e.message, 'error'); }
  }

  return (
    <div
      className="fixed inset-0 z-50 flex items-start justify-center bg-black/75 backdrop-blur-sm overflow-y-auto py-8 px-4"
      onClick={e => { if (e.target === e.currentTarget) close(); }}
    >
      <div
        className="relative bg-[#0d1117] border border-[#21262d] rounded-xl shadow-2xl w-full max-w-3xl"
        onClick={e => e.stopPropagation()}
      >
        {/* Close button */}
        <button
          onClick={close}
          className="absolute top-4 right-4 w-7 h-7 flex items-center justify-center rounded text-[#444] hover:text-[#aaa] hover:bg-[#161b22] transition-colors z-10 text-base"
        >✕</button>

        {loading ? (
          <div className="text-center py-20 text-sm text-[#1e293b] animate-pulse">Loading…</div>
        ) : wine ? (
          <WineDetail
            wine={wine}
            reviews={reviews}
            onEnrich={enrichSingle}
            onClose={close}
          />
        ) : (
          <div className="p-8 text-red-400 text-sm">Failed to load wine.</div>
        )}
      </div>
    </div>
  );
}

// ─── Detail view ──────────────────────────────────────────────────────────────

function WineDetail({ wine: w, reviews, onEnrich, onClose }) {
  // Best review = highest score across all sources
  const best = reviews.length > 0
    ? reviews.reduce((a, b) => ((a.score_20 || 0) >= (b.score_20 || 0) ? a : b))
    : null;

  const STATUS_COLOR = {
    downloaded: { text: '#34d399', bg: '#34d39912', border: '#34d39940' },
    found:      { text: '#38bdf8', bg: '#38bdf812', border: '#38bdf840' },
    pending:    { text: '#64748b', bg: '#64748b12', border: '#64748b40' },
    not_found:  { text: '#ef4444', bg: '#ef444412', border: '#ef444440' },
  }[w.enrichment_status] || { text: '#64748b', bg: '#64748b12', border: '#64748b40' };

  const metaFields = [
    ['Vintage',   w.vintage     || 'NV'],
    ['Region',    w.region || w.appellation || '—'],
    ['Colour',    w.colour || w.maaike_colour || '—'],
    ['LWIN',      w.lwin        || '—'],
    ['LWIN11',    w.lwin11      || '—'],
    ['Price EUR', w.price_eur   || '—'],
    ['Stock',     w.stock       || '—'],
    ['Unit Size', w.unit_size   || '—'],
  ];

  return (
    <div className="p-6">

      {/* ── Header ─────────────────────────────────────────────────── */}
      <div className="pr-8 mb-5">
        <div className="flex flex-wrap items-center gap-2 mb-1">
          <h2 className="text-lg font-bold text-[#e6edf3] leading-snug">{w.name}</h2>
          <span
            className="text-[10px] font-semibold px-2 py-0.5 rounded-full"
            style={{ color: STATUS_COLOR.text, background: STATUS_COLOR.bg, border: `1px solid ${STATUS_COLOR.border}` }}
          >
            {w.enrichment_status}
          </span>
        </div>
        {w.maaike_score_20 && (
          <div className="text-xs text-[#475569]">
            Best score:{' '}
            <span className="text-[#fbbf24] font-bold">{w.maaike_score_20.toFixed(1)}/20</span>
            {w.maaike_reviewer && <span> · {w.maaike_reviewer}</span>}
            {w.maaike_drink_from && w.maaike_drink_to && (
              <span className="ml-2 font-mono">🍷 {w.maaike_drink_from}–{w.maaike_drink_to}</span>
            )}
          </div>
        )}
      </div>

      {/* ── Meta grid ──────────────────────────────────────────────── */}
      <div className="grid grid-cols-4 gap-2.5 mb-5 p-3 rounded-lg bg-[#080c14] border border-[#0f172a]">
        {metaFields.map(([label, val]) => (
          <div key={label}>
            <div className="text-[9px] font-bold uppercase tracking-widest text-[#1e293b] mb-0.5">{label}</div>
            <div className="text-xs text-[#475569] font-mono truncate">{val}</div>
          </div>
        ))}
      </div>

      {/* ── Actions ────────────────────────────────────────────────── */}
      <div className="flex flex-wrap gap-2 mb-6">
        {w.maaike_review_url && (
          <a
            href={w.maaike_review_url}
            target="_blank"
            rel="noreferrer"
            className="text-xs px-3 py-1.5 rounded border text-teal-400 border-teal-700/50 bg-teal-900/20 hover:bg-teal-800/30 transition-colors"
          >
            Open Best Review ↗
          </a>
        )}
        {w.supplier_url && (
          <a
            href={w.supplier_url}
            target="_blank"
            rel="noreferrer"
            className="text-xs px-3 py-1.5 rounded border text-[#475569] border-[#1e293b] bg-[#0a0f1a] hover:text-[#94a3b8] transition-colors"
          >
            Supplier ↗
          </a>
        )}
        <button
          onClick={onEnrich}
          className="text-xs px-3 py-1.5 rounded border text-sky-400 border-sky-700/50 bg-sky-900/20 hover:bg-sky-800/30 transition-colors"
        >
          🔄 Re-search Reviews
        </button>
        <button
          onClick={onClose}
          className="text-xs px-3 py-1.5 rounded border text-[#475569] border-[#1e293b] bg-[#0a0f1a] hover:text-[#94a3b8] transition-colors ml-auto"
        >
          Close
        </button>
      </div>

      {/* ── Reviews ────────────────────────────────────────────────── */}
      {reviews.length === 0 ? (
        <div className="text-center py-12 border border-dashed border-[#1e293b] rounded-lg">
          <div className="text-4xl mb-3">🔍</div>
          <div className="text-sm text-[#334155] mb-4">No reviews found yet.</div>
          <button
            onClick={onEnrich}
            className="text-xs px-4 py-2 rounded border text-teal-400 border-teal-700/50 bg-teal-900/20"
          >
            Search Reviews Now
          </button>
        </div>
      ) : (
        <div>
          <div className="flex items-center justify-between mb-3">
            <span className="text-[10px] font-bold uppercase tracking-widest text-[#1e293b]">
              All Reviews ({reviews.length})
            </span>
            <span className="text-[10px] text-[#1e293b]">click row to open modal · ★ = best score</span>
          </div>

          <div className="flex flex-col gap-2.5">
            {[...reviews]
              .sort((a, b) => (b.score_20 || 0) - (a.score_20 || 0))
              .map(r => {
                const isBest = r.id === best?.id;
                const s = r.score_20;
                const scoreColor =
                  s == null  ? '#475569' :
                  s >= 19.5  ? '#fbbf24' :
                  s >= 19    ? '#34d399' :
                  s >= 18    ? '#38bdf8' :
                  s >= 17    ? '#a78bfa' :
                  s >= 16    ? '#fb923c' : '#6b7280';
                const hasNote = r.note && r.note.trim().length > 0;
                const srcColor = SRC_COLOR[r.source] || '#64748b';

                return (
                  <div
                    key={r.id}
                    className="rounded-lg border p-3.5 relative"
                    style={{
                      borderColor: isBest ? '#f59e0b44' : '#0f172a',
                      background:  isBest ? '#f59e0b08' : '#080c14',
                    }}
                  >
                    {isBest && (
                      <span className="absolute top-2.5 right-2.5 text-[9px] font-bold text-amber-400 uppercase tracking-wider">
                        ★ BEST
                      </span>
                    )}

                    {/* Review header */}
                    <div className="flex items-center gap-3 mb-2">
                      <span
                        className="text-xl font-bold tabular-nums leading-none"
                        style={{ color: scoreColor, minWidth: 42 }}
                      >
                        {s != null ? s.toFixed(1) : '—'}
                        <span className="text-xs text-[#1e293b]">/20</span>
                      </span>
                      <div className="flex-1 min-w-0">
                        <div className="text-xs font-medium text-[#94a3b8] truncate">
                          {r.reviewer || 'Unknown reviewer'}
                        </div>
                        <div className="text-[10px] text-[#334155]">{r.date_tasted || '—'}</div>
                      </div>
                      <span
                        className="text-[10px] font-bold px-1.5 py-0.5 rounded text-white shrink-0"
                        style={{ background: srcColor }}
                        title={SRC_FULL[r.source] || r.source}
                      >
                        {SRC_LABELS[r.source] || r.source}
                      </span>
                    </div>

                    {/* Note */}
                    {hasNote ? (
                      <div className="text-[11px] leading-relaxed text-[#475569] border-t border-[#0f172a] pt-2">
                        {r.note}
                      </div>
                    ) : (
                      <div className="text-[11px] italic text-[#1e293b] border-t border-[#0f172a] pt-2">
                        No tasting note available
                      </div>
                    )}

                    {/* Footer: drink window + link */}
                    {(r.drink_from || r.drink_to || r.review_url) && (
                      <div className="flex items-center gap-3 mt-2 pt-2 border-t border-[#0a0a12]">
                        {r.drink_from && r.drink_to && (
                          <span className="text-[11px] text-[#475569] font-mono">
                            🍷 {r.drink_from}–{r.drink_to}
                          </span>
                        )}
                        {r.review_url && (
                          <a
                            href={r.review_url}
                            target="_blank"
                            rel="noreferrer"
                            className="text-[11px] text-[#0ea5e9] hover:text-cyan-300 transition-colors ml-auto"
                            onClick={e => e.stopPropagation()}
                          >
                            Open Review ↗
                          </a>
                        )}
                      </div>
                    )}
                  </div>
                );
              })}
          </div>
        </div>
      )}
    </div>
  );
}
