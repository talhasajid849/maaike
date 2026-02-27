import { useEffect, useState, useCallback } from 'react';
import { useApp } from '../context/AppContext.jsx';
import { ap, scls } from '../api.js';

const SRC_NAMES = {
  jancisrobinson: 'Jancis Robinson',
  decanter: 'Decanter',
  wine_spectator: 'Wine Spectator',
};

export default function WineModal() {
  const { wineModalId, setWineModalId, apiKey, addToast } = useApp();
  const [wine, setWine] = useState(null);
  const [reviews, setReviews] = useState([]);
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    if (!wineModalId) return;
    setLoading(true);
    try {
      const [w, rv] = await Promise.all([
        ap(`/wines/${wineModalId}`, {}, apiKey),
        ap(`/wines/${wineModalId}/reviews`, {}, apiKey),
      ]);
      setWine(w);
      setReviews(rv.reviews);
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

  async function enrichSingle() {
    try {
      await ap(`/wines/${wineModalId}/enrich`, { method: 'POST' }, apiKey);
      addToast('Searching reviews… (check dashboard log)', 'info');
    } catch (e) { addToast('Error: ' + e.message, 'error'); }
  }

  function close() { setWineModalId(null); }

  const best = reviews.length > 0
    ? reviews.reduce((a, b) => ((a.score_20 || 0) >= (b.score_20 || 0) ? a : b))
    : null;

  return (
    <div className="mov active" onClick={e => { if (e.target === e.currentTarget) close(); }}>
      <div className="mod">
        <button className="mod-close" onClick={close}>✕</button>
        {loading ? (
          <div style={{ textAlign: 'center', padding: 40, color: 'var(--text2)' }}>Loading…</div>
        ) : wine ? (
          <WineDetail wine={wine} reviews={reviews} best={best} onEnrich={enrichSingle} onClose={close} />
        ) : (
          <div style={{ color: 'var(--red)' }}>Failed to load wine.</div>
        )}
      </div>
    </div>
  );
}

function WineDetail({ wine: w, reviews, best, onEnrich, onClose }) {
  const lwinHtml = w.lwin
    ? <span style={{ fontFamily: 'monospace', fontSize: 11, background: 'var(--bg4)', padding: '2px 7px', borderRadius: 4, color: 'var(--teal)' }}>{w.lwin}</span>
    : '—';

  const metaFields = [
    ['Colour', w.colour || w.maaike_colour || '—'],
    ['LWIN', lwinHtml],
    ['LWIN7', w.lwin7 || '—'],
    ['Price EUR', w.price_eur || '—'],
    ['Stock', w.stock || '—'],
    ['Unit Size', w.unit_size || '—'],
  ];

  return (
    <>
      <div style={{ fontSize: 20, fontWeight: 700, marginBottom: 3 }}>{w.name}</div>
      <div style={{ fontSize: 13, color: 'var(--text2)', marginBottom: 16 }}>
        {w.vintage || 'NV'} &nbsp;·&nbsp; {w.region || w.appellation || 'Region unknown'}
        {w.added_manually && <span className="manual-badge" style={{ verticalAlign: 'middle', marginLeft: 6 }}>MANUAL</span>}
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill,minmax(150px,1fr))', gap: 10, marginBottom: 16 }}>
        {metaFields.map(([label, val]) => (
          <div key={label}>
            <div style={{ fontSize: 10, fontWeight: 700, textTransform: 'uppercase', letterSpacing: 1, color: 'var(--text3)', marginBottom: 3 }}>{label}</div>
            <div style={{ fontSize: 13 }}>{val}</div>
          </div>
        ))}
      </div>

      <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', marginTop: 4 }}>
        {w.maaike_review_url && (
          <a href={w.maaike_review_url} target="_blank" rel="noreferrer" className="btn btn-teal" onClick={e => e.stopPropagation()}>
            Open Best Review ↗
          </a>
        )}
        {w.supplier_url && (
          <a href={w.supplier_url} target="_blank" rel="noreferrer" className="btn btn-outline" onClick={e => e.stopPropagation()}>
            Rue Pinard ↗
          </a>
        )}
        <button className="btn btn-blue" onClick={onEnrich}>🔄 Re-search Reviews</button>
        <button className="btn btn-outline" onClick={onClose}>Close</button>
      </div>

      {reviews.length > 0 ? (
        <div style={{ marginTop: 20 }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 10 }}>
            <div style={{ fontSize: 12, fontWeight: 700, textTransform: 'uppercase', letterSpacing: 1, color: 'var(--text3)' }}>
              All Reviews ({reviews.length})
            </div>
            <div style={{ fontSize: 11, color: 'var(--text3)' }}>Best score highlighted</div>
          </div>
          <div className="rev-list">
            {reviews.map(r => {
              const isBest = r.id === best?.id;
              const s = r.score_20;
              return (
                <div key={r.id} className={`rev-card ${isBest ? 'best' : ''}`}>
                  {isBest && <div className="rev-best-badge">★ BEST</div>}
                  <div className="rev-header">
                    <div className={`rev-score ${!s ? 'low' : ''}`}>
                      {s ? s.toFixed(1) : '—'}<span style={{ fontSize: 14, color: 'var(--text3)' }}>/20</span>
                    </div>
                    <div className="rev-meta">
                      <div className="rev-reviewer">{r.reviewer || 'Unknown reviewer'}</div>
                      <div className="rev-date">{r.date_tasted || 'Date unknown'}</div>
                    </div>
                    <span className="rev-source">{SRC_NAMES[r.source] || r.source}</span>
                  </div>
                  {r.note
                    ? <div className="rev-note">{r.note}</div>
                    : <div style={{ color: 'var(--text3)', fontSize: 12, fontStyle: 'italic' }}>No tasting note available</div>
                  }
                  <div className="rev-footer">
                    {r.drink_from && r.drink_to && (
                      <span className="rev-drink">🍷 Drink: {r.drink_from}–{r.drink_to}</span>
                    )}
                    {r.review_url && (
                      <a href={r.review_url} target="_blank" rel="noreferrer" className="btn btn-outline" style={{ fontSize: 11, padding: '3px 9px' }} onClick={e => e.stopPropagation()}>
                        Open Review ↗
                      </a>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      ) : (
        <div style={{ marginTop: 16, background: 'var(--bg3)', border: '1px dashed var(--border2)', borderRadius: 'var(--r2)', padding: 20, textAlign: 'center', color: 'var(--text3)' }}>
          <div style={{ fontSize: 24, marginBottom: 8 }}>🔍</div>
          No reviews found yet.
          <button className="btn btn-teal" style={{ marginTop: 10, display: 'block', marginLeft: 'auto', marginRight: 'auto' }} onClick={onEnrich}>
            Search Reviews Now
          </button>
        </div>
      )}
    </>
  );
}
