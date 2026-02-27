import { useState, useEffect, useCallback } from 'react';
import { useApp } from '../context/AppContext.jsx';
import { ap, scls } from '../api.js';

const INIT_FORM = { name: '', vintage: '', lwin: '', price: '', region: '', colour: '', country: '', autoSearch: true };

export default function AddWine() {
  const { apiKey, addToast, setWineModalId } = useApp();
  const [form, setForm] = useState(INIT_FORM);
  const [status, setStatus] = useState(null); // { type, msg }
  const [result, setResult] = useState(null); // { wine, reviews }
  const [manualWines, setManualWines] = useState([]);

  const loadManualWines = useCallback(async () => {
    try {
      const d = await ap('/wines?sort=created_at&dir=desc&per_page=8', {}, apiKey);
      setManualWines(d.wines.filter(w => w.added_manually));
    } catch {}
  }, [apiKey]);

  useEffect(() => { loadManualWines(); }, [loadManualWines]);

  function setField(key, val) {
    setForm(prev => ({ ...prev, [key]: val }));
  }

  async function pollWineResult(wineId, attempts = 0) {
    if (attempts > 30) return;
    try {
      const w = await ap(`/wines/${wineId}`, {}, apiKey);
      if (w.enrichment_status !== 'pending') {
        const rv = await ap(`/wines/${wineId}/reviews`, {}, apiKey);
        setResult({ wine: w, reviews: rv.reviews });
        loadManualWines();
        return;
      }
      setTimeout(() => pollWineResult(wineId, attempts + 1), 2000);
    } catch {}
  }

  async function addWine() {
    if (!form.name.trim()) { addToast('Wine name is required', 'error'); return; }
    setStatus({ type: 'loading', msg: '⏳ Adding wine' + (form.autoSearch ? ' and searching reviews…' : '…') });
    setResult(null);
    try {
      const d = await ap('/wines/add', {
        method: 'POST',
        body: JSON.stringify({
          name: form.name.trim(), vintage: form.vintage, lwin: form.lwin,
          price_eur: form.price, region: form.region, colour: form.colour,
          country: form.country, auto_enrich: form.autoSearch,
        }),
      }, apiKey);

      if (d.existed) {
        setStatus({ type: 'loading', msg: `ℹ️ Wine already exists (ID: ${d.wine_id}).` });
      } else {
        setStatus({ type: 'ok', msg: `✓ Added! Wine ID: ${d.wine_id}${form.autoSearch ? ' — searching reviews in background…' : ''}` });
        if (form.autoSearch) pollWineResult(d.wine_id);
        setForm(INIT_FORM);
        loadManualWines();
      }
      addToast(d.existed ? 'Wine already exists' : 'Wine added!', d.existed ? 'info' : 'success');
    } catch (e) {
      setStatus({ type: 'err', msg: '✗ Error: ' + e.message });
      addToast('Add failed: ' + e.message, 'error');
    }
  }

  async function enrichSingle(wineId) {
    try {
      await ap(`/wines/${wineId}/enrich`, { method: 'POST' }, apiKey);
      addToast('Searching reviews…', 'info');
    } catch (e) { addToast('Error: ' + e.message, 'error'); }
  }

  return (
    <div style={{ maxWidth: 640 }}>
      <div className="panel">
        <div className="ph">
          <span style={{ fontSize: 16 }}>🍾</span>
          <span className="pt">Add Single Wine & Search Reviews</span>
        </div>
        <div className="pb">
          <p style={{ color: 'var(--text2)', fontSize: 13, marginBottom: 16 }}>
            Add a wine manually and instantly search for its review on all enabled sources.
            The LWIN code greatly improves match accuracy.
          </p>
          <div className="add-wine-form">
            <div className="fg span2">
              <label>Wine Name *</label>
              <input type="text" value={form.name} onChange={e => setField('name', e.target.value)}
                placeholder="e.g. Giuseppe Rinaldi, Barolo, Brunate" style={{ fontSize: 14, padding: '9px 12px' }} />
            </div>
            <div className="fg">
              <label>Vintage</label>
              <input type="number" value={form.vintage} onChange={e => setField('vintage', e.target.value)}
                placeholder="e.g. 2021" min="1900" max="2030" style={{ width: '100%' }} />
            </div>
            <div className="fg">
              <label>LWIN (full, recommended)</label>
              <input type="text" value={form.lwin} onChange={e => setField('lwin', e.target.value)}
                placeholder="e.g. LWIN110419020211200750" />
            </div>
            <div className="fg">
              <label>Price EUR</label>
              <input type="text" value={form.price} onChange={e => setField('price', e.target.value)}
                placeholder="e.g. €320.00" />
            </div>
            <div className="fg">
              <label>Region / Appellation</label>
              <input type="text" value={form.region} onChange={e => setField('region', e.target.value)}
                placeholder="e.g. Barolo, Piedmont" />
            </div>
            <div className="fg">
              <label>Colour</label>
              <select value={form.colour} onChange={e => setField('colour', e.target.value)} style={{ width: '100%' }}>
                <option value="">Unknown</option>
                <option value="Red">Red</option>
                <option value="White">White</option>
                <option value="Rosé">Rosé</option>
                <option value="Sparkling">Sparkling</option>
                <option value="Dessert">Dessert</option>
                <option value="Fortified">Fortified</option>
              </select>
            </div>
            <div className="fg">
              <label>Country</label>
              <input type="text" value={form.country} onChange={e => setField('country', e.target.value)}
                placeholder="e.g. Italy" />
            </div>
            <div className="fg span2" style={{ flexDirection: 'row', alignItems: 'center', gap: 10, paddingTop: 6 }}>
              <input type="checkbox" id="aw-auto" checked={form.autoSearch}
                onChange={e => setField('autoSearch', e.target.checked)} style={{ width: 'auto', padding: 0 }} />
              <label htmlFor="aw-auto" style={{ fontSize: 12, textTransform: 'none', letterSpacing: 0, color: 'var(--text2)', cursor: 'pointer' }}>
                Auto-search reviews immediately after adding
              </label>
            </div>
            <div className="fg span2">
              <button className="btn btn-teal" style={{ width: '100%', padding: 11, fontSize: 14 }} onClick={addWine}>
                🔍 Add Wine & Search Reviews
              </button>
            </div>
          </div>

          {status && (
            <div className={`add-status ${status.type}`} dangerouslySetInnerHTML={{ __html: status.msg }} />
          )}

          {result && (
            <div style={{ marginTop: 16 }}>
              {result.reviews.length > 0 ? (
                <div style={{ background: 'var(--bg3)', border: '1px solid var(--teal)', borderRadius: 'var(--r2)', padding: 16 }}>
                  <div style={{ fontSize: 12, fontWeight: 700, textTransform: 'uppercase', letterSpacing: 1, color: 'var(--teal)', marginBottom: 10 }}>
                    ✓ {result.reviews.length} Review(s) Found
                  </div>
                  {result.reviews.map(r => (
                    <div key={r.id} style={{ background: 'var(--bg4)', borderRadius: 'var(--r)', padding: 12, marginBottom: 8, borderLeft: '3px solid var(--teal)' }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 6 }}>
                        <span style={{ fontSize: 22, fontWeight: 800, color: 'var(--teal)' }}>
                          {r.score_20 ? r.score_20.toFixed(1) : '—'}<span style={{ fontSize: 12, color: 'var(--text3)' }}>/20</span>
                        </span>
                        <div>
                          <div style={{ fontWeight: 600 }}>{r.reviewer || '?'}</div>
                          <div style={{ fontSize: 11, color: 'var(--text3)' }}>
                            {r.date_tasted || ''} {r.drink_from ? `· Drink ${r.drink_from}–${r.drink_to}` : ''}
                          </div>
                        </div>
                        {r.review_url && (
                          <a href={r.review_url} target="_blank" rel="noreferrer" className="btn btn-outline"
                            style={{ marginLeft: 'auto', fontSize: 11, padding: '3px 9px' }}>Open ↗</a>
                        )}
                      </div>
                      {r.note && (
                        <div style={{ fontSize: 12, color: 'var(--text2)', borderLeft: '2px solid var(--border2)', paddingLeft: 10, lineHeight: 1.7 }}>
                          {r.note.slice(0, 600)}{r.note.length > 600 ? '…' : ''}
                        </div>
                      )}
                    </div>
                  ))}
                  <button className="btn btn-outline" style={{ marginTop: 6, fontSize: 11 }} onClick={() => setResult(null)}>Dismiss</button>
                </div>
              ) : (
                <div style={{ background: 'var(--bg3)', border: '1px solid var(--border)', borderRadius: 'var(--r2)', padding: 14, color: 'var(--text2)', fontSize: 13 }}>
                  No reviews found on JancisRobinson for this wine.
                </div>
              )}
            </div>
          )}
        </div>
      </div>

      {/* Recently added manually */}
      <div className="panel">
        <div className="ph">
          <span className="pt">🕐 Recently Added Manually</span>
          <button className="btn btn-outline" style={{ marginLeft: 'auto', fontSize: 10, padding: '3px 8px' }} onClick={loadManualWines}>Refresh</button>
        </div>
        <div className="pb">
          {manualWines.length === 0
            ? <div style={{ color: 'var(--text3)', fontSize: 12 }}>No manually added wines yet.</div>
            : manualWines.map(w => (
              <div key={w.id} style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '8px 0', borderBottom: '1px solid var(--border)', cursor: 'pointer' }}
                onClick={() => setWineModalId(w.id)}>
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ fontWeight: 500, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>{w.name}</div>
                  <div style={{ fontSize: 11, color: 'var(--text3)' }}>{w.vintage || 'NV'} {w.lwin7 ? `· LWIN7:${w.lwin7}` : ''}</div>
                </div>
                {w.maaike_score_20 && <span className={`sbadge ${scls(w.maaike_score_20)}`} style={{ fontSize: 11 }}>{w.maaike_score_20.toFixed(1)}</span>}
                <span className={`spill ${w.enrichment_status}`}>{w.enrichment_status}</span>
                <button className="btn-ghost" onClick={e => { e.stopPropagation(); enrichSingle(w.id); }} title="Re-search reviews">🔄</button>
              </div>
            ))
          }
        </div>
      </div>
    </div>
  );
}
