import { useState, useEffect, useCallback } from 'react';
import { useApp } from '../context/AppContext.jsx';
import { winesApi } from '../services/api.js';
import { Panel, PanelHeader, PanelTitle, PanelBody, Button, FormField, ScoreBadge, SourceBadge, StatusPill, inputCls, selectCls } from '../components/ui/index.jsx';

const INIT_FORM = { name: '', vintage: '', lwin: '', price: '', region: '', colour: '', country: '', autoSearch: true };

export default function AddWine() {
  const { apiKey, addToast, setWineModalId } = useApp();
  const [form,         setForm]        = useState(INIT_FORM);
  const [status,       setStatus]      = useState(null);
  const [result,       setResult]      = useState(null);
  const [manualWines,  setManualWines] = useState([]);

  const loadManualWines = useCallback(async () => {
    try {
      const d = await winesApi.list({ sort: 'created_at', dir: 'desc', per_page: 8 }, apiKey);
      setManualWines(d.wines.filter(w => w.added_manually));
    } catch {}
  }, [apiKey]);

  useEffect(() => { loadManualWines(); }, [loadManualWines]);

  function setField(key, val) { setForm(prev => ({ ...prev, [key]: val })); }

  async function pollResult(wineId, attempts = 0) {
    if (attempts > 30) return;
    try {
      const w = await winesApi.get(wineId, apiKey);
      if (w.enrichment_status !== 'pending') {
        const rv = await winesApi.reviews(wineId, apiKey);
        setResult({ wine: w, reviews: rv.reviews });
        loadManualWines();
        return;
      }
      setTimeout(() => pollResult(wineId, attempts + 1), 2000);
    } catch {}
  }

  async function handleAdd() {
    if (!form.name.trim()) { addToast('Wine name is required', 'error'); return; }
    setStatus({ type: 'loading', msg: '⏳ Adding wine' + (form.autoSearch ? ' and searching reviews…' : '…') });
    setResult(null);
    try {
      const d = await winesApi.add({
        name: form.name.trim(), vintage: form.vintage, lwin: form.lwin,
        price_eur: form.price, region: form.region, colour: form.colour,
        country: form.country, auto_enrich: form.autoSearch,
      }, apiKey);

      if (d.existed) {
        setStatus({ type: 'info', msg: `ℹ️ Wine already exists (ID: ${d.wine_id})` });
        addToast('Wine already exists', 'info');
      } else {
        setStatus({ type: 'ok', msg: `✓ Added! Wine ID: ${d.wine_id}${form.autoSearch ? ' — searching reviews…' : ''}` });
        addToast('Wine added!', 'success');
        if (form.autoSearch) pollResult(d.wine_id);
        setForm(INIT_FORM);
        loadManualWines();
      }
    } catch (e) {
      setStatus({ type: 'err', msg: '✗ Error: ' + e.message });
      addToast('Add failed: ' + e.message, 'error');
    }
  }

  async function enrichSingle(wineId) {
    try {
      await winesApi.enrich(wineId, apiKey);
      addToast('Searching reviews…', 'info');
    } catch (e) { addToast('Error: ' + e.message, 'error'); }
  }

  const STATUS_CLS = {
    ok:      'bg-green/10 border border-green text-green',
    err:     'bg-red/10 border border-red text-red',
    loading: 'bg-bg4 border border-border2 text-text2',
    info:    'bg-blue/10 border border-blue text-blue',
  };

  return (
    <div style={{ maxWidth: 640 }}>
      <Panel>
        <PanelHeader>
          <span className="text-base">🍾</span>
          <PanelTitle>Add Single Wine & Search Reviews</PanelTitle>
        </PanelHeader>
        <PanelBody>
          <p className="text-text2 text-sm mb-4">
            Add a wine manually and search all enabled sources for its review.
            A full LWIN code greatly improves match accuracy.
          </p>

          <div className="grid grid-cols-2 gap-3">
            <FormField label="Wine Name *" className="col-span-2">
              <input type="text" value={form.name} onChange={e => setField('name', e.target.value)}
                placeholder="e.g. Giuseppe Rinaldi, Barolo, Brunate"
                className={inputCls + ' text-md py-2.5'} />
            </FormField>

            <FormField label="Vintage">
              <input type="number" value={form.vintage} onChange={e => setField('vintage', e.target.value)}
                placeholder="2021" min="1900" max="2030" className={inputCls} />
            </FormField>

            <FormField label="LWIN (recommended)">
              <input type="text" value={form.lwin} onChange={e => setField('lwin', e.target.value)}
                placeholder="e.g. LWIN11041902021…" className={inputCls} />
            </FormField>

            <FormField label="Price EUR">
              <input type="text" value={form.price} onChange={e => setField('price', e.target.value)}
                placeholder="€320.00" className={inputCls} />
            </FormField>

            <FormField label="Region / Appellation">
              <input type="text" value={form.region} onChange={e => setField('region', e.target.value)}
                placeholder="Barolo, Piedmont" className={inputCls} />
            </FormField>

            <FormField label="Colour">
              <select value={form.colour} onChange={e => setField('colour', e.target.value)} className={selectCls}>
                <option value="">Unknown</option>
                {['Red','White','Rosé','Sparkling','Dessert','Fortified'].map(c =>
                  <option key={c} value={c}>{c}</option>
                )}
              </select>
            </FormField>

            <FormField label="Country">
              <input type="text" value={form.country} onChange={e => setField('country', e.target.value)}
                placeholder="Italy" className={inputCls} />
            </FormField>

            {/* Auto-search checkbox */}
            <div className="col-span-2 flex items-center gap-2.5 pt-1">
              <input type="checkbox" id="aw-auto" checked={form.autoSearch}
                onChange={e => setField('autoSearch', e.target.checked)}
                className="w-auto cursor-pointer accent-teal" />
              <label htmlFor="aw-auto" className="text-xs text-text2 cursor-pointer">
                Auto-search reviews immediately after adding
              </label>
            </div>

            <div className="col-span-2">
              <Button variant="teal" className="w-full py-2.5 text-md justify-center" onClick={handleAdd}>
                🔍 Add Wine & Search Reviews
              </Button>
            </div>
          </div>

          {status && (
            <div className={`rounded p-3 text-sm mt-3 ${STATUS_CLS[status.type] || STATUS_CLS.loading}`}>
              {status.msg}
            </div>
          )}

          {result && (
            <div className="mt-4">
              {result.reviews.length > 0 ? (
                <div className="bg-bg3 border border-teal rounded-lg p-4">
                  <div className="text-2xs font-bold uppercase tracking-widest text-teal mb-2.5">
                    ✓ {result.reviews.length} Review(s) Found
                  </div>
                  {result.reviews.map(r => (
                    <div key={r.id} className="bg-bg4 rounded p-3 mb-2 border-l-[3px] border-teal">
                      <div className="flex items-center gap-2.5 mb-1.5">
                        <span className="text-2xl font-extrabold text-teal">
                          {r.score_20?.toFixed(1) ?? '—'}
                          <span className="text-xs text-text3 font-normal">/20</span>
                        </span>
                        <div>
                          <div className="flex items-center gap-1.5">
                            <SourceBadge source={r.source} />
                            <span className="font-semibold text-sm">{r.reviewer || '?'}</span>
                          </div>
                          <div className="text-xs text-text3 mt-0.5">
                            {r.date_tasted} {r.drink_from ? `· Drink ${r.drink_from}–${r.drink_to}` : ''}
                          </div>
                        </div>
                        {r.review_url && (
                          <a href={r.review_url} target="_blank" rel="noreferrer"
                            className="ml-auto text-xs text-teal border border-teal/30 px-2 py-0.5 rounded hover:bg-teal/10">
                            Open ↗
                          </a>
                        )}
                      </div>
                      {r.note && (
                        <div className="text-xs text-text2 border-l-2 border-teal pl-2.5 leading-relaxed">
                          {r.note.slice(0, 600)}{r.note.length > 600 ? '…' : ''}
                        </div>
                      )}
                    </div>
                  ))}
                  <Button variant="outline" className="mt-1.5 text-xs py-1" onClick={() => setResult(null)}>
                    Dismiss
                  </Button>
                </div>
              ) : (
                <div className="bg-bg3 border border-border rounded-lg p-3.5 text-text2 text-sm">
                  No reviews found on any source for this wine.
                </div>
              )}
            </div>
          )}
        </PanelBody>
      </Panel>

      {/* Recent manual wines */}
      <Panel>
        <PanelHeader>
          <PanelTitle>🕐 Recently Added Manually</PanelTitle>
          <Button variant="outline" className="ml-auto text-2xs py-0.5 px-2" onClick={loadManualWines}>
            Refresh
          </Button>
        </PanelHeader>
        <PanelBody>
          {manualWines.length === 0 ? (
            <div className="text-text3 text-sm">No manually added wines yet.</div>
          ) : manualWines.map(w => (
            <div key={w.id}
              className="flex items-center gap-2.5 py-2 border-b border-border last:border-0 cursor-pointer hover:bg-bg3 -mx-4 px-4 transition-colors"
              onClick={() => setWineModalId(w.id)}
            >
              <div className="flex-1 min-w-0">
                <div className="font-medium text-sm overflow-hidden text-ellipsis whitespace-nowrap">{w.name}</div>
                <div className="text-xs text-text3">{w.vintage || 'NV'}</div>
              </div>
              {w.maaike_score_20 && <ScoreBadge score={w.maaike_score_20} />}
              <StatusPill status={w.enrichment_status} />
              <button className="bg-transparent border-none text-text3 p-1 cursor-pointer hover:text-text1 transition-colors"
                onClick={e => { e.stopPropagation(); enrichSingle(w.id); }} title="Re-search reviews">
                🔄
              </button>
            </div>
          ))}
        </PanelBody>
      </Panel>
    </div>
  );
}