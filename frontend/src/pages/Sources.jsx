import { useState, useEffect, useCallback } from 'react';
import { useApp } from '../context/AppContext.jsx';
import { statsApi } from '../services/api.js';
import { Panel, PanelHeader, PanelTitle, PanelBody } from '../components/ui/index.jsx';
import { sourceLabel } from '../config/sources.js';

export default function Sources() {
  const { apiKey } = useApp();
  const [sources,  setSources]  = useState({});
  const [srcStats, setSrcStats] = useState([]);

  const load = useCallback(async () => {
    try {
      const [d, stats] = await Promise.all([
        statsApi.sources(apiKey),
        statsApi.get(apiKey),
      ]);
      setSources(d.sources || {});
      setSrcStats(stats.by_source || []);
    } catch {}
  }, [apiKey]);

  useEffect(() => { load(); }, [load]);

  return (
    <div style={{ maxWidth: 900 }}>
      <Panel>
        <PanelHeader>
          <span className="text-base">🌐</span>
          <PanelTitle>Review Sources</PanelTitle>
        </PanelHeader>
        <PanelBody>
          <p className="text-text2 text-sm mb-4">
            MAAIKE supports multiple review sources. Add a new source by editing{' '}
            <code className="text-teal">backend/config/sources.py</code> and dropping a scraper in{' '}
            <code className="text-teal">backend/sources/</code>.
          </p>

          {/* Source cards */}
          <div className="grid grid-cols-[repeat(auto-fill,minmax(200px,1fr))] gap-3 mb-5">
            {Object.keys(sources).length === 0 ? (
              <div className="text-text3 text-sm">Loading…</div>
            ) : Object.entries(sources).map(([key, s]) => (
              <div key={key}
                className={`bg-bg3 border rounded-lg p-4 transition-opacity
                  ${s.enabled ? 'border-teal' : 'border-border opacity-50'}`}>
                <div className="text-2xl mb-1.5">{s.icon}</div>
                <div className="font-bold text-md text-text1">{s.name}</div>
                <div className="text-xs text-text3 mt-0.5">{s.url}</div>
                <div className={`mt-2 text-xs font-bold tracking-wide ${s.enabled ? 'text-green' : 'text-text3'}`}>
                  {s.enabled ? '● ACTIVE' : '○ INACTIVE'}
                </div>
                {s.needs_cookies && (
                  <div className="text-2xs text-text3 mt-1">Requires authenticated cookies</div>
                )}
              </div>
            ))}
          </div>

          {/* How-to guide */}
          <div className="bg-bg3 border border-border rounded-lg p-4 mb-4">
            <div className="text-xs font-bold text-text2 uppercase tracking-widest mb-2.5">
              How to Add a New Source
            </div>
            <ol className="text-xs text-text2 leading-loose list-decimal ml-4">
              <li>Open <code className="text-teal">backend/config/sources.py</code> → uncomment or add entry to <code className="text-teal">SOURCES</code></li>
              <li>Create <code className="text-teal">backend/sources/your_source.py</code> with <code className="text-teal">load_session()</code> + <code className="text-teal">search_wine()</code></li>
              <li>Add display meta to <code className="text-teal">frontend/src/config/sources.js</code></li>
              <li>Restart the server — it appears here automatically</li>
            </ol>
          </div>

          {/* Reviews by source stats */}
          <div>
            <div className="text-xs font-bold text-text2 uppercase tracking-widest mb-2">
              Reviews by Source
            </div>
            {srcStats.length === 0 ? (
              <div className="text-text3 text-sm">No reviews yet</div>
            ) : srcStats.map(s => (
              <div key={s.source}
                className="flex justify-between py-1.5 border-b border-border last:border-0 text-sm">
                <span className="text-text2">{sourceLabel(s.source)}</span>
                <span className="text-teal font-bold">{s.count.toLocaleString()} reviews</span>
              </div>
            ))}
          </div>
        </PanelBody>
      </Panel>
    </div>
  );
}