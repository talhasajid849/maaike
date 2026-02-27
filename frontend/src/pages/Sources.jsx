import { useState, useEffect, useCallback } from 'react';
import { useApp } from '../context/AppContext.jsx';
import { ap } from '../api.js';

export default function Sources() {
  const { apiKey } = useApp();
  const [sources, setSources] = useState({});
  const [srcStats, setSrcStats] = useState([]);

  const load = useCallback(async () => {
    try {
      const [d, stats] = await Promise.all([ap('/sources', {}, apiKey), ap('/stats', {}, apiKey)]);
      setSources(d.sources || {});
      setSrcStats(stats.by_source || []);
    } catch {}
  }, [apiKey]);

  useEffect(() => { load(); }, [load]);

  return (
    <div className="panel" style={{ maxWidth: 900 }}>
      <div className="ph"><span className="pt">🌐 Review Sources</span></div>
      <div className="pb">
        <p style={{ color: 'var(--text2)', fontSize: 13, marginBottom: 16 }}>
          MAAIKE supports multiple review sources. Currently only JancisRobinson is active.
          More sources can be added by updating <code style={{ color: 'var(--teal)' }}>SOURCES</code> in <code style={{ color: 'var(--teal)' }}>api.py</code>.
        </p>

        <div className="src-grid">
          {Object.keys(sources).length === 0
            ? <div style={{ color: 'var(--text3)' }}>Loading…</div>
            : Object.entries(sources).map(([key, s]) => (
              <div key={key} className={`src-card ${s.enabled ? 'enabled' : 'disabled'}`}>
                <div className="src-icon">{s.icon}</div>
                <div className="src-name">{s.name}</div>
                <div className="src-url">{s.url}</div>
                <div className={`src-status ${s.enabled ? 'on' : 'off'}`}>{s.enabled ? '● ACTIVE' : '○ INACTIVE'}</div>
                {s.needs_cookies && (
                  <div style={{ fontSize: 10, color: 'var(--text3)', marginTop: 4 }}>Requires authenticated cookies</div>
                )}
              </div>
            ))
          }
        </div>

        <div style={{ marginTop: 20, background: 'var(--bg3)', border: '1px solid var(--border)', borderRadius: 'var(--r2)', padding: 16 }}>
          <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--text2)', marginBottom: 10, textTransform: 'uppercase', letterSpacing: 1 }}>How to Add a New Source</div>
          <div style={{ fontSize: 12, color: 'var(--text2)', lineHeight: 1.8 }}>
            1. Open <code style={{ color: 'var(--teal)' }}>backend/api.py</code> → find the <code style={{ color: 'var(--teal)' }}>SOURCES</code> dict at the top<br />
            2. Uncomment or add a new entry with <code style={{ color: 'var(--teal)' }}>name, url, enabled, icon, color</code><br />
            3. Create <code style={{ color: 'var(--teal)' }}>backend/sources/your_source.py</code> with a <code style={{ color: 'var(--teal)' }}>search_wine(session, name, vintage, lwin)</code> function<br />
            4. Import it in <code style={{ color: 'var(--teal)' }}>maaike_phase1.py</code> and add to the search chain<br />
            5. Restart the server — the new source appears here automatically
          </div>
        </div>

        <div style={{ marginTop: 14 }}>
          <div style={{ fontSize: 12, fontWeight: 700, color: 'var(--text2)', marginBottom: 8, textTransform: 'uppercase', letterSpacing: 1 }}>Reviews by Source</div>
          {srcStats.length === 0
            ? <div style={{ color: 'var(--text3)', fontSize: 12 }}>No reviews yet</div>
            : srcStats.map(s => (
              <div key={s.source} style={{ display: 'flex', justifyContent: 'space-between', padding: '5px 0', borderBottom: '1px solid var(--border)', fontSize: 12 }}>
                <span>{s.source}</span>
                <span style={{ color: 'var(--teal)', fontWeight: 700 }}>{s.count} reviews</span>
              </div>
            ))
          }
        </div>
      </div>
    </div>
  );
}
