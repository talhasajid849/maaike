import { useState, useEffect, useRef, useCallback } from 'react';
import { useApp } from '../context/AppContext.jsx';
import { ap } from '../api.js';
import { getSocket } from '../socket.js';

export default function Dashboard() {
  const { apiKey, addToast } = useApp();
  const [stats, setStats] = useState(null);
  const [enrichRunning, setEnrichRunning] = useState(false);
  const [progress, setProgress] = useState({ pct: 0, label: '—' });
  const [logs, setLogs] = useState([]);
  const [limit, setLimit] = useState(50);
  const [sleep, setSleep] = useState(1.2);
  const [scope, setScope] = useState('pending');
  const [collapsed, setCollapsed] = useState(false);
  const logRef = useRef(null);

  const loadStats = useCallback(async () => {
    try {
      const d = await ap('/stats', {}, apiKey);
      setStats(d);
      if (d.enrichment_running) setEnrichRunning(true);
    } catch {}
  }, [apiKey]);

  useEffect(() => {
    loadStats();
    const sock = getSocket();
    sock.on('enrich_log', d => {
      setLogs(prev => [...prev, { msg: d.msg, level: d.level }]);
    });
    sock.on('enrich_progress', d => {
      setProgress({
        pct: d.pct,
        label: `${d.done}/${d.total} · ✓${d.found} found · ✗${d.errors} err`,
      });
      if (!d.running && d.done > 0) {
        setEnrichRunning(false);
        loadStats();
        addToast(`Done: ${d.found}/${d.total} found (${d.pct}%)`, 'success');
      }
    });
    sock.on('wine_enriched', d => {
      if (d.found) addToast(`Wine ID ${d.wine_id}: ${d.count} review(s) found (${d.score}/20)`, 'success');
      else if (d.error) addToast(`Error enriching wine: ${d.error}`, 'error');
      else addToast(`Wine ID ${d.wine_id}: not found on JancisRobinson`, 'info');
      loadStats();
    });
    return () => {
      sock.off('enrich_log');
      sock.off('enrich_progress');
      sock.off('wine_enriched');
    };
  }, [loadStats, addToast]);

  useEffect(() => {
    if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight;
  }, [logs]);

  async function startEnrich() {
    try {
      await ap('/enrich/start', {
        method: 'POST',
        body: JSON.stringify({ limit: parseInt(limit) || 0, sleep: parseFloat(sleep), only_pending: scope === 'pending' }),
      }, apiKey);
      setEnrichRunning(true);
      addToast('Enrichment started!', 'success');
    } catch (e) { addToast('Failed: ' + e.message, 'error'); }
  }

  async function stopEnrich() {
    try {
      await ap('/enrich/stop', { method: 'POST' }, apiKey);
      addToast('Stop signal sent.', 'info');
    } catch (e) { addToast('Error: ' + e.message, 'error'); }
  }

  const s = stats;

  return (
    <>
      {/* Stats grid */}
      <div className="stats-grid">
        <StatCard label="Total Wines" val={s?.total?.toLocaleString()} />
        <StatCard label="Reviews Found" val={s?.found?.toLocaleString()} color="var(--green)" />
        <StatCard label="Pending" val={s?.pending?.toLocaleString()} color="var(--yellow)" />
        <StatCard label="Not Found" val={s?.not_found?.toLocaleString()} color="var(--text2)" />
        <StatCard label="Coverage" val={s ? s.coverage + '%' : '—'} color="var(--teal)" sub="of total wines" />
        <StatCard label="Avg Score /20" val={s?.avg_score ? s.avg_score.toFixed(2) : '—'} color="var(--blue)" />
        <StatCard label="Total Reviews" val={s?.total_reviews?.toLocaleString()} color="var(--purple)" sub="across all sources" />
        <StatCard label="With LWIN" val={s?.with_lwin?.toLocaleString()} color="var(--orange)" />
      </div>

      {/* Enrichment panel */}
      <div className="panel">
        <div className="ph">
          <span style={{ fontSize: 15 }}>⚡</span>
          <span className="pt">Jancis Enrichment</span>
          {enrichRunning && (
            <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginLeft: 8 }}>
              <div className="dot pulse" style={{ background: 'var(--teal)', display: 'inline-block' }} />
              <span style={{ fontSize: 12, color: 'var(--teal)' }}>Running…</span>
            </div>
          )}
          <button className="ptoggle" onClick={() => setCollapsed(c => !c)}>
            {collapsed ? '▸' : '▾'}
          </button>
        </div>
        <div className="pcollapse" style={{ maxHeight: collapsed ? 0 : undefined }}>
          <div className="pb">
            <div className="enrich-row">
              <div className="fg">
                <label>Limit (0=all)</label>
                <input type="number" value={limit} min="0" style={{ width: 90 }} onChange={e => setLimit(e.target.value)} />
              </div>
              <div className="fg" style={{ flex: 1, maxWidth: 200 }}>
                <label>Sleep between requests</label>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <input type="range" min="0.3" max="5" step="0.1" value={sleep} style={{ flex: 1 }}
                    onChange={e => setSleep(parseFloat(e.target.value))} />
                  <span className="rv">{sleep.toFixed(1)}s</span>
                </div>
              </div>
              <div className="fg">
                <label>Scope</label>
                <select value={scope} onChange={e => setScope(e.target.value)} style={{ width: 160 }}>
                  <option value="pending">Pending only</option>
                  <option value="all">Pending + Not found</option>
                </select>
              </div>
              <div className="fg" style={{ justifyContent: 'flex-end' }}>
                <label>&nbsp;</label>
                <div style={{ display: 'flex', gap: 7 }}>
                  {!enrichRunning
                    ? <button className="btn btn-teal" onClick={startEnrich}>▶ Run Enrichment</button>
                    : <button className="btn btn-red" onClick={stopEnrich}>⏹ Stop</button>
                  }
                </div>
              </div>
            </div>
            <div className="pbar-wrap"><div className="pbar" style={{ width: progress.pct + '%' }} /></div>
            <div className="pbar-label"><span>{progress.pct}%</span><span>{progress.label}</span></div>
            <div className="logbox" ref={logRef}>
              {logs.map((l, i) => (
                <div key={i} className={`ll ${l.level}`}>{l.msg}</div>
              ))}
            </div>
            <div style={{ display: 'flex', justifyContent: 'flex-end', marginTop: 7 }}>
              <button className="btn btn-outline" onClick={() => setLogs([])}>Clear Log</button>
            </div>
          </div>
        </div>
      </div>

      {/* Bottom panels */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 14 }}>
        <div className="panel">
          <div className="ph"><span className="pt">🏆 Top Reviewers</span></div>
          <div className="pb">
            {s?.reviewers?.length
              ? s.reviewers.map(r => (
                <div key={r.name} style={{ display: 'flex', justifyContent: 'space-between', padding: '5px 0', borderBottom: '1px solid var(--border)' }}>
                  <span>{r.name}</span>
                  <span style={{ color: 'var(--teal)', fontWeight: 700 }}>{r.count}</span>
                </div>
              ))
              : <div style={{ color: 'var(--text3)', fontSize: 12 }}>No data yet</div>
            }
          </div>
        </div>
        <div className="panel">
          <div className="ph"><span className="pt">📊 Score Distribution</span></div>
          <div className="pb">
            {s?.score_dist?.length
              ? (() => {
                  const mx = Math.max(...s.score_dist.map(x => x.count));
                  return s.score_dist.map(x => {
                    const p = mx ? Math.round(x.count / mx * 100) : 0;
                    return (
                      <div key={x.band} style={{ marginBottom: 7 }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 2 }}>
                          <span style={{ fontSize: 12 }}>{x.band}</span>
                          <span style={{ fontSize: 12, color: 'var(--text2)' }}>{x.count}</span>
                        </div>
                        <div style={{ background: 'var(--bg4)', borderRadius: 3, height: 4 }}>
                          <div style={{ width: p + '%', height: '100%', background: 'var(--teal)', borderRadius: 3 }} />
                        </div>
                      </div>
                    );
                  });
                })()
              : <div style={{ color: 'var(--text3)', fontSize: 12 }}>No scored wines yet</div>
            }
          </div>
        </div>
      </div>
    </>
  );
}

function StatCard({ label, val, color, sub }) {
  return (
    <div className="sc2">
      <div className="sc2-label">{label}</div>
      <div className="sc2-val" style={color ? { color } : {}}>{val ?? '—'}</div>
      {sub && <div className="sc2-sub">{sub}</div>}
    </div>
  );
}
