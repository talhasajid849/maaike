import { useState, useEffect, useCallback } from 'react';
import { useApp } from '../context/AppContext.jsx';
import { ap, API } from '../api.js';

export default function Settings() {
  const { apiKey, addToast } = useApp();
  const [cookieStatus, setCookieStatus] = useState(null);
  const [sysInfo, setSysInfo] = useState(null);

  const loadCookieStatus = useCallback(async () => {
    try {
      const d = await ap('/cookies/status', {}, apiKey);
      setCookieStatus(d);
    } catch {}
  }, [apiKey]);

  const loadSysInfo = useCallback(async () => {
    try {
      const d = await ap('/stats', {}, apiKey);
      setSysInfo(d);
    } catch {}
  }, [apiKey]);

  useEffect(() => { loadCookieStatus(); loadSysInfo(); }, [loadCookieStatus, loadSysInfo]);

  async function uploadCookies(file) {
    const form = new FormData();
    form.append('file', file);
    try {
      const r = await fetch(`${API}/cookies`, { method: 'POST', headers: { 'X-API-Key': apiKey }, body: form });
      const d = await r.json();
      if (d.ok) { addToast(`Cookies updated (${d.cookies})!`, 'success'); loadCookieStatus(); }
      else addToast(d.error, 'error');
    } catch (e) { addToast('Error: ' + e, 'error'); }
  }

  async function resetNotFound() {
    if (!confirm('Reset all "not_found" to "pending"?')) return;
    try {
      const d = await ap('/admin/reset-not-found', { method: 'POST' }, apiKey);
      addToast(`Reset ${d.reset} wines to pending`, 'success');
      loadSysInfo();
    } catch (e) { addToast('Error: ' + e, 'error'); }
  }

  const ck = cookieStatus;
  const ckCls = ck?.ok && ck?.days_remaining > 5 ? 'ok' : 'warn';

  return (
    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 14, maxWidth: 780 }}>
      {/* Cookie Status */}
      <div className="panel">
        <div className="ph"><span className="pt">🍪 Cookie Status</span></div>
        <div className="pb">
          {!ck
            ? <div style={{ color: 'var(--text3)' }}>Loading…</div>
            : ck.ok ? (
              <>
                <div className={`cstatus ${ckCls}`}>{ckCls === 'ok' ? '✓' : '⚠'} JWT valid — {ck.days_remaining} days</div>
                <div style={{ marginTop: 10, fontSize: 12, lineHeight: 2, color: 'var(--text2)' }}>
                  Member: <strong>{ck.is_member ? 'Yes' : 'No'}</strong><br />
                  Tasting notes: <strong>{ck.tasting_access ? 'Yes' : 'No'}</strong><br />
                  Session cookie: <strong style={{ color: ck.has_session ? 'var(--green)' : 'var(--red)' }}>
                    {ck.has_session ? '✓ Present' : '✗ Missing'}
                  </strong><br />
                  Cookies loaded: <strong>{ck.cookie_count}</strong>
                </div>
              </>
            ) : (
              <div className="cstatus warn">⚠ {ck.message}</div>
            )
          }
        </div>
      </div>

      {/* Update Cookies */}
      <div className="panel">
        <div className="ph"><span className="pt">🔄 Update Cookies</span></div>
        <div className="pb">
          <p style={{ color: 'var(--text2)', fontSize: 12, marginBottom: 10 }}>
            Upload a fresh real_cookies.json exported from your browser.
          </p>
          <div className="dz" style={{ padding: 18 }} onClick={() => document.getElementById('ckf').click()}>
            <input type="file" id="ckf" accept=".json" style={{ display: 'none' }}
              onChange={e => uploadCookies(e.target.files[0])} />
            <div>📂 Click to upload real_cookies.json</div>
          </div>
        </div>
      </div>

      {/* System Info */}
      <div className="panel">
        <div className="ph"><span className="pt">ℹ️ System Info</span></div>
        <div className="pb">
          {!sysInfo
            ? <div style={{ color: 'var(--text3)' }}>Loading…</div>
            : (
              <div style={{ fontSize: 12, lineHeight: 2, color: 'var(--text2)' }}>
                Total wines: <strong style={{ color: 'var(--text)' }}>{sysInfo.total?.toLocaleString()}</strong><br />
                Total reviews: <strong style={{ color: 'var(--purple)' }}>{(sysInfo.total_reviews || 0).toLocaleString()}</strong><br />
                Coverage: <strong style={{ color: 'var(--teal)' }}>{sysInfo.coverage}%</strong><br />
                Avg score: <strong>{sysInfo.avg_score ? sysInfo.avg_score.toFixed(2) + '/20' : '—'}</strong><br />
                Enrichment: <strong style={{ color: sysInfo.enrichment_running ? 'var(--teal)' : 'var(--text2)' }}>
                  {sysInfo.enrichment_running ? 'Running' : 'Idle'}
                </strong>
              </div>
            )
          }
        </div>
      </div>

      {/* Danger Zone */}
      <div className="panel">
        <div className="ph"><span className="pt">⚠️ Danger Zone</span></div>
        <div className="pb" style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          <div>
            <p style={{ color: 'var(--text2)', fontSize: 12, marginBottom: 8 }}>
              Reset "not_found" wines back to pending for re-enrichment.
            </p>
            <button className="btn" style={{ border: '1px solid var(--yellow)', color: 'var(--yellow)', background: 'transparent' }}
              onClick={resetNotFound}>
              Reset Not Found → Pending
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
