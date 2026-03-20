import { useState, useEffect, useCallback } from 'react';
import { useApp } from '../context/AppContext.jsx';
import { cookiesApi, adminApi, statsApi } from '../services/api.js';
import { Panel, PanelHeader, PanelTitle, PanelBody, DropZone } from '../components/ui/index.jsx';

export default function Settings() {
  const { apiKey, addToast } = useApp();
  const [cookieStatus, setCookieStatus] = useState(null);
  const [sysInfo,      setSysInfo]      = useState(null);

  const loadCookieStatus = useCallback(async () => {
    try { setCookieStatus(await cookiesApi.status(apiKey)); } catch {}
  }, [apiKey]);

  const loadSysInfo = useCallback(async () => {
    try { setSysInfo(await statsApi.get(apiKey)); } catch {}
  }, [apiKey]);

  useEffect(() => { loadCookieStatus(); loadSysInfo(); }, [loadCookieStatus, loadSysInfo]);

  async function uploadCookies(file, source = 'jancisrobinson') {
    try {
      const d = await cookiesApi.upload(file, source, apiKey);
      if (d.ok) {
        addToast(`Cookies updated for ${source}!`, 'success');
        loadCookieStatus();
      } else {
        addToast(d.error || 'Upload failed', 'error');
      }
    } catch (e) { addToast('Error: ' + e, 'error'); }
  }

  async function resetNotFound() {
    if (!confirm('Reset all "not_found" wines back to "pending"?')) return;
    try {
      const d = await adminApi.resetNotFound(apiKey);
      addToast(`Reset ${d.reset} wines to pending`, 'success');
      loadSysInfo();
    } catch (e) { addToast('Error: ' + e, 'error'); }
  }

  async function resetFound() {
    if (!confirm('Re-fetch missing notes:\n\n1. Clears paywall/empty stubs from reviews\n2. Resets wines with no real note back to Pending\n3. You must then go to Dashboard → Run enrichment\n\nMake sure your cookies are fresh first. Proceed?')) return;
    try {
      const d = await adminApi.resetFound(apiKey);
      if (d.reset > 0) {
        addToast(`✓ ${d.reset} wines reset to Pending — now go to Dashboard and run enrichment to fetch the notes`, 'success');
      } else {
        addToast('No wines needed resetting — all enriched wines already have notes', 'info');
      }
      loadSysInfo();
    } catch (e) { addToast('Error: ' + e, 'error'); }
  }

  async function fixNotes() {
    try {
      const d = await adminApi.fixNotes(apiKey);
      addToast(`Fixed: ${d.notes_nulled} session notes + ${d.paywall_cleared ?? 0} paywall stubs cleared. ${d.reset_to_pending ?? 0} wines reset to pending.`, 'success');
    } catch (e) { addToast('Error: ' + e, 'error'); }
  }

  async function wipeAllWines() {
    if (!confirm('⚠️ DELETE ALL WINES AND REVIEWS?\n\nThis cannot be undone. Use this before re-importing a fresh CSV.')) return;
    if (!confirm('Are you absolutely sure? This deletes everything.')) return;
    try {
      const d = await adminApi.wipeWines(apiKey);
      addToast(`Deleted ${d.deleted} wines. Database is now empty.`, 'success');
      loadSysInfo();
    } catch (e) { addToast('Error: ' + e, 'error'); }
  }

  return (
    <div className="grid grid-cols-2 gap-3.5" style={{ maxWidth: 780 }}>

      {/* Cookie Status — one card per source */}
      {cookieStatus && Object.entries(cookieStatus).map(([source, ck]) => (
        <Panel key={source}>
          <PanelHeader>
            <span className="text-base">🍪</span>
            <PanelTitle>{source} — Cookie Status</PanelTitle>
          </PanelHeader>
          <PanelBody>
            <CookieStatusCard ck={ck} />
          </PanelBody>
        </Panel>
      ))}

      {/* Update Cookies — one upload per source */}
      {cookieStatus && Object.keys(cookieStatus).map(source => (
        <Panel key={source + '_upload'}>
          <PanelHeader>
            <span className="text-base">🔄</span>
            <PanelTitle>Update {source} Cookies</PanelTitle>
          </PanelHeader>
          <PanelBody>
            <p className="text-text2 text-xs mb-2.5">
              Upload a fresh cookies.json exported from your browser for <strong>{source}</strong>.
            </p>
            <DropZone
              onClick={() => document.getElementById(`ck-${source}`)?.click()}
              onDrop={e => { e.preventDefault(); uploadCookies(e.dataTransfer.files[0], source); }}
            >
              <input id={`ck-${source}`} type="file" accept=".json" className="hidden"
                onChange={e => uploadCookies(e.target.files[0], source)} />
              <div className="text-sm">📂 Click to upload cookies.json</div>
            </DropZone>
          </PanelBody>
        </Panel>
      ))}

      {/* System info */}
      <Panel>
        <PanelHeader><span className="text-base">ℹ️</span><PanelTitle>System Info</PanelTitle></PanelHeader>
        <PanelBody>
          {!sysInfo ? (
            <div className="text-text3 text-sm">Loading…</div>
          ) : (
            <div className="text-sm text-text2 leading-loose">
              <InfoRow label="Total wines"   value={sysInfo.total?.toLocaleString()} color="text-text1" />
              <InfoRow label="Total reviews" value={(sysInfo.total_reviews || 0).toLocaleString()} color="text-purple" />
              <InfoRow label="Coverage"      value={sysInfo.coverage + '%'}  color="text-teal" />
              <InfoRow label="Avg score"     value={sysInfo.avg_score ? sysInfo.avg_score.toFixed(2) + '/20' : '—'} />
              <InfoRow label="Enrichment"    value={sysInfo.enrichment_running ? 'Running' : 'Idle'}
                color={sysInfo.enrichment_running ? 'text-teal' : 'text-text2'} />
            </div>
          )}
        </PanelBody>
      </Panel>

      {/* Danger zone */}
      <Panel className="col-span-2">
        <PanelHeader><span className="text-base">⚠️</span><PanelTitle>Danger Zone</PanelTitle></PanelHeader>
        <PanelBody className="flex gap-6 flex-wrap">
          <div className="flex-1" style={{minWidth: 240}}>
            <p className="text-text2 text-xs mb-2">
              Reset all "not_found" wines back to pending for re-enrichment.
            </p>
            <button
              onClick={resetNotFound}
              className="bg-transparent border border-yellow text-yellow px-3.5 py-1.5 rounded text-sm cursor-pointer hover:bg-yellow/10 transition-colors"
            >
              Reset Not Found → Pending
            </button>
          </div>
          <div className="flex-1 border-l border-border pl-6" style={{minWidth: 240}}>
            <p className="text-text2 text-xs mb-2">
              <strong className="text-yellow">Re-fetch missing notes</strong> — resets wines that have a
              score but <em>no review text</em> (expired cookies during bulk scan) back to pending.
              Upload fresh cookies first, then start enrichment.
            </p>
            <button
              onClick={resetFound}
              className="bg-transparent border border-yellow text-yellow px-3.5 py-1.5 rounded text-sm cursor-pointer hover:bg-yellow/10 transition-colors"
            >
              🔄 Re-fetch Missing Notes
            </button>
          </div>
          <div className="flex-1 border-l border-border pl-6" style={{minWidth: 240}}>
            <p className="text-text2 text-xs mb-2">
              <strong className="text-yellow">Fix duplicate notes</strong> — strips session-level notes
              wrongly assigned to multiple wines from the same JR tasting session.
              Scores are kept. Safe to run anytime.
            </p>
            <button
              onClick={fixNotes}
              className="bg-transparent border border-yellow text-yellow px-3.5 py-1.5 rounded text-sm cursor-pointer hover:bg-yellow/10 transition-colors"
            >
              🔧 Fix Duplicate Session Notes
            </button>
          </div>
          <div className="flex-1 border-l border-border pl-6" style={{minWidth: 240}}>
            <p className="text-text2 text-xs mb-2">
              <strong className="text-red">Wipe entire database</strong> — delete all wines and reviews.
              Use before re-importing a fresh CSV to start clean.
            </p>
            <button
              onClick={wipeAllWines}
              className="bg-transparent border border-red text-red px-3.5 py-1.5 rounded text-sm cursor-pointer hover:bg-red/10 transition-colors"
            >
              🗑 Wipe All Wines &amp; Reviews
            </button>
          </div>
        </PanelBody>
      </Panel>

    </div>
  );
}

function CookieStatusCard({ ck }) {
  if (!ck) return <div className="text-text3 text-sm">Loading…</div>;
  const isOk = ck.ok && (ck.days_remaining ?? 99) > 5;
  return ck.ok ? (
    <>
      <div className={`flex items-center gap-2 text-sm px-3 py-2 rounded border mb-3
        ${isOk ? 'bg-green/10 border-green text-green' : 'bg-yellow/10 border-yellow text-yellow'}`}>
        {isOk ? '✓' : '⚠'} JWT valid — {ck.days_remaining} days remaining
      </div>
      <div className="text-xs text-text2 leading-loose">
        {ck.is_member !== undefined    && <InfoRow label="Member"        value={ck.is_member ? 'Yes' : 'No'} />}
        {ck.tasting_access !== undefined && <InfoRow label="Tasting notes" value={ck.tasting_access ? 'Yes' : 'No'} />}
        {ck.has_session !== undefined  && (
          <InfoRow label="Session cookie"
            value={ck.has_session ? '✓ Present' : '✗ Missing'}
            color={ck.has_session ? 'text-green' : 'text-red'} />
        )}
        {ck.user_id && <InfoRow label="User ID" value={ck.user_id} />}
        <InfoRow label="Cookies loaded" value={ck.cookie_count} />
      </div>
    </>
  ) : (
    <div className="bg-yellow/10 border border-yellow text-yellow text-sm px-3 py-2 rounded">
      ⚠ {ck.message}
    </div>
  );
}

function InfoRow({ label, value, color = '' }) {
  return (
    <div className="flex justify-between py-0.5 border-b border-border last:border-0">
      <span className="text-text3">{label}</span>
      <strong className={color || 'text-text1'}>{value ?? '—'}</strong>
    </div>
  );
}