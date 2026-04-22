/**
 * pages/UploadCSV.jsx
 * ====================
 * Three upload modes:
 *  1. Inventory CSV
 *  2. Review CSV
 *  3. XLSX enrich (supports multi-source parallel jobs with stop/resume)
 */

import { useState, useRef, useEffect, useCallback } from 'react';
import { useApp } from '../context/AppContext.jsx';
import { csvApi, xlsxApi, statsApi } from '../services/api.js';
import { Panel, PanelHeader, PanelTitle, PanelBody } from '../components/ui/index.jsx';

const TAB_INVENTORY = 'inventory';
const TAB_REVIEWS = 'reviews';
const TAB_XLSX = 'xlsx';

const POLL_MS = 2500;
const MAX_XLSX_CONCURRENT = 6;
const XLSX_JOBS_STORAGE_KEY = 'maaike_xlsx_jobs';
const DEFAULT_XLSX_SOURCES = [
  { key: 'robertparker', label: 'Robert Parker' },
  { key: 'jancisrobinson', label: 'Jancis Robinson' },
  { key: 'jamessuckling', label: 'James Suckling' },
];

function isXlsxFile(file) {
  if (!file?.name) return false;
  const n = file.name.toLowerCase();
  return n.endsWith('.xlsx') || n.endsWith('.xlsm');
}

export default function UploadCSV() {
  const { apiKey, addToast, uploadTab, setUploadTab } = useApp();

  const [tab, setTab] = useState(uploadTab || TAB_INVENTORY);
  const [isDragging, setIsDragging] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [result, setResult] = useState(null);

  const [xlsxSources, setXlsxSources] = useState(DEFAULT_XLSX_SOURCES);
  const [xlsxSource, setXlsxSource] = useState('robertparker');
  const [xlsxJobs, setXlsxJobs] = useState([]);
  const [xlsxStartItem, setXlsxStartItem] = useState('1');
  const [xlsxLwinFilter, setXlsxLwinFilter] = useState('');

  const inputRef = useRef(null);
  const sourceInputRefs = useRef({});
  const pollTimers = useRef({});
  const xlsxLogRefs = useRef({});

  const upsertXlsxJob = useCallback((jobId, patch) => {
    setXlsxJobs((prev) => {
      const idx = prev.findIndex((j) => j.jobId === jobId);
      if (idx === -1) return prev;
      const next = [...prev];
      next[idx] = { ...next[idx], ...patch };
      return next;
    });
  }, []);

  const removeXlsxJob = useCallback((jobId) => {
    setXlsxJobs((prev) => prev.filter((j) => j.jobId !== jobId));
    if (pollTimers.current[jobId]) {
      clearTimeout(pollTimers.current[jobId]);
      delete pollTimers.current[jobId];
    }
  }, []);

  const startPolling = useCallback((jobId) => {
    if (pollTimers.current[jobId]) clearTimeout(pollTimers.current[jobId]);
    pollTimers.current[jobId] = setTimeout(async () => {
      try {
        const d = await xlsxApi.status(jobId, apiKey);
        if (!d?.ok) return;
        upsertXlsxJob(jobId, d);
        if (d.status === 'running' || d.status === 'pending') {
          startPolling(jobId);
        } else {
          clearTimeout(pollTimers.current[jobId]);
          delete pollTimers.current[jobId];
        }
      } catch (e) {
        if (e?.status === 404) {
          removeXlsxJob(jobId);
          addToast('An old XLSX job was cleared because the backend restarted.', 'info');
          return;
        }
        startPolling(jobId);
      }
    }, POLL_MS);
  }, [addToast, apiKey, removeXlsxJob, upsertXlsxJob]);

  useEffect(() => () => {
    Object.values(pollTimers.current).forEach((t) => clearTimeout(t));
    pollTimers.current = {};
  }, []);

  useEffect(() => {
    if (uploadTab && uploadTab !== tab) setTab(uploadTab);
  }, [uploadTab, tab]);

  useEffect(() => {
    let cancelled = false;
    async function loadSources() {
      try {
        const d = await statsApi.sources(apiKey);
        const sourceMap = d?.sources || {};
        const options = Object.entries(sourceMap)
          .filter(([, cfg]) => cfg?.enabled)
          .map(([key, cfg]) => ({
            key,
            label: cfg?.name || cfg?.short || key,
          }));
        if (!cancelled && options.length > 0) {
          setXlsxSources(options);
          if (!options.some((s) => s.key === xlsxSource)) {
            setXlsxSource(options[0].key);
          }
        }
      } catch {
        if (!cancelled) setXlsxSources(DEFAULT_XLSX_SOURCES);
      }
    }
    loadSources();
    return () => { cancelled = true; };
  }, [apiKey]);

  const getSourceLabel = useCallback((sourceKey) => {
    return xlsxSources.find((s) => s.key === sourceKey)?.label || sourceKey || 'Unknown source';
  }, [xlsxSources]);

  const logDigest = xlsxJobs.map((j) => `${j.jobId}:${(j.log || []).length}`).join('|');
  useEffect(() => {
    xlsxJobs.forEach((job) => {
      const el = xlsxLogRefs.current[job.jobId];
      if (el) el.scrollTop = el.scrollHeight;
    });
  }, [logDigest, xlsxJobs]);

  useEffect(() => {
    const compact = xlsxJobs.map((j) => ({
      jobId: j.jobId,
      fileName: j.fileName || '',
      source: j.source || '',
    }));
    if (compact.length > 0) {
      localStorage.setItem(XLSX_JOBS_STORAGE_KEY, JSON.stringify(compact));
      sessionStorage.setItem(XLSX_JOBS_STORAGE_KEY, JSON.stringify(compact));
    } else {
      localStorage.removeItem(XLSX_JOBS_STORAGE_KEY);
      sessionStorage.removeItem(XLSX_JOBS_STORAGE_KEY);
    }
  }, [xlsxJobs]);

  useEffect(() => {
    let cancelled = false;
    async function restoreJobs() {
      const raw = localStorage.getItem(XLSX_JOBS_STORAGE_KEY)
        || sessionStorage.getItem(XLSX_JOBS_STORAGE_KEY);
      if (!raw) return;

      let saved = [];
      try {
        saved = JSON.parse(raw);
      } catch {
        return;
      }
      if (!Array.isArray(saved) || saved.length === 0) return;

      const restored = await Promise.all(saved.map(async (meta) => {
        const jobId = meta?.jobId;
        if (!jobId) return null;
        try {
          const d = await xlsxApi.status(jobId, apiKey);
          if (!d?.ok) return null;
          return {
            jobId,
            fileName: meta.fileName || 'template.xlsx',
            source: meta.source || d.source || 'jancisrobinson',
            ...d,
          };
        } catch {
          return null;
        }
      }));

      if (cancelled) return;

      const valid = restored.filter(Boolean);
      if (valid.length === 0) return;
      setXlsxJobs(valid);

      valid.forEach((job) => {
        if (job.status === 'running' || job.status === 'pending') {
          startPolling(job.jobId);
        }
      });
    }

    restoreJobs();
    return () => { cancelled = true; };
  }, [apiKey, startPolling]);

  useEffect(() => {
    function handleOffline() {
      addToast('Internet lost. XLSX jobs will auto-stop after repeated network errors.', 'info');
    }
    function handleOnline() {
      addToast('Internet restored. You can resume any stopped XLSX job.', 'success');
    }
    window.addEventListener('offline', handleOffline);
    window.addEventListener('online', handleOnline);
    return () => {
      window.removeEventListener('offline', handleOffline);
      window.removeEventListener('online', handleOnline);
    };
  }, [addToast]);

  function normalizedStartItem() {
    const n = Number.parseInt(String(xlsxStartItem || '1').trim(), 10);
    return Number.isFinite(n) && n > 0 ? n : 1;
  }

  async function startXlsxJob(file, sourceKey = xlsxSource) {
    const startItem = normalizedStartItem();
    const d = await xlsxApi.upload(file, apiKey, sourceKey, 2.5, startItem, xlsxLwinFilter);
    if (!d?.ok) {
      addToast(d?.error || `Upload failed for ${file.name} (${getSourceLabel(sourceKey)})`, 'error');
      return;
    }

    const selectedTotal = d.total || 0;
    const originalTotal = d.file_total || selectedTotal;
    const job = {
      jobId: d.job_id,
      fileName: file.name,
      source: sourceKey,
      status: 'pending',
      total: selectedTotal,
      done: Math.max(0, (d.start_item || startItem) - 1),
      found: 0,
      pct: selectedTotal > 0 ? Math.round((Math.max(0, (d.start_item || startItem) - 1) / selectedTotal) * 1000) / 10 : 0,
      start_item: d.start_item || startItem,
      ready: false,
      error: null,
      lwin_filter: d.lwin_filter || {},
      log: [],
    };

    setXlsxJobs((prev) => [job, ...prev.filter((x) => x.jobId !== job.jobId)]);
    if (d?.lwin_filter?.enabled) {
      const invalidCount = (d.lwin_filter.invalid_values || []).length;
      const unmatchedCount = (d.lwin_filter.unmatched_values || []).length;
      let msg = `Started ${selectedTotal} selected row(s) from ${originalTotal} total row(s).`;
      if (invalidCount) msg += ` Ignored ${invalidCount} invalid LWIN value(s).`;
      if (unmatchedCount) msg += ` ${unmatchedCount} requested LWIN(s) were not present in the file.`;
      addToast(msg, 'info');
    }
    startPolling(job.jobId);
  }

  async function handleXlsxFiles(files, sourceKey = xlsxSource) {
    const list = Array.from(files || []).filter(Boolean);
    if (!list.length) return;

    const bad = list.find((f) => !isXlsxFile(f));
    if (bad) {
      addToast(`Invalid file type: ${bad.name}`, 'error');
      return;
    }

    const running = xlsxJobs.filter((j) => j.status === 'running' || j.status === 'pending').length;
    const slots = Math.max(0, MAX_XLSX_CONCURRENT - running);
    if (slots <= 0) {
      addToast(`Only ${MAX_XLSX_CONCURRENT} XLSX jobs can run at once.`, 'error');
      return;
    }

    const toStart = list.slice(0, slots);
    if (list.length > slots) {
      addToast(`Started ${slots} file(s). Remaining skipped due to max parallel limit.`, 'info');
    }

    setUploading(true);
    try {
      await Promise.all(toStart.map((f) => startXlsxJob(f, sourceKey)));
    } catch (e) {
      addToast('Upload error: ' + e.message, 'error');
    } finally {
      setUploading(false);
    }
  }

  async function handleFile(payload) {
    if (!payload) return;

    if (tab === TAB_XLSX) {
      const files = Array.isArray(payload) ? payload : [payload];
      await handleXlsxFiles(files, xlsxSource);
      return;
    }

    const file = Array.isArray(payload) ? payload[0] : payload;
    if (!file) return;

    setUploading(true);
    setResult(null);
    try {
      const d = tab === TAB_REVIEWS
        ? await csvApi.uploadReviews(file, apiKey)
        : await csvApi.upload(file, apiKey);

      if (d?.ok) {
        setResult(d);
        addToast(`Imported ${d.inserted} new, updated ${d.updated || 0}`, 'success');
      } else {
        addToast(d?.error || 'Upload failed', 'error');
      }
    } catch (e) {
      addToast('Upload error: ' + e.message, 'error');
    } finally {
      setUploading(false);
    }
  }

  function handleDrop(e) {
    e.preventDefault();
    setIsDragging(false);
    if (tab === TAB_XLSX) handleFile(Array.from(e.dataTransfer.files || []));
    else handleFile(e.dataTransfer.files?.[0]);
  }

  function switchTab(id) {
    setTab(id);
    setUploadTab(id);
    setResult(null);
  }

  function handleXlsxDownload(job) {
    if (!job?.jobId || !job?.ready) return;
    const url = xlsxApi.downloadUrl(job.jobId, apiKey);
    const a = document.createElement('a');
    a.href = url;
    a.download = `maaike_${(job.fileName || 'template').replace(/\.xlsx?$/i, '')}_filled.xlsx`;
    a.click();
  }

  async function handleXlsxStop(jobId) {
    if (!jobId) return;
    try {
      const d = await xlsxApi.stop(jobId, apiKey);
      if (d?.ok) {
        upsertXlsxJob(jobId, d);
        startPolling(jobId);
        addToast('Stop requested. Preparing partial XLSX...', 'info');
      } else {
        addToast(d?.error || 'Stop failed', 'error');
      }
    } catch (e) {
      addToast('Stop error: ' + e.message, 'error');
    }
  }

  async function handleXlsxResume(jobId) {
    if (!jobId) return;
    try {
      const d = await xlsxApi.resume(jobId, apiKey);
      if (d?.ok) {
        upsertXlsxJob(jobId, d);
        startPolling(jobId);
        addToast('Resumed from last processed row.', 'success');
      } else {
        addToast(d?.error || 'Resume failed', 'error');
      }
    } catch (e) {
      addToast('Resume error: ' + e.message, 'error');
    }
  }

  const acceptAttr = tab === TAB_XLSX ? '.xlsx,.xlsm' : '.csv';
  const inputCls = 'bg-[#0d0d0d] border border-dashed rounded-lg p-10 text-center cursor-pointer transition-colors ' +
    (isDragging ? 'border-teal-500 bg-teal-500/5' : 'border-[#2a2a2a] hover:border-[#444]');

  return (
    <div className="max-w-full p-4">
      <Panel>
        <PanelHeader>
          <span className="text-base">Upload</span>
          <PanelTitle>Upload</PanelTitle>
        </PanelHeader>

        <div className="flex border-b border-[#1e1e1e]">
          {[
            { id: TAB_INVENTORY, label: 'Inventory CSV' },
            { id: TAB_REVIEWS, label: 'Review CSV' },
            { id: TAB_XLSX, label: 'XLSX Enrich' },
          ].map((t) => (
            <button
              key={t.id}
              onClick={() => switchTab(t.id)}
              className={`px-5 py-2.5 text-xs font-medium transition-colors border-b-2 -mb-px ${
                tab === t.id
                  ? 'border-teal-500 text-teal-300'
                  : 'border-transparent text-[#666] hover:text-[#999]'
              }`}
            >
              {t.label}
            </button>
          ))}
        </div>

        <PanelBody>
          {tab === TAB_INVENTORY && (
            <p className="text-[#777] text-xs mb-4 leading-relaxed">
              Upload a Rue Pinard inventory CSV. Columns auto-detected. LWIN is read automatically.
              Wines are deduplicated by LWIN11 and pack-size variants are grouped into one row
              (prefers 1x75cl). Price and stock are updated if the wine already exists.
              <br /><br />
              Expected columns: <code className="text-teal-500">name, lwin, vintage, unit-size, price, url, stock-level</code>
            </p>
          )}

          {tab === TAB_REVIEWS && (
            <p className="text-[#777] text-xs mb-4 leading-relaxed">
              Upload a review CSV in the standard MAAIKE format. Each row is one wine+review.
              Wines are matched by full LWIN. Existing wines are updated and missing wines are created.
              <br /><br />
              Required columns: <code className="text-teal-500">Publisher, LWIN, Product_Name, Vintage, Critic_Name, Score_20, Score_100, Drink_From, Drink_To, Review_Date, Review</code>
            </p>
          )}

          {tab === TAB_XLSX && (
            <div className="mb-4">
              <p className="text-[#777] text-xs mb-3 leading-relaxed">
                Upload separate XLSX files for each source and run them in parallel. Each job can be stopped and resumed from the last processed row.
                If internet drops, the backend auto-stops after repeated network errors so you can resume later.
                Optional source-specific hint columns such as <code className="text-teal-500">url on the js</code>,
                <code className="text-teal-500"> url on the rp</code>, or <code className="text-teal-500"> url on the jr</code> are also supported.
              </p>
              <div className="mb-3">
                <label className="block text-[11px] text-[#666] mb-1.5 uppercase tracking-wider">Default source (main drop area)</label>
                <select
                  value={xlsxSource}
                  onChange={(e) => setXlsxSource(e.target.value)}
                  className="w-full bg-[#0a0a0a] border border-[#2a2a2a] rounded px-2.5 py-2 text-xs text-[#ddd]"
                >
                  {xlsxSources.map((s) => (
                    <option key={s.key} value={s.key}>{s.label}</option>
                  ))}
                </select>
              </div>
              <div className="mb-3">
                <label className="block text-[11px] text-[#666] mb-1.5 uppercase tracking-wider">Start from item #</label>
                <input
                  type="number"
                  min="1"
                  step="1"
                  value={xlsxStartItem}
                  onChange={(e) => setXlsxStartItem(e.target.value)}
                  className="w-full bg-[#0a0a0a] border border-[#2a2a2a] rounded px-2.5 py-2 text-xs text-[#ddd]"
                  placeholder="1"
                />
                <div className="mt-1 text-[11px] text-[#555]">
                  Leave as <code className="text-teal-500">1</code> to start from the first wine.
                  If a job stopped at <code className="text-teal-500">3598/5301</code>, enter <code className="text-teal-500">3599</code>.
                </div>
              </div>
              <div className="mb-3">
                <label className="block text-[11px] text-[#666] mb-1.5 uppercase tracking-wider">Only these LWINs (optional)</label>
                <textarea
                  value={xlsxLwinFilter}
                  onChange={(e) => setXlsxLwinFilter(e.target.value)}
                  className="w-full min-h-[84px] bg-[#0a0a0a] border border-[#2a2a2a] rounded px-2.5 py-2 text-xs text-[#ddd]"
                  placeholder="10012342020, 10045672019, 1234567"
                />
                <div className="mt-1 text-[11px] text-[#555]">
                  Paste comma-separated LWIN11 or LWIN7 values to process only those wines from the full workbook.
                  Leave blank to run every row in the file.
                </div>
              </div>
              {xlsxSources.length > 1 && (
                <div className="mb-3 grid grid-cols-1 sm:grid-cols-2 gap-2">
                  {xlsxSources.map((s) => (
                    <div key={`source-upload-${s.key}`} className="bg-[#0a0a0a] border border-[#1e1e1e] rounded-lg p-2.5">
                      <div className="text-[11px] text-[#aaa] mb-2">{s.label}</div>
                      <input
                        ref={(el) => { sourceInputRefs.current[s.key] = el; }}
                        type="file"
                        accept=".xlsx,.xlsm"
                        multiple
                        className="hidden"
                        onChange={(e) => {
                          const files = Array.from(e.target.files || []);
                          handleXlsxFiles(files, s.key);
                          e.target.value = '';
                        }}
                      />
                      <button
                        onClick={() => sourceInputRefs.current[s.key]?.click()}
                        className="w-full py-1.5 rounded border border-[#2a2a2a] text-xs text-[#ddd] hover:border-[#3a3a3a]"
                      >
                        Upload file for {s.label}
                      </button>
                    </div>
                  ))}
                </div>
              )}
              {xlsxJobs.length === 0 && (
                <div className="bg-[#0a0a0a] border border-[#1e1e1e] rounded-lg p-3.5">
                  <div className="text-[#444] text-[10px] font-semibold mb-2.5 tracking-widest uppercase">Expected columns</div>
                  <div className="grid text-[11px]" style={{ gridTemplateColumns: 'auto 1fr', gap: '3px 14px' }}>
                    {[
                      ['A', 'Publisher', getSourceLabel(xlsxSource)],
                      ['B', 'LWIN11', '10098312020 (optional if name set)'],
                      ['C', 'Product_Name', 'Chateau X, Appellation, Cru 2020'],
                      ['D', 'Vintage', '2020 or NV'],
                      ['E', 'Critic_Name', 'filled by MAAIKE'],
                      ['F', 'Score', 'filled by MAAIKE'],
                      ['G', 'Drink_From', 'filled by MAAIKE'],
                      ['H', 'Drink_To', 'filled by MAAIKE'],
                      ['I', 'Review_Date', 'filled by MAAIKE'],
                      ['J', 'Review', 'filled by MAAIKE'],
                      ['K', 'Source_URL', 'filled by MAAIKE'],
                    ].map(([col, name, desc]) => (
                      <div key={col} className="contents">
                        <div className="text-blue-500 font-mono font-bold">{col}</div>
                        <div>
                          <span className="text-[#ccc] font-semibold">{name}</span>
                          <span className="text-[#555] ml-2">{desc}</span>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          )}

          <div
            className={inputCls}
            onClick={() => inputRef.current?.click()}
            onDrop={handleDrop}
            onDragOver={(e) => { e.preventDefault(); setIsDragging(true); }}
            onDragLeave={() => setIsDragging(false)}
          >
            <input
              ref={inputRef}
              type="file"
              accept={acceptAttr}
              multiple={tab === TAB_XLSX}
              className="hidden"
              onChange={(e) => {
                const files = Array.from(e.target.files || []);
                if (tab === TAB_XLSX) handleFile(files);
                else handleFile(files[0]);
                e.target.value = '';
              }}
            />
            <div className="text-4xl mb-3">{isDragging ? 'Drop' : tab === TAB_XLSX ? 'XLSX' : 'CSV'}</div>
            <div className="text-sm text-[#aaa]">
              {uploading
                ? <span className="text-teal-400">Uploading...</span>
                : tab === TAB_XLSX
                  ? <span>Click or drag .xlsx files here for <strong className="text-white">{getSourceLabel(xlsxSource)}</strong></span>
                  : <span>Click or drag a <strong className="text-white">.csv</strong> file here</span>}
            </div>
            <div className="text-xs text-[#555] mt-1.5">Max 20MB</div>
          </div>

          {result && (
            <div className="mt-4 p-3.5 rounded-lg bg-[#0d1a0d] border border-green-900/40">
              <div className="text-green-400 font-bold text-sm mb-2.5">Upload successful</div>
              <div className="grid grid-cols-2 gap-x-6 gap-y-1 text-xs">
                <div className="text-[#666]">New wines inserted:</div>
                <div className="text-white font-semibold">{result.inserted}</div>
                <div className="text-[#666]">Existing wines updated:</div>
                <div className="text-white font-semibold">{result.updated ?? 0}</div>
                {result.total_wines != null && <>
                  <div className="text-[#666]">Total unique wines:</div>
                  <div className="text-white font-semibold">{result.total_wines}</div>
                </>}
                {result.pack_size_dupes > 0 && <>
                  <div className="text-[#666]">Pack-size variants skipped:</div>
                  <div className="text-[#aaa] font-semibold">{result.pack_size_dupes}</div>
                </>}
                {result.errors > 0 && <>
                  <div className="text-[#666]">Parse errors:</div>
                  <div className="text-red-400 font-semibold">{result.errors}</div>
                </>}
                {result.upload_batch && <>
                  <div className="text-[#666]">Upload batch:</div>
                  <div className="text-[#888] font-mono text-2xs">{result.upload_batch}</div>
                </>}
              </div>
            </div>
          )}

          {tab === TAB_XLSX && xlsxJobs.map((job) => {
            const isRunning = job.status === 'running' || job.status === 'pending';
            const isDone = job.status === 'done';
            const isStopped = job.status === 'stopped';
            const isError = job.status === 'error';
            const resumable = (isStopped || isError) && (job.done || 0) < (job.total || 0);

            return (
              <div key={job.jobId} className="mt-4 bg-[#111] border border-[#1e1e1e] rounded-lg p-4">
                <div className="mb-3">
                  <div className="flex justify-between items-center mb-1.5 text-xs">
                    <span className="text-[#888]">
                      {isRunning && 'Running '}
                      {isDone && 'Done '}
                      {isStopped && 'Stopped '}
                      {isError && 'Error '}
                      {isRunning
                        ? `Searching ${getSourceLabel(job.source || xlsxSource)}...`
                        : isDone
                          ? 'Complete'
                          : isStopped
                            ? (job.auto_stopped ? 'Auto-stopped (network)' : 'Stopped')
                            : 'Error'}
                      {job.fileName && <span className="text-[#555] ml-2">{job.fileName}</span>}
                      {(job.start_item || 1) > 1 && (
                        <span className="text-[#555] ml-2">start #{job.start_item}</span>
                      )}
                    </span>
                    <span className="text-[#aaa] tabular-nums">
                      {job.done || 0}/{job.total || 0}
                      {' '}|{' '}
                      <span className="text-teal-400">{job.found || 0} found</span>
                      {(job.total || 0) > 0 && (
                        <span className="text-[#555]"> ({Math.round(((job.found || 0) / (job.total || 1)) * 100)}%)</span>
                      )}
                    </span>
                  </div>
                  <div className="h-1.5 bg-[#1a1a1a] rounded-full overflow-hidden">
                    <div
                      className="h-full rounded-full transition-all duration-300"
                      style={{
                        width: `${job.pct || 0}%`,
                        background: isError
                          ? '#7f1d1d'
                          : isDone
                            ? 'linear-gradient(90deg,#0d9488,#2dd4bf)'
                            : 'linear-gradient(90deg,#0d9488,#5eead4)',
                      }}
                    />
                  </div>
                </div>

                {isError && job.error && (
                  <div className="mb-3 p-2.5 bg-[#2b0d0d] border border-red-900/50 rounded text-red-300 text-xs">
                    {job.error}
                  </div>
                )}

                {isStopped && job.auto_stopped && (
                  <div className="mb-3 p-2.5 bg-[#1d2d3a] border border-[#2d4b63] rounded text-[#9ed4ff] text-xs">
                    Auto-stopped after network errors. Use Resume when connection is stable.
                  </div>
                )}

                {isRunning && (
                  <button
                    onClick={() => handleXlsxStop(job.jobId)}
                    className="w-full py-2 rounded-lg text-sm font-semibold text-white mb-2 transition-opacity hover:opacity-90"
                    style={{ background: 'linear-gradient(135deg,#b91c1c,#ef4444)' }}
                  >
                    Stop and prepare partial XLSX
                  </button>
                )}

                {resumable && (
                  <button
                    onClick={() => handleXlsxResume(job.jobId)}
                    className="w-full py-2 rounded-lg text-sm font-semibold text-white mb-2 transition-opacity hover:opacity-90"
                    style={{ background: 'linear-gradient(135deg,#0ea5e9,#0284c7)' }}
                  >
                    Resume from where stopped
                  </button>
                )}

                {(isDone || isStopped) && job.ready && (
                  <button
                    onClick={() => handleXlsxDownload(job)}
                    className="w-full py-2 rounded-lg text-sm font-semibold text-white mb-3 transition-opacity hover:opacity-90"
                    style={{ background: 'linear-gradient(135deg,#0d9488,#0891b2)' }}
                  >
                    Download {isStopped ? 'partial' : 'filled'} XLSX
                  </button>
                )}

                {job.log && job.log.length > 0 && (
                  <div
                    ref={(el) => { xlsxLogRefs.current[job.jobId] = el; }}
                    className="bg-[#0a0a0a] border border-[#1a1a1a] rounded p-2.5 font-mono text-[11px] leading-relaxed max-h-56 overflow-y-auto"
                  >
                    {job.log.map((line, i) => (
                      <div
                        key={`${job.jobId}-${i}`}
                        className={
                          line.includes('Done -') || line.includes('Click Download') ? 'text-teal-400'
                          : line.includes('+ score') ? 'text-green-400'
                          : line.includes('x error') || line.includes('ERROR') ? 'text-red-400'
                          : line.includes('not found') ? 'text-[#555]'
                          : 'text-[#777]'
                        }
                      >
                        {line}
                      </div>
                    ))}
                    {isRunning && <div className="text-[#333]">...</div>}
                  </div>
                )}
              </div>
            );
          })}
        </PanelBody>
      </Panel>
    </div>
  );
}
