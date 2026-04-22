import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useApp } from '../context/AppContext.jsx';
import { statsApi, xlsxApi } from '../services/api.js';
import { sourceLabel } from '../config/sources.js';
import {
  Button,
  DropZone,
  EmptyState,
  FormField,
  Panel,
  PanelBody,
  PanelHeader,
  PanelTitle,
  SourceBadge,
  Spinner,
  inputCls,
  selectCls,
} from '../components/ui/index.jsx';

const POLL_MS = 3000;

function fmtBytes(value) {
  const n = Number(value || 0);
  if (!Number.isFinite(n) || n <= 0) return '0 B';
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MB`;
}

function fmtPct(done, total) {
  if (!total) return '0%';
  return `${((Number(done || 0) / Number(total || 1)) * 100).toFixed(1)}%`;
}

function fileStatusTone(status) {
  if (status === 'done') return 'text-green';
  if (status === 'running' || status === 'pending') return 'text-teal';
  if (status === 'error') return 'text-red';
  if (status === 'stopped') return 'text-yellow';
  return 'text-text2';
}

function ProgressButton({
  children,
  pct = 0,
  active = false,
  disabled = false,
  variant = 'outline',
  className = '',
  onClick,
}) {
  const variants = {
    teal: 'bg-teal text-bg hover:bg-teal2 font-semibold',
    red: 'bg-red text-white hover:bg-red/80 font-semibold',
    outline: 'bg-transparent border border-border2 text-text2 hover:border-teal hover:text-teal',
    ghost: 'bg-transparent text-text3 hover:text-text1 p-1',
    blue: 'bg-blue text-white hover:bg-blue/80 font-semibold',
    yellow: 'bg-yellow text-bg hover:bg-yellow/80 font-semibold',
  };
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      className={`relative inline-flex items-center justify-center gap-1.5 px-3.5 py-1.5 rounded text-sm cursor-pointer transition-all duration-150 border-none overflow-hidden disabled:opacity-55 disabled:cursor-not-allowed ${variants[variant] || variants.outline} ${className}`}
    >
      {active && (
        <span
          className="absolute inset-y-0 left-0 bg-white/25 transition-all duration-300"
          style={{ width: `${Math.max(8, Math.min(100, pct))}%` }}
        />
      )}
      {active && pct < 100 && (
        <span className="absolute inset-0 bg-gradient-to-r from-transparent via-white/20 to-transparent animate-pulse" />
      )}
      {active && <span className="absolute inset-0 bg-white/10 animate-pulse" />}
      <span className="relative z-10">{children}</span>
    </button>
  );
}

export default function XlsxFiles() {
  const { apiKey, addToast } = useApp();
  const [files, setFiles] = useState([]);
  const [detail, setDetail] = useState(null);
  const [selectedFileId, setSelectedFileId] = useState('');
  const [loading, setLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [downloadingKind, setDownloadingKind] = useState('');
  const [downloadPhase, setDownloadPhase] = useState('');
  const [downloadPct, setDownloadPct] = useState(0);
  const [downloadElapsed, setDownloadElapsed] = useState(0);
  const [stopPreparing, setStopPreparing] = useState(false);
  const [stopPct, setStopPct] = useState(0);
  const [busy, setBusy] = useState(false);
  const [isDragging, setIsDragging] = useState(false);
  const [sourceOptions, setSourceOptions] = useState([]);
  const [uploadSource, setUploadSource] = useState('robertparker');
  const [restartSource, setRestartSource] = useState('robertparker');
  const [startItem, setStartItem] = useState('1');
  const [uploadLwinFilter, setUploadLwinFilter] = useState('');
  const [restartLwinFilter, setRestartLwinFilter] = useState('');
  const [pollTick, setPollTick] = useState(0);
  const inputRef = useRef(null);

  const hasActiveFiles = useMemo(
    () => files.some((f) => f.status === 'running' || f.status === 'pending'),
    [files]
  );

  const loadSources = useCallback(async () => {
    try {
      const d = await statsApi.sources(apiKey);
      const options = Object.entries(d?.sources || {})
        .filter(([, cfg]) => cfg?.enabled)
        .map(([key, cfg]) => ({ key, label: cfg?.name || sourceLabel(key) }));
      if (options.length) {
        setSourceOptions(options);
        setUploadSource((current) => options.some((s) => s.key === current) ? current : options[0].key);
        setRestartSource((current) => options.some((s) => s.key === current) ? current : options[0].key);
      }
    } catch {}
  }, [apiKey]);

  const loadFiles = useCallback(async (preferredId = '') => {
    const d = await xlsxApi.files(apiKey);
    const nextFiles = d?.files || [];
    setFiles(nextFiles);
    return nextFiles;
  }, [apiKey]);

  const loadDetail = useCallback(async (fileId, opts = {}) => {
    if (!fileId) {
      setDetail(null);
      return null;
    }
    setDetailLoading(opts.preview !== true);
    try {
      const d = await xlsxApi.file(fileId, apiKey, { preview: opts.preview === true });
      setDetail((current) => {
        if (opts.preview === true) return d;
        if (current?.file_id === d?.file_id && current?.preview_rows?.length) {
          return { ...d, preview_rows: current.preview_rows, preview_count: current.preview_count, preview_deferred: false };
        }
        return d;
      });
      setRestartSource(d?.source || 'robertparker');
      return d;
    } finally {
      setDetailLoading(false);
    }
  }, [apiKey]);

  useEffect(() => {
    let cancelled = false;
    async function boot() {
      setLoading(true);
      try {
        const [, nextFiles] = await Promise.all([loadSources(), loadFiles()]);
        const initialId = nextFiles[0]?.file_id || '';
        if (!cancelled && initialId) {
          setSelectedFileId(initialId);
          setLoading(false);
        }
      } catch (e) {
        if (!cancelled) addToast(e.message || 'Failed to load XLSX files', 'error');
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    boot();
    return () => { cancelled = true; };
  }, [addToast, loadDetail, loadFiles, loadSources]);

  useEffect(() => {
    if (!selectedFileId) return;
    loadDetail(selectedFileId, { preview: false }).catch(() => {});
  }, [selectedFileId, pollTick, loadDetail]);

  useEffect(() => {
    if (!hasActiveFiles) return;
    const timer = setTimeout(async () => {
      try {
        await loadFiles();
        setPollTick((x) => x + 1);
      } catch {}
    }, POLL_MS);
    return () => clearTimeout(timer);
  }, [hasActiveFiles, loadFiles, pollTick]);

  async function refreshAll(preferredId = '') {
    const nextFiles = await loadFiles(preferredId);
    const targetId = preferredId || selectedFileId || nextFiles[0]?.file_id || '';
    if (targetId) await loadDetail(targetId, { preview: false });
    else setDetail(null);
  }

  async function loadPreviewRows() {
    if (!detail?.file_id) return;
    setPreviewLoading(true);
    try {
      await loadDetail(detail.file_id, { preview: true });
    } catch (e) {
      addToast(e.message || 'Failed to load preview rows', 'error');
    } finally {
      setPreviewLoading(false);
    }
  }

  function filenameFromDisposition(header, fallback) {
    const text = String(header || '');
    const utf = text.match(/filename\*=UTF-8''([^;]+)/i);
    if (utf?.[1]) return decodeURIComponent(utf[1].replace(/"/g, ''));
    const plain = text.match(/filename="?([^"]+)"?/i);
    return plain?.[1] || fallback;
  }

  async function downloadFile(kind) {
    if (!detail?.file_id) return;
    setDownloadingKind(kind);
    setDownloadPhase(kind === 'progress' ? 'Preparing XLSX from current progress...' : 'Preparing download...');
    setDownloadPct(8);
    setDownloadElapsed(0);
    const startedAt = Date.now();
    const timer = window.setInterval(() => {
      const elapsed = Date.now() - startedAt;
      setDownloadElapsed(Math.floor(elapsed / 1000));
      setDownloadPct((current) => {
        if (current >= 98) return current;
        if (elapsed < 7000) {
          return Math.max(current, Math.min(88, 8 + Math.floor(elapsed / 350) * 4));
        }
        const slowNext = 88 + Math.min(10, Math.floor((elapsed - 7000) / 1200));
        return Math.max(current, Math.min(98, slowNext));
      });
    }, 350);
    try {
      const res = await fetch(xlsxApi.fileDownloadUrl(detail.file_id, apiKey, kind), {
        headers: { 'X-API-Key': apiKey },
      });
      if (!res.ok) {
        let message = `Download failed (${res.status})`;
        try {
          const payload = await res.json();
          message = payload?.error || payload?.message || message;
        } catch {}
        throw new Error(message);
      }
      setDownloadPhase('Downloading file...');
      setDownloadPct(92);
      const blob = await res.blob();
      const fallbackName = kind === 'original' ? detail.original_name : `maaike_${detail.original_name || 'reviews'}`;
      const filename = filenameFromDisposition(res.headers.get('content-disposition'), fallbackName);
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
      setDownloadPct(100);
      addToast('Download started', 'success');
    } catch (e) {
      addToast(e.message || 'Download failed', 'error');
    } finally {
      window.clearInterval(timer);
      setDownloadingKind('');
      window.setTimeout(() => {
        setDownloadPhase('');
        setDownloadPct(0);
        setDownloadElapsed(0);
      }, 450);
    }
  }

  async function handleUpload(filesList) {
    const picked = Array.from(filesList || []).filter(Boolean);
    if (!picked.length) return;
    setBusy(true);
    try {
      for (const file of picked) {
        if (!/\.(xlsx|xlsm)$/i.test(file.name || '')) {
          throw new Error(`Invalid XLSX file: ${file.name}`);
        }
        const d = await xlsxApi.upload(file, apiKey, uploadSource, 2.5, 1, uploadLwinFilter);
        if (!d?.ok) throw new Error(d?.error || `Upload failed for ${file.name}`);
        setSelectedFileId(d.file_id);
        if (d?.lwin_filter?.enabled) {
          const selectedTotal = d.total || 0;
          const originalTotal = d.file_total || selectedTotal;
          const invalidCount = (d.lwin_filter.invalid_values || []).length;
          const unmatchedCount = (d.lwin_filter.unmatched_values || []).length;
          let msg = `${file.name} saved and started for ${selectedTotal}/${originalTotal} selected row(s).`;
          if (invalidCount) msg += ` Ignored ${invalidCount} invalid LWIN value(s).`;
          if (unmatchedCount) msg += ` ${unmatchedCount} requested LWIN(s) were not in the file.`;
          addToast(msg, 'success');
        } else {
          addToast(`${file.name} saved to backend and started`, 'success');
        }
      }
      await refreshAll();
    } catch (e) {
      addToast(e.message || 'Upload failed', 'error');
    } finally {
      setBusy(false);
      setIsDragging(false);
      if (inputRef.current) inputRef.current.value = '';
    }
  }

  async function stopCurrentJob() {
    const jobId = detail?.active_job?.job_id || detail?.active_job_id;
    if (!jobId) return;
    setBusy(true);
    setStopPreparing(true);
    setStopPct(Math.max(8, Number(activeJob?.pct || 0)));
    const startedAt = Date.now();
    const timer = window.setInterval(() => {
      setStopPct((current) => {
        if (current >= 92) return current;
        const elapsed = Date.now() - startedAt;
        return Math.max(current, Math.min(92, 12 + Math.floor(elapsed / 400) * 5));
      });
    }, 400);
    try {
      await xlsxApi.stop(jobId, apiKey);
      setStopPct(100);
      addToast('Stop requested. Preparing partial XLSX...', 'success');
      await refreshAll(detail.file_id);
    } catch (e) {
      addToast(e.message || 'Failed to stop job', 'error');
    } finally {
      window.clearInterval(timer);
      setBusy(false);
      window.setTimeout(() => {
        setStopPreparing(false);
        setStopPct(0);
      }, 600);
    }
  }

  async function resumeLastJob() {
    const jobId = detail?.last_job?.job_id || detail?.active_job?.job_id || detail?.last_job_id || detail?.active_job_id;
    if (!jobId) return;
    setBusy(true);
    try {
      await xlsxApi.resume(jobId, apiKey);
      addToast('Job resumed', 'success');
      await refreshAll(detail.file_id);
    } catch (e) {
      addToast(e.message || 'Failed to resume job', 'error');
    } finally {
      setBusy(false);
    }
  }

  async function restartFile() {
    if (!detail?.file_id) return;
    setBusy(true);
    try {
      await xlsxApi.restartFile(detail.file_id, {
        source: restartSource,
        start_item: Number.parseInt(String(startItem || '1').trim(), 10) || 1,
        sleep_sec: 2.5,
        lwin_filter: restartLwinFilter,
      }, apiKey);
      addToast('New enrichment run started from the saved backend file', 'success');
      await refreshAll(detail.file_id);
    } catch (e) {
      addToast(e.message || 'Failed to restart file', 'error');
    } finally {
      setBusy(false);
    }
  }

  async function deleteSelectedFile() {
    if (!detail?.file_id) return;
    if (!window.confirm(`Delete ${detail.original_name}? This removes the saved backend file and its job history.`)) return;
    setBusy(true);
    try {
      await xlsxApi.deleteFile(detail.file_id, apiKey);
      addToast('File deleted', 'success');
      const deletedId = detail.file_id;
      setDetail(null);
      setSelectedFileId('');
      const next = await xlsxApi.files(apiKey);
      const nextFiles = next?.files || [];
      setFiles(nextFiles);
      const nextId = nextFiles.find((f) => f.file_id !== deletedId)?.file_id || nextFiles[0]?.file_id || '';
      if (nextId) {
        setSelectedFileId(nextId);
        await loadDetail(nextId);
      }
    } catch (e) {
      addToast(e.message || 'Failed to delete file', 'error');
    } finally {
      setBusy(false);
    }
  }

  useEffect(() => {
    const activeStatus = detail?.active_job?.status;
    const stillRunning = activeStatus === 'running' || activeStatus === 'pending';
    if (!stillRunning && stopPreparing) {
      setStopPreparing(false);
      setStopPct(0);
    }
  }, [detail?.active_job?.status, stopPreparing]);

  if (loading) {
    return (
      <div className="flex items-center gap-3 text-text2">
        <Spinner />
        <span>Loading saved XLSX files...</span>
      </div>
    );
  }

  const activeJob = detail?.active_job;
  const latestJob = detail?.last_job || activeJob;
  const latestStatus = latestJob?.status || detail?.status;
  const latestDone = Number(latestJob?.done ?? detail?.done_rows ?? 0);
  const latestTotal = Number(latestJob?.total ?? detail?.total_rows ?? 0);
  const isRunning = ['running', 'pending'].includes(activeJob?.status);
  const canResume = ['stopped', 'error'].includes(latestStatus) && (!latestTotal || latestDone < latestTotal);
  const hasAnyJob = Boolean(detail?.active_job_id || detail?.last_job_id || latestJob?.job_id);
  const isComplete = detail?.status === 'done' || latestStatus === 'done';
  const canDownloadFilled = isComplete && detail?.has_output;
  const isStopped = latestStatus === 'stopped' || latestStatus === 'error';
  const canDownloadProgress = hasAnyJob && isStopped && !isRunning && !isComplete && Boolean(latestJob?.ready || detail?.has_output);
  const backendPreparingOutput = Boolean(latestJob?.preparing_output);
  const partialPreparing = hasAnyJob && isStopped && !isRunning && !isComplete && !canDownloadProgress && backendPreparingOutput;
  const canPrepareProgress = hasAnyJob && isStopped && !isRunning && !isComplete && !canDownloadProgress && !backendPreparingOutput;
  const activeJobPct = Number(activeJob?.pct || fmtPct(detail?.done_rows, detail?.total_rows).replace('%', '') || 0);

  return (
    <div className="grid grid-cols-1 xl:grid-cols-[360px_minmax(0,1fr)] gap-4">
      <div className="space-y-4">
        <Panel>
          <PanelHeader>
            <PanelTitle>Upload To Backend</PanelTitle>
          </PanelHeader>
          <PanelBody className="space-y-3">
            <FormField label="Source">
              <select className={selectCls} value={uploadSource} onChange={(e) => setUploadSource(e.target.value)}>
                {sourceOptions.map((s) => <option key={s.key} value={s.key}>{s.label}</option>)}
              </select>
            </FormField>
            <FormField label="Only These LWINs">
              <textarea
                className={`${inputCls} min-h-[84px]`}
                value={uploadLwinFilter}
                onChange={(e) => setUploadLwinFilter(e.target.value)}
                placeholder="10012342020, 10045672019, 1234567"
              />
            </FormField>
            <DropZone
              isDragging={isDragging}
              onClick={() => inputRef.current?.click()}
              onDrop={(e) => {
                e.preventDefault();
                handleUpload(e.dataTransfer.files);
              }}
            >
              <div
                onDragEnter={() => setIsDragging(true)}
                onDragLeave={() => setIsDragging(false)}
                className="space-y-2"
              >
                <div className="text-sm">Save `.xlsx` or `.xlsm` files to the backend and start enrichment.</div>
                <div className="text-2xs text-text3">The file remains available even after page refresh or backend recovery.</div>
              </div>
            </DropZone>
            <input
              ref={inputRef}
              type="file"
              multiple
              accept=".xlsx,.xlsm"
              className="hidden"
              onChange={(e) => handleUpload(e.target.files)}
            />
          </PanelBody>
        </Panel>

        <Panel className="min-h-[420px]">
          <PanelHeader>
            <PanelTitle>Saved Files</PanelTitle>
          </PanelHeader>
          <PanelBody className="p-0">
            {files.length === 0 ? (
              <div className="p-4">
                <EmptyState icon="-" message="No backend-saved XLSX files yet" />
              </div>
            ) : files.map((file) => (
              <button
                key={file.file_id}
                type="button"
                onClick={() => setSelectedFileId(file.file_id)}
                className={`w-full text-left px-4 py-3 border-b border-border last:border-b-0 transition-colors ${
                  selectedFileId === file.file_id ? 'bg-bg4' : 'hover:bg-bg4/60'
                }`}
              >
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <div className="text-sm font-semibold text-text1 truncate">{file.original_name}</div>
                    <div className="text-2xs text-text3 mt-1">
                      {sourceLabel(file.source)} | {file.done_rows}/{file.total_rows} rows | {fmtBytes(file.size_bytes)}
                    </div>
                  </div>
                  <div className={`text-2xs font-semibold uppercase tracking-wider ${fileStatusTone(file.status)}`}>
                    {file.status}
                  </div>
                </div>
              </button>
            ))}
          </PanelBody>
        </Panel>
      </div>

      <div>
        {!detail ? (
          detailLoading ? (
            <div className="flex items-center gap-3 text-text2">
              <Spinner />
              <span>Loading file details...</span>
            </div>
          ) : (
            <EmptyState icon="-" message="Select a saved XLSX file to inspect it" />
          )
        ) : (
          <div className="space-y-4">
            <Panel>
              <PanelHeader className="justify-between">
                <div className="flex items-center gap-2">
                  <PanelTitle>File Details</PanelTitle>
                  <SourceBadge source={detail.source} />
                </div>
                <div className={`text-sm font-semibold ${fileStatusTone(detail.status)}`}>{detail.status}</div>
              </PanelHeader>
              <PanelBody className="space-y-4">
                <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
                  <Info label="File name" value={detail.original_name} />
                  <Info label="Rows" value={`${detail.done_rows}/${detail.total_rows}`} />
                  <Info label="Found" value={detail.found_rows} />
                  <Info label="Progress" value={fmtPct(detail.done_rows, detail.total_rows)} />
                  <Info label="Prefilled" value={detail.prefilled_rows} />
                  <Info label="Size" value={fmtBytes(detail.size_bytes)} />
                  <Info label="Created" value={detail.created_at || '-'} />
                  <Info label="Updated" value={detail.updated_at || '-'} />
                </div>

                {detail.last_error && (
                  <div className="rounded border border-red/40 bg-red/10 px-3 py-2 text-sm text-red">
                    {detail.last_error}
                  </div>
                )}

                {downloadPhase && (
                  <div className="rounded border border-teal/40 bg-teal/10 px-3 py-3">
                    <div className="flex items-center justify-between gap-3 text-sm">
                      <span className="font-semibold text-teal">{downloadPhase}</span>
                      <span className="text-text2">
                        {downloadPct}%{downloadElapsed > 0 ? ` | ${downloadElapsed}s` : ''}
                      </span>
                    </div>
                    <div className="mt-2 h-2 rounded bg-bg4 overflow-hidden">
                      <div
                        className="h-full bg-teal transition-all duration-300"
                        style={{ width: `${downloadPct}%` }}
                      />
                    </div>
                    {downloadPct >= 98 && (
                      <div className="text-2xs text-text2 mt-2">
                        Still preparing. Large XLSX files can take a little longer to package.
                      </div>
                    )}
                  </div>
                )}

                {isComplete && detail.has_output && (
                  <div className="rounded border border-green/40 bg-green/10 px-3 py-3">
                    <div className="text-sm font-semibold text-green">Filled file ready</div>
                    <div className="text-2xs text-text2 mt-1">
                      The completed XLSX is available below as “Download filled file”.
                    </div>
                  </div>
                )}

                {isRunning && (
                  <div className="rounded border border-yellow/40 bg-yellow/10 px-3 py-3">
                    <div className="text-sm font-semibold text-yellow">Download will be available after stop</div>
                    <div className="text-2xs text-text2 mt-1">
                      Use “Stop current job” to prepare a partial XLSX, then download it when the job is stopped.
                    </div>
                  </div>
                )}

                <div className="flex flex-wrap gap-2">
                  <ProgressButton
                    onClick={() => downloadFile('original')}
                    disabled={Boolean(downloadingKind)}
                    active={downloadingKind === 'original'}
                    pct={downloadPct}
                  >
                    {downloadingKind === 'original' ? 'Downloading...' : 'Download original'}
                  </ProgressButton>
                  {canDownloadFilled && (
                    <ProgressButton
                      variant="blue"
                      onClick={() => downloadFile('output')}
                      disabled={Boolean(downloadingKind)}
                      active={downloadingKind === 'output'}
                      pct={downloadPct}
                    >
                      {downloadingKind === 'output' ? 'Downloading...' : 'Download filled file'}
                    </ProgressButton>
                  )}
                  {(canDownloadProgress || partialPreparing || canPrepareProgress) && (
                    <ProgressButton
                      variant={(partialPreparing || canPrepareProgress) ? 'yellow' : 'teal'}
                      onClick={() => (canDownloadProgress || canPrepareProgress) && downloadFile('progress')}
                      disabled={Boolean(downloadingKind) || partialPreparing}
                      active={downloadingKind === 'progress' || partialPreparing}
                      pct={downloadingKind === 'progress' ? downloadPct : Math.max(12, Math.min(95, Number(latestJob?.pct || fmtPct(latestDone, latestTotal).replace('%', '') || 0)))}
                    >
                      {downloadingKind === 'progress'
                        ? 'Preparing partial XLSX...'
                        : partialPreparing
                          ? 'Preparing partial XLSX...'
                          : canPrepareProgress
                            ? 'Prepare partial XLSX'
                          : 'Download current progress'}
                    </ProgressButton>
                  )}
                  <Button variant="ghost" onClick={() => refreshAll(detail.file_id)} disabled={busy}>Refresh</Button>
                  <Button variant="red" onClick={deleteSelectedFile} disabled={busy || isRunning}>Delete file</Button>
                </div>

                <div className="grid grid-cols-1 lg:grid-cols-[minmax(0,1fr)_320px] gap-4">
                  <Panel className="mb-0">
                    <PanelHeader>
                      <PanelTitle>Run Controls</PanelTitle>
                    </PanelHeader>
                    <PanelBody className="space-y-3">
                      <div className="flex flex-wrap gap-2">
                        <ProgressButton
                          variant="red"
                          onClick={stopCurrentJob}
                          disabled={(busy && !stopPreparing) || !isRunning}
                          active={stopPreparing || isRunning}
                          pct={stopPreparing ? stopPct : activeJobPct}
                        >
                          {stopPreparing ? 'Stopping and preparing...' : 'Stop current job'}
                        </ProgressButton>
                        <Button variant="teal" onClick={resumeLastJob} disabled={busy || !canResume}>Resume last job</Button>
                      </div>
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                        <FormField label="Restart source">
                          <select className={selectCls} value={restartSource} onChange={(e) => setRestartSource(e.target.value)}>
                            {sourceOptions.map((s) => <option key={s.key} value={s.key}>{s.label}</option>)}
                          </select>
                        </FormField>
                        <FormField label="Restart from row">
                          <input className={inputCls} value={startItem} onChange={(e) => setStartItem(e.target.value)} />
                        </FormField>
                      </div>
                      <FormField label="Restart Only These LWINs">
                        <textarea
                          className={`${inputCls} min-h-[84px]`}
                          value={restartLwinFilter}
                          onChange={(e) => setRestartLwinFilter(e.target.value)}
                          placeholder="Leave blank to run all rows in the saved file"
                        />
                      </FormField>
                      <Button variant="blue" onClick={restartFile} disabled={busy || isRunning}>
                        Start new run from saved file
                      </Button>
                    </PanelBody>
                  </Panel>

                  <Panel className="mb-0">
                    <PanelHeader>
                      <PanelTitle>Latest Job</PanelTitle>
                    </PanelHeader>
                    <PanelBody className="space-y-2 text-sm">
                      <div>Status: <span className={fileStatusTone(latestStatus)}>{latestStatus || '-'}</span></div>
                      <div>Source: {sourceLabel(latestJob?.source || detail.source)}</div>
                      <div>Start item: {latestJob?.start_item || '-'}</div>
                      <div>Processed: {latestJob?.done ?? detail.done_rows}</div>
                      <div>Found: {latestJob?.found ?? detail.found_rows}</div>
                      {canResume && <div className="text-text3">Resume continues from the next unprocessed row.</div>}
                    </PanelBody>
                  </Panel>
                </div>
              </PanelBody>
            </Panel>

            <Panel>
              <PanelHeader>
                <PanelTitle>Preview Rows</PanelTitle>
              </PanelHeader>
              <PanelBody className="overflow-auto">
                {detail.preview_deferred ? (
                  <div className="flex flex-wrap items-center gap-3">
                    <div className="text-sm text-text2">
                      Preview rows are loaded only when needed so this page opens faster.
                    </div>
                    <Button variant="ghost" onClick={loadPreviewRows} disabled={previewLoading}>
                      {previewLoading ? 'Loading preview...' : 'Load preview rows'}
                    </Button>
                  </div>
                ) : detail.preview_rows?.length ? (
                  <table className="min-w-full text-sm">
                    <thead className="text-text3">
                      <tr className="border-b border-border">
                        <th className="py-2 pr-3 text-left">Row</th>
                        <th className="py-2 pr-3 text-left">Wine</th>
                        <th className="py-2 pr-3 text-left">Vintage</th>
                        <th className="py-2 pr-3 text-left">LWIN</th>
                        <th className="py-2 text-left">Prefilled</th>
                      </tr>
                    </thead>
                    <tbody>
                      {detail.preview_rows.map((row) => (
                        <tr key={row.row_idx} className="border-b border-border/60">
                          <td className="py-2 pr-3 text-text3">{row.row_idx}</td>
                          <td className="py-2 pr-3 text-text1">{row.raw_name || row.name || '-'}</td>
                          <td className="py-2 pr-3 text-text2">{row.vintage || 'NV'}</td>
                          <td className="py-2 pr-3 text-text2 font-mono text-2xs">{row.lwin || '-'}</td>
                          <td className="py-2 text-text2">{row.prefilled ? 'Yes' : 'No'}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                ) : (
                  <EmptyState icon="-" message={detail.preview_error || 'No preview rows available'} />
                )}
              </PanelBody>
            </Panel>

            <Panel>
              <PanelHeader>
                <PanelTitle>Job History</PanelTitle>
              </PanelHeader>
              <PanelBody className="overflow-auto">
                {(detail.jobs || []).length ? (
                  <table className="min-w-full text-sm">
                    <thead className="text-text3">
                      <tr className="border-b border-border">
                        <th className="py-2 pr-3 text-left">Job</th>
                        <th className="py-2 pr-3 text-left">Status</th>
                        <th className="py-2 pr-3 text-left">Source</th>
                        <th className="py-2 pr-3 text-left">Processed</th>
                        <th className="py-2 pr-3 text-left">Found</th>
                        <th className="py-2 text-left">Updated</th>
                      </tr>
                    </thead>
                    <tbody>
                      {detail.jobs.map((job) => (
                        <tr key={job.job_id} className="border-b border-border/60">
                          <td className="py-2 pr-3 font-mono text-2xs text-text2">{job.job_id.slice(0, 8)}</td>
                          <td className={`py-2 pr-3 ${fileStatusTone(job.status)}`}>{job.status}</td>
                          <td className="py-2 pr-3">{sourceLabel(job.source)}</td>
                          <td className="py-2 pr-3">{job.done}/{job.total}</td>
                          <td className="py-2 pr-3">{job.found}</td>
                          <td className="py-2">{job.updated_at || job.created_at || '-'}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                ) : (
                  <EmptyState icon="-" message="No job history yet" />
                )}
              </PanelBody>
            </Panel>
          </div>
        )}
      </div>
    </div>
  );
}

function Info({ label, value }) {
  return (
    <div className="rounded border border-border bg-bg4/50 px-3 py-2">
      <div className="text-2xs uppercase tracking-wider text-text3">{label}</div>
      <div className="text-sm text-text1 mt-1 break-words">{value || '-'}</div>
    </div>
  );
}
