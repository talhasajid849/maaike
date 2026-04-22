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

export default function XlsxFiles() {
  const { apiKey, addToast } = useApp();
  const [files, setFiles] = useState([]);
  const [detail, setDetail] = useState(null);
  const [selectedFileId, setSelectedFileId] = useState('');
  const [loading, setLoading] = useState(true);
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
        if (!options.some((s) => s.key === uploadSource)) setUploadSource(options[0].key);
        if (!options.some((s) => s.key === restartSource)) setRestartSource(options[0].key);
      }
    } catch {}
  }, [apiKey, restartSource, uploadSource]);

  const loadFiles = useCallback(async (preferredId = '') => {
    const d = await xlsxApi.files(apiKey);
    const nextFiles = d?.files || [];
    setFiles(nextFiles);
    const targetId = preferredId || selectedFileId || nextFiles[0]?.file_id || '';
    if (targetId) setSelectedFileId(targetId);
    return nextFiles;
  }, [apiKey, selectedFileId]);

  const loadDetail = useCallback(async (fileId) => {
    if (!fileId) {
      setDetail(null);
      return null;
    }
    const d = await xlsxApi.file(fileId, apiKey);
    setDetail(d);
    setRestartSource(d?.source || restartSource || uploadSource || 'robertparker');
    return d;
  }, [apiKey, restartSource, uploadSource]);

  useEffect(() => {
    let cancelled = false;
    async function boot() {
      setLoading(true);
      try {
        await loadSources();
        const nextFiles = await loadFiles();
        const initialId = nextFiles[0]?.file_id || '';
        if (!cancelled && initialId) {
          await loadDetail(initialId);
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
    loadDetail(selectedFileId).catch(() => {});
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
    if (targetId) await loadDetail(targetId);
    else setDetail(null);
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
    const jobId = detail?.active_job?.job_id;
    if (!jobId) return;
    setBusy(true);
    try {
      await xlsxApi.stop(jobId, apiKey);
      addToast('Stop requested for this XLSX job', 'success');
      await refreshAll(detail.file_id);
    } catch (e) {
      addToast(e.message || 'Failed to stop job', 'error');
    } finally {
      setBusy(false);
    }
  }

  async function resumeLastJob() {
    const jobId = detail?.last_job?.job_id;
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

  if (loading) {
    return (
      <div className="flex items-center gap-3 text-text2">
        <Spinner />
        <span>Loading saved XLSX files...</span>
      </div>
    );
  }

  const canResume = ['stopped', 'error'].includes(detail?.last_job?.status);
  const isRunning = ['running', 'pending'].includes(detail?.active_job?.status);

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
          <EmptyState icon="-" message="Select a saved XLSX file to inspect it" />
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

                <div className="flex flex-wrap gap-2">
                  <a href={xlsxApi.fileDownloadUrl(detail.file_id, apiKey, 'original')}>
                    <Button>Download original</Button>
                  </a>
                  {detail.has_output && (
                    <a href={xlsxApi.fileDownloadUrl(detail.file_id, apiKey, 'output')}>
                      <Button variant="blue">Download filled</Button>
                    </a>
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
                        <Button variant="red" onClick={stopCurrentJob} disabled={busy || !isRunning}>Stop current job</Button>
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
                      <div>Status: <span className={fileStatusTone(detail.last_job?.status)}>{detail.last_job?.status || '-'}</span></div>
                      <div>Source: {sourceLabel(detail.last_job?.source || detail.source)}</div>
                      <div>Start item: {detail.last_job?.start_item || '-'}</div>
                      <div>Processed: {detail.last_job?.done ?? detail.done_rows}</div>
                      <div>Found: {detail.last_job?.found ?? detail.found_rows}</div>
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
                {detail.preview_rows?.length ? (
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
