/**
 * pages/XlsxExport.jsx
 * =====================
 * Upload a JR-format XLSX template → software searches JR for each wine
 * → fills in Critic_Name, Score, Drink dates, Review_Date, Review text
 * → download the filled XLSX.
 *
 * Template format (JR export):
 *   Row 1: Headers — Publisher, LWIN11, Product_Name, Vintage,
 *                    Critic_Name, Score, Drink_From, Drink_To, Review_Date, Review
 *   Row 2: Instructions (skipped)
 *   Row 3+: Wine data
 */

import { useState, useRef, useEffect, useCallback } from 'react';
import { useApp } from '../context/AppContext.jsx';
import { xlsxApi } from '../services/api.js';

const POLL_MS = 2500;

export default function XlsxExport() {
  const { apiKey, addToast } = useApp();

  const [isDragging, setIsDragging]   = useState(false);
  const [uploading,  setUploading]    = useState(false);
  const [job,        setJob]          = useState(null);   // { jobId, status, total, done, found, pct, ready, log }
  const [fileName,   setFileName]     = useState('');

  const inputRef  = useRef(null);
  const logRef    = useRef(null);
  const pollTimer = useRef(null);

  // Auto-scroll log to bottom
  useEffect(() => {
    if (logRef.current) {
      logRef.current.scrollTop = logRef.current.scrollHeight;
    }
  }, [job?.log]);

  // Poll job status while running
  const pollStatus = useCallback(async (jobId) => {
    try {
      const d = await xlsxApi.status(jobId, apiKey);
      if (!d.ok) return;
      setJob(prev => ({ ...prev, ...d }));
      if (d.status === 'running' || d.status === 'pending') {
        pollTimer.current = setTimeout(() => pollStatus(jobId), POLL_MS);
      }
    } catch (e) {
      if (e?.status === 404) {
        clearTimeout(pollTimer.current);
        pollTimer.current = null;
        setJob(null);
        addToast('The XLSX job no longer exists on the backend. Please upload again.', 'info');
        return;
      }
      pollTimer.current = setTimeout(() => pollStatus(jobId), POLL_MS);
    }
  }, [addToast, apiKey]);

  useEffect(() => () => clearTimeout(pollTimer.current), []);

  async function handleFile(file) {
    if (!file) return;
    if (!file.name.toLowerCase().endsWith('.xlsx')) {
      addToast('Please upload an .xlsx file', 'error');
      return;
    }
    setUploading(true);
    setJob(null);
    setFileName(file.name);

    try {
      const d = await xlsxApi.upload(file, apiKey);
      if (!d.ok) {
        addToast(d.error || 'Upload failed', 'error');
        setUploading(false);
        return;
      }
      const initial = {
        jobId: d.job_id, status: 'pending',
        total: d.total, done: 0, found: 0, pct: 0, ready: false, log: [],
      };
      setJob(initial);
      pollTimer.current = setTimeout(() => pollStatus(d.job_id), POLL_MS);
    } catch (e) {
      addToast('Upload error: ' + e.message, 'error');
    } finally {
      setUploading(false);
    }
  }

  function handleDrop(e) {
    e.preventDefault();
    setIsDragging(false);
    handleFile(e.dataTransfer.files[0]);
  }

  function handleDownload() {
    if (!job?.jobId || !job?.ready) return;
    const url = xlsxApi.downloadUrl(job.jobId, apiKey);
    const a = document.createElement('a');
    a.href = url;
    a.download = `maaike_reviews_${fileName.replace('.xlsx', '')}_filled.xlsx`;
    a.click();
  }

  const isRunning = job?.status === 'running' || job?.status === 'pending';
  const isDone    = job?.status === 'done';
  const isError   = job?.status === 'error';

  const zoneCls = [
    'border border-dashed rounded-lg p-10 text-center cursor-pointer transition-colors',
    isDragging ? 'border-teal-400 bg-teal-400/5' : 'border-[#2a2a2a] hover:border-[#444] bg-[#0d0d0d]',
  ].join(' ');

  return (
    <div style={{ maxWidth: 720 }}>
      {/* Header */}
      <div style={{ marginBottom: 20 }}>
        <h2 style={{ color: '#e8e8e8', fontSize: 18, fontWeight: 700, margin: 0 }}>
          XLSX Review Export
        </h2>
        <p style={{ color: '#666', fontSize: 12, marginTop: 6, lineHeight: 1.6 }}>
          Upload your JR-format XLSX template. The software will search Jancis Robinson
          for each wine and fill in the <span style={{ color: '#aaa' }}>Critic_Name</span>,{' '}
          <span style={{ color: '#aaa' }}>Score</span>,{' '}
          <span style={{ color: '#aaa' }}>Drink dates</span>,{' '}
          <span style={{ color: '#aaa' }}>Review_Date</span> and{' '}
          <span style={{ color: '#aaa' }}>Review</span> columns
          using the most recent review. Download the completed file when done.
        </p>
      </div>

      {/* Drop zone */}
      <div
        className={zoneCls}
        onClick={() => inputRef.current?.click()}
        onDrop={handleDrop}
        onDragOver={e => { e.preventDefault(); setIsDragging(true); }}
        onDragLeave={() => setIsDragging(false)}
      >
        <input
          ref={inputRef}
          type="file"
          accept=".xlsx,.xlsm"
          className="hidden"
          onChange={e => handleFile(e.target.files[0])}
        />
        <div style={{ fontSize: 36, marginBottom: 10 }}>
          {isDragging ? '📂' : '📊'}
        </div>
        <div style={{ fontSize: 14, color: '#aaa' }}>
          {uploading
            ? <span style={{ color: '#2dd4bf' }}>Uploading…</span>
            : <span>Click or drag an <strong style={{ color: '#fff' }}>.xlsx</strong> file here</span>
          }
        </div>
        <div style={{ fontSize: 11, color: '#555', marginTop: 6 }}>
          JR template format — Row 1: headers, Row 2: instructions, Row 3+: wines
        </div>
      </div>

      {/* Job progress */}
      {job && (
        <div style={{
          marginTop: 20,
          background: '#111',
          border: '1px solid #1e1e1e',
          borderRadius: 10,
          padding: 18,
        }}>
          {/* Stats row */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 16, marginBottom: 14 }}>
            <div style={{ flex: 1 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 5 }}>
                <span style={{ color: '#888', fontSize: 12 }}>
                  {isRunning && '⏳ '}
                  {isDone   && '✓ '}
                  {isError  && '✗ '}
                  {isRunning ? 'Searching…' : isDone ? 'Complete' : isError ? 'Error' : 'Starting…'}
                  {fileName && <span style={{ color: '#555', marginLeft: 8 }}>{fileName}</span>}
                </span>
                <span style={{ color: '#aaa', fontSize: 12, fontVariantNumeric: 'tabular-nums' }}>
                  {job.done}/{job.total} &nbsp;·&nbsp;
                  <span style={{ color: '#2dd4bf' }}>{job.found} found</span>
                  {job.total > 0 && (
                    <span style={{ color: '#555' }}>
                      {' '}({Math.round(job.found / job.total * 100)}% hit)
                    </span>
                  )}
                </span>
              </div>

              {/* Progress bar */}
              <div style={{
                height: 6, background: '#1a1a1a', borderRadius: 3, overflow: 'hidden',
              }}>
                <div style={{
                  height: '100%',
                  width: `${job.pct}%`,
                  background: isError
                    ? '#7f1d1d'
                    : isDone
                    ? 'linear-gradient(90deg, #0d9488, #2dd4bf)'
                    : 'linear-gradient(90deg, #0d9488, #5eead4)',
                  transition: 'width 0.4s ease',
                  borderRadius: 3,
                }} />
              </div>
            </div>

            {/* Download button */}
            {isDone && job.ready && (
              <button
                onClick={handleDownload}
                style={{
                  background: 'linear-gradient(135deg, #0d9488, #0891b2)',
                  color: '#fff',
                  border: 'none',
                  borderRadius: 7,
                  padding: '8px 18px',
                  fontSize: 13,
                  fontWeight: 600,
                  cursor: 'pointer',
                  whiteSpace: 'nowrap',
                  flexShrink: 0,
                }}
              >
                ⬇ Download XLSX
              </button>
            )}
          </div>

          {/* Error message */}
          {isError && job.error && (
            <div style={{
              background: '#2b0d0d', border: '1px solid #7f1d1d',
              borderRadius: 6, padding: '8px 12px',
              color: '#fca5a5', fontSize: 12, marginBottom: 10,
            }}>
              {job.error}
            </div>
          )}

          {/* Log */}
          {job.log && job.log.length > 0 && (
            <div
              ref={logRef}
              style={{
                background: '#0a0a0a',
                border: '1px solid #1a1a1a',
                borderRadius: 6,
                padding: '10px 12px',
                fontFamily: 'monospace',
                fontSize: 11,
                color: '#888',
                maxHeight: 260,
                overflowY: 'auto',
                lineHeight: 1.7,
              }}
            >
              {job.log.map((line, i) => {
                const isFound  = line.includes('✓');
                const isNotFound = line.includes('–');
                const isError2 = line.includes('✗');
                const isDoneLine = line.includes('Done —') || line.includes('XLSX ready');
                return (
                  <div
                    key={i}
                    style={{
                      color: isDoneLine ? '#2dd4bf'
                           : isFound    ? '#4ade80'
                           : isError2   ? '#f87171'
                           : isNotFound ? '#666'
                           : '#888',
                    }}
                  >
                    {line}
                  </div>
                );
              })}
              {isRunning && (
                <div style={{ color: '#444' }}>▌</div>
              )}
            </div>
          )}
        </div>
      )}

      {/* Format guide */}
      {!job && (
        <div style={{
          marginTop: 18,
          background: '#0d0d0d',
          border: '1px solid #1e1e1e',
          borderRadius: 8,
          padding: 16,
        }}>
          <div style={{ color: '#555', fontSize: 11, fontWeight: 600, marginBottom: 10, letterSpacing: '0.08em', textTransform: 'uppercase' }}>
            Expected column layout
          </div>
          <div style={{ display: 'grid', gridTemplateColumns: 'auto 1fr', gap: '4px 16px' }}>
            {[
              ['A', 'Publisher', 'Jancis Robinson'],
              ['B', 'LWIN11',       '10098312020 (optional if name set)'],
              ['C', 'Product_Name', 'Chateau X, Appellation, Cru 2020'],
              ['D', 'Vintage',      '2020 or NV'],
              ['E', 'Critic_Name',  '← filled by MAAIKE'],
              ['F', 'Score',        '← filled by MAAIKE (JR 20-pt scale)'],
              ['G', 'Drink_From',   '← filled by MAAIKE'],
              ['H', 'Drink_To',     '← filled by MAAIKE'],
              ['I', 'Review_Date',  '← filled by MAAIKE'],
              ['J', 'Review',       '← filled by MAAIKE (full tasting note)'],
              ['K', 'Source_URL',   '← filled by MAAIKE'],
            ].map(([col, name, desc]) => (
              <>
                <div key={col+'-col'} style={{ color: '#3b82f6', fontSize: 11, fontFamily: 'monospace', fontWeight: 700 }}>{col}</div>
                <div key={col+'-desc'} style={{ fontSize: 11 }}>
                  <span style={{ color: '#ccc', fontWeight: 600 }}>{name}</span>
                  <span style={{ color: '#555', marginLeft: 8 }}>{desc}</span>
                </div>
              </>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
