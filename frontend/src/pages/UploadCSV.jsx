import { useState, useRef } from 'react';
import { useApp } from '../context/AppContext.jsx';
import { API } from '../api.js';

export default function UploadCSV() {
  const { apiKey, addToast } = useApp();
  const [isDragging, setIsDragging] = useState(false);
  const [result, setResult] = useState(null);
  const [uploading, setUploading] = useState(false);
  const inputRef = useRef(null);

  async function handleFile(file) {
    if (!file) return;
    setUploading(true);
    setResult(null);
    const form = new FormData();
    form.append('file', file);
    try {
      const r = await fetch(`${API}/upload`, {
        method: 'POST',
        headers: { 'X-API-Key': apiKey },
        body: form,
      });
      const d = await r.json();
      if (d.ok) {
        setResult(d);
        addToast(`Imported ${d.inserted} wines!`, 'success');
      } else {
        addToast(d.error || 'Upload failed', 'error');
      }
    } catch (e) {
      addToast('Upload error: ' + e, 'error');
    } finally {
      setUploading(false);
    }
  }

  function handleDrop(e) {
    e.preventDefault();
    setIsDragging(false);
    const file = e.dataTransfer.files[0];
    if (file) handleFile(file);
  }

  return (
    <div className="panel" style={{ maxWidth: 580 }}>
      <div className="ph"><span className="pt">📁 Upload Rue Pinard CSV</span></div>
      <div className="pb">
        <p style={{ color: 'var(--text2)', fontSize: 13, marginBottom: 14 }}>
          Upload your Rue Pinard export. Columns auto-detected. LWIN is read automatically.
          Columns: <code style={{ color: 'var(--teal)' }}>name, lwin, vintage, unit-size, price, url, stock-level</code>
        </p>
        <div
          className={`dz ${isDragging ? 'drag' : ''}`}
          onClick={() => inputRef.current?.click()}
          onDragOver={e => { e.preventDefault(); setIsDragging(true); }}
          onDragLeave={() => setIsDragging(false)}
          onDrop={handleDrop}
        >
          <input ref={inputRef} type="file" accept=".csv" style={{ display: 'none' }}
            onChange={e => handleFile(e.target.files[0])} />
          <div style={{ fontSize: 28, marginBottom: 8 }}>📄</div>
          <div>{uploading ? 'Uploading…' : <>Click or drag a <strong>.csv</strong> file here</>}</div>
          <div style={{ fontSize: 11, marginTop: 5, color: 'var(--text3)' }}>Max 20MB</div>
        </div>

        {result && (
          <div style={{ marginTop: 14, padding: 14, borderRadius: 'var(--r)', background: 'var(--bg4)', border: '1px solid var(--border2)' }}>
            <div style={{ color: 'var(--green)', fontWeight: 700, marginBottom: 8 }}>✓ Upload successful</div>
            <div style={{ fontSize: 13, color: 'var(--text2)', lineHeight: 2 }}>
              Inserted: <strong style={{ color: 'var(--text)' }}>{result.inserted}</strong> &nbsp;
              Duplicates: <strong style={{ color: 'var(--text)' }}>{result.dupes}</strong> &nbsp;
              Errors: <strong style={{ color: 'var(--red)' }}>{result.errors}</strong> &nbsp;
              Total: <strong style={{ color: 'var(--text)' }}>{result.total}</strong>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
