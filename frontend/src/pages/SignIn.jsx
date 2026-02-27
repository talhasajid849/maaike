import { useState, useEffect, useRef } from 'react';
import { useApp } from '../context/AppContext.jsx';
import { API } from '../api.js';

export default function SignIn() {
  const { setApiKey } = useApp();
  const [key, setKey] = useState('');
  const [err, setErr] = useState('');
  const [loading, setLoading] = useState(false);
  const inputRef = useRef(null);

  useEffect(() => { setTimeout(() => inputRef.current?.focus(), 80); }, []);

  async function doSignin() {
    setErr('');
    if (!key.trim()) { setErr('Enter API key.'); return; }
    setLoading(true);
    try {
      const r = await fetch(`${API}/auth`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ api_key: key.trim() }),
      });
      const d = await r.json();
      if (d.ok) setApiKey(key.trim());
      else setErr('Invalid API key.');
    } catch (e) {
      setErr('Connection error: ' + e.message);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div id="signin-screen" className="active">
      <div className="sc">
        <div className="sc-logo">MAAIKE</div>
        <div className="sc-sub">WINE REVIEW INTELLIGENCE</div>
        <input
          ref={inputRef}
          type="password"
          value={key}
          onChange={e => setKey(e.target.value)}
          onKeyDown={e => { if (e.key === 'Enter') doSignin(); }}
          placeholder="Enter API key…"
          autoComplete="current-password"
        />
        <button onClick={doSignin} disabled={loading}>
          {loading ? 'Signing in…' : 'Sign In'}
        </button>
        <div className="sc-err">{err}</div>
      </div>
    </div>
  );
}
