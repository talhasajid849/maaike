import { useState, useEffect, useRef } from 'react';
import { useApp } from '../context/AppContext.jsx';
import { authApi } from '../services/api.js';

export default function SignIn() {
  const { setApiKey } = useApp();
  const [key,     setKey]     = useState('');
  const [err,     setErr]     = useState('');
  const [loading, setLoading] = useState(false);
  const inputRef = useRef(null);

  useEffect(() => { setTimeout(() => inputRef.current?.focus(), 80); }, []);

  async function doSignin() {
    setErr('');
    if (!key.trim()) { setErr('Enter API key.'); return; }
    setLoading(true);
    try {
      const d = await authApi.verify(key.trim());
      if (d.ok) setApiKey(key.trim());
      else setErr('Invalid API key.');
    } catch (e) {
      setErr('Connection error: ' + e.message);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="fixed inset-0 z-[9999] bg-[radial-gradient(ellipse_at_50%_40%,#0d2137_0%,#0d1117_65%)] flex items-center justify-center">
      <div className="bg-bg2 border border-border rounded-xl p-11 w-[360px] text-center">
        <div className="text-[34px] font-extrabold tracking-[4px] text-teal">MAAIKE</div>
        <div className="text-text2 text-2xs tracking-[2px] mb-7">WINE REVIEW INTELLIGENCE</div>
        <input
          ref={inputRef}
          type="password"
          value={key}
          onChange={e => setKey(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && doSignin()}
          placeholder="Enter API key…"
          autoComplete="current-password"
          className="w-full bg-bg4 border border-border rounded text-text1 text-md px-3.5 py-2.5 mb-3.5 outline-none focus:border-teal transition-colors"
        />
        <button
          onClick={doSignin}
          disabled={loading}
          className="w-full py-3 bg-teal text-bg font-bold text-md rounded hover:bg-teal2 transition-colors disabled:opacity-40 cursor-pointer border-none"
        >
          {loading ? 'Signing in…' : 'Sign In'}
        </button>
        {err && <div className="text-red text-xs mt-2">{err}</div>}
      </div>
    </div>
  );
}