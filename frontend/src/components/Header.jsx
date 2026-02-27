import { useEffect, useState } from 'react';
import { useApp } from '../context/AppContext.jsx';
import { getSocket, disconnectSocket } from '../socket.js';

const PAGES = [
  { key: 'dashboard', label: 'Dashboard' },
  { key: 'wines',     label: 'Wine List' },
  { key: 'addwine',  label: '+ Add Wine' },
  { key: 'upload',   label: 'Upload CSV' },
  { key: 'sources',  label: 'Sources' },
  { key: 'settings', label: 'Settings' },
];

export default function Header() {
  const { currentPage, setCurrentPage, setApiKey, apiKey } = useApp();
  const [wsStatus, setWsStatus] = useState({ cls: 'dot warn', label: 'Connecting…' });

  useEffect(() => {
    const sock = getSocket();
    sock.on('connect',       () => setWsStatus({ cls: 'dot', label: 'Live' }));
    sock.on('disconnect',    () => setWsStatus({ cls: 'dot err', label: 'Disconnected' }));
    sock.on('connect_error', () => setWsStatus({ cls: 'dot warn', label: 'Reconnecting…' }));
    return () => {
      sock.off('connect');
      sock.off('disconnect');
      sock.off('connect_error');
    };
  }, []);

  function signout() {
    setApiKey('');
    disconnectSocket();
  }

  return (
    <header>
      <div className="logo">MAAIKE <span>WINE INTELLIGENCE</span></div>
      <nav>
        {PAGES.map(p => (
          <button
            key={p.key}
            className={`nb ${currentPage === p.key ? 'active' : ''}`}
            onClick={() => setCurrentPage(p.key)}
          >
            {p.label}
          </button>
        ))}
      </nav>
      <div className="hs">
        <div className={wsStatus.cls} />
        <span>{wsStatus.label}</span>
        <button
          className="btn btn-outline"
          style={{ marginLeft: 6, padding: '4px 9px', fontSize: 11 }}
          onClick={signout}
        >
          Sign Out
        </button>
      </div>
    </header>
  );
}
