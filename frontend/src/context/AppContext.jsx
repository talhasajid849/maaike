/**
 * context/AppContext.jsx
 * =======================
 * Global app state — apiKey, current page, toast queue, wine modal.
 * Think of this like your Express req.user / session state.
 */
import { createContext, useContext, useState, useCallback } from 'react';

const AppContext = createContext(null);

export function AppProvider({ children }) {
  const [apiKey,      setApiKeyState] = useState(() => sessionStorage.getItem('maaike_key') || '');
  const [currentPage, setCurrentPage] = useState('dashboard');
  const [uploadTab,   setUploadTab]   = useState('inventory');
  const [wineModalId, setWineModalId] = useState(null);
  const [toasts,      setToasts]      = useState([]);

  const setApiKey = useCallback((key) => {
    setApiKeyState(key);
    if (key) sessionStorage.setItem('maaike_key', key);
    else     sessionStorage.removeItem('maaike_key');
  }, []);

  const addToast = useCallback((msg, type = 'info') => {
    const id = Date.now() + Math.random();
    setToasts(prev => [...prev, { id, msg, type }]);
    setTimeout(() => setToasts(prev => prev.filter(t => t.id !== id)), 4000);
  }, []);

  return (
    <AppContext.Provider value={{
      apiKey, setApiKey,
      currentPage, setCurrentPage,
      uploadTab, setUploadTab,
      wineModalId, setWineModalId,
      toasts, addToast,
    }}>
      {children}
    </AppContext.Provider>
  );
}

export function useApp() {
  const ctx = useContext(AppContext);
  if (!ctx) throw new Error('useApp must be used inside AppProvider');
  return ctx;
}
