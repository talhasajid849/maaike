import { createContext, useContext, useState, useCallback } from 'react';

const AppContext = createContext(null);

export function AppProvider({ children }) {
  const [apiKey, setApiKeyState] = useState(() => localStorage.getItem('maaike_ak') || '');
  const [currentPage, setCurrentPage] = useState('dashboard');
  const [toasts, setToasts] = useState([]);
  const [wineModalId, setWineModalId] = useState(null);

  const setApiKey = useCallback((key) => {
    setApiKeyState(key);
    if (key) localStorage.setItem('maaike_ak', key);
    else localStorage.removeItem('maaike_ak');
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
      toasts, addToast,
      wineModalId, setWineModalId,
    }}>
      {children}
    </AppContext.Provider>
  );
}

export function useApp() {
  return useContext(AppContext);
}
