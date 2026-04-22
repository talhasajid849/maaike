/**
 * context/AppContext.jsx
 * =======================
 * Global app state — apiKey, current page, toast queue, wine modal.
 * Think of this like your Express req.user / session state.
 */
import { createContext, useContext, useState, useCallback, useEffect, useRef } from 'react';

const AppContext = createContext(null);

const API_KEY_STORAGE_KEY = 'maaike_key';
const PAGE_SLUGS = {
  dashboard: '/dashboard',
  wines: '/wines',
  addwine: '/add-wine',
  upload: '/upload',
  xlsxfiles: '/xlsx-files',
  sources: '/sources',
  settings: '/settings',
};
const UPLOAD_TAB_SLUGS = {
  inventory: 'inventory',
  reviews: 'reviews',
  xlsx: 'xlsx',
};

function readStoredApiKey() {
  return localStorage.getItem(API_KEY_STORAGE_KEY)
    || sessionStorage.getItem(API_KEY_STORAGE_KEY)
    || '';
}

function parseRoute(pathname = '/') {
  const path = String(pathname || '/').replace(/\/+$/, '') || '/';
  if (path === '/' || path === '/dashboard') {
    return { currentPage: 'dashboard', uploadTab: 'inventory' };
  }
  if (path === '/wines') {
    return { currentPage: 'wines', uploadTab: 'inventory' };
  }
  if (path === '/add-wine') {
    return { currentPage: 'addwine', uploadTab: 'inventory' };
  }
  if (path === '/sources') {
    return { currentPage: 'sources', uploadTab: 'inventory' };
  }
  if (path === '/xlsx-files') {
    return { currentPage: 'xlsxfiles', uploadTab: 'inventory' };
  }
  if (path === '/settings') {
    return { currentPage: 'settings', uploadTab: 'inventory' };
  }
  if (path === '/signin') {
    return { currentPage: 'dashboard', uploadTab: 'inventory' };
  }
  if (path === '/upload') {
    return { currentPage: 'upload', uploadTab: 'inventory' };
  }
  if (path.startsWith('/upload/')) {
    const tabSlug = path.split('/')[2] || 'inventory';
    const uploadTab = Object.entries(UPLOAD_TAB_SLUGS).find(([, slug]) => slug === tabSlug)?.[0] || 'inventory';
    return { currentPage: 'upload', uploadTab };
  }
  return { currentPage: 'dashboard', uploadTab: 'inventory' };
}

function buildPageHref(page, uploadTab = 'inventory') {
  if (page === 'upload') {
    const slug = UPLOAD_TAB_SLUGS[uploadTab] || UPLOAD_TAB_SLUGS.inventory;
    return `/upload/${slug}`;
  }
  return PAGE_SLUGS[page] || PAGE_SLUGS.dashboard;
}

export function AppProvider({ children }) {
  const routeState = parseRoute(window.location.pathname);
  const [apiKey, setApiKeyState] = useState(readStoredApiKey);
  const [currentPageState, setCurrentPageState] = useState(routeState.currentPage);
  const [uploadTabState, setUploadTabState] = useState(routeState.uploadTab);
  const [wineModalId, setWineModalId] = useState(null);
  const [toasts, setToasts] = useState([]);
  const currentPageRef = useRef(routeState.currentPage);
  const uploadTabRef = useRef(routeState.uploadTab);

  useEffect(() => {
    currentPageRef.current = currentPageState;
  }, [currentPageState]);

  useEffect(() => {
    uploadTabRef.current = uploadTabState;
  }, [uploadTabState]);

  const setApiKey = useCallback((key) => {
    setApiKeyState(key);
    if (key) {
      localStorage.setItem(API_KEY_STORAGE_KEY, key);
      sessionStorage.setItem(API_KEY_STORAGE_KEY, key);
    } else {
      localStorage.removeItem(API_KEY_STORAGE_KEY);
      sessionStorage.removeItem(API_KEY_STORAGE_KEY);
    }
  }, []);

  const syncRoute = useCallback((page, tab, mode = 'push') => {
    const href = buildPageHref(page, tab);
    const next = `${window.location.pathname}${window.location.search}${window.location.hash}`;
    if (href === next) return;
    const fn = mode === 'replace' ? window.history.replaceState : window.history.pushState;
    fn.call(window.history, {}, '', href);
  }, []);

  const setCurrentPage = useCallback((page, options = {}) => {
    const nextPage = page || 'dashboard';
    const nextTab = nextPage === 'upload'
      ? (options.uploadTab || uploadTabRef.current || 'inventory')
      : (options.uploadTab || uploadTabRef.current || 'inventory');

    if (nextPage === 'upload') {
      setUploadTabState(nextTab);
    }
    setCurrentPageState(nextPage);
    syncRoute(nextPage, nextTab);
  }, [syncRoute]);

  const setUploadTab = useCallback((tab, options = {}) => {
    const nextTab = tab || 'inventory';
    setUploadTabState(nextTab);
    if (options.navigateToUpload) {
      setCurrentPageState('upload');
      syncRoute('upload', nextTab);
      return;
    }
    if (currentPageRef.current === 'upload') {
      syncRoute('upload', nextTab);
    }
  }, [syncRoute]);

  const addToast = useCallback((msg, type = 'info') => {
    const id = Date.now() + Math.random();
    setToasts(prev => [...prev, { id, msg, type }]);
    setTimeout(() => setToasts(prev => prev.filter(t => t.id !== id)), 4000);
  }, []);

  const getPageHref = useCallback((page, options = {}) => {
    const tab = options.uploadTab || uploadTabRef.current || 'inventory';
    return buildPageHref(page, tab);
  }, []);

  useEffect(() => {
    function handlePopState() {
      const route = parseRoute(window.location.pathname);
      setCurrentPageState(route.currentPage);
      setUploadTabState(route.uploadTab);
    }
    window.addEventListener('popstate', handlePopState);
    handlePopState();
    return () => window.removeEventListener('popstate', handlePopState);
  }, []);

  return (
    <AppContext.Provider value={{
      apiKey, setApiKey,
      currentPage: currentPageState, setCurrentPage,
      uploadTab: uploadTabState, setUploadTab,
      getPageHref,
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
