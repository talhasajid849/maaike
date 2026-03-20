import { useEffect } from 'react';
import { useApp } from './context/AppContext.jsx';
import { statsApi } from './services/api.js';
import SignIn    from './pages/SignIn.jsx';
import Header    from './components/Header.jsx';
import Toast     from './components/Toast.jsx';
import WineModal from './components/WineModal.jsx';
import Dashboard   from './pages/Dashboard.jsx';
import WineList    from './pages/WineList.jsx';
import AddWine     from './pages/AddWine.jsx';
import UploadCSV   from './pages/UploadCSV.jsx';
import Sources     from './pages/Sources.jsx';
import Settings    from './pages/Settings.jsx';

/**
 * PAGES registry — add new pages here only.
 * To add a page: create the component, add it here, add nav entry in Header.jsx.
 */
const PAGES = {
  dashboard: Dashboard,
  wines:     WineList,
  addwine:   AddWine,
  upload:    UploadCSV,
  sources:   Sources,
  settings:  Settings,
};

export default function App() {
  const { apiKey, setApiKey, currentPage, wineModalId } = useApp();

  // Verify stored API key on mount
  useEffect(() => {
    if (!apiKey) return;
    statsApi.get(apiKey).catch(() => setApiKey(''));
  }, []); // eslint-disable-line

  if (!apiKey) return <SignIn />;

  const PageComponent = PAGES[currentPage] || Dashboard;

  return (
    <div className="flex flex-col min-h-screen">
      <Header />
      <main className="flex-1 p-[18px_20px] max-w-[1700px] w-full mx-auto">
        <PageComponent />
      </main>
      {wineModalId && <WineModal />}
      <Toast />
    </div>
  );
}