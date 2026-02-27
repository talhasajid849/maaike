import { useEffect } from 'react';
import { useApp } from './context/AppContext.jsx';
import { ap } from './api.js';
import SignIn from './pages/SignIn.jsx';
import Header from './components/Header.jsx';
import Toast from './components/Toast.jsx';
import WineModal from './components/WineModal.jsx';
import Dashboard from './pages/Dashboard.jsx';
import WineList from './pages/WineList.jsx';
import AddWine from './pages/AddWine.jsx';
import UploadCSV from './pages/UploadCSV.jsx';
import Sources from './pages/Sources.jsx';
import Settings from './pages/Settings.jsx';

const PAGES = {
  dashboard: Dashboard,
  wines: WineList,
  addwine: AddWine,
  upload: UploadCSV,
  sources: Sources,
  settings: Settings,
};

export default function App() {
  const { apiKey, setApiKey, currentPage, wineModalId } = useApp();

  // Verify stored key on mount
  useEffect(() => {
    if (!apiKey) return;
    ap('/stats', {}, apiKey).catch(() => setApiKey(''));
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  if (!apiKey) return <SignIn />;

  const PageComponent = PAGES[currentPage] || Dashboard;

  return (
    <div id="app">
      <Header />
      <main>
        <PageComponent />
      </main>
      {wineModalId && <WineModal />}
      <Toast />
    </div>
  );
}
