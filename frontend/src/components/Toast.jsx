import { useApp } from '../context/AppContext.jsx';

export default function Toast() {
  const { toasts } = useApp();

  return (
    <div id="toast-container">
      {toasts.map(t => (
        <div key={t.id} className={`toast ${t.type}`}>{t.msg}</div>
      ))}
    </div>
  );
}
