import { useApp } from '../context/AppContext.jsx';

const TYPE_CLS = {
  success: 'border-green text-green',
  error:   'border-red text-red',
  info:    'border-blue text-blue',
};

export default function Toast() {
  const { toasts } = useApp();
  if (!toasts.length) return null;
  return (
    <div className="fixed bottom-5 right-5 z-[9999] flex flex-col gap-1.5">
      {toasts.map(t => (
        <div
          key={t.id}
          className={`bg-bg3 border rounded px-3.5 py-2 text-sm min-w-[220px] animate-toast-in ${TYPE_CLS[t.type] || TYPE_CLS.info}`}
        >
          {t.msg}
        </div>
      ))}
    </div>
  );
}