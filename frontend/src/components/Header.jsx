import { useApp } from '../context/AppContext.jsx';

const NAV = [
  { id: 'dashboard', label: 'Dashboard'   },
  { id: 'wines',     label: 'Wine List'   },
  { id: 'addwine',   label: 'Add Wine'    },
  { id: 'upload',    label: 'Upload'      },
  { id: 'xlsxfiles', label: 'XLSX Files'  },
  { id: 'sources',   label: 'Sources'     },
  { id: 'settings',  label: 'Settings'    },
];

export default function Header() {
  const { currentPage, setCurrentPage, getPageHref } = useApp();

  return (
    <header className="bg-bg2 border-b border-border h-[50px] flex items-center gap-3.5 px-5 sticky top-0 z-50">
      <div className="text-lg font-extrabold tracking-[3px] text-teal whitespace-nowrap">
        MAAIKE
        <span className="text-text3 font-normal text-2xs tracking-wide ml-2">
          WINE INTELLIGENCE
        </span>
      </div>

      <nav className="flex gap-0.5 ml-auto">
        {NAV.map(n => (
          <a
            key={n.id}
            href={getPageHref(n.id)}
            onClick={(e) => {
              e.preventDefault();
              setCurrentPage(n.id);
            }}
            className={`px-3 py-1.5 rounded text-sm border-none transition-all duration-150 cursor-pointer
              ${currentPage === n.id
                ? 'bg-bg4 text-text1'
                : 'bg-transparent text-text2 hover:bg-bg4 hover:text-text1'
              }`}
          >
            {n.label}
          </a>
        ))}
      </nav>
    </header>
  );
}
