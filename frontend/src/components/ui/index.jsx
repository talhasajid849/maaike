/**
 * components/ui/index.jsx
 * ========================
 * Reusable UI primitives using Tailwind.
 * Use these everywhere instead of raw HTML — keeps the design consistent.
 *
 * Panel, Badge, Pill, ScoreBadge, Button, FormField, StatCard, SourceBadge
 */
import { sourceColor, sourceShort, scoreColorClass } from '../../config/sources.js';

// ── Panel ─────────────────────────────────────────────────────────────────────

export function Panel({ children, className = '' }) {
  return (
    <div className={`bg-bg2 border border-border rounded-lg mb-3.5 ${className}`}>
      {children}
    </div>
  );
}

export function PanelHeader({ children, className = '' }) {
  return (
    <div className={`flex items-center gap-2.5 px-4 py-3 border-b border-border ${className}`}>
      {children}
    </div>
  );
}

export function PanelTitle({ children }) {
  return (
    <span className="text-2xs font-bold uppercase tracking-widest text-text2">
      {children}
    </span>
  );
}

export function PanelBody({ children, className = '' }) {
  return <div className={`p-4 ${className}`}>{children}</div>;
}

// ── Buttons ───────────────────────────────────────────────────────────────────

const BTN_VARIANTS = {
  teal:    'bg-teal text-bg hover:bg-teal2 font-semibold',
  red:     'bg-red text-white hover:bg-red/80 font-semibold',
  outline: 'bg-transparent border border-border2 text-text2 hover:border-teal hover:text-teal',
  ghost:   'bg-transparent text-text3 hover:text-text1 p-1',
  blue:    'bg-blue text-white hover:bg-blue/80 font-semibold',
};

export function Button({ variant = 'outline', className = '', children, ...props }) {
  const base = 'inline-flex items-center gap-1.5 px-3.5 py-1.5 rounded text-sm cursor-pointer transition-all duration-150 border-none disabled:opacity-40 disabled:cursor-not-allowed';
  return (
    <button className={`${base} ${BTN_VARIANTS[variant] || BTN_VARIANTS.outline} ${className}`} {...props}>
      {children}
    </button>
  );
}

// ── FormField ─────────────────────────────────────────────────────────────────

export function FormField({ label, children, className = '' }) {
  return (
    <div className={`flex flex-col gap-1 ${className}`}>
      {label && (
        <label className="text-2xs font-bold uppercase tracking-widest text-text2">
          {label}
        </label>
      )}
      {children}
    </div>
  );
}

// Common input/select base classes
export const inputCls = 'bg-bg4 border border-border rounded text-text1 text-base px-2.5 py-1.5 outline-none focus:border-teal transition-colors w-full';
export const selectCls = inputCls;

// ── StatCard ──────────────────────────────────────────────────────────────────

export function StatCard({ label, value, color, sub }) {
  return (
    <div className="bg-bg2 border border-border rounded-lg p-3.5">
      <div className="text-2xs font-bold uppercase tracking-widest text-text2 mb-1.5">{label}</div>
      <div className="text-2xl font-bold" style={color ? { color } : {}}>
        {value ?? '—'}
      </div>
      {sub && <div className="text-xs text-text3 mt-0.5">{sub}</div>}
    </div>
  );
}

// ── ScoreBadge ────────────────────────────────────────────────────────────────

export function ScoreBadge({ score }) {
  if (!score) return <span className="text-text3 text-sm">—</span>;
  const cls = scoreColorClass(score);
  return (
    <span className={`inline-flex items-center justify-center border rounded-full px-2.5 py-0.5 font-bold text-sm min-w-[48px] ${cls}`}>
      {score.toFixed(1)}
    </span>
  );
}

// ── StatusPill ────────────────────────────────────────────────────────────────

const STATUS_CLS = {
  found:     'bg-green/10 text-green border border-green/30',
  pending:   'bg-yellow/10 text-yellow border border-yellow/30',
  not_found: 'bg-bg4 text-text3 border border-border',
};

export function StatusPill({ status }) {
  return (
    <span className={`inline-block px-2 py-0.5 rounded-full text-2xs font-semibold ${STATUS_CLS[status] || STATUS_CLS.pending}`}>
      {status}
    </span>
  );
}

// ── SourceBadge ───────────────────────────────────────────────────────────────

export function SourceBadge({ source }) {
  const color = sourceColor(source);
  const short = sourceShort(source);
  return (
    <span
      className="inline-block px-1.5 py-0.5 rounded text-2xs font-bold uppercase tracking-wide border"
      style={{ color, borderColor: color + '60', background: color + '18' }}
    >
      {short}
    </span>
  );
}

// ── DropZone ──────────────────────────────────────────────────────────────────

export function DropZone({ onClick, onDrop, isDragging, children }) {
  return (
    <div
      onClick={onClick}
      onDragOver={e => { e.preventDefault(); }}
      onDrop={onDrop}
      className={`border-2 border-dashed rounded-lg p-7 text-center text-text2 cursor-pointer transition-all duration-200
        ${isDragging ? 'border-teal text-teal bg-teal/5' : 'border-border2 hover:border-teal hover:text-teal hover:bg-teal/5'}`}
    >
      {children}
    </div>
  );
}

// ── EmptyState ────────────────────────────────────────────────────────────────

export function EmptyState({ icon = '🍷', message = 'No results found' }) {
  return (
    <div className="text-center py-12 text-text3">
      <div className="text-4xl mb-2.5">{icon}</div>
      <div>{message}</div>
    </div>
  );
}

// ── Spinner ───────────────────────────────────────────────────────────────────

export function Spinner({ className = '' }) {
  return (
    <div className={`w-4 h-4 border-2 border-border border-t-teal rounded-full animate-spin-slow ${className}`} />
  );
}