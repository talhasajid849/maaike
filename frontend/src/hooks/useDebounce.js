/**
 * hooks/useDebounce.js
 * =====================
 * Delay a callback until the user stops typing.
 *
 * Uses a ref to always call the *latest* fn even if the callback identity
 * changes between renders (avoids stale-closure bugs with useCallback deps).
 */
import { useRef } from 'react';

export function useDebounce(fn, delay = 320) {
  const fnRef    = useRef(fn);
  fnRef.current  = fn;          // always points to the latest callback
  const timerRef = useRef(null);

  // Return a stable function (created once, never changes identity)
  const stableRef = useRef((...args) => {
    clearTimeout(timerRef.current);
    timerRef.current = setTimeout(() => fnRef.current(...args), delay);
  });
  return stableRef.current;
}