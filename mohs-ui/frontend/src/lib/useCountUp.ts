import { useEffect, useRef, useState } from "react";

const DURATION_MS = 600;

function easeOutCubic(t: number): number {
  return 1 - Math.pow(1 - t, 3);
}

/**
 * Eases a StatCard's numeric value toward each new update instead of jump-cutting to it, so a
 * live refresh reads as a count (100, 101, 102…) rather than a snap. The very first real value
 * (arriving after a `null` loading state) is shown immediately — only value-to-value changes
 * animate, so the card doesn't zoom up from zero on initial load.
 *
 * <p>Every animation starts from what is on screen right now, not from the last value that
 * finished animating. A snapshot arriving before the current run lands — a manual refresh lands
 * whenever the request returns — would otherwise resume from the stale value and jump the number
 * backwards.
 */
export function useCountUp(value: number | null): number | null {
  const [display, setDisplay] = useState<number | null>(value);
  const displayedRef = useRef<number | null>(value);
  const frameRef = useRef<number>(undefined);

  useEffect(() => {
    const displayed = displayedRef.current;

    if (value === null || displayed === null || displayed === value) {
      displayedRef.current = value;
      setDisplay(value);
      return;
    }

    const from: number = displayed;
    const to: number = value;
    const start = performance.now();

    function tick(now: number) {
      const progress = Math.min(1, (now - start) / DURATION_MS);
      const current = Math.round(from + (to - from) * easeOutCubic(progress));
      displayedRef.current = current;
      setDisplay(current);
      if (progress < 1) {
        frameRef.current = requestAnimationFrame(tick);
      }
    }

    frameRef.current = requestAnimationFrame(tick);

    return () => cancelAnimationFrame(frameRef.current!);
  }, [value]);

  return display;
}
