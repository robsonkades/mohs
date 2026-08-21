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
 */
export function useCountUp(value: number | null): number | null {
  const [display, setDisplay] = useState<number | null>(value);
  const previousRef = useRef<number | null>(value);
  const frameRef = useRef<number>(undefined);

  useEffect(() => {
    if (value === null || previousRef.current === null || previousRef.current === value) {
      previousRef.current = value;
      setDisplay(value);
      return;
    }

    const from = previousRef.current;
    const to = value;
    const start = performance.now();

    function tick(now: number) {
      const progress = Math.min(1, (now - start) / DURATION_MS);
      setDisplay(Math.round(from + (to - from) * easeOutCubic(progress)));
      if (progress < 1) {
        frameRef.current = requestAnimationFrame(tick);
      } else {
        previousRef.current = to;
      }
    }
    frameRef.current = requestAnimationFrame(tick);

    return () => cancelAnimationFrame(frameRef.current!);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [value]);

  return display;
}
