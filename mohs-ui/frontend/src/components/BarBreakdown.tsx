import { useCountUp } from "@/lib/useCountUp";

export interface BreakdownRow {
  label: string;
  value: number;
  color: string;
}

/**
 * A compact per-category magnitude breakdown — each row is direct-labeled, so color never
 * carries identity alone (dataviz: "identity never color-alone").
 *
 * <p>Plain CSS instead of a chart library on purpose: this panel is fed by the 2s stream, and a
 * Recharts bar replays its enter animation from zero on every new snapshot — the numbers barely
 * move, but the panel reads as reloading. A width transition eases from the previous value to the
 * next one, which is what a live magnitude should look like.
 */
export function BarBreakdown({ rows }: { rows: BreakdownRow[] }) {
  const max = Math.max(1, ...rows.map((row) => row.value));

  return (
    <div className="flex flex-col gap-3 py-1">
      {rows.map((row) => (
        <BreakdownBar key={row.label} row={row} max={max} />
      ))}
    </div>
  );
}

function BreakdownBar({ row, max }: { row: BreakdownRow; max: number }) {
  const animated = useCountUp(row.value) ?? row.value;

  return (
    <div className="flex items-center gap-3">
      <span className="mono-label w-28 shrink-0 truncate text-muted-foreground">{row.label}</span>
      <div className="h-1.5 min-w-0 flex-1 overflow-hidden rounded-full bg-muted">
        <div
          className="h-full rounded-full transition-[width] duration-500 ease-out motion-reduce:transition-none"
          style={{ width: `${(row.value / max) * 100}%`, backgroundColor: row.color }}
        />
      </div>
      <span className="w-12 shrink-0 text-right font-mono text-xs tabular-nums">{animated}</span>
    </div>
  );
}
