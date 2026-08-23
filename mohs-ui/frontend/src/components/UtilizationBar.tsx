import { TONE_COLOR_VAR, type Tone } from "./Badge";
import { cn } from "@/lib/utils";

/**
 * Where "busy" turns into "worth looking at". One scale for every occupancy on the dashboard —
 * a runner at 92% and a rate limit at 92% are the same news, so they get the same color.
 */
export function utilizationTone(percent: number): Tone {
  if (percent >= 90) {
    return "critical";
  }
  if (percent >= 75) {
    return "serious";
  }
  return "good";
}

/**
 * Occupancy as a recessed groove with a semantic fill.
 *
 * <p>Two things were wrong with using the generic `Progress` here. The track was `bg-muted`,
 * which this theme aliases to `--surface` — the exact color of the card it sits on, so the bar
 * was invisible, and at 0% the fill is translated away too: the whole control disappeared
 * precisely when it was saying "idle". And the fill was the primary periwinkle at every value,
 * so 10% and 95% drew the same picture; the only color change was a cliff at overload, which
 * meant 7/8 and 1/8 looked identical.
 *
 * <p>The groove is `--page`, one tonal layer BELOW the card — depth by layering, which is how
 * this design system does depth (never a shadow). The fill carries the status palette through
 * {@link utilizationTone}, so saturation is legible from across a room, and the value is exposed
 * to assistive technology instead of being pure decoration.
 */
export function UtilizationBar({
  percent,
  label,
  tone = utilizationTone(percent),
  className,
}: {
  percent: number;
  /** What is being measured — read out by screen readers, which a bare bar never is. */
  label: string;
  tone?: Tone;
  className?: string;
}) {
  const clamped = Math.min(100, Math.max(0, percent));

  return (
    <div
      role="progressbar"
      aria-label={label}
      aria-valuenow={Math.round(clamped)}
      aria-valuemin={0}
      aria-valuemax={100}
      // The ring is what makes an EMPTY meter still read as a meter. `--hairline` is only a
      // shade off the card and disappeared at 0%; `--baseline` is the system's outline-variant,
      // quiet at half strength but unmistakably an edge.
      className={cn("h-1.5 w-full overflow-hidden rounded-full bg-page ring-1 ring-inset ring-baseline/50", className)}
    >
      <div
        className="h-full rounded-full transition-[width,background-color] duration-500 ease-out motion-reduce:transition-none"
        style={{ width: `${clamped}%`, backgroundColor: TONE_COLOR_VAR[tone] }}
      />
    </div>
  );
}
