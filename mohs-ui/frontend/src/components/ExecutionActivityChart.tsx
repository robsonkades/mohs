import { useState } from "react";
import { Area, AreaChart, CartesianGrid, XAxis, YAxis } from "recharts";
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "@/components/ui/chart";
import { ACTIVITY_SERIES, useExecutionActivity, type ActivitySample } from "../lib/executionActivity";
import { rateFormatter } from "../lib/format";

/**
 * The gauges keep the colors the badges use — a chart is a second encoding of the same domain, so
 * a RUNNING that is blue in the table and green here would be two vocabularies.
 *
 * <p>The gauges say `· cluster` because the Overview tile beside this chart says `· this node`, and
 * they are different numbers: measured under load, 59/64/61 cluster-wide against 55/57/59 on the
 * node. Labelling only one of the two leaves the operator reading a divergence as a
 * bug.
 *
 * <p>The rate takes a color of its OWN, outside the state palette, because it is not a state —
 * there is no "Executions/s" badge to agree with. Borrowing one is a category error either way:
 * the SUCCEEDED green would promise success from a series that counts failures as work, and the
 * RUNNING blue would put two indistinguishable blues in one legend exactly under load, when both
 * series are drawn.
 */
const chartConfig = {
  ratePerSecond: { label: "Executions/s", color: "var(--metric-rate)" },
  ENQUEUED: { label: "Enqueued · cluster", color: "var(--status-neutral)" },
  RUNNING: { label: "Running · cluster", color: "var(--status-info)" },
  RETRY_WAITING: { label: "Retry scheduled · cluster", color: "var(--status-warning)" },
} satisfies ChartConfig;

const timeFormatter = new Intl.DateTimeFormat("en", {
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
});

/** Below two readings there is no line to draw — one point is a dot, not a trend. */
const MIN_SAMPLES = 2;

/** Two scales: a backlog runs to tens of thousands, a runner's concurrency is capped at 64. */
const RATE_AXIS = "rate";
const GAUGE_AXIS = "gauge";

/**
 * Only the series that actually carry work in this window.
 *
 * <p>A stacked area strokes an all-zero series along the top of the stack below it, so a state
 * that never happened draws a line exactly where the busy state's edge is — and the last one
 * rendered wins the color. On an idle backlog that reads as "retry scheduled: 410". Dropping the
 * empty series is what keeps each band's color meaning what the badges say it means.
 *
 * <p>No all-zero fallback: the rate series is always drawn, so the chart and its legend cannot go
 * blank — and keeping the three empty gauges to avoid that would produce exactly the overlapping
 * baseline this function exists to prevent.
 */
function seriesWithWork(samples: ActivitySample[]) {
  return ACTIVITY_SERIES.filter((state) => samples.some((sample) => sample[state] > 0));
}

/**
 * Activity over the last five minutes: the RATE on the left axis, the live gauges stacked on the
 * right one.
 *
 * <p>The rate is the series that answers the question the panel is asked — "is anything
 * happening"; the gauges alone could not, and the measurement says why. They
 * stay because "how deep is the queue" is a real second question — on its own axis, because a
 * backlog of 13k and a concurrency of 64 share no scale.
 *
 * <p>Deliberately NOT built from the stream's `executions` frame: that frame carries the 50 most
 * recent rows (`ORDER BY id DESC`, capped by the page size), not a time window — the span those
 * 50 cover shrinks as throughput rises, so bucketing them would draw a fall in activity that is
 * really the cap sliding. The `overview` frame carries counts the server computed over the whole
 * table, on a fixed 2s cadence, carrying the instant it read — which is what a time axis needs.
 */
export function ExecutionActivityChart() {
  const samples = useExecutionActivity();
  const [hidden, setHidden] = useState<ReadonlySet<string>>(() => new Set());
  const series = seriesWithWork(samples);
  const available = ["ratePerSecond", ...series] as const;
  function toggle(key: string) {
    setHidden((current) => {
      const next = new Set(current);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }

  if (samples.length < MIN_SAMPLES) {
    return (
      <div className="flex h-[312px] flex-col items-center justify-center gap-1.5 text-center">
        <span className="size-2 animate-pulse rounded-full bg-info motion-reduce:animate-none" />
        <p className="text-sm font-medium">Waiting for live readings</p>
        <p className="max-w-sm text-xs text-muted-foreground">
          The chart appears after two readings. Check the connection indicator if updates do not arrive.
        </p>
      </div>
    );
  }

  return (
    <div className="flex h-[312px] flex-col">
      <div className="mb-3 flex flex-wrap gap-2" role="group" aria-label="Chart series">
        {available.map((key) => (
          <button key={key} type="button" aria-pressed={!hidden.has(key)} onClick={() => toggle(key)}
            className="inline-flex items-center gap-2 rounded-md border px-2.5 py-1.5 text-xs transition-colors hover:bg-accent aria-pressed:bg-secondary focus-visible:ring-2 motion-reduce:transition-none">
            <span aria-hidden className="size-2 rounded-full" style={{ background: hidden.has(key) ? "var(--ink-muted)" : chartConfig[key].color }} />
            <span className={hidden.has(key) ? "text-muted-foreground line-through" : "text-foreground"}>{chartConfig[key].label}</span>
          </button>
        ))}
      </div>
      {available.every((key) => hidden.has(key)) && <p role="status" className="mb-2 text-xs text-muted-foreground">All series hidden. Select a series above to show it.</p>}
    <ChartContainer config={chartConfig} className="min-h-0 w-full flex-1 aspect-auto">
      <AreaChart accessibilityLayer data={samples} margin={{ left: 4, right: 12, top: 4 }}>
        <defs>
          <linearGradient id="fill-ratePerSecond" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor="var(--color-ratePerSecond)" stopOpacity={0.5} />
            <stop offset="95%" stopColor="var(--color-ratePerSecond)" stopOpacity={0.03} />
          </linearGradient>
          {series.map((state) => (
            <linearGradient key={state} id={`fill-${state}`} x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor={`var(--color-${state})`} stopOpacity={0.7} />
              <stop offset="95%" stopColor={`var(--color-${state})`} stopOpacity={0.05} />
            </linearGradient>
          ))}
        </defs>
        {/* Bound to the RATE axis: every cartesian child carries an implicit `yAxisId` of 0, so the
            moment the two axes were named there was no axis 0 left for the grid to measure, and the
            horizontal lines vanish without an error. */}
        <CartesianGrid vertical={false} yAxisId={RATE_AXIS} />
        {/* A numeric time axis preserves gaps when readings stall; categorical spacing would
            draw the samples before and after an interruption as if they were consecutive. */}
        <XAxis
          dataKey="at"
          type="number"
          scale="time"
          domain={["dataMin", "dataMax"]}
          tickLine={false}
          axisLine={false}
          tickMargin={8}
          minTickGap={48}
          tick={{ fill: "var(--muted-foreground)", fontSize: 11, fontFamily: "var(--font-mono)" }}
          tickFormatter={(at: number) => timeFormatter.format(at)}
        />
        {/* Left axis: the rate. Decimals ON — a job every second is 1.0/s, and rounding that to an
            integer is how a working system reads as idle. */}
        <YAxis
          yAxisId={RATE_AXIS}
          orientation="left"
          tickLine={false}
          axisLine={false}
          width={40}
          domain={[0, (dataMax: number) => Math.max(dataMax * 1.2, 1)]}
          tickFormatter={(value: number) => rateFormatter.format(value)}
          tick={{ fill: "var(--muted-foreground)", fontSize: 11, fontFamily: "var(--font-mono)" }}
        />
        {/* Right axis: the gauges, on their OWN scale. A backlog of 13k and a concurrency of 64 on
            one axis makes the second invisible; that is what the old single-axis stack did. */}
        <YAxis
          yAxisId={GAUGE_AXIS}
          orientation="right"
          tickLine={false}
          axisLine={false}
          width={36}
          allowDecimals={false}
          // An idle engine is the common case, and a [0,0] domain renders an axis with no ticks at
          // all — that reads as broken, not as at rest. The floor gives the baseline a scale to sit on.
          domain={[0, (dataMax: number) => Math.max(dataMax, 4)]}
          tick={{ fill: "var(--muted-foreground)", fontSize: 11, fontFamily: "var(--font-mono)" }}
        />
        <ChartTooltip
          cursor={{ stroke: "var(--ink-muted)", strokeDasharray: "3 3" }}
          content={<ChartTooltipContent indicator="dot" labelFormatter={(_, payload) =>
            timeFormatter.format(Number(payload?.[0]?.payload?.at))} />}
        />
        {/* The rate first so it draws UNDER nothing — it is the headline series, and it must stay
            readable when a backlog spike dwarfs it on the other axis. */}
        <Area
          yAxisId={RATE_AXIS}
          dataKey="ratePerSecond"
          hide={hidden.has("ratePerSecond")}
          activeDot={{ r: 4, strokeWidth: 2 }}
          type="monotone"
          fill="url(#fill-ratePerSecond)"
          stroke="var(--color-ratePerSecond)"
          strokeWidth={2}
          dot={false}
          isAnimationActive={false}
        />
        {series.map((state) => (
          <Area
            key={state}
            yAxisId={GAUGE_AXIS}
            dataKey={state}
            hide={hidden.has(state)}
            activeDot={{ r: 4, strokeWidth: 2 }}
            type="monotone"
            stackId="live"
            fill={`url(#fill-${state})`}
            stroke={`var(--color-${state})`}
            strokeWidth={1.5}
            // One point every 2s turns into solid noise with dots on; the line already is the series.
            dot={false}
            isAnimationActive={false}
          />
        ))}
      </AreaChart>
    </ChartContainer>
    </div>
  );
}
