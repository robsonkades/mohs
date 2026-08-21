import { Area, AreaChart, CartesianGrid, XAxis, YAxis } from "recharts";
import {
  ChartContainer,
  ChartLegend,
  ChartLegendContent,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "@/components/ui/chart";
import { ACTIVITY_SERIES, useExecutionActivity, type ActivitySample } from "../lib/executionActivity";
import type { ExecutionState } from "../types/api";

/**
 * Live work by state, in the same colors the badges use — a chart is a second encoding of the
 * same domain, so a RUNNING that is blue in the table and green here would be two vocabularies.
 */
const chartConfig = {
  ENQUEUED: { label: "Enqueued", color: "var(--status-neutral)" },
  RUNNING: { label: "Running", color: "var(--status-info)" },
  RETRY_SCHEDULED: { label: "Retry scheduled", color: "var(--status-warning)" },
} satisfies ChartConfig;

const timeFormatter = new Intl.DateTimeFormat("en", {
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
});

/** Below two readings there is no line to draw — one point is a dot, not a trend. */
const MIN_SAMPLES = 2;

/**
 * Only the series that actually carry work in this window.
 *
 * <p>A stacked area strokes an all-zero series along the top of the stack below it, so a state
 * that never happened draws a line exactly where the busy state's edge is — and the last one
 * rendered wins the color. On an idle backlog that reads as "retry scheduled: 410". Dropping the
 * empty series is what keeps each band's color meaning what the badges say it means.
 *
 * <p>All zero (idle engine) keeps all three: the lines overlap harmlessly on the baseline, and the
 * legend still says what this chart tracks instead of going blank.
 */
function seriesWithWork(samples: ActivitySample[]): readonly ExecutionState[] {
  const active = ACTIVITY_SERIES.filter((state) => samples.some((sample) => sample[state] > 0));
  return active.length > 0 ? active : ACTIVITY_SERIES;
}

/**
 * The live-work series pushed by `GET /overview/stream`, stacked so the top edge reads as total
 * work in flight while each band keeps its own magnitude.
 *
 * <p>Deliberately NOT built from the stream's `executions` frame: that frame carries the 50 most
 * recent rows (`ORDER BY id DESC`, capped by the page size), not a time window — the span those
 * 50 cover shrinks as throughput rises, so bucketing them would draw a fall in activity that is
 * really the cap sliding. The `overview` frame carries counts the server computed over the whole
 * table, on a fixed 2s cadence, carrying the instant it read — which is what a time axis needs.
 */
export function ExecutionActivityChart() {
  const samples = useExecutionActivity();

  if (samples.length < MIN_SAMPLES) {
    return (
      <div className="flex h-[220px] flex-col items-center justify-center gap-1.5 text-center">
        <span className="h-2 w-2 animate-pulse rounded-full bg-info" />
        <p className="text-sm font-medium">Sampling…</p>
        <p className="max-w-sm text-xs text-muted-foreground">
          The series is built from what the stream pushes every 2s, so it starts when this page
          opens and is not restored on reload.
        </p>
      </div>
    );
  }

  const series = seriesWithWork(samples);

  return (
    <ChartContainer config={chartConfig} className="aspect-auto h-[220px] w-full">
      <AreaChart accessibilityLayer data={samples} margin={{ left: 4, right: 12, top: 4 }}>
        <defs>
          {series.map((state) => (
            <linearGradient key={state} id={`fill-${state}`} x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor={`var(--color-${state})`} stopOpacity={0.7} />
              <stop offset="95%" stopColor={`var(--color-${state})`} stopOpacity={0.05} />
            </linearGradient>
          ))}
        </defs>
        <CartesianGrid vertical={false} />
        {/* Eixo de TEMPO, não de categoria: sem `type="number"` o recharts espaça os pontos
            igualmente por índice, e um stall do stream some: as leituras antes e depois do buraco
            ficariam lado a lado, com a mesma largura de dois pontos consecutivos, escondendo
            exatamente o que vale a pena ver. */}
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
        <YAxis
          tickLine={false}
          axisLine={false}
          width={32}
          allowDecimals={false}
          // An idle engine is the common case, and a [0,0] domain renders an axis with no ticks at
          // all — that reads as broken, not as at rest. The floor gives the baseline a scale to sit on.
          domain={[0, (dataMax: number) => Math.max(dataMax, 4)]}
          tick={{ fill: "var(--muted-foreground)", fontSize: 11, fontFamily: "var(--font-mono)" }}
        />
        <ChartTooltip
          cursor={false}
          content={<ChartTooltipContent indicator="dot" labelFormatter={(_, payload) =>
            timeFormatter.format(Number(payload?.[0]?.payload?.at))} />}
        />
        {series.map((state) => (
          <Area
            key={state}
            dataKey={state}
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
        <ChartLegend content={<ChartLegendContent />} />
      </AreaChart>
    </ChartContainer>
  );
}
