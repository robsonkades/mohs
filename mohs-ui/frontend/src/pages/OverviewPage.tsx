import { lazy, Suspense, useState, type ReactNode } from "react";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import { fetchExecutions, fetchJobs, fetchNodes, fetchOverview, fetchRunners } from "../lib/api";
import { queryKeys } from "../lib/queryKeys";
import { STREAM_OVERVIEW_WINDOW } from "../lib/useLiveUpdates";
import { StatCard } from "../components/StatCard";
import { Panel } from "../components/Panel";
import { PageStack, StatGrid } from "../components/Layout";
import { BarBreakdown, type BreakdownRow } from "../components/BarBreakdown";
import { EmptyState, ErrorState, Spinner } from "../components/Feedback";
import { EngineStateBadge, ExecutionStateBadge, JobPausedBadge, StatusBadge } from "../components/Badge";
import { formatDuration, rateFormatter, relativeTime, shortId } from "../lib/format";
import { nodeFreshness, isNodeOnline } from "../lib/nodeStatus";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import {
  IconActivity,
  IconArrowRight,
  IconClock,
  IconGauge,
  IconListChecks,
  IconServer,
} from "../components/Icons";
import type { JobResponse } from "../types/api";

/**
 * Recharts is the single heaviest thing this dashboard ships, and it is used by exactly one panel
 * on one route. Deferring it lets the overview paint its cards and lists immediately and stream
 * the chart in behind them. The fallback is the SAME height as the chart, so the arrival swaps
 * pixels in place instead of pushing the page down.
 */
const ExecutionActivityChart = lazy(() =>
  import("../components/ExecutionActivityChart").then((module) => ({ default: module.ExecutionActivityChart })),
);

const CHART_HEIGHT = "h-[220px]";

/**
 * The windows the selector offers. `PT1M` is first on purpose: it is the one the SSE stream pushes,
 * so opening the dashboard on it means the snapshot arrives pushed, with no extra GET — the other
 * two fall back to polling, which is the price of choosing a window.
 */
const THROUGHPUT_WINDOWS = [STREAM_OVERVIEW_WINDOW, "PT15M", "PT1H"] as const;

/** The default runner — `RunnerRegistry.DEFAULT_RUNNER`, the one the server refuses to boot without. */
const DEFAULT_RUNNER = "io";

/** "…" while loading, "—" if this one metric failed — a broken metric never blocks the rest of the page. */
function statValue(isPending: boolean, isError: boolean, value: string | number) {
  if (isPending) return "…";
  if (isError) return "—";
  return value;
}

/** The destination is optional: a node without a heartbeat has no page to link to in this scope. */
function AttentionRow({
  badge,
  children,
  meta,
  to,
  search,
}: {
  badge: ReactNode;
  children: ReactNode;
  meta: string;
  to?: string;
  search?: Record<string, string>;
}) {
  return (
    <li className="flex items-center gap-3 py-2.5">
      {badge}
      {to ? (
        <Link to={to} search={search} className="min-w-0 truncate text-sm hover:text-primary hover:underline">
          {children}
        </Link>
      ) : (
        <span className="min-w-0 truncate text-sm">{children}</span>
      )}
      <span className="ml-auto shrink-0 text-xs text-muted-foreground">{meta}</span>
    </li>
  );
}

function ViewAllLink({ to, search, label }: { to: string; search?: Record<string, string>; label: string }) {
  return (
    <Link to={to} search={search} className="mono-label flex items-center gap-1 text-primary hover:text-primary/80">
      {label}
      <IconArrowRight className="size-3" />
    </Link>
  );
}

/**
 * What pausing this job actually stopped.
 *
 * <p>Pause suspends the TRIGGER and nothing else: `POST /jobs/{key}/schedule` is still accepted
 * and still runs (REST-API-DESIGN, and `JobStore#findDueRecurring` — "pause bloqueia exatamente o
 * trigger; on-demand continua valendo mesmo pausado"). For a recurring job that reads the way an
 * operator expects. For an ON_DEMAND job there is no trigger to suspend, so the flag changes
 * nothing at all — and this row used to claim "will not fire" for exactly that job, which is the
 * opposite of what happens the next time anyone posts to it.
 */
function pausedConsequence(job: JobResponse): string {
  return job.schedule.type === "ON_DEMAND"
    ? "is paused, but it has no trigger to suspend — manual runs still execute"
    : "is paused: it will not fire on its schedule, though manual runs still execute";
}

/**
 * There is no "upcoming fires" endpoint — but `GET /jobs` already returns `nextFireAt` for every
 * job, and the definition list is bounded by nature (it is what the application declared at boot),
 * not by history. Sorting client-side is honest here; it would be wrong if the list were paged.
 */
type ScheduledJob = JobResponse & { nextFireAt: string };

function upcoming(jobs: JobResponse[], limit: number): ScheduledJob[] {
  return jobs
    .filter((job): job is ScheduledJob => job.nextFireAt !== null && !job.paused)
    .sort((left, right) => left.nextFireAt.localeCompare(right.nextFireAt))
    .slice(0, limit);
}

export function OverviewPage() {
  const [window, setWindow] = useState<string>(STREAM_OVERVIEW_WINDOW);

  const overview = useQuery({
    queryKey: queryKeys.overview(window),
    queryFn: () => fetchOverview(window),
    // Switching the window changes the key, and dropping to `undefined` would blank the throughput
    // panel and the three stat cards fed by this query. Keeping the previous snapshot lets the new
    // one ease in over it — the numbers change, the layout does not.
    placeholderData: keepPreviousData,
  });
  const jobs = useQuery({ queryKey: queryKeys.jobs(), queryFn: fetchJobs });
  const nodes = useQuery({ queryKey: queryKeys.nodes(), queryFn: fetchNodes });
  // Seeded by the stream's `runners` frame every 2s (ADR-0063); the fetch is the fallback for when
  // SSE never comes up.
  const runners = useQuery({ queryKey: queryKeys.runners(), queryFn: fetchRunners });
  const failedRecent = useQuery({
    queryKey: queryKeys.executions({ status: "FAILED" }),
    queryFn: () => fetchExecutions({ status: "FAILED" }, undefined, 5),
  });

  const counts = overview.data?.executionCountsByStatus;
  const pausedJobs = (jobs.data ?? []).filter((job) => job.paused);
  const onlineNodes = (nodes.data ?? []).filter((node) => isNodeOnline(node.lastHeartbeatAt)).length;
  const staleNodes = (nodes.data ?? []).filter((node) => !isNodeOnline(node.lastHeartbeatAt));

  const attentionLoaded = !!failedRecent.data && !!jobs.data && !!nodes.data;
  const attentionEmpty =
    attentionLoaded && failedRecent.data!.items.length === 0 && pausedJobs.length === 0 && staleNodes.length === 0;

  // Node-local by contract (ADR-0063): `max` is this process's cap and `/runners` answers for
  // whichever node served the request. overview.RUNNING is cluster-wide, so the two must not be
  // divided into each other — the tile says "this node" instead of implying a cluster ratio.
  //
  // Matched by NAME, not by `mode === "IO"`: `/runners` is sorted by name, so an app that declares
  // its own IO runner would silently tile that one instead; and the default runner is allowed to
  // be CPU-mode (`mohs.runners.io.mode=cpu`), which would tile nothing. The name is the identity
  // the server refuses to boot without (RunnerRegistry.DEFAULT_RUNNER).
  const defaultRunner = (runners.data ?? []).find((runner) => runner.name === DEFAULT_RUNNER);
  const concurrencyLabel = defaultRunner ? `${defaultRunner.running}/${defaultRunner.max}` : "no runner";
  // One `?.`, on the pending query and nothing else: both writers of this cache key go through
  // `isOverview` (the fetch and the stream's frame), so `recent` is there whenever `data` is. A
  // second `?.` here would be dead code arguing that the guard cannot be trusted.
  const rate = overview.data?.recent.ratePerSecond;
  const rateLabel = rate === undefined ? "—" : rateFormatter.format(rate);

  const throughput = overview.data?.throughput;
  const throughputRows: BreakdownRow[] = [
    { label: "SUCCEEDED", value: throughput?.succeeded ?? 0, color: "var(--status-good)" },
    { label: "FAILED", value: throughput?.failed ?? 0, color: "var(--status-critical)" },
  ];

  const nextUp = upcoming(jobs.data ?? [], 6);

  return (
    <PageStack>
      <StatGrid columns={6}>
        <StatCard
          label="Jobs"
          value={statValue(jobs.isPending, !!jobs.error, jobs.data?.length ?? 0)}
          icon={<IconListChecks className="size-4" />}
          accent
        />
        <StatCard
          label="Paused"
          value={statValue(jobs.isPending, !!jobs.error, pausedJobs.length)}
          icon={<IconGauge className="size-4" />}
        />
        <StatCard
          label="Executions/s"
          value={statValue(overview.isPending, !!overview.error, rateLabel)}
          icon={<IconActivity className="size-4" />}
        />
        <StatCard
          label={`Running · ${DEFAULT_RUNNER} @ this node`}
          value={statValue(runners.isPending, !!runners.error, concurrencyLabel)}
          icon={<IconGauge className="size-4" />}
        />
        <StatCard
          label="Retry scheduled"
          value={statValue(overview.isPending, !!overview.error, counts?.RETRY_WAITING ?? 0)}
          icon={<IconClock className="size-4" />}
        />
        <StatCard
          label="Nodes online"
          value={statValue(nodes.isPending, !!nodes.error, `${onlineNodes}/${nodes.data?.length ?? 0}`)}
          icon={<IconServer className="size-4" />}
        />
      </StatGrid>

      <Panel title="Needs attention">
        {!attentionLoaded && !failedRecent.error && <Spinner label="Checking" />}
        {failedRecent.error && (
          <ErrorState message={failedRecent.error.message} onRetry={() => failedRecent.refetch()} />
        )}
        {attentionEmpty && (
          <div className="flex items-center gap-2.5 py-6">
            <span className="size-2 shrink-0 rounded-full bg-good" />
            <p className="text-sm font-medium">All clear — nothing needs attention right now.</p>
          </div>
        )}
        {attentionLoaded && !attentionEmpty && (
          <ul className="flex flex-col divide-y">
            {failedRecent.data!.items.map((execution) => (
              <AttentionRow
                key={execution.executionId}
                badge={<ExecutionStateBadge state="FAILED" />}
                meta={relativeTime(execution.scheduledAt)}
                to="/executions"
                search={{ executionId: execution.executionId }}
              >
                {execution.jobKey} failed ·{" "}
                <span className="font-mono text-xs">{shortId(execution.executionId)}</span>
              </AttentionRow>
            ))}
            {pausedJobs.map((job) => (
              <AttentionRow
                key={job.jobKey}
                badge={<JobPausedBadge paused />}
                meta={job.schedule.type === "ON_DEMAND" ? "on demand" : "recurring"}
                to="/jobs"
                search={{ jobKey: job.jobKey }}
              >
                {job.jobKey} {pausedConsequence(job)}
              </AttentionRow>
            ))}
            {staleNodes.map((node) => {
              const status = nodeFreshness(node.lastHeartbeatAt);
              return (
                <AttentionRow
                  key={node.nodeId}
                  badge={<StatusBadge tone={status.tone} label={status.label} />}
                  meta={`last heartbeat ${relativeTime(node.lastHeartbeatAt)}`}
                >
                  <span className="font-mono text-xs">{node.nodeId}</span> stopped sending heartbeats
                </AttentionRow>
              );
            })}
          </ul>
        )}
      </Panel>

      <Panel title="Activity">
        <Suspense fallback={<div className={CHART_HEIGHT} aria-hidden />}>
          <ExecutionActivityChart />
        </Suspense>
        <p className="pt-2 text-xs text-muted-foreground">
          Executions per second on the left, queue and concurrency on the right — two scales because
          a backlog and a concurrency cap share none. The rate is what says the engine is working: a
          fast job is queued and owned for milliseconds, so the gauges read zero even under load.
        </p>
      </Panel>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Panel
          title="Throughput"
          action={
            <ToggleGroup
              type="single"
              size="sm"
              value={window}
              onValueChange={(next) => next && setWindow(next)}
            >
              {THROUGHPUT_WINDOWS.map((candidate) => (
                <ToggleGroupItem key={candidate} value={candidate} className="mono-label px-2">
                  {formatDuration(candidate)}
                </ToggleGroupItem>
              ))}
            </ToggleGroup>
          }
        >
          {overview.isPending && <Spinner label="Loading" />}
          {overview.error && <ErrorState message={overview.error.message} onRetry={() => overview.refetch()} />}
          {throughput && <BarBreakdown rows={throughputRows} />}
          {throughput && (
            <p className="pt-2 text-xs text-muted-foreground">
              Terminal executions in the last {formatDuration(throughput.window)}.
            </p>
          )}
        </Panel>

        <Panel title="Nodes">
          {nodes.isPending && <Spinner label="Loading" />}
          {nodes.error && <ErrorState message={nodes.error.message} onRetry={() => nodes.refetch()} />}
          {nodes.data && nodes.data.length === 0 && <EmptyState title="No node heartbeats recorded yet" />}
          {nodes.data && nodes.data.length > 0 && (
            <ul className="flex flex-col divide-y">
              {nodes.data.map((node) => {
                const status = nodeFreshness(node.lastHeartbeatAt);
                return (
                  <li key={node.nodeId} className="flex items-center justify-between gap-3 py-2.5">
                    <span className="truncate font-mono text-sm" title={node.nodeId}>
                      {node.nodeId}
                    </span>
                    <div className="flex shrink-0 items-center gap-3">
                      <EngineStateBadge state={node.state} />
                      <span className="text-xs text-muted-foreground">
                        {relativeTime(node.lastHeartbeatAt)}
                      </span>
                      <StatusBadge tone={status.tone} label={status.label} live={status.tone === "good"} />
                    </div>
                  </li>
                );
              })}
            </ul>
          )}
        </Panel>
      </div>

      <Panel title="Up next" action={<ViewAllLink to="/jobs" label="View all" />}>
        {jobs.isPending && <Spinner label="Loading" />}
        {jobs.error && <ErrorState message={jobs.error.message} onRetry={() => jobs.refetch()} />}
        {jobs.data && nextUp.length === 0 && (
          <EmptyState
            title="Nothing scheduled"
            description="Recurring jobs that are not paused show up here, soonest first."
          />
        )}
        {nextUp.length > 0 && (
          <ul className="flex flex-col divide-y">
            {nextUp.map((job) => (
              <li key={job.jobKey} className="flex items-center justify-between gap-3 py-2.5">
                <div className="flex min-w-0 flex-col">
                  <Link
                    to="/jobs"
                    search={{ jobKey: job.jobKey }}
                    className="truncate text-sm font-medium hover:text-primary hover:underline"
                  >
                    {job.jobKey}
                  </Link>
                  <span className="truncate text-xs text-muted-foreground">{job.name}</span>
                </div>
                <span className="shrink-0 font-mono text-xs tabular-nums text-muted-foreground">
                  {relativeTime(job.nextFireAt)}
                </span>
              </li>
            ))}
          </ul>
        )}
      </Panel>
    </PageStack>
  );
}

