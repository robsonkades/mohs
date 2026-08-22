import { useState, type ReactNode } from "react";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import { fetchExecutions, fetchJobs, fetchNodes, fetchOverview } from "../lib/api";
import { queryKeys } from "../lib/queryKeys";
import { STREAM_OVERVIEW_WINDOW } from "../lib/useLiveUpdates";
import { StatCard } from "../components/StatCard";
import { Panel } from "../components/Panel";
import { BarBreakdown, type BreakdownRow } from "../components/BarBreakdown";
import { ExecutionActivityChart } from "../components/ExecutionActivityChart";
import { EmptyState, ErrorState, Spinner } from "../components/Feedback";
import { EngineStateBadge, ExecutionStateBadge, JobPausedBadge, StatusBadge } from "../components/Badge";
import { formatDuration, relativeTime, shortId } from "../lib/format";
import { nodeFreshness, isNodeOnline } from "../lib/nodeStatus";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import {
  IconActivity,
  IconArrowRight,
  IconClock,
  IconGauge,
  IconListChecks,
  IconServer,
} from "../components/icons";
import { LIVE_STATES, type JobResponse } from "../types/api";

/**
 * The windows the selector offers. `PT1M` is first on purpose: it is the one the SSE stream pushes,
 * so opening the dashboard on it means the snapshot arrives pushed, with no extra GET — the other
 * two fall back to polling, which is the price of choosing a window.
 */
const THROUGHPUT_WINDOWS = [STREAM_OVERVIEW_WINDOW, "PT15M", "PT1H"] as const;

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
      <IconArrowRight className="h-3 w-3" />
    </Link>
  );
}

/**
 * There is no "upcoming fires" endpoint — but `GET /jobs` already returns `nextFireAt` for every
 * job, and the definition list is bounded by nature (it is what the application declared at boot),
 * not by history. Sorting client-side is honest here; it would be wrong if the list were paged.
 */
function upcoming(jobs: JobResponse[], limit: number): JobResponse[] {
  return jobs
    .filter((job) => job.nextFireAt !== null && !job.paused)
    .sort((left, right) => left.nextFireAt!.localeCompare(right.nextFireAt!))
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
  const failedRecent = useQuery({
    queryKey: queryKeys.executions({ status: "FAILED" }),
    queryFn: () => fetchExecutions({ status: "FAILED" }, undefined, 5),
  });

  const counts = overview.data?.executionCountsByStatus;
  const liveTotal = LIVE_STATES.reduce((sum, state) => sum + (counts?.[state] ?? 0), 0);
  const pausedJobs = (jobs.data ?? []).filter((job) => job.paused);
  const onlineNodes = (nodes.data ?? []).filter((node) => isNodeOnline(node.lastHeartbeatAt)).length;
  const staleNodes = (nodes.data ?? []).filter((node) => !isNodeOnline(node.lastHeartbeatAt));

  const attentionLoaded = !!failedRecent.data && !!jobs.data && !!nodes.data;
  const attentionEmpty =
    attentionLoaded && failedRecent.data!.items.length === 0 && pausedJobs.length === 0 && staleNodes.length === 0;

  const throughput = overview.data?.throughput;
  const throughputRows: BreakdownRow[] = [
    { label: "SUCCEEDED", value: throughput?.succeeded ?? 0, color: "var(--status-good)" },
    { label: "FAILED", value: throughput?.failed ?? 0, color: "var(--status-critical)" },
  ];

  const nextUp = upcoming(jobs.data ?? [], 6);

  return (
    <div className="flex flex-col gap-6">
      <div className="grid grid-cols-2 gap-4 lg:grid-cols-3 xl:grid-cols-6">
        <StatCard
          label="Jobs"
          value={statValue(jobs.isPending, !!jobs.error, jobs.data?.length ?? 0)}
          icon={<IconListChecks className="h-4 w-4" />}
          accent
        />
        <StatCard
          label="Paused"
          value={statValue(jobs.isPending, !!jobs.error, pausedJobs.length)}
          icon={<IconGauge className="h-4 w-4" />}
        />
        <StatCard
          label="Live executions"
          value={statValue(overview.isPending, !!overview.error, liveTotal)}
          icon={<IconActivity className="h-4 w-4" />}
        />
        <StatCard
          label="Running"
          value={statValue(overview.isPending, !!overview.error, counts?.RUNNING ?? 0)}
          icon={<IconActivity className="h-4 w-4" />}
        />
        <StatCard
          label="Retry scheduled"
          value={statValue(overview.isPending, !!overview.error, counts?.RETRY_WAITING ?? 0)}
          icon={<IconClock className="h-4 w-4" />}
        />
        <StatCard
          label="Nodes online"
          value={statValue(nodes.isPending, !!nodes.error, `${onlineNodes}/${nodes.data?.length ?? 0}`)}
          icon={<IconServer className="h-4 w-4" />}
        />
      </div>

      <Panel title="Needs attention">
        {!attentionLoaded && !failedRecent.error && <Spinner label="Checking" />}
        {failedRecent.error && (
          <ErrorState message={failedRecent.error.message} onRetry={() => failedRecent.refetch()} />
        )}
        {attentionEmpty && (
          <div className="flex items-center gap-2.5 py-6">
            <span className="h-2 w-2 shrink-0 rounded-full bg-good" />
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
                {job.jobKey} is paused and will not fire
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

      <Panel title="Live work">
        <ExecutionActivityChart />
        <p className="pt-2 text-xs text-muted-foreground">
          Terminal states carry no all-time count by contract — history grows without bound, and this
          is a polling anchor. Recent terminal activity is the throughput panel.
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
                  {relativeTime(job.nextFireAt!)}
                </span>
              </li>
            ))}
          </ul>
        )}
      </Panel>
    </div>
  );
}

