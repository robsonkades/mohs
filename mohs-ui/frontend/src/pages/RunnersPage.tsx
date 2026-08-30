import { useQuery } from "@tanstack/react-query";
import { fetchNodes, fetchRunners } from "../lib/api";
import { queryKeys } from "../lib/queryKeys";
import { EmptyState, ErrorState, Spinner } from "../components/Feedback";
import { Panel } from "../components/Panel";
import { PageStack, StatGrid } from "../components/Layout";
import { StatCard } from "../components/StatCard";
import { Badge } from "@/components/ui/badge";
import { UtilizationBar, utilizationTone } from "../components/UtilizationBar";
import { IconServer } from "../components/Icons";
import type { RunnerResponse } from "../types/api";

/**
 * How busy a runner is right now, as a percentage of its declared ceiling. Clamped because
 * `running` counts what was accepted: a CPU runner with a queue can legitimately hold more than
 * `max` in flight, and a bar past 100% would read as a bug rather than as a backlog.
 */
function loadPercent(runner: RunnerResponse): number {
  if (runner.max <= 0) {
    return 0;
  }
  return Math.min(100, Math.max(0, (runner.running / runner.max) * 100));
}

function RunnerRow({ runner }: { runner: RunnerResponse }) {
  // Fila é conceito do modo CPU. No IO não há nada entre aceitar e executar — se running
  // passar de max ali, é a janela entre o incremento do contador e o semáforo, não acúmulo:
  // dizer "waiting in the queue" seria inventar um sintoma.
  const queued = runner.mode === "CPU" ? Math.max(0, runner.running - runner.max) : 0;

  return (
    <li className="flex flex-col gap-2 py-3">
      <div className="flex items-center justify-between gap-3">
        <div className="flex min-w-0 items-center gap-2.5">
          <span className="truncate font-medium">{runner.name}</span>
          <Badge variant="outline" className="mono-label shrink-0">
            {runner.mode}
          </Badge>
        </div>
        <span className="shrink-0 font-mono text-sm tabular-nums">
          {runner.running}
          <span className="text-muted-foreground"> / {runner.max}</span>
        </span>
      </div>
      {/*
        Past the ceiling the bar is pinned at 100%, so saturation has to be said in another
        channel — otherwise 8/8 and 503/8 draw the same picture and the queue we chose to count
        becomes invisible. The tone is forced to critical when there IS a queue, rather than
        letting the clamped percentage decide.
      */}
      <UtilizationBar
        percent={loadPercent(runner)}
        label={`${runner.name} occupancy`}
        tone={queued > 0 ? "critical" : utilizationTone(loadPercent(runner))}
      />
      {queued > 0 ? (
        <p className="text-xs font-medium text-destructive">
          {queued} waiting in the queue — this runner is accepting faster than it drains.
        </p>
      ) : (
        <p className="text-xs text-muted-foreground">
          {runner.mode === "IO"
            ? "Virtual threads behind a semaphore — past the cap, work is rejected, not queued."
            : "Platform thread pool — running includes what is waiting in the queue."}
        </p>
      )}
    </li>
  );
}

/**
 * The runners of the node that answered this request — not of the cluster.
 *
 * <p>A runner is a thread pool, and a thread pool lives in a process: two instances of the same
 * runner on different nodes have independent occupancy, and there is no sum that would mean
 * anything. Behind a load balancer, two refreshes can legitimately report different numbers. The
 * page says so out loud, because every other screen here shows cluster-wide state and the reader
 * has no reason to assume this one is different.
 */
export function RunnersPage() {
  // Fed by the stream's `runners` frame (ADR-0063 §5, which reverses the "node-local data stays
  // out of a cluster-wide channel" call recorded in DASHBOARD-STREAM-REVIEW §5). No interval of
  // its own: with both, an SSE pinned to one node and a poll round-robining across the load
  // balancer would write two different nodes' counters into one cache key twice a second.
  const runners = useQuery({
    queryKey: queryKeys.runners(),
    queryFn: fetchRunners,
  });
  const nodes = useQuery({ queryKey: queryKeys.nodes(), queryFn: fetchNodes });

  const data = runners.data ?? [];
  const totalRunning = data.reduce((sum, runner) => sum + runner.running, 0);
  const totalCapacity = data.reduce((sum, runner) => sum + runner.max, 0);

  return (
    <PageStack>
      <StatGrid columns={4}>
        <StatCard
          label="Runners"
          value={runners.isPending ? "…" : data.length}
          icon={<IconServer className="size-4" />}
          accent
        />
        <StatCard label="In flight" value={runners.isPending ? "…" : totalRunning} />
        <StatCard label="Capacity" value={runners.isPending ? "…" : totalCapacity} />
        <StatCard
          label="Nodes in cluster"
          value={nodes.isPending ? "…" : (nodes.data?.length ?? 0)}
        />
      </StatGrid>

      <Panel title="Runners on this node">
        <p className="pb-3 text-xs text-muted-foreground">
          A runner is a thread pool, so this page describes the single node that served the
          request — not the cluster. With more than one node behind a load balancer, two refreshes
          can report different numbers, and that is not a bug.
        </p>
        {runners.isPending && <Spinner label="Loading runners" />}
        {runners.error && <ErrorState message={runners.error.message} onRetry={() => runners.refetch()} />}
        {runners.data && data.length === 0 && (
          <EmptyState
            title="No runners registered"
            description="Every node declares at least the default 'io' runner, so an empty list means the engine did not start."
          />
        )}
        {data.length > 0 && (
          <ul className="flex flex-col divide-y">
            {data.map((runner) => (
              <RunnerRow key={runner.name} runner={runner} />
            ))}
          </ul>
        )}
      </Panel>
    </PageStack>
  );
}
