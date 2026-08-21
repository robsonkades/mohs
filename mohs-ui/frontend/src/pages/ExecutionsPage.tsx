import { useMemo, useState } from "react";
import { getRouteApi } from "@tanstack/react-router";
import { keepPreviousData, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { createColumnHelper } from "@tanstack/react-table";
import { CalendarIcon, XIcon } from "lucide-react";
import { cancelExecution, fetchExecution, fetchExecutions, retryExecution } from "../lib/api";
import { CopyButton } from "../components/CopyButton";
import type { ExecutionState, ExecutionSummaryResponse } from "../types/api";
import { CursorPager, DataTable } from "../components/DataTable";
import { FilterBar } from "../components/Form";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Calendar } from "@/components/ui/calendar";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { JobPicker } from "../components/JobPicker";
import { EXECUTION_STATE_TONE, ExecutionStateBadge, TONE_COLOR_VAR } from "../components/Badge";
import { EmptyState, ErrorState, Spinner } from "../components/Feedback";
import { Drawer, Field } from "../components/Drawer";
import { ConfirmDialog } from "../components/ConfirmDialog";
import { IconBan, IconRefresh } from "../components/icons";
import { absoluteTime, relativeTime, shortDateTime, shortId } from "../lib/format";
import { queryKeys } from "../lib/queryKeys";
import { useCursorHistory } from "../lib/useCursorHistory";
import type { AppFeatures } from "../lib/table";

type ExecutionAction = "cancel" | "retry";

const ACTION_COPY: Record<
  ExecutionAction,
  { title: string; description: string; confirmLabel: string; tone: "default" | "danger" }
> = {
  cancel: {
    title: "Cancel this execution?",
    description:
      "Cancellation is cooperative: an execution that hasn't started is cancelled outright, while a running one is asked to stop and the owning node finishes it as Cancelled once its handler observes the request.",
    confirmLabel: "Cancel execution",
    tone: "danger",
  },
  retry: {
    title: "Retry this execution?",
    description:
      "The same execution is rearmed as due now and competes for the normal claim — this bypasses the retry policy, which is already exhausted.",
    confirmLabel: "Retry execution",
    tone: "default",
  },
};

/** States that can still be cancelled: the non-terminal ones. */
const CANCELLABLE: readonly ExecutionState[] = ["ENQUEUED", "RUNNING", "RETRY_SCHEDULED"];

export const TIME_WINDOWS = {
  "1h": 60 * 60 * 1000,
  "6h": 6 * 60 * 60 * 1000,
  "24h": 24 * 60 * 60 * 1000,
} as const;

export type TimeWindow = keyof typeof TIME_WINDOWS;

export interface ExecutionsSearch {
  jobKey?: string;
  status?: ExecutionState;
  executionId?: string;
  window?: TimeWindow;
  /** Custom range (ISO) — takes precedence over `window`. */
  from?: string;
  to?: string;
}

const routeApi = getRouteApi("/executions");
const PAGE_SIZE = 20;
const columnHelper = createColumnHelper<AppFeatures, ExecutionSummaryResponse>();

function timeInputValue(date: Date): string {
  const pad = (value: number) => String(value).padStart(2, "0");
  return `${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function withTime(date: Date, hhmm: string): Date {
  const [hours, minutes] = hhmm.split(":").map(Number);
  const next = new Date(date);
  next.setHours(hours, minutes, 0, 0);
  return next;
}

function withDay(date: Date, day: Date): Date {
  const next = new Date(date);
  next.setFullYear(day.getFullYear(), day.getMonth(), day.getDate());
  return next;
}

/** Custom `scheduledAt` range: a calendar for the day span, plus a time input per end. */
function CustomRangeFilter({
  active,
  from,
  to,
  onChange,
  onClear,
}: {
  active: boolean;
  from: Date;
  to: Date;
  onChange: (range: { from: Date; to: Date }) => void;
  onClear: () => void;
}) {
  const [open, setOpen] = useState(false);

  return (
    <div className="flex items-center gap-1.5">
      <Popover open={open} onOpenChange={setOpen}>
        <PopoverTrigger asChild>
          <Button variant={active ? "secondary" : "outline"} className="font-mono text-xs">
            <CalendarIcon className="size-3.5" />
            {active ? `${shortDateTime(from.toISOString())} → ${shortDateTime(to.toISOString())}` : "Custom"}
          </Button>
        </PopoverTrigger>
        <PopoverContent align="start" className="w-auto p-0">
          <Calendar
            mode="range"
            selected={{ from, to }}
            defaultMonth={from}
            onSelect={(range) => {
              if (!range?.from) return;
              onChange({ from: withDay(from, range.from), to: withDay(to, range.to ?? range.from) });
            }}
          />
          <div className="flex items-center justify-center gap-2 border-t p-3">
            <Input
              type="time"
              value={timeInputValue(from)}
              onChange={(event) => event.target.value && onChange({ from: withTime(from, event.target.value), to })}
              className="w-auto"
            />
            <span className="text-muted-foreground">→</span>
            <Input
              type="time"
              value={timeInputValue(to)}
              onChange={(event) => event.target.value && onChange({ from, to: withTime(to, event.target.value) })}
              className="w-auto"
            />
          </div>
        </PopoverContent>
      </Popover>
      {active && (
        <Button variant="ghost" size="icon-xs" onClick={onClear} aria-label="Clear custom time range">
          <XIcon />
        </Button>
      )}
    </div>
  );
}

export function ExecutionsPage() {
  const search = routeApi.useSearch();
  const navigate = routeApi.useNavigate();
  const queryClient = useQueryClient();

  // Minute-snapped and memoized per window, so paging within one window keeps a stable cursor
  // chain — a cursor issued over a `from` that moves every render does not page.
  const presetFrom = useMemo(
    () =>
      search.window
        ? new Date(Math.floor(Date.now() / 60_000) * 60_000 - TIME_WINDOWS[search.window]).toISOString()
        : undefined,
    [search.window],
  );
  const from = search.from ?? presetFrom;
  const filters = useMemo(
    () => ({ jobKey: search.jobKey, status: search.status, from, to: search.to }),
    [search.jobKey, search.status, from, search.to],
  );

  const rangeDefaults = useMemo(() => {
    const to = search.to ? new Date(search.to).getTime() : Math.floor(Date.now() / 60_000) * 60_000;
    const start = search.from
      ? new Date(search.from).getTime()
      : to - (search.window ? TIME_WINDOWS[search.window] : TIME_WINDOWS["1h"]);
    return { from: start, to };
  }, [search.from, search.to, search.window]);

  const { after, pageNumber, hasPrev, next, prev } = useCursorHistory(JSON.stringify(filters));

  const executionsQuery = useQuery({
    queryKey: [...queryKeys.executions(filters), after],
    queryFn: () => fetchExecutions(filters, after, PAGE_SIZE),
    placeholderData: keepPreviousData,
  });

  const executionDetail = useQuery({
    queryKey: queryKeys.execution(search.executionId ?? ""),
    queryFn: () => fetchExecution(search.executionId!),
    enabled: !!search.executionId,
  });

  const [pendingAction, setPendingAction] = useState<ExecutionAction | null>(null);
  const executionActionMutation = useMutation({
    mutationFn: (action: ExecutionAction) =>
      action === "cancel" ? cancelExecution(search.executionId!) : retryExecution(search.executionId!),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: queryKeys.executions({}) });
      void queryClient.invalidateQueries({ queryKey: queryKeys.execution(search.executionId!) });
      setPendingAction(null);
    },
  });

  function startAction(action: ExecutionAction) {
    executionActionMutation.reset();
    setPendingAction(action);
  }

  function patchSearch(patch: Partial<ExecutionsSearch>) {
    void navigate({ search: (prev) => ({ ...prev, ...patch }) });
  }

  const columns = useMemo(
    () => [
      columnHelper.accessor("executionId", {
        header: "Execution",
        enableHiding: false, // the identity column — hiding it would leave the table looking unrecoverable
        cell: (info) => (
          <span className="inline-flex items-center gap-1">
            <span className="font-mono text-xs" title={info.getValue()}>
              {shortId(info.getValue())}
            </span>
            <CopyButton value={info.getValue()} label="Copy execution id" />
          </span>
        ),
      }),
      columnHelper.accessor("jobKey", {
        header: "Job",
        cell: (info) => (
          <button
            onClick={(event) => {
              event.stopPropagation();
              patchSearch({ jobKey: info.getValue() });
            }}
            className="truncate text-left font-mono text-xs font-medium text-muted-foreground hover:text-primary hover:underline"
          >
            {info.getValue()}
          </button>
        ),
      }),
      columnHelper.accessor("state", {
        header: "State",
        cell: (info) => <ExecutionStateBadge state={info.getValue()} />,
      }),
      columnHelper.accessor("scheduledAt", { header: "Scheduled", cell: (info) => relativeTime(info.getValue()) }),
      columnHelper.accessor("firedAt", {
        header: "Fired",
        cell: (info) =>
          info.getValue() ? relativeTime(info.getValue()!) : <span className="text-muted-foreground">—</span>,
      }),
      columnHelper.accessor("actor", {
        header: "Actor",
        cell: (info) => <span className="font-mono text-xs">{info.getValue()}</span>,
      }),
    ],
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [],
  );

  const detail = executionDetail.data;

  return (
    <div className="flex flex-col gap-4">
      <FilterBar>
        <ToggleGroup
          type="single"
          variant="outline"
          value={search.window && !search.from ? search.window : ""}
          onValueChange={(value) =>
            patchSearch({ window: (value || undefined) as TimeWindow | undefined, from: undefined, to: undefined })
          }
          aria-label="Scheduled within"
        >
          {(Object.keys(TIME_WINDOWS) as TimeWindow[]).map((window) => (
            <ToggleGroupItem key={window} value={window} className="font-mono text-xs">
              {window}
            </ToggleGroupItem>
          ))}
        </ToggleGroup>

        <CustomRangeFilter
          active={!!(search.from || search.to)}
          from={new Date(search.from ?? new Date(rangeDefaults.from).toISOString())}
          to={new Date(search.to ?? new Date(rangeDefaults.to).toISOString())}
          onChange={({ from: start, to }) =>
            patchSearch({ from: start.toISOString(), to: to.toISOString(), window: undefined })
          }
          onClear={() => patchSearch({ from: undefined, to: undefined })}
        />

        <JobPicker jobKey={search.jobKey} onChange={(jobKey) => patchSearch({ jobKey })} />
        <Select
          value={search.status ?? "all"}
          onValueChange={(value) => patchSearch({ status: value === "all" ? undefined : (value as ExecutionState) })}
        >
          <SelectTrigger className="w-40">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All states</SelectItem>
            <SelectItem value="ENQUEUED">Enqueued</SelectItem>
            <SelectItem value="RUNNING">Running</SelectItem>
            <SelectItem value="RETRY_SCHEDULED">Retry scheduled</SelectItem>
            <SelectItem value="SUCCEEDED">Succeeded</SelectItem>
            <SelectItem value="FAILED">Failed</SelectItem>
            <SelectItem value="CANCELLED">Cancelled</SelectItem>
          </SelectContent>
        </Select>
      </FilterBar>

      {executionsQuery.isPending && <Spinner label="Loading executions" />}
      {executionsQuery.error && (
        <ErrorState message={executionsQuery.error.message} onRetry={() => executionsQuery.refetch()} />
      )}
      {executionsQuery.data && executionsQuery.data.items.length === 0 && (
        <EmptyState title="No executions match these filters" description="Try clearing a filter." />
      )}
      {executionsQuery.data && executionsQuery.data.items.length > 0 && (
        <div className={executionsQuery.isFetching ? "opacity-60 transition-opacity" : "transition-opacity"}>
          <DataTable
            data={executionsQuery.data.items}
            columns={columns}
            getRowId={(row) => row.executionId}
            onRowClick={(row) => patchSearch({ executionId: row.executionId })}
            rowAccent={(row) => TONE_COLOR_VAR[EXECUTION_STATE_TONE[row.state]]}
          />
          <CursorPager
            pageNumber={pageNumber}
            hasPrev={hasPrev}
            hasNext={!!executionsQuery.data.nextCursor}
            onPrev={prev}
            onNext={() => next(executionsQuery.data!.nextCursor)}
          />
        </div>
      )}

      <Drawer
        open={!!search.executionId}
        title="Execution details"
        onClose={() => patchSearch({ executionId: undefined })}
      >
        {executionDetail.isPending && <Spinner />}
        {executionDetail.error && <ErrorState message={executionDetail.error.message} />}
        {detail && (
          <div className="flex flex-col gap-5">
            {(detail.state === "FAILED" || CANCELLABLE.includes(detail.state)) && (
              <div className="flex flex-wrap items-center gap-2">
                {detail.state === "FAILED" && (
                  <Button variant="outline" onClick={() => startAction("retry")}>
                    <IconRefresh className="h-4 w-4" />
                    Retry
                  </Button>
                )}
                {CANCELLABLE.includes(detail.state) && (
                  <Button
                    variant="outline"
                    onClick={() => startAction("cancel")}
                    className="text-critical hover:bg-critical/10 hover:text-critical"
                  >
                    <IconBan className="h-4 w-4" />
                    Cancel
                  </Button>
                )}
              </div>
            )}

            <div className="flex flex-col divide-y">
              <Field label="Execution id">
                <span className="inline-flex items-center gap-1.5">
                  <span className="font-mono text-xs">{detail.executionId}</span>
                  <CopyButton value={detail.executionId} label="Copy execution id" />
                </span>
              </Field>
              <Field label="Job">
                <button
                  onClick={() => patchSearch({ executionId: undefined, jobKey: detail.jobKey })}
                  className="font-mono text-xs text-primary hover:underline"
                >
                  {detail.jobKey}
                </button>
              </Field>
              <Field label="State">
                <ExecutionStateBadge state={detail.state} />
              </Field>
              <Field label="Actor">
                <span className="font-mono text-xs">{detail.actor}</span>
              </Field>
              <Field label="Scheduled at">{absoluteTime(detail.scheduledAt)}</Field>
              <Field label="Fired at">{detail.firedAt ? absoluteTime(detail.firedAt) : "—"}</Field>
            </div>

            <div>
              <h3 className="mono-label mb-3 text-muted-foreground">Attempts</h3>
              {detail.attempts.length === 0 && (
                <p className="text-xs text-muted-foreground">
                  Not attempted yet — this execution has not been claimed by any node.
                </p>
              )}
              {detail.attempts.length > 0 && (
                <ol className="flex flex-col gap-3 border-l pl-4">
                  {detail.attempts.map((attempt) => (
                    <li key={attempt.number} className="relative">
                      <span
                        className="absolute -left-[21px] top-1 h-2 w-2 rounded-full ring-4 ring-background"
                        style={{ backgroundColor: TONE_COLOR_VAR[EXECUTION_STATE_TONE[attempt.outcome]] }}
                      />
                      <div className="flex items-center gap-2 text-sm">
                        <span className="font-medium">Attempt {attempt.number}</span>
                        <ExecutionStateBadge state={attempt.outcome} />
                      </div>
                      <div className="text-xs text-muted-foreground">
                        {absoluteTime(attempt.startedAt)}
                        {attempt.finishedAt ? ` → ${absoluteTime(attempt.finishedAt)}` : " · still running"}
                      </div>
                      {attempt.error && (
                        <div className="mt-1 break-all rounded-sm border border-critical/20 bg-critical/10 p-2 font-mono text-xs text-critical">
                          {attempt.error}
                        </div>
                      )}
                    </li>
                  ))}
                </ol>
              )}
            </div>
          </div>
        )}
      </Drawer>

      <ConfirmDialog
        open={!!pendingAction}
        title={pendingAction ? ACTION_COPY[pendingAction].title : ""}
        description={pendingAction ? ACTION_COPY[pendingAction].description : ""}
        confirmLabel={pendingAction ? ACTION_COPY[pendingAction].confirmLabel : ""}
        tone={pendingAction ? ACTION_COPY[pendingAction].tone : "default"}
        pending={executionActionMutation.isPending}
        error={executionActionMutation.error?.message}
        onConfirm={() => executionActionMutation.mutate(pendingAction!)}
        onCancel={() => setPendingAction(null)}
      />
    </div>
  );
}
