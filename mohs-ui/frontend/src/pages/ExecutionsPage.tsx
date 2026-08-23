import { useCallback, useMemo, useState } from "react";
import { getRouteApi } from "@tanstack/react-router";
import { keepPreviousData, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { createColumnHelper } from "@tanstack/react-table";
import { CalendarIcon, XIcon } from "lucide-react";
import type { DateRange } from "react-day-picker";
import { cancelExecution, fetchExecution, fetchExecutions, retryExecution } from "../lib/api";
import { CopyButton } from "../components/CopyButton";
import { EXECUTION_STATES, type ExecutionState, type ExecutionSummaryResponse } from "../types/api";
import { CursorPager, DataTable } from "../components/DataTable";
import { PageStack, Section } from "../components/Layout";
import { Panel } from "../components/Panel";
import { FilterBar } from "../components/Form";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Calendar } from "@/components/ui/calendar";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { JobPicker } from "../components/JobPicker";
import {
  EXECUTION_STATE_LABEL,
  EXECUTION_STATE_TONE,
  ExecutionStateBadge,
  TONE_COLOR_VAR,
} from "../components/Badge";
import { EmptyState, ErrorState, Spinner } from "../components/Feedback";
import { Drawer, Field } from "../components/Drawer";
import { ConfirmDialog } from "../components/ConfirmDialog";
import { IconBan, IconRefresh } from "../components/Icons";
import { PauseIcon, PlayIcon } from "lucide-react";
import { absoluteTime, rangeDay, relativeTime, shortId } from "../lib/format";
import { EXECUTIONS_KEY_PREFIX, queryKeys } from "../lib/queryKeys";
import { useCursorHistory } from "../lib/useCursorHistory";
import { setExecutionsLive, useExecutionsLive } from "../lib/liveExecutions";
import type { AppFeatures } from "../lib/table";
import { TIME_WINDOWS, TIME_WINDOW_KEYS, TIME_WINDOW_LABEL, type TimeWindow } from "../lib/timeWindows";

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
const CANCELLABLE: readonly ExecutionState[] = ["ENQUEUED", "RUNNING", "RETRY_WAITING"];

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

/**
 * Applies an `<input type="time">` value to a date. A partial or empty value returns the date
 * unchanged instead of an Invalid Date: destructuring `"1".split(":")` yields `undefined` for the
 * minutes, and `setHours(1, undefined)` produces NaN — a filter that silently stops matching
 * anything, which is worse than a filter that does not move.
 */
function withTime(date: Date, hhmm: string): Date {
  const [hours, minutes] = hhmm.split(":").map(Number);
  if (hours === undefined || minutes === undefined || !Number.isFinite(hours) || !Number.isFinite(minutes)) {
    return date;
  }
  const next = new Date(date);
  next.setHours(hours, minutes, 0, 0);
  return next;
}

function withDay(date: Date, day: Date): Date {
  const next = new Date(date);
  next.setFullYear(day.getFullYear(), day.getMonth(), day.getDate());
  return next;
}

/**
 * The grid's shape, built once from the single thing it needs from the page: what to do when a
 * job key is clicked. Lifting it out of the component is what makes the dependency honest — the
 * previous version memoized the columns on `[]` while the cells closed over the page's navigate
 * helper, so the click handler was frozen at first render and only a disabled lint rule stood
 * between that and a bug. Sizes are declared because the widths must not follow the content
 * (see {@link ../components/DataTable}).
 */
function executionColumns(onJobKeyClick: (jobKey: string) => void) {
  return [
    columnHelper.accessor("executionId", {
      header: "Execution",
      size: 150,
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
      size: 230,
      cell: (info) => (
        <button
          onClick={(event) => {
            event.stopPropagation();
            onJobKeyClick(info.getValue());
          }}
          className="truncate text-left font-mono text-xs font-medium text-muted-foreground hover:text-primary hover:underline"
        >
          {info.getValue()}
        </button>
      ),
    }),
    columnHelper.accessor("state", {
      header: "State",
      size: 130,
      cell: (info) => <ExecutionStateBadge state={info.getValue()} />,
    }),
    columnHelper.accessor("scheduledAt", {
      header: "Scheduled",
      size: 130,
      cell: (info) => <span className="tabular-nums">{relativeTime(info.getValue())}</span>,
    }),
    columnHelper.accessor("firedAt", {
      header: "Fired",
      size: 130,
      cell: (info) => {
        const firedAt = info.getValue();
        return firedAt === null ? (
          <span className="text-muted-foreground">—</span>
        ) : (
          <span className="tabular-nums">{relativeTime(firedAt)}</span>
        );
      },
    }),
    columnHelper.accessor("actor", {
      header: "Actor",
      size: 120,
      cell: (info) => <span className="font-mono text-xs">{info.getValue()}</span>,
    }),
  ];
}

function startOfDay(daysAgo: number): Date {
  const date = new Date();
  date.setDate(date.getDate() - daysAgo);
  date.setHours(0, 0, 0, 0);
  return date;
}

function endOfDay(daysAgo: number): Date {
  const date = startOfDay(daysAgo);
  date.setHours(23, 59, 59, 999);
  return date;
}

/**
 * Calendar-day anchors, in LOCAL time — "yesterday" is a day on the operator's wall clock, not a
 * 24-hour slice ending now, and the two are only the same at midnight. Bounded on both ends,
 * which is exactly what the rolling windows cannot express.
 */
const DAY_RANGES: ReadonlyArray<{ label: string; range: () => { from: Date; to: Date } }> = [
  { label: "Today", range: () => ({ from: startOfDay(0), to: new Date() }) },
  { label: "Yesterday", range: () => ({ from: startOfDay(1), to: endOfDay(1) }) },
  { label: "Last 7 days", range: () => ({ from: startOfDay(6), to: new Date() }) },
];

/**
 * The trigger's label. Same day on both ends collapses to one date — "Aug 22, 2026 – Aug 22,
 * 2026" says nothing the shorter form does not, and the time inputs below carry the precision.
 */
function rangeLabel(from: Date, to: Date): string {
  const start = rangeDay(from);
  const end = rangeDay(to);
  return start === end ? start : `${start} – ${end}`;
}

/** Custom `scheduledAt` range: day anchors, a calendar for the span, plus a time input per end. */
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
  const selected: DateRange = { from, to };

  return (
    <div className="flex items-center gap-1.5">
      <Popover open={open} onOpenChange={setOpen}>
        <PopoverTrigger asChild>
          <Button variant={active ? "secondary" : "outline"} className="justify-start font-normal">
            <CalendarIcon className="size-3.5" />
            {active ? rangeLabel(from, to) : <span>Pick a date range</span>}
          </Button>
        </PopoverTrigger>
        <PopoverContent align="start" className="w-auto p-0">
          {/*
            Anchored intervals, which the toggle group above cannot express: every preset there is
            a rolling window ending NOW, so "yesterday" — a bounded interval in the past — had no
            control at all and could only be reached by driving the calendar by hand. The data was
            always one query away; what was missing was a way to aim at it.
          */}
          <div className="flex gap-1.5 border-b p-2">
            {DAY_RANGES.map((preset) => (
              <Button
                key={preset.label}
                variant="ghost"
                size="sm"
                className="flex-1"
                onClick={() => {
                  onChange(preset.range());
                  setOpen(false);
                }}
              >
                {preset.label}
              </Button>
            ))}
          </div>
          {/*
            Two months side by side: a range that crosses a month boundary — most of the useful
            ones do — costs a click through the pager with one, and none with two. Only the DAY
            is taken from the calendar; the time-of-day inputs below keep whatever was set, so
            picking a new day never silently resets a narrowed window back to midnight.
          */}
          <Calendar
            mode="range"
            numberOfMonths={2}
            selected={selected}
            defaultMonth={from}
            onSelect={(range: DateRange | undefined) => {
              if (!range?.from) {
                return;
              }
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
  const presetFrom = useMemo(() => {
    const span = search.window ? TIME_WINDOWS[search.window] : null;
    // `all` (and an absent window) carry no lower bound — the search goes back as far as the
    // retained history does, which is the point of having the option.
    return span === null ? undefined : new Date(Math.floor(Date.now() / 60_000) * 60_000 - span).toISOString();
  }, [search.window]);
  const from = search.from ?? presetFrom;
  const filters = useMemo(
    () => ({ jobKey: search.jobKey, status: search.status, from, to: search.to }),
    [search.jobKey, search.status, from, search.to],
  );

  const rangeDefaults = useMemo(() => {
    const to = search.to ? new Date(search.to).getTime() : Math.floor(Date.now() / 60_000) * 60_000;
    // The calendar has to open on SOME span, so "all" falls back to an hour here — this only
    // seeds the picker's initial selection, it is not the query.
    const span = (search.window ? TIME_WINDOWS[search.window] : null) ?? TIME_WINDOWS["1h"];
    const start = search.from ? new Date(search.from).getTime() : to - span;
    return { from: start, to };
  }, [search.from, search.to, search.window]);

  const { after, pageNumber, hasPrev, next, prev } = useCursorHistory(JSON.stringify(filters));
  const live = useExecutionsLive();

  /**
   * Paging is the other unambiguous "I am reading, not watching". A cursor page refetched every
   * 2s is not a page you can read — and the operator who clicked Next did not ask to be returned
   * to the head of the stream.
   */
  const goToNextPage = useCallback(
    (cursor: string | null) => {
      setExecutionsLive(false);
      next(cursor);
    },
    [next],
  );

  const executionsQuery = useQuery({
    queryKey: [...queryKeys.executions(filters), after],
    queryFn: () => fetchExecutions(filters, after, PAGE_SIZE),
    placeholderData: keepPreviousData,
  });

  // Narrowed once, into a name the closures below can capture. The alternative — `enabled` plus
  // `search.executionId!` in every callback — asks the reader to re-derive the same invariant at
  // four call sites, and a `!` is only ever as true as the guard someone remembers to keep.
  const selectedId = search.executionId;

  const executionDetail = useQuery({
    queryKey: queryKeys.execution(selectedId ?? ""),
    queryFn: () => fetchExecution(selectedId ?? ""),
    enabled: selectedId !== undefined,
  });

  const [pendingAction, setPendingAction] = useState<ExecutionAction | null>(null);
  const executionActionMutation = useMutation({
    mutationFn: ({ action, executionId }: { action: ExecutionAction; executionId: string }) =>
      action === "cancel" ? cancelExecution(executionId) : retryExecution(executionId),
    onSuccess: (_result, { executionId }) => {
      void queryClient.invalidateQueries({ queryKey: EXECUTIONS_KEY_PREFIX });
      void queryClient.invalidateQueries({ queryKey: queryKeys.execution(executionId) });
      setPendingAction(null);
    },
  });

  const actionCopy = pendingAction === null ? null : ACTION_COPY[pendingAction];

  function startAction(action: ExecutionAction) {
    executionActionMutation.reset();
    setPendingAction(action);
  }

  const patchSearch = useCallback(
    (patch: Partial<ExecutionsSearch>) => {
      void navigate({ search: (prev) => ({ ...prev, ...patch }) });
    },
    [navigate],
  );

  const onJobKeyClick = useCallback((jobKey: string) => patchSearch({ jobKey }), [patchSearch]);

  const columns = useMemo(() => executionColumns(onJobKeyClick), [onJobKeyClick]);

  const detail = executionDetail.data;

  const executions = executionsQuery.data;

  return (
    <PageStack>
      <Section>
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
          {TIME_WINDOW_KEYS.map((window) => (
            <ToggleGroupItem key={window} value={window} className="font-mono text-xs">
              {TIME_WINDOW_LABEL[window]}
            </ToggleGroupItem>
          ))}
        </ToggleGroup>

        <CustomRangeFilter
          active={!!(search.from || search.to)}
          from={new Date(search.from ?? new Date(rangeDefaults.from).toISOString())}
          to={new Date(search.to ?? new Date(rangeDefaults.to).toISOString())}
          onChange={({ from: start, to }) => {
            // Choosing an explicit range is an unambiguous statement of intent: the operator is
            // reading, not watching. Stopping the stream here is what makes the range readable.
            setExecutionsLive(false);
            patchSearch({ from: start.toISOString(), to: to.toISOString(), window: undefined });
          }}
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
            {EXECUTION_STATES.map((state) => (
              <SelectItem key={state} value={state}>
                {EXECUTION_STATE_LABEL[state]}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>

          <Button
            variant={live ? "secondary" : "outline"}
            className="ml-auto gap-1.5"
            onClick={() => setExecutionsLive(!live)}
            aria-pressed={live}
            title={
              live
                ? "Following the stream — new executions appear as they happen"
                : "Paused — the list stays put until you refresh or resume"
            }
          >
            {live ? (
              <>
                <span className="live-dot size-1.5 rounded-full bg-good" />
                Live
                <PauseIcon className="size-3.5" />
              </>
            ) : (
              <>
                <PlayIcon className="size-3.5" />
                Paused
              </>
            )}
          </Button>
        </FilterBar>

        {executionsQuery.isPending && <Panel title="Executions"><Spinner label="Loading executions" /></Panel>}
        {executionsQuery.error && (
          <Panel title="Executions">
            <ErrorState message={executionsQuery.error.message} onRetry={() => executionsQuery.refetch()} />
          </Panel>
        )}
        {executions && executions.items.length === 0 && (
          <Panel title="Executions">
            <EmptyState title="No executions match these filters" description="Try clearing a filter." />
          </Panel>
        )}
        {executions && executions.items.length > 0 && (
          // Dimmed only while showing the PREVIOUS page's rows for a key the user just changed —
          // `isPlaceholderData`, not `isFetching`. The stream invalidates this query every 2s, so
          // keying the dim on `isFetching` made the whole table blink twice a minute for a refresh
          // nobody asked for: the rows update in place, and that is the point of the live view.
          <div className={executionsQuery.isPlaceholderData ? "opacity-60 transition-opacity" : "transition-opacity"}>
            <DataTable
              title="Executions"
              data={executions.items}
              columns={columns}
              getRowId={(row) => row.executionId}
              onRowClick={(row) => patchSearch({ executionId: row.executionId })}
              rowAccent={(row) => TONE_COLOR_VAR[EXECUTION_STATE_TONE[row.state]]}
              footer={
                <CursorPager
                  pageNumber={pageNumber}
                  hasPrev={hasPrev}
                  hasNext={executions.nextCursor !== null}
                  onPrev={prev}
                  onNext={() => goToNextPage(executions.nextCursor)}
                />
              }
            />
          </div>
        )}
      </Section>

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
                    <IconRefresh className="size-4" />
                    Retry
                  </Button>
                )}
                {CANCELLABLE.includes(detail.state) && (
                  <Button
                    variant="outline"
                    onClick={() => startAction("cancel")}
                    className="text-critical hover:bg-critical/10 hover:text-critical"
                  >
                    <IconBan className="size-4" />
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
                        className="absolute -left-[21px] top-1 size-2 rounded-full ring-4 ring-background"
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
        open={pendingAction !== null}
        title={actionCopy?.title ?? ""}
        description={actionCopy?.description ?? ""}
        confirmLabel={actionCopy?.confirmLabel ?? ""}
        tone={actionCopy?.tone ?? "default"}
        pending={executionActionMutation.isPending}
        error={executionActionMutation.error?.message}
        onConfirm={() => {
          if (pendingAction !== null && selectedId !== undefined) {
            executionActionMutation.mutate({ action: pendingAction, executionId: selectedId });
          }
        }}
        onCancel={() => setPendingAction(null)}
      />
    </PageStack>
  );
}
