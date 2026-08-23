import { useMemo, useState } from "react";
import { getRouteApi, Link } from "@tanstack/react-router";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { createColumnHelper } from "@tanstack/react-table";
import { fetchJob, fetchJobs, pauseJob, rescheduleJob, resumeJob, scheduleJob } from "../lib/api";
import { queryKeys } from "../lib/queryKeys";
import type { AcceptedExecutionResponse, JobResponse } from "../types/api";
import { DataTable } from "../components/DataTable";
import { PageStack, Section } from "../components/Layout";
import { Panel } from "../components/Panel";
import { FilterBar } from "../components/Form";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { JobPausedBadge, TONE_COLOR_VAR } from "../components/Badge";
import { EmptyState, ErrorState, Spinner } from "../components/Feedback";
import { Drawer, Field } from "../components/Drawer";
import { ConfirmDialog } from "../components/ConfirmDialog";
import { CopyButton } from "../components/CopyButton";
import { RescheduleForm } from "../components/RescheduleForm";
import { IconArrowRight, IconPause, IconPlay, IconSearch, IconTicks } from "../components/Icons";
import { absoluteTime, formatDuration, relativeTime } from "../lib/format";
import { scheduleLabel, scheduleTypeLabel } from "../lib/schedule";
import { useDebouncedValue } from "../lib/useDebouncedValue";
import type { AppFeatures } from "../lib/table";

type JobAction = "pause" | "resume" | "schedule";

const ACTION_COPY: Record<JobAction, { title: string; description: string; confirmLabel: string }> = {
  schedule: {
    title: "Run this job now?",
    description:
      "Creates an immediately due Execution with an empty payload, independent of the job's schedule. You'll be taken to it once it's accepted.",
    confirmLabel: "Run now",
  },
  pause: {
    title: "Pause this job?",
    description:
      "Automatic firing stops until you resume it. Manual scheduling stays allowed, and executions already in flight aren't affected.",
    confirmLabel: "Pause job",
  },
  resume: {
    title: "Resume this job?",
    description: "Automatic firing starts again from the job's schedule.",
    confirmLabel: "Resume job",
  },
};

/**
 * Pause suspends the TRIGGER and nothing else. An ON_DEMAND job has no trigger, so pausing it
 * changes nothing about how it runs — the operator deserves to read that BEFORE confirming, not
 * discover it the next time a manual schedule goes straight through.
 */
function pauseCaveat(job: JobResponse | undefined): string {
  return job?.schedule.type === "ON_DEMAND"
    ? " This job is on demand: it has no trigger to suspend, so pausing it will not stop anything."
    : "";
}

export interface JobsSearch {
  paused?: "true" | "false";
  search?: string;
  jobKey?: string;
}

const ALL = "all";
const routeApi = getRouteApi("/jobs");
const columnHelper = createColumnHelper<AppFeatures, JobResponse>();

const columns = [
  columnHelper.accessor("jobKey", {
    header: "Job",
    size: 240,
    enableHiding: false, // the identity column — hiding it would leave the table looking unrecoverable
    cell: (info) => (
      <div className="flex flex-col">
        <span className="font-medium">{info.getValue()}</span>
        <span className="truncate text-xs text-muted-foreground">{info.row.original.name}</span>
      </div>
    ),
  }),
  columnHelper.accessor("paused", {
    header: "State",
    size: 110,
    cell: (info) => <JobPausedBadge paused={info.getValue()} />,
  }),
  columnHelper.display({
    id: "schedule",
    header: "Schedule",
    size: 200,
    cell: (info) => (
      <div className="flex items-center gap-1.5">
        <span className="font-mono text-xs">{scheduleLabel(info.row.original.schedule)}</span>
        <span className="mono-label rounded-sm border bg-background px-1.5 py-0.5 text-[10px] text-muted-foreground">
          {scheduleTypeLabel(info.row.original.schedule)}
        </span>
      </div>
    ),
  }),
  columnHelper.accessor("nextFireAt", {
    header: "Next fire",
    size: 140,
    cell: (info) => {
      const nextFireAt = info.getValue();
      return nextFireAt === null ? (
        <span className="text-muted-foreground">—</span>
      ) : (
        <span className="tabular-nums">{relativeTime(nextFireAt)}</span>
      );
    },
  }),
  columnHelper.accessor("runner", {
    header: "Runner",
    size: 130,
    cell: (info) => info.getValue() ?? <span className="text-muted-foreground">—</span>,
  }),
  columnHelper.accessor("rateLimit", {
    header: "Rate limit",
    size: 130,
    cell: (info) => info.getValue() ?? <span className="text-muted-foreground">—</span>,
  }),
];

/**
 * `GET /jobs` returns the whole list, unfiltered and unpaged — definitions are what the application
 * declared at boot, not history, so the set is bounded by construction. Filtering client-side is
 * the right reading of that contract; paging here would invent a problem the API does not have.
 */
function visibleJobs(jobs: JobResponse[], term: string, paused: string): JobResponse[] {
  const needle = term.trim().toLowerCase();
  return jobs.filter((job) => {
    if (paused !== ALL && String(job.paused) !== paused) {
      return false;
    }
    if (!needle) {
      return true;
    }
    return (
      job.jobKey.toLowerCase().includes(needle) ||
      job.name.toLowerCase().includes(needle) ||
      job.handlerType.toLowerCase().includes(needle)
    );
  });
}

export function JobsPage() {
  const search = routeApi.useSearch();
  const navigate = routeApi.useNavigate();
  const queryClient = useQueryClient();

  const debouncedSearch = useDebouncedValue(search.search ?? "");
  const jobsQuery = useQuery({ queryKey: queryKeys.jobs(), queryFn: fetchJobs });

  const jobDetail = useQuery({
    queryKey: queryKeys.job(search.jobKey ?? ""),
    queryFn: () => fetchJob(search.jobKey!),
    enabled: !!search.jobKey,
  });

  const rows = useMemo(
    () => visibleJobs(jobsQuery.data ?? [], debouncedSearch, search.paused ?? ALL),
    [jobsQuery.data, debouncedSearch, search.paused],
  );

  const [pendingAction, setPendingAction] = useState<JobAction | null>(null);
  const jobActionMutation = useMutation({
    mutationFn: (action: JobAction): Promise<JobResponse | AcceptedExecutionResponse> => {
      const jobKey = search.jobKey!;
      if (action === "pause") return pauseJob(jobKey);
      if (action === "resume") return resumeJob(jobKey);
      return scheduleJob(jobKey, { payload: {} });
    },
    onSuccess: (result) => {
      void queryClient.invalidateQueries({ queryKey: queryKeys.jobs() });
      void queryClient.invalidateQueries({ queryKey: queryKeys.job(search.jobKey!) });
      void queryClient.invalidateQueries({ queryKey: queryKeys.executions({}) });
      setPendingAction(null);
      if (result && "executionId" in result) {
        void navigate({ to: "/executions", search: { executionId: result.executionId } });
      }
    },
  });

  const [notice, setNotice] = useState<string | null>(null);
  const rescheduleMutation = useMutation({
    mutationFn: (schedule: Parameters<typeof rescheduleJob>[1]) => rescheduleJob(search.jobKey!, schedule),
    onSuccess: (response) => {
      setNotice(response.notice);
      void queryClient.invalidateQueries({ queryKey: queryKeys.jobs() });
      void queryClient.invalidateQueries({ queryKey: queryKeys.job(search.jobKey!) });
    },
  });

  function patchSearch(patch: Partial<JobsSearch>) {
    void navigate({ search: (prev) => ({ ...prev, ...patch }) });
  }

  function startAction(action: JobAction) {
    jobActionMutation.reset();
    setPendingAction(action);
  }

  function closeDrawer() {
    setNotice(null);
    rescheduleMutation.reset();
    patchSearch({ jobKey: undefined });
  }

  return (
    <PageStack>
      <Section>
        <FilterBar>
        <div className="relative">
          <IconSearch className="pointer-events-none absolute left-2.5 top-1/2 size-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            placeholder="Search jobs…"
            className="w-56 pl-8"
            value={search.search ?? ""}
            onChange={(event) => patchSearch({ search: event.target.value || undefined })}
          />
        </div>
        <Select
          value={search.paused ?? ALL}
          onValueChange={(value) =>
            patchSearch({ paused: value === ALL ? undefined : (value as JobsSearch["paused"]) })
          }
        >
          <SelectTrigger className="w-36">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value={ALL}>All states</SelectItem>
            <SelectItem value="false">Active</SelectItem>
            <SelectItem value="true">Paused</SelectItem>
          </SelectContent>
        </Select>
        </FilterBar>

        {jobsQuery.isPending && (
          <Panel title="Jobs">
            <Spinner label="Loading jobs" />
          </Panel>
        )}
        {jobsQuery.error && (
          <Panel title="Jobs">
            <ErrorState message={jobsQuery.error.message} onRetry={() => jobsQuery.refetch()} />
          </Panel>
        )}
        {jobsQuery.data && rows.length === 0 && (
          <Panel title="Jobs">
            <EmptyState
              title={jobsQuery.data.length === 0 ? "No jobs registered" : "No jobs match these filters"}
              description={
                jobsQuery.data.length === 0
                  ? "Jobs are declared by the host application at boot, with @MohsJob or the programmatic builder."
                  : "Try clearing a filter or widening your search."
              }
            />
          </Panel>
        )}
        {rows.length > 0 && (
          <DataTable
            title="Jobs"
            description={rows.length + " of " + (jobsQuery.data?.length ?? 0) + " declared"}
            data={rows}
            columns={columns}
            getRowId={(row) => row.jobKey}
            onRowClick={(row) => patchSearch({ jobKey: row.jobKey })}
            rowAccent={(row) => TONE_COLOR_VAR[row.paused ? "warning" : "good"]}
          />
        )}
      </Section>

      <Drawer open={!!search.jobKey} title="Job details" onClose={closeDrawer}>
        {jobDetail.isPending && <Spinner />}
        {jobDetail.error && <ErrorState message={jobDetail.error.message} />}
        {jobDetail.data && (
          <div className="flex flex-col gap-5">
            <Button asChild>
              <Link to="/executions" search={{ jobKey: jobDetail.data.jobKey }}>
                View executions for this job
                <IconArrowRight className="size-4" />
              </Link>
            </Button>

            <div className="flex flex-wrap items-center gap-2">
              <Button variant="outline" onClick={() => startAction("schedule")}>
                <IconTicks className="size-4" />
                Run now
              </Button>
              {jobDetail.data.paused ? (
                <Button variant="outline" onClick={() => startAction("resume")}>
                  <IconPlay className="size-4" />
                  Resume
                </Button>
              ) : (
                <Button variant="outline" onClick={() => startAction("pause")}>
                  <IconPause className="size-4" />
                  Pause
                </Button>
              )}
            </div>

            <div className="flex flex-col divide-y">
              <Field label="Job key">
                <span className="inline-flex items-center gap-1.5">
                  <span className="font-mono text-xs">{jobDetail.data.jobKey}</span>
                  <CopyButton value={jobDetail.data.jobKey} label="Copy job key" />
                </span>
              </Field>
              <Field label="Name">{jobDetail.data.name}</Field>
              <Field label="Handler">
                <span className="font-mono text-xs">{jobDetail.data.handlerType}</span>
              </Field>
              <Field label="State">
                <JobPausedBadge paused={jobDetail.data.paused} />
              </Field>
              <Field label="Schedule">
                <span className="font-mono text-xs">{scheduleLabel(jobDetail.data.schedule)}</span>
              </Field>
              <Field label="Next fire at">
                {jobDetail.data.nextFireAt ? absoluteTime(jobDetail.data.nextFireAt) : "—"}
              </Field>
              <Field label="Misfire">{jobDetail.data.misfire}</Field>
              <Field label="Retries">{jobDetail.data.retries}</Field>
              <Field label="Retry policy">{jobDetail.data.retryPolicy ?? "—"}</Field>
              <Field label="Timeout">
                {jobDetail.data.timeout ? formatDuration(jobDetail.data.timeout) : "—"}
              </Field>
              <Field label="Runner">{jobDetail.data.runner ?? "—"}</Field>
              <Field label="Execution window">{jobDetail.data.window ?? "—"}</Field>
              <Field label="Rate limit">{jobDetail.data.rateLimit ?? "—"}</Field>
              <Field label="Declared by">{jobDetail.data.source}</Field>
            </div>

            <div>
              <h3 className="mono-label mb-3 text-muted-foreground">Reschedule · runtime</h3>
              <RescheduleForm
                current={jobDetail.data.schedule}
                pending={rescheduleMutation.isPending}
                error={rescheduleMutation.error?.message}
                onSubmit={(schedule) => rescheduleMutation.mutate(schedule)}
              />
              {notice && <p className="pt-2 text-xs text-warning">{notice}</p>}
            </div>
          </div>
        )}
      </Drawer>

      <ConfirmDialog
        open={!!pendingAction}
        title={pendingAction ? ACTION_COPY[pendingAction].title : ""}
        description={
          pendingAction === null
            ? ""
            : ACTION_COPY[pendingAction].description +
              (pendingAction === "pause" ? pauseCaveat(jobDetail.data) : "")
        }
        confirmLabel={pendingAction ? ACTION_COPY[pendingAction].confirmLabel : ""}
        tone="default"
        pending={jobActionMutation.isPending}
        error={jobActionMutation.error?.message}
        onConfirm={() => jobActionMutation.mutate(pendingAction!)}
        onCancel={() => setPendingAction(null)}
      />
    </PageStack>
  );
}
