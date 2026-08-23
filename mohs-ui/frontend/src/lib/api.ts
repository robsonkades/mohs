import type {
  AcceptedExecutionResponse,
  CursorPage,
  ExecutionFilters,
  ExecutionResponse,
  ExecutionSummaryResponse,
  JobResponse,
  NodeResponse,
  OverviewResponse,
  RateLimitResponse,
  RunnerResponse,
  RuntimePatchResponse,
  ScheduleView,
} from "../types/api";

// Assumes the default mohs.api.base-path (io.mohs.rest.ApiPaths.V1). A host that overrides that
// property must serve this dashboard from a build with a matching prefix.
export const API_BASE = "/api/mohs/v1";

/** Where the dashboard is mounted — MohsUiAutoConfiguration's resource handler serves it here. */
export const UI_BASE = "/mohs-ui";

export class ApiError extends Error {
  /**
   * A field and an assignment rather than a `readonly status` parameter property: parameter
   * properties are the one piece of TypeScript that cannot be erased by stripping types, so they
   * are rejected under `erasableSyntaxOnly` — the flag that keeps this source transpilable by
   * anything that only deletes annotations (esbuild's fast path, Node's own type stripping).
   */
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

/**
 * The error body is ProblemDetail (RFC 7807), served by io.mohs.rest.error.RestExceptionHandler:
 * `detail` is the actionable sentence, `title` the type label. Preferring `detail`, then `title`,
 * before statusText is what puts the near-miss job name the server computed on screen for an
 * unknown-job 404, instead of a mute "Not Found".
 */
async function problemDetail(response: Response): Promise<string | undefined> {
  return response
    .json()
    .then((body: { detail?: string; title?: string }) => body.detail ?? body.title)
    .catch(() => undefined);
}

async function request<T>(path: string, params?: Record<string, string | number | undefined>): Promise<T> {
  const url = new URL(API_BASE + path, window.location.origin);
  for (const [key, value] of Object.entries(params ?? {})) {
    if (value !== undefined && value !== "") {
      url.searchParams.set(key, String(value));
    }
  }

  const response = await fetch(url);
  if (!response.ok) {
    throw new ApiError(response.status, (await problemDetail(response)) ?? response.statusText);
  }
  return response.json() as Promise<T>;
}

async function command<T = void>(
  path: string,
  options?: { method?: "POST" | "PATCH" | "DELETE"; body?: unknown },
): Promise<T> {
  const init: RequestInit = { method: options?.method ?? "POST" };
  if (options?.body !== undefined) {
    init.headers = { "Content-Type": "application/json" };
    init.body = JSON.stringify(options.body);
  }

  const response = await fetch(new URL(API_BASE + path, window.location.origin), init);
  if (!response.ok) {
    throw new ApiError(response.status, (await problemDetail(response)) ?? response.statusText);
  }
  if (response.status === 204) {
    return undefined as T;
  }
  return response.json() as Promise<T>;
}

// ---- Overview ----------------------------------------------------------------------------

/**
 * The polling anchor: live-work counts plus throughput over the window. `window` accepts `15m` or
 * `PT15M`; absent means 60s, and the server clamps to 1s–1h.
 */
export function fetchOverview(window?: string) {
  return request<OverviewResponse>("/overview", { window });
}

// ---- Jobs --------------------------------------------------------------------------------

export function fetchJobs() {
  return request<JobResponse[]>("/jobs");
}

export function fetchJob(jobKey: string) {
  return request<JobResponse>(`/jobs/${encodeURIComponent(jobKey)}`);
}

export function pauseJob(jobKey: string) {
  return command<JobResponse>(`/jobs/${encodeURIComponent(jobKey)}/pause`);
}

export function resumeJob(jobKey: string) {
  return command<JobResponse>(`/jobs/${encodeURIComponent(jobKey)}/resume`);
}

/**
 * Manual invocation. `at` (absolute) and `delay` (ISO-8601, computed server-side) are mutually
 * exclusive; both absent means now. Returns 202 with the created execution.
 */
export function scheduleJob(
  jobKey: string,
  body: { payload: Record<string, unknown>; at?: string; delay?: string; priority?: string },
) {
  return command<AcceptedExecutionResponse>(`/jobs/${encodeURIComponent(jobKey)}/schedule`, { body });
}

/** Changes the schedule at runtime (ADR-0036) — holds until the next boot, as the response `notice` says. */
export function rescheduleJob(jobKey: string, schedule: ScheduleView) {
  return command<RuntimePatchResponse<JobResponse>>(`/jobs/${encodeURIComponent(jobKey)}/schedule`, {
    method: "PATCH",
    body: schedule,
  });
}

export function fetchJobExecutions(jobKey: string, cursor: string | undefined, size: number) {
  return request<CursorPage<ExecutionSummaryResponse>>(`/jobs/${encodeURIComponent(jobKey)}/executions`, {
    cursor,
    size,
  });
}

// ---- Executions --------------------------------------------------------------------------

export function fetchExecutions(filters: ExecutionFilters, cursor: string | undefined, size: number) {
  return request<CursorPage<ExecutionSummaryResponse>>("/executions", { ...filters, cursor, size });
}

export function fetchExecution(executionId: string) {
  return request<ExecutionResponse>(`/executions/${encodeURIComponent(executionId)}`);
}

export function cancelExecution(executionId: string) {
  return command<ExecutionResponse>(`/executions/${encodeURIComponent(executionId)}/cancel`);
}

/** Manual retry of a FAILED execution (ADR-0033): the SAME row is rearmed. State other than FAILED → 409. */
export function retryExecution(executionId: string) {
  return command<AcceptedExecutionResponse>(`/executions/${encodeURIComponent(executionId)}/retry`);
}

// ---- Rate limits -------------------------------------------------------------------------

export function fetchRateLimits() {
  return request<RateLimitResponse[]>("/rate-limits");
}

/** Cluster-wide throughput adjustment (ADR-0042). Undeclared limit → 404: declaring one is a boot-time act. */
export function patchRateLimit(name: string, body: { max: number; window: string }) {
  return command<RuntimePatchResponse<RateLimitResponse>>(`/rate-limits/${encodeURIComponent(name)}`, {
    method: "PATCH",
    body,
  });
}

// ---- Runners -----------------------------------------------------------------------------

/** Node-local: describes the process that answered, not the cluster. */
export function fetchRunners() {
  return request<RunnerResponse[]>("/runners");
}

// ---- Nodes -------------------------------------------------------------------------------

export function fetchNodes() {
  return request<NodeResponse[]>("/nodes");
}
