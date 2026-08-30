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
 * A 200 whose body this client cannot read — version skew, not an HTTP failure. Deliberately NOT
 * an `ApiError`: that type's `status` is the status of the failure, and the first
 * `retry: (n, e) => e.status >= 500` written against it would treat an unusable body as a success.
 */
export class UnsupportedServerError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "UnsupportedServerError";
  }
}

/** A throughput reading is only usable once `ratePerSecond` is there — it is the field the chart plots. */
function isThroughput(value: unknown): boolean {
  if (typeof value !== "object" || value === null) {
    return false;
  }
  return typeof (value as Record<string, unknown>).ratePerSecond === "number";
}

/**
 * Structural check of the fields every reader of the overview dereferences without guarding.
 *
 * <p>Exported because this shape enters through TWO doors — this fetch and the stream's `overview`
 * frame — and both write the SAME cache key. A guard on one door is worse than no guard: it reads
 * as protection while the other door still admits a body the panels crash on.
 */
export function isOverview(data: unknown): data is OverviewResponse {
  if (typeof data !== "object" || data === null) {
    return false;
  }
  const { executionCountsByStatus, throughput, recent } = data as Record<string, unknown>;
  return (
    typeof executionCountsByStatus === "object" &&
    executionCountsByStatus !== null &&
    isThroughput(throughput) &&
    // `recent` is required, not optional: it is what the activity panel plots, and a server older
    // than ADR-0063 answers 200 without it. Failing the query here is what turns a TypeError three
    // components away into the retry button the page already knows how to draw.
    isThroughput(recent)
  );
}

/**
 * The polling anchor: live-work counts plus throughput over the window. `window` accepts `15m` or
 * `PT15M`; absent means 60s, and the server clamps to 1s–1h.
 *
 * <p>Validated rather than cast: the type is what the server PROMISES, and this is the one call
 * whose promise changed (ADR-0063 added `recent`). A cast would let a body without `recent` reach
 * the panel and throw inside the render.
 */
export async function fetchOverview(window?: string): Promise<OverviewResponse> {
  const body = await request<unknown>("/overview", { window });
  if (!isOverview(body)) {
    throw new UnsupportedServerError("This dashboard needs a newer Mohs server: /overview did not carry `recent`.");
  }
  return body;
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
