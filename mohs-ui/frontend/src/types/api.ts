// Mirrors the records returned by io.mohs.rest.*. Instant serializes as an ISO-8601 string and
// Duration as an ISO-8601 duration string ("PT1M", "PT15M") — Spring Boot's default Jackson
// JSR-310 config, and the shape mohs-rest's own contract tests assert.

/** io.mohs.core.execution.ExecutionState — declaration order is lifecycle order, and exposed order is contract. */
export type ExecutionState =
  | "ENQUEUED"
  | "RUNNING"
  | "RETRY_SCHEDULED"
  | "SUCCEEDED"
  | "FAILED"
  | "CANCELLED";

/** The three states /overview always carries: the live work. Terminal states have no all-time count. */
export const LIVE_STATES = ["ENQUEUED", "RUNNING", "RETRY_SCHEDULED"] as const satisfies readonly ExecutionState[];

export type Priority = "CRITICAL" | "HIGH" | "NORMAL" | "LOW" | "BACKGROUND";

/** io.mohs.core.schedule.Misfire */
export type Misfire = "IGNORE" | "FIRE_NOW";

/** io.mohs.core.definition.DefinitionSource */
export type DefinitionSource = "ANNOTATION" | "PROGRAMMATIC";

export type ScheduleType = "CRON" | "INTERVAL" | "ON_DEMAND";

/**
 * io.mohs.rest.job.ScheduleView — sealed union discriminated by `type`, using the same
 * discriminant name Java's @JsonTypeInfo emits. It is also the request body of
 * PATCH /jobs/{jobKey}/schedule, not only a read shape.
 */
export type ScheduleView =
  | { type: "CRON"; expression: string; zone: string }
  | { type: "INTERVAL"; interval: string; afterFinish: boolean }
  | { type: "ON_DEMAND" };

/** io.mohs.rest.CursorPage — `items`/`nextCursor`; a null cursor means this was the last page. */
export interface CursorPage<T> {
  items: T[];
  nextCursor: string | null;
}

/** io.mohs.rest.job.JobResponse */
export interface JobResponse {
  jobKey: string;
  name: string;
  handlerType: string;
  schedule: ScheduleView;
  runner: string | null;
  window: string | null;
  rateLimit: string | null;
  misfire: Misfire;
  retries: number;
  timeout: string | null;
  retryPolicy: string | null;
  source: DefinitionSource;
  paused: boolean;
  nextFireAt: string | null;
}

/** io.mohs.rest.execution.ExecutionSummaryResponse — what the paged search returns (no attempts). */
export interface ExecutionSummaryResponse {
  executionId: string;
  jobKey: string;
  state: ExecutionState;
  scheduledAt: string;
  firedAt: string | null;
  actor: string;
}

/** io.mohs.rest.execution.AttemptResponse */
export interface AttemptResponse {
  number: number;
  startedAt: string;
  finishedAt: string | null;
  outcome: ExecutionState;
  error: string | null;
}

/** io.mohs.rest.execution.ExecutionResponse — the detail view, with attempts. */
export interface ExecutionResponse extends ExecutionSummaryResponse {
  attempts: AttemptResponse[];
}

/** io.mohs.rest.overview.ThroughputView */
export interface ThroughputView {
  window: string;
  succeeded: number;
  failed: number;
}

/** io.mohs.rest.overview.OverviewResponse */
export interface OverviewResponse {
  executionCountsByStatus: Partial<Record<ExecutionState, number>>;
  throughput: ThroughputView;
}

/** io.mohs.rest.ratelimit.RateLimitResponse */
export interface RateLimitResponse {
  name: string;
  max: number;
  window: string;
  available: number;
}

/** io.mohs.core.EngineState */
export type EngineState = "CREATED" | "RUNNING" | "PAUSED" | "DRAINING" | "STOPPED";

/** io.mohs.rest.runner.RunnerResponse — node-local: describes the process that answered, not the cluster. */
export interface RunnerResponse {
  name: string;
  mode: "IO" | "CPU";
  max: number;
  running: number;
}

/** io.mohs.rest.node.NodeResponse */
export interface NodeResponse {
  nodeId: string;
  state: EngineState;
  lastHeartbeatAt: string;
}

/** io.mohs.rest.AcceptedExecutionResponse — the 202 body, for both schedule and retry. */
export interface AcceptedExecutionResponse {
  executionId: string;
  jobKey: string;
  scheduledAt: string;
  actor: string;
}

/**
 * io.mohs.rest.RuntimePatchResponse — the runtime-PATCH envelope. `notice` is never null: the
 * change holds until the next boot, and the UI surfaces that instead of pretending it is permanent.
 */
export interface RuntimePatchResponse<T> {
  resource: T;
  notice: string;
}

export interface ExecutionFilters {
  status?: ExecutionState;
  jobKey?: string;
  from?: string;
  to?: string;
}
