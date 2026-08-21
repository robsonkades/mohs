import type { ExecutionFilters } from "../types/api";

/**
 * The React Query cache keys, in one place because the SSE stream writes into them from outside
 * the pages (see ./useLiveUpdates). A key duplicated as a string literal on both ends is how the
 * push silently stops updating the screen: the page reads one, the stream writes another, nothing
 * breaks — the view just goes stale.
 */
export const queryKeys = {
  overview: (window: string) => ["overview", window] as const,
  jobs: () => ["jobs"] as const,
  job: (jobKey: string) => ["job", jobKey] as const,
  nodes: () => ["nodes"] as const,
  rateLimits: () => ["rate-limits"] as const,
  executions: (filters: ExecutionFilters) => ["executions", filters] as const,
  execution: (executionId: string) => ["execution", executionId] as const,
  jobExecutions: (jobKey: string) => ["job-executions", jobKey] as const,
} as const;

/** The prefix the SSE `executions` frame invalidates — the search is filtered, so it cannot be seeded. */
export const EXECUTIONS_KEY_PREFIX = ["executions"] as const;
