import { EXECUTION_STATES, type ExecutionState } from "../types/api.ts";
import { isTimeWindow, type TimeWindow } from "./timeWindows.ts";

export interface ExecutionsSearch {
  jobKey?: string;
  status?: ExecutionState;
  executionId?: string;
  window?: TimeWindow;
  from?: string;
  to?: string;
  filterWarning?: string;
}

function nonEmpty(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function instant(value: unknown): string | undefined {
  const text = nonEmpty(value);
  // Require an explicit zone: shared links must describe the same instants on every browser.
  if (!text) return undefined;
  const parts = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2})(?:\.\d+)?)?(?:Z|[+-]\d{2}:\d{2})$/i.exec(text);
  if (!parts || !Number.isFinite(Date.parse(text))) return undefined;
  const [, year, month, day, hour, minute, second = "0"] = parts;
  const lastDay = new Date(0);
  lastDay.setUTCFullYear(Number(year), Number(month), 0);
  if (Number(month) < 1 || Number(month) > 12 || Number(day) < 1 || Number(day) > lastDay.getUTCDate()
    || Number(hour) > 23 || Number(minute) > 59 || Number(second) > 59) return undefined;
  return text;
}

const FILTER_WARNING = "Invalid filters were removed. Time ranges need valid dates with a timezone and an end after the start.";

export function validateExecutionsSearch(search: Record<string, unknown>): ExecutionsSearch {
  let from = instant(search.from);
  let to = instant(search.to);
  const rawStatus = nonEmpty(search.status);
  const status = EXECUTION_STATES.find((state) => state === rawStatus);
  const invalidRange = (nonEmpty(search.from) !== undefined && !from)
    || (nonEmpty(search.to) !== undefined && !to)
    || (from !== undefined && to !== undefined && Date.parse(from) >= Date.parse(to));
  if (invalidRange) { from = undefined; to = undefined; }
  const invalidWindow = search.window !== undefined && !isTimeWindow(search.window);
  const invalidStatus = rawStatus !== undefined && status === undefined;
  return {
    jobKey: nonEmpty(search.jobKey),
    executionId: nonEmpty(search.executionId),
    status,
    window: invalidRange || invalidWindow ? "1h" : isTimeWindow(search.window) ? search.window : "1h",
    from,
    to,
    // The router may validate an already-normalized search again before rendering it.
    filterWarning: invalidRange || invalidWindow || invalidStatus || search.filterWarning === FILTER_WARNING
      ? FILTER_WARNING
      : undefined,
  };
}
