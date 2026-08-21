import type { EngineState, ExecutionState, Priority } from "../types/api";
import { titleCase } from "../lib/format";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";

export type Tone = "good" | "warning" | "serious" | "critical" | "info" | "neutral" | "cancelled";

const TONE_CLASSES: Record<Tone, string> = {
  good: "bg-good/10 text-good border-good/20",
  warning: "bg-warning/10 text-warning border-warning/20",
  serious: "bg-serious/10 text-serious border-serious/20",
  critical: "bg-critical/10 text-critical border-critical/20",
  info: "bg-info/10 text-info border-info/20",
  neutral: "bg-neutral/10 text-neutral border-neutral/20",
  cancelled: "bg-cancelled/10 text-cancelled border-cancelled/20",
};

/**
 * Design-system "dot + label" badge: solid semantic dot on a 10%-tinted, subtly rounded
 * rectangle (never a pill — keeps the high-density grid alignment). The label itself
 * distinguishes states, so color never carries identity alone.
 */
export function StatusBadge({ tone, label, live = false }: { tone: Tone; label: string; live?: boolean }) {
  return (
    <Badge variant="outline" className={cn("mono-label h-auto rounded px-2 py-0.5 font-mono", TONE_CLASSES[tone])}>
      <span className={"h-1.5 w-1.5 shrink-0 rounded-full bg-current" + (live ? " live-dot" : "")} />
      {label}
    </Badge>
  );
}

export const TONE_COLOR_VAR: Record<Tone, string> = {
  good: "var(--status-good)",
  warning: "var(--status-warning)",
  serious: "var(--status-serious)",
  critical: "var(--status-critical)",
  info: "var(--status-info)",
  neutral: "var(--status-neutral)",
  cancelled: "var(--status-cancelled)",
};

/**
 * A job has no state enum here: it has `paused`, and the schedule says the rest. A boolean badge
 * is what the data supports — an Active/Paused/Cancelled lifecycle would promise transitions the
 * API does not have, since cancelling acts on an execution, never on a job.
 */
export function JobPausedBadge({ paused }: { paused: boolean }) {
  return paused ? <StatusBadge tone="warning" label="Paused" /> : <StatusBadge tone="good" label="Active" />;
}

export const EXECUTION_STATE_TONE: Record<ExecutionState, Tone> = {
  ENQUEUED: "neutral",
  RUNNING: "info",
  RETRY_SCHEDULED: "warning",
  SUCCEEDED: "good",
  FAILED: "critical",
  CANCELLED: "cancelled",
};

/** RETRY_SCHEDULED has an underscore; titleCase alone would render "Retry_scheduled". */
const EXECUTION_STATE_LABEL: Record<ExecutionState, string> = {
  ENQUEUED: "Enqueued",
  RUNNING: "Running",
  RETRY_SCHEDULED: "Retry scheduled",
  SUCCEEDED: "Succeeded",
  FAILED: "Failed",
  CANCELLED: "Cancelled",
};

export function ExecutionStateBadge({ state }: { state: ExecutionState }) {
  return (
    <StatusBadge
      tone={EXECUTION_STATE_TONE[state]}
      label={EXECUTION_STATE_LABEL[state]}
      live={state === "RUNNING"}
    />
  );
}

export const ENGINE_STATE_TONE: Record<EngineState, Tone> = {
  CREATED: "neutral",
  RUNNING: "good",
  PAUSED: "warning",
  DRAINING: "serious",
  STOPPED: "cancelled",
};

export function EngineStateBadge({ state }: { state: EngineState }) {
  return <StatusBadge tone={ENGINE_STATE_TONE[state]} label={titleCase(state)} live={state === "RUNNING"} />;
}

const PRIORITY_OPACITY: Record<Priority, string> = {
  CRITICAL: "opacity-100 font-semibold",
  HIGH: "opacity-90 font-medium",
  NORMAL: "opacity-70",
  LOW: "opacity-55",
  BACKGROUND: "opacity-40",
};

export function PriorityTag({ priority }: { priority: Priority }) {
  return (
    <span className={"font-mono text-[11px] uppercase tracking-wide text-muted-foreground " + PRIORITY_OPACITY[priority]}>
      {priority}
    </span>
  );
}
