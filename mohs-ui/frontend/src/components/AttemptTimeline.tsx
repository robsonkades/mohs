import type { AttemptResponse } from "../types/api";
import { ExecutionStateBadge, EXECUTION_STATE_TONE, TONE_COLOR_VAR } from "./Badge";
import { CopyButton } from "./CopyButton";
import { absoluteTime } from "../lib/format";

function elapsed(attempt: AttemptResponse): string | null {
  if (!attempt.finishedAt) return null;
  const ms = Date.parse(attempt.finishedAt) - Date.parse(attempt.startedAt);
  if (!Number.isFinite(ms) || ms < 0) return null;
  return ms < 1000 ? `${ms} ms` : `${(ms / 1000).toLocaleString("en", { maximumFractionDigits: 2 })} s`;
}

export function AttemptTimeline({ attempts }: { attempts: AttemptResponse[] }) {
  return (
    <section aria-label="Attempt history">
      <div className="mb-4 flex items-center justify-between">
        <h3 className="text-sm font-semibold">Attempt history</h3>
        <span className="text-xs tabular-nums text-muted-foreground">{attempts.length} recorded</span>
      </div>
      {attempts.length === 0 && <p className="text-sm text-muted-foreground">No attempts recorded yet.</p>}
      <ol className="ml-1 border-l pl-5">
        {attempts.map((attempt) => (
          <li key={attempt.number} className="relative pb-6 last:pb-0">
            <span aria-hidden className="absolute -left-[25px] top-1.5 size-2 rounded-full ring-4 ring-background"
              style={{ backgroundColor: TONE_COLOR_VAR[EXECUTION_STATE_TONE[attempt.outcome]] }} />
            <div className="flex flex-wrap items-center gap-2">
              <span className="text-sm font-medium">Attempt {attempt.number}</span>
              <ExecutionStateBadge state={attempt.outcome} />
              <span className="ml-auto text-xs tabular-nums text-muted-foreground">{elapsed(attempt)}</span>
            </div>
            <dl className="mt-2 grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 text-xs text-muted-foreground">
              <dt>Started</dt><dd><time dateTime={attempt.startedAt}>{absoluteTime(attempt.startedAt)}</time></dd>
              <dt>Finished</dt><dd>{attempt.finishedAt ? <time dateTime={attempt.finishedAt}>{absoluteTime(attempt.finishedAt)}</time> : "Not recorded"}</dd>
            </dl>
            {attempt.error && (
              <details className="mt-3 overflow-hidden rounded-lg border border-critical/20 bg-critical/5">
                <summary className="cursor-pointer px-3 py-2 text-xs font-medium text-critical">View error · attempt {attempt.number}</summary>
                <div className="flex items-center justify-between border-t border-critical/15 px-3 py-1">
                  <span className="text-xs text-muted-foreground">Error details</span>
                  <CopyButton value={attempt.error} label={`Copy error from attempt ${attempt.number}`} />
                </div>
                <pre className="max-h-64 overflow-auto whitespace-pre-wrap break-words px-3 pb-3 font-mono text-xs leading-relaxed text-critical [overflow-wrap:anywhere]">{attempt.error}</pre>
              </details>
            )}
          </li>
        ))}
      </ol>
    </section>
  );
}
