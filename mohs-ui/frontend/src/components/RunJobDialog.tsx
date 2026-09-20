import { useRef, useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { ApiError, scheduleJob } from "../lib/api";
import { newSubmissionKey, parseJobPayload } from "../lib/manualSubmission";
import type { AcceptedExecutionResponse } from "../types/api";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "./ui/dialog";
import { Button } from "./ui/button";
import { Label } from "./ui/label";
import { Textarea } from "./ui/textarea";

/** Mounted for one intent. Once submitted, retries preserve both the payload and deduplication key. */
export function RunJobDialog({ jobKey, onClose, onAccepted }: {
  jobKey: string;
  onClose: () => void;
  onAccepted: (receipt: AcceptedExecutionResponse) => void;
}) {
  const [text, setText] = useState("{}");
  const [validation, setValidation] = useState<string | null>(null);
  const intent = useRef<{ key: string; payload: Record<string, unknown> } | null>(null);
  const mutation = useMutation({
    mutationFn: (submission: NonNullable<typeof intent.current>) => scheduleJob(jobKey, { payload: submission.payload }, submission.key),
    onSuccess: onAccepted,
  });
  const submitted = intent.current !== null;
  const rejected = mutation.error instanceof ApiError && mutation.error.status >= 400 && mutation.error.status < 500;

  function submit() {
    if (mutation.isPending) return;
    try {
      intent.current ??= { key: newSubmissionKey(), payload: parseJobPayload(text) };
      setValidation(null);
      mutation.mutate(intent.current);
    } catch (error) { setValidation(error instanceof Error ? error.message : "Invalid payload"); }
  }

  return <Dialog open onOpenChange={(open) => { if (!open && !mutation.isPending) onClose(); }}>
    <DialogContent className="sm:max-w-xl" showCloseButton={!mutation.isPending}>
      <DialogHeader>
        <DialogTitle>Run {jobKey} now?</DialogTitle>
        <DialogDescription>Creates an immediately due execution with this payload, independent of the job’s schedule.</DialogDescription>
      </DialogHeader>
      <form className="flex flex-col gap-3" onSubmit={(event) => { event.preventDefault(); submit(); }}>
        <Label htmlFor="run-job-payload">Payload · JSON object</Label>
        <Textarea id="run-job-payload" className="min-h-40 font-mono" value={text} disabled={submitted || mutation.isPending}
          aria-invalid={!!validation} aria-describedby={validation ? "run-job-payload-error" : undefined}
          onChange={(event) => { setText(event.target.value); setValidation(null); }} spellCheck={false} />
        {validation && <p id="run-job-payload-error" role="alert" className="text-sm text-critical">{validation}</p>}
        {mutation.error && <div role="alert" className="space-y-2 text-sm text-critical">
          <p>{mutation.error.message}</p>
          {!rejected && <p>Acceptance could not be confirmed. Retry repeats this same request safely; check executions before starting a separate run.</p>}
        </div>}
        {mutation.isPending && <p role="status" className="text-sm text-muted-foreground">Requesting execution…</p>}
        <div className="flex flex-wrap justify-end gap-2">
          <Button type="button" variant="outline" disabled={mutation.isPending} onClick={onClose}>Close</Button>
          {rejected && <Button type="button" variant="outline" onClick={() => { intent.current = null; mutation.reset(); }}>Edit payload</Button>}
          <Button type="submit" disabled={mutation.isPending}>{submitted ? "Retry request" : "Run now"}</Button>
        </div>
      </form>
    </DialogContent>
  </Dialog>;
}
