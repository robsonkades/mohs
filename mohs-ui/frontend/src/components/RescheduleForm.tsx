import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { durationSeconds } from "../lib/format";
import type { ScheduleType, ScheduleView } from "../types/api";

/**
 * The body of PATCH /jobs/{jobKey}/schedule. Three shapes, because the union is sealed
 * at three: cron, interval and on-demand — and picking ON_DEMAND is what disarms recurrence, not a
 * separate button.
 *
 * The cron expression is not validated here: the server is what knows whether it is satisfiable,
 * and answers 422 with the reason. Reimplementing the parser would create a second truth about
 * what a valid expression is, and this one would be the wrong one.
 */
export function RescheduleForm({
  current,
  pending,
  error,
  onSubmit,
}: {
  current: ScheduleView;
  pending: boolean;
  error?: string;
  onSubmit: (schedule: ScheduleView) => void;
}) {
  const [type, setType] = useState<ScheduleType>(current.type);
  const [expression, setExpression] = useState(current.type === "CRON" ? current.expression : "");
  const [zone, setZone] = useState(
    current.type === "CRON" ? current.zone : Intl.DateTimeFormat().resolvedOptions().timeZone,
  );
  const [seconds, setSeconds] = useState(
    current.type === "INTERVAL" ? String(durationSeconds(current.interval) ?? "") : "",
  );
  const [afterFinish, setAfterFinish] = useState(current.type === "INTERVAL" ? current.afterFinish : false);

  function submit() {
    if (type === "CRON") {
      onSubmit({ type: "CRON", expression, zone });
    } else if (type === "INTERVAL") {
      onSubmit({ type: "INTERVAL", interval: `PT${Number(seconds)}S`, afterFinish });
    } else {
      onSubmit({ type: "ON_DEMAND" });
    }
  }

  return (
    <form
      className="flex flex-col gap-3"
      onSubmit={(event) => {
        event.preventDefault();
        submit();
      }}
    >
      <div className="space-y-1">
        <Label className="text-[11px]" htmlFor="reschedule-type">
          Type
        </Label>
        <Select value={type} onValueChange={(value) => setType(value as ScheduleType)}>
          <SelectTrigger id="reschedule-type">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="CRON">Cron</SelectItem>
            <SelectItem value="INTERVAL">Interval</SelectItem>
            <SelectItem value="ON_DEMAND">On demand</SelectItem>
          </SelectContent>
        </Select>
      </div>

      {type === "CRON" && (
        <>
          <div className="space-y-1">
            <Label className="text-[11px]" htmlFor="reschedule-expression">
              Expression · seconds first
            </Label>
            <Input
              id="reschedule-expression"
              className="font-mono"
              placeholder="0 */5 * * * *"
              value={expression}
              onChange={(event) => setExpression(event.target.value)}
            />
          </div>
          <div className="space-y-1">
            <Label className="text-[11px]" htmlFor="reschedule-zone">
              Zone
            </Label>
            <Input
              id="reschedule-zone"
              className="font-mono"
              value={zone}
              onChange={(event) => setZone(event.target.value)}
            />
          </div>
        </>
      )}

      {type === "INTERVAL" && (
        <>
          <div className="space-y-1">
            <Label className="text-[11px]" htmlFor="reschedule-interval">
              Interval (seconds)
            </Label>
            <Input
              id="reschedule-interval"
              type="number"
              min={1}
              value={seconds}
              onChange={(event) => setSeconds(event.target.value)}
            />
          </div>
          <label className="flex items-center gap-2 text-xs text-muted-foreground">
            <input
              type="checkbox"
              checked={afterFinish}
              onChange={(event) => setAfterFinish(event.target.checked)}
            />
            Measure the interval from the end of the previous run (fixed delay)
          </label>
        </>
      )}

      {type === "ON_DEMAND" && (
        <p className="text-xs text-muted-foreground">
          Disarms recurrence — the job only runs when something schedules it explicitly.
        </p>
      )}

      <Button type="submit" size="sm" disabled={pending} className="self-start">
        Apply
      </Button>
      {error && <p className="text-xs text-critical">{error}</p>}
    </form>
  );
}
