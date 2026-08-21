import { formatDuration } from "./format";
import type { ScheduleView } from "../types/api";

/**
 * Short label for a schedule, for tables and badges. The union is sealed in Java (`ScheduleView`
 * permits Cron/Interval/OnDemand), so this switch is exhaustive by construction — with no
 * `default`, TypeScript will flag the new variant the day the contract grows one.
 */
export function scheduleLabel(schedule: ScheduleView): string {
  switch (schedule.type) {
    case "CRON":
      return schedule.expression;
    case "INTERVAL":
      return `every ${formatDuration(schedule.interval)}${schedule.afterFinish ? " after finish" : ""}`;
    case "ON_DEMAND":
      return "on demand";
  }
}

/** The type label, for the chip next to the value. */
export function scheduleTypeLabel(schedule: ScheduleView): string {
  switch (schedule.type) {
    case "CRON":
      return "cron";
    case "INTERVAL":
      return schedule.afterFinish ? "fixed delay" : "fixed rate";
    case "ON_DEMAND":
      return "on demand";
  }
}
