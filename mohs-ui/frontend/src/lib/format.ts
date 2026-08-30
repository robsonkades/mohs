const relativeFormatter = new Intl.RelativeTimeFormat("en", { numeric: "auto" });
const dateTimeFormatter = new Intl.DateTimeFormat("en", {
  dateStyle: "medium",
  timeStyle: "medium",
});
const shortDateTimeFormatter = new Intl.DateTimeFormat("en", {
  month: "short",
  day: "numeric",
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
});

const rangeDayFormatter = new Intl.DateTimeFormat("en", { month: "short", day: "2-digit", year: "numeric" });

const DIVISIONS: { amount: number; unit: Intl.RelativeTimeFormatUnit }[] = [
  { amount: 60, unit: "seconds" },
  { amount: 60, unit: "minutes" },
  { amount: 24, unit: "hours" },
  { amount: 7, unit: "days" },
  { amount: 4.34524, unit: "weeks" },
  { amount: 12, unit: "months" },
  { amount: Number.POSITIVE_INFINITY, unit: "years" },
];

/** "3 minutes ago" / "in 2 hours", relative to now. */
export function relativeTime(iso: string): string {
  let duration = (new Date(iso).getTime() - Date.now()) / 1000;
  for (const division of DIVISIONS) {
    if (Math.abs(duration) < division.amount) {
      return relativeFormatter.format(Math.round(duration), division.unit);
    }
    duration /= division.amount;
  }
  return relativeFormatter.format(Math.round(duration), "years");
}

export function absoluteTime(iso: string): string {
  return dateTimeFormatter.format(new Date(iso));
}

/**
 * "Aug 22, 2026" — the label a date-range trigger wears. Intl rather than a formatting library:
 * date-fns is only in this tree transitively, under react-day-picker, and depending on a
 * dependency's dependency is a break waiting for the next upgrade. The output is identical.
 */
export function rangeDay(date: Date): string {
  return rangeDayFormatter.format(date);
}

/** "Aug 10, 09:44" — compact enough for a filter chip label. */
export function shortDateTime(iso: string): string {
  return shortDateTimeFormatter.format(new Date(iso));
}

/**
 * Middle truncation, not a prefix.
 *
 * <p>Execution ids are UUIDv7: the leading hex IS the millisecond timestamp, so every row created
 * in the same minute shares it. Showing `id.slice(0, 8)` printed the identical string down the
 * whole identity column — `01a02f72` on every line of a live table — which is the one thing that
 * column exists not to do. Keeping both ends distinguishes rows on screen and still lets an
 * operator match either end of an id they copied from a log. The full value stays in the cell's
 * `title` and one click away in the copy button.
 */
export function shortId(id: string): string {
  return id.length <= 13 ? id : `${id.slice(0, 8)}…${id.slice(-4)}`;
}

export function titleCase(value: string): string {
  return value.charAt(0) + value.slice(1).toLowerCase();
}

const ISO_DURATION = /^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:([\d.]+)S)?)?$/;

/**
 * Seconds in a Java ISO-8601 duration ("PT1M", "PT15M", "PT1H30M", "PT0.5S"). Mohs serializes every
 * `Duration` this way, and none of them carry months or years — this grammar covers what the API
 * actually produces (days and below), not all of ISO-8601.
 */
export function durationSeconds(iso: string): number | null {
  const match = ISO_DURATION.exec(iso);
  if (!match) {
    return null;
  }
  const [, days, hours, minutes, seconds] = match;
  return (
    Number(days ?? 0) * 86400 + Number(hours ?? 0) * 3600 + Number(minutes ?? 0) * 60 + Number(seconds ?? 0)
  );
}

/** "PT15M" → "15m". Falls back to the raw string when it cannot parse, rather than inventing a number. */
export function formatDuration(iso: string): string {
  const total = durationSeconds(iso);
  if (total === null) {
    return iso;
  }
  if (total === 0) {
    return "0s";
  }
  const parts: string[] = [];
  const units: [number, string][] = [
    [86400, "d"],
    [3600, "h"],
    [60, "m"],
    [1, "s"],
  ];
  let remaining = total;
  for (const [size, suffix] of units) {
    const value = Math.floor(remaining / size);
    if (value > 0) {
      parts.push(`${value}${suffix}`);
      remaining -= value * size;
    }
  }
  return parts.join(" ");
}

/**
 * One format for the rate, everywhere it is shown: mixing "7.2" and "11" on one axis reads as a
 * rounding bug, and a tile saying "12" beside a tick saying "12.4" is the same defect spread over
 * two components. Lives here rather than in the chart because the chart is lazy-loaded — importing
 * it from there would pull the whole chart bundle into the Overview's first paint.
 */
export const rateFormatter = new Intl.NumberFormat("en", { maximumFractionDigits: 1 });
