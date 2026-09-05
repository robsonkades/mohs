/**
 * The executions page's preset ranges, in their own module because the ROUTER validates them and
 * the PAGE renders them. Importing the value from the page would drag that page — and everything
 * it imports — back into the entry chunk, undoing the route-level code splitting: `validateSearch`
 * is eager by necessity, the component is not.
 *
 * <p>`all` is the one entry that is not a duration: it drops the lower bound, which is the only
 * way to reach anything older than the longest preset without opening the calendar. The router
 * used to coerce a missing window to "1h", so "everything" was not a state this page could be in.
 */
export const TIME_WINDOWS = {
  "1h": 60 * 60 * 1000,
  "6h": 6 * 60 * 60 * 1000,
  "24h": 24 * 60 * 60 * 1000,
  "7d": 7 * 24 * 60 * 60 * 1000,
  "30d": 30 * 24 * 60 * 60 * 1000,
  all: null,
} as const;

export type TimeWindow = keyof typeof TIME_WINDOWS;

export const TIME_WINDOW_KEYS = Object.keys(TIME_WINDOWS) as TimeWindow[];

export function isTimeWindow(value: unknown): value is TimeWindow {
  return typeof value === "string" && value in TIME_WINDOWS;
}

/** The label the toggle shows — "all" reads better spelled out than as a unit that is not one. */
export const TIME_WINDOW_LABEL: Record<TimeWindow, string> = {
  "1h": "1h",
  "6h": "6h",
  "24h": "24h",
  "7d": "7d",
  "30d": "30d",
  all: "All",
};
