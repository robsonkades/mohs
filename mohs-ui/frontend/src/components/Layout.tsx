import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

/**
 * The page rhythm, in one place.
 *
 * <p>Every route used to pick its own: two pages stacked their sections at `gap-6` and two at
 * `gap-4`, so moving between them shifted the whole grid by 8px for no reason a reader could
 * name. Spacing is a system, not a per-file decision — the two components here are the only two
 * vertical gaps a page is allowed to have.
 *
 * <p>`PageStack` separates SECTIONS (24px). `Section` groups the parts of one idea — a filter bar
 * and the table it filters, a heading and its list — at 12px, close enough to read as one block.
 */
export function PageStack({ children, className }: { children: ReactNode; className?: string }) {
  return <div className={cn("flex flex-col gap-6", className)}>{children}</div>;
}

export function Section({ children, className }: { children: ReactNode; className?: string }) {
  return <div className={cn("flex flex-col gap-3", className)}>{children}</div>;
}

/**
 * A shared metric strip: two columns on phones, expanding with the available space.
 * Hairline dividers group the readings without framing each number as a separate panel.
 */
export function StatGrid({ columns, children }: { columns: 3 | 4 | 6; children: ReactNode }) {
  return <div className={cn("metric-strip grid grid-cols-2 overflow-hidden rounded-xl border bg-card/40", STAT_GRID_COLUMNS[columns])}>{children}</div>;
}

/** Written out rather than interpolated — Tailwind only sees class names it can find in the source. */
const STAT_GRID_COLUMNS: Record<3 | 4 | 6, string> = {
  3: "lg:grid-cols-3",
  4: "sm:grid-cols-4",
  6: "lg:grid-cols-3 xl:grid-cols-6",
};
