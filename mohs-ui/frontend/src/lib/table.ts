import { columnSizingFeature, columnVisibilityFeature, tableFeatures } from "@tanstack/react-table";

/**
 * These tables are read-only grids over data the API already paginated and filtered server-side
 * (cursor endpoints) — sorting, filtering and grouping never go through react-table itself.
 *
 * <p>Column visibility is the one client-side interaction that makes sense here. Column SIZING is
 * registered for a different reason: it is not a feature the operator drives, it is what stops
 * the grid from re-laying itself out on every push. Under the default `table-layout: auto` the
 * browser measures each column from its content, so a cell going from "now" to "12m ago" on the
 * 2s stream tick widens that column and shifts every other one — a visible twitch on a screen
 * nobody touched. With declared sizes the widths are a function of the COLUMN SET, never of the
 * data (see {@link ../components/DataTable}).
 */
export const features = tableFeatures({ columnVisibilityFeature, columnSizingFeature });

export type AppFeatures = typeof features;
