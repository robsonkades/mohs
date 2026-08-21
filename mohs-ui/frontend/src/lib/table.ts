import { columnVisibilityFeature, tableFeatures } from "@tanstack/react-table";

/**
 * These tables are read-only grids over data the API already paginated and filtered server-side
 * (cursor endpoints) — sorting, filtering and grouping never go through react-table itself.
 * Column visibility is the one client-side feature that makes sense here, so it is the only one
 * registered.
 */
export const features = tableFeatures({ columnVisibilityFeature });

export type AppFeatures = typeof features;
