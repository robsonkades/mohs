import { createRootRoute, createRoute, createRouter, lazyRouteComponent, Outlet } from "@tanstack/react-router";
import { AppLayout } from "./components/AppLayout";
import { UI_BASE } from "./lib/api";
import type { JobsSearch } from "./pages/JobsPage";
import type { ExecutionsSearch } from "./pages/ExecutionsPage";
import { isTimeWindow } from "./lib/timeWindows";

/*
 * One chunk per route, via `lazyRouteComponent` below. Every page used to sit in the entry
 * bundle, so the overview paid for Recharts AND the day picker that only the executions filter
 * ever opens — a megabyte of JavaScript to render six stat cards.
 *
 * Only the COMPONENT is deferred: the `validateSearch` functions stay eager, because the router
 * has to parse the URL of a route it has not navigated to yet. The two imports above are
 * type-only, and `verbatimModuleSyntax` erases them, so naming the page modules here does not
 * drag them back into the entry chunk.
 */

function str(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

const rootRoute = createRootRoute({
  component: () => (
    <AppLayout>
      <Outlet />
    </AppLayout>
  ),
});

const indexRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  component: lazyRouteComponent(() => import("./pages/OverviewPage"), "OverviewPage"),
});

const jobsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/jobs",
  validateSearch: (search: Record<string, unknown>): JobsSearch => ({
    paused: search.paused === "true" || search.paused === "false" ? search.paused : undefined,
    search: str(search.search),
    jobKey: str(search.jobKey),
  }),
  component: lazyRouteComponent(() => import("./pages/JobsPage"), "JobsPage"),
});

const executionsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/executions",
  validateSearch: (search: Record<string, unknown>): ExecutionsSearch => ({
    jobKey: str(search.jobKey),
    status: str(search.status) as ExecutionsSearch["status"],
    executionId: str(search.executionId),
    // An unrecognised window falls back to the last hour rather than to "all": landing on a
    // full-history scan because of a typo in a shared URL is the wrong kind of surprise.
    window: isTimeWindow(search.window) ? search.window : "1h",
    from: str(search.from),
    to: str(search.to),
  }),
  component: lazyRouteComponent(() => import("./pages/ExecutionsPage"), "ExecutionsPage"),
});

const rateLimitsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/rate-limits",
  component: lazyRouteComponent(() => import("./pages/RateLimitsPage"), "RateLimitsPage"),
});

const runnersRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/runners",
  component: lazyRouteComponent(() => import("./pages/RunnersPage"), "RunnersPage"),
});

const routeTree = rootRoute.addChildren([indexRoute, jobsRoute, executionsRoute, rateLimitsRoute, runnersRoute]);

export const router = createRouter({ routeTree, basepath: UI_BASE });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
