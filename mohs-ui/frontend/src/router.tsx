import { createRootRoute, createRoute, createRouter, Outlet } from "@tanstack/react-router";
import { AppLayout } from "./components/app-layout";
import { OverviewPage } from "./pages/OverviewPage";
import { JobsPage, type JobsSearch } from "./pages/JobsPage";
import { ExecutionsPage, type ExecutionsSearch } from "./pages/ExecutionsPage";
import { RateLimitsPage } from "./pages/RateLimitsPage";
import { RunnersPage } from "./pages/RunnersPage";
import { UI_BASE } from "./lib/api";

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
  component: OverviewPage,
});

const jobsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/jobs",
  validateSearch: (search: Record<string, unknown>): JobsSearch => ({
    paused: search.paused === "true" || search.paused === "false" ? search.paused : undefined,
    search: str(search.search),
    jobKey: str(search.jobKey),
  }),
  component: JobsPage,
});

const executionsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/executions",
  validateSearch: (search: Record<string, unknown>): ExecutionsSearch => ({
    jobKey: str(search.jobKey),
    status: str(search.status) as ExecutionsSearch["status"],
    executionId: str(search.executionId),
    window: ["1h", "6h", "24h"].includes(search.window as string)
      ? (search.window as ExecutionsSearch["window"])
      : "1h",
    from: str(search.from),
    to: str(search.to),
  }),
  component: ExecutionsPage,
});

const rateLimitsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/rate-limits",
  component: RateLimitsPage,
});

const runnersRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/runners",
  component: RunnersPage,
});

const routeTree = rootRoute.addChildren([indexRoute, jobsRoute, executionsRoute, rateLimitsRoute, runnersRoute]);

export const router = createRouter({ routeTree, basepath: UI_BASE });

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
