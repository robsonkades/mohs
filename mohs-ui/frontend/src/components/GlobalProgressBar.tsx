import { useRouterState } from "@tanstack/react-router";
import { useIsLoadingFresh } from "@/lib/useIsLoadingFresh";

/** Slim animated bar at the very top of the viewport — visible during route transitions and
 * while a request that has nothing cached yet is in flight, so navigation and first loads give
 * feedback. Background refreshes (the stream's 2s cadence) deliberately do not light it up:
 * the data updates in place. */
export function GlobalProgressBar() {
  const isLoading = useIsLoadingFresh();
  const isNavigating = useRouterState({ select: (s) => s.isLoading });
  const active = isLoading || isNavigating;

  return (
    <div
      className={
        "fixed inset-x-0 top-0 z-50 h-[3px] overflow-hidden transition-opacity duration-150 " +
        (active ? "opacity-100" : "pointer-events-none opacity-0")
      }
      role="status"
      aria-label={active ? "Loading" : undefined}
    >
      <div className="progress-bar-fill" />
    </div>
  );
}
