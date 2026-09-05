import { useIsFetching } from "@tanstack/react-query";

/**
 * Whether some query is fetching with nothing to show yet — the only kind of fetch worth a
 * loading indicator.
 *
 * The SSE stream invalidates the executions queries every 2s, so a plain `useIsFetching()` turns
 * every background refetch into a visible blink: the top progress bar lights up, the header flips
 * to "updating…", the refresh button spins. A query that already holds data is refreshed in
 * place, and the user should see the new numbers arrive, not the page announce a reload.
 */
export function useIsLoadingFresh(): boolean {
  return useIsFetching({ predicate: (query) => query.state.data === undefined }) > 0;
}
