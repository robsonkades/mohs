import { useSyncExternalStore } from "react";

function subscribe(onChange: () => void): () => void {
  document.addEventListener("visibilitychange", onChange);
  return () => document.removeEventListener("visibilitychange", onChange);
}

function isVisible(): boolean {
  return document.visibilityState === "visible";
}

/**
 * Whether this tab is the one being looked at.
 *
 * <p>`useSyncExternalStore` rather than `useState` + an effect: the visibility flag is browser
 * state this app reads, not app state it owns, and the subscribe/getSnapshot pair is what keeps a
 * change that lands mid-render from being torn across components.
 *
 * <p>The server value is `true` — nothing here renders on a server today, and a dashboard that
 * assumed "hidden" would start life with its live connection closed.
 */
export function useDocumentVisible(): boolean {
  return useSyncExternalStore(subscribe, isVisible, () => true);
}
