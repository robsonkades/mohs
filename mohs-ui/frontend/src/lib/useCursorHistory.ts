import { useEffect, useState } from "react";

/**
 * Client-side "Previous"/"Next" over a keyset-cursor API (CursorPage carries no total count and
 * no offset — see io.mohs.rest.CursorPage). Remembers every cursor seen, so going back and
 * forward again re-hits the TanStack Query cache instead of the network.
 */
export function useCursorHistory(resetKey: string) {
  const [cursors, setCursors] = useState<(string | undefined)[]>([undefined]);
  const [pageIndex, setPageIndex] = useState(0);

  useEffect(() => {
    setCursors([undefined]);
    setPageIndex(0);
  }, [resetKey]);

  const after = cursors[pageIndex];

  function next(nextCursor: string | null) {
    if (!nextCursor) return;
    setCursors((prev) => (pageIndex + 1 < prev.length ? prev : [...prev, nextCursor]));
    setPageIndex((i) => i + 1);
  }

  function prev() {
    setPageIndex((i) => Math.max(0, i - 1));
  }

  return { after, pageNumber: pageIndex + 1, hasPrev: pageIndex > 0, next, prev };
}
