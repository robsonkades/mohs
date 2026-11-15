import { useState } from "react";
import { advanceCursor, type CursorHistory } from "./cursorHistory";

/**
 * Client-side "Previous"/"Next" over a keyset-cursor API (CursorPage carries no total count and
 * no offset — see io.mohs.rest.CursorPage). Remembers every cursor seen, so going back and
 * forward again re-hits the TanStack Query cache instead of the network.
 */
export function useCursorHistory(resetKey: string) {
  const [saved, setSaved] = useState<CursorHistory & { resetKey: string }>({ resetKey, cursors: [undefined], pageIndex: 0 });
  const history = saved.resetKey === resetKey ? saved : { resetKey, cursors: [undefined], pageIndex: 0 };
  // Reset before the query renders, so a new filter never fetches with an old filter's cursor.
  if (saved.resetKey !== resetKey) setSaved(history);
  const { cursors, pageIndex } = history;
  const after = cursors[pageIndex];

  function next(nextCursor: string | null) {
    setSaved((current) => current.resetKey === resetKey
      ? { ...advanceCursor(current, pageIndex, nextCursor), resetKey }
      : current);
  }

  function prev() {
    setSaved((current) => ({ ...current, pageIndex: Math.max(0, current.pageIndex - 1) }));
  }

  function reset() { setSaved({ resetKey, cursors: [undefined], pageIndex: 0 }); }

  return { after, pageNumber: pageIndex + 1, hasPrev: pageIndex > 0, next, prev, reset };
}
