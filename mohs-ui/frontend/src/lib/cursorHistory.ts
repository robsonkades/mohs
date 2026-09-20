export interface CursorHistory {
  cursors: (string | undefined)[];
  pageIndex: number;
}

export function advanceCursor(history: CursorHistory, pageIndex: number, nextCursor: string | null): CursorHistory {
  if (!nextCursor || history.pageIndex !== pageIndex || history.cursors[pageIndex] === nextCursor) return history;
  // Re-read pages may have a different continuation. Drop the old branch instead of skipping rows.
  return { cursors: [...history.cursors.slice(0, pageIndex + 1), nextCursor], pageIndex: pageIndex + 1 };
}
