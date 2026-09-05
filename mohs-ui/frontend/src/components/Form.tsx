import type { ReactNode } from "react";

/**
 * The filter row every list page opens with. No outer margin on purpose: it used to carry
 * `mb-4` while its parent also set a gap, so the space under the filters was double the space
 * everywhere else on the same page. Spacing belongs to the container (see ./Layout), never to
 * the child — a component that spaces itself cannot be composed.
 */
export function FilterBar({ children }: { children: ReactNode }) {
  return <div className="flex flex-wrap items-center gap-2.5">{children}</div>;
}
