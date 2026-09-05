import type { ReactNode } from "react";
import { Loader2Icon } from "lucide-react";
import { IconAlertTriangle, IconInbox } from "./Icons";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

/**
 * One reserved height for all three async states.
 *
 * <p>These render in place of the content they are waiting on, and they replace each other:
 * loading → error → retry → loading → empty. Sized independently they would each hand the page a
 * different height and the surrounding layout would step every time one swapped for another —
 * the same class of jump as a table that re-measures its columns. A single floor means the panel
 * takes its shape once and keeps it.
 */
const RESERVED = "flex min-h-40 min-w-0 flex-col items-center justify-center gap-3 px-4 py-10 text-center [overflow-wrap:anywhere]";

export function Spinner({ label = "Loading", className }: { label?: string; className?: string }) {
  return (
    <div className={cn(RESERVED, "text-muted-foreground", className)} role="status" aria-live="polite">
      <Loader2Icon className="size-5 animate-spin motion-reduce:animate-none" />
      <span className="text-sm">{label}…</span>
    </div>
  );
}

export function ErrorState({
  message,
  onRetry,
  className,
}: {
  message: string;
  onRetry?: () => void;
  className?: string;
}) {
  return (
    <div className={cn(RESERVED, className)} role="alert">
      <IconAlertTriangle className="size-8 text-critical" />
      <p className="max-w-sm text-sm text-muted-foreground">{message}</p>
      {onRetry && (
        <Button variant="outline" onClick={onRetry}>
          Try again
        </Button>
      )}
    </div>
  );
}

export function EmptyState({
  title,
  description,
  className,
}: {
  title: string;
  description?: ReactNode;
  className?: string;
}) {
  return (
    <div className={cn(RESERVED, className)}>
      <IconInbox className="size-8 text-muted-foreground" />
      <p className="text-sm font-medium">{title}</p>
      {description && <p className="max-w-sm text-sm text-muted-foreground">{description}</p>}
    </div>
  );
}
