import type { ReactNode } from "react";
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";

/**
 * The titled container every page section lives in — one header treatment, one divider, one set
 * of paddings, so panels line up across routes instead of each page inventing a card.
 *
 * <p>`action` is the panel's own control (a window selector, a column menu). Keep those at
 * `size="sm"`: page-level toolbars sit at the 32px control height, panel-local controls one step
 * below, and that difference is what tells the eye which scope a control belongs to.
 *
 * <p>`flush` drops the content padding for children that draw their own edge — a table whose
 * border should meet the panel's, not float inside it.
 */
export function Panel({
  title,
  description,
  action,
  flush = false,
  children,
}: {
  title: string;
  description?: string;
  action?: ReactNode;
  flush?: boolean;
  children: ReactNode;
}) {
  return (
    <Card className="gap-0 py-0">
      <CardHeader className="border-b py-3">
        <CardTitle className="text-headline">{title}</CardTitle>
        {description && <CardDescription className="text-xs">{description}</CardDescription>}
        {action && <CardAction>{action}</CardAction>}
      </CardHeader>
      <CardContent className={cn(flush ? "p-0" : "py-3")}>{children}</CardContent>
    </Card>
  );
}

/**
 * A panel footer for controls that belong to the content above it — pagination under a table.
 * Separated by the same hairline as the header, so a panel reads as header / body / footer at any
 * size instead of the footer floating off the bottom edge.
 */
export function PanelFooter({ children }: { children: ReactNode }) {
  return <div className="flex items-center justify-end gap-2 border-t px-4 py-2.5">{children}</div>;
}
