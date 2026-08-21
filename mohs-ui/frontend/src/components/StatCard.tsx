import type { ReactNode } from "react";
import { Card, CardAction, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";
import { useCountUp } from "@/lib/useCountUp";

export function StatCard({
  label,
  value,
  icon,
  accent = false,
}: {
  label: string;
  value: ReactNode;
  icon?: ReactNode;
  accent?: boolean;
}) {
  // Only plain numbers count up — formatted values ("17.5 h", "1/37", "…") render as-is.
  const numericValue = typeof value === "number" ? value : null;
  const animatedValue = useCountUp(numericValue);
  const displayValue = numericValue !== null ? (animatedValue ?? numericValue) : value;

  return (
    <Card
      className={cn(
        "gap-3 transition-colors",
        accent ? "ring-primary/40 hover:ring-primary/60" : "hover:ring-primary/50",
      )}
    >
      <CardHeader>
        <CardTitle className="mono-label font-mono text-muted-foreground">{label}</CardTitle>
        {icon && <CardAction className={accent ? "text-primary" : "text-muted-foreground"}>{icon}</CardAction>}
      </CardHeader>
      <p className="px-(--card-spacing) text-display tabular-nums">{displayValue}</p>
    </Card>
  );
}
