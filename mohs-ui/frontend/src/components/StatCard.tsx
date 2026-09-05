import type { ReactNode } from "react";
import { Link } from "@tanstack/react-router";
import { IconArrowRight } from "./Icons";
import { cn } from "@/lib/utils";
import { useCountUp } from "@/lib/useCountUp";

export function StatCard({
  label,
  value,
  icon,
  accent = false,
  to,
  search,
  detail,
}: {
  label: string;
  value: ReactNode;
  icon?: ReactNode;
  accent?: boolean;
  to?: string;
  search?: Record<string, string>;
  detail?: string;
}) {
  // Only plain numbers count up — formatted values ("17.5 h", "1/37", "…") render as-is.
  const numericValue = typeof value === "number" ? value : null;
  const animatedValue = useCountUp(numericValue);
  const displayValue = numericValue !== null ? (animatedValue ?? numericValue) : value;

  const content = (
    <>
      <div className="flex items-start justify-between gap-2 text-muted-foreground">
        <span className="text-xs font-medium leading-5">{label}</span>
        <span className={cn("mt-0.5 shrink-0", accent && "text-primary")}>{icon}</span>
      </div>
      <p className={cn("mt-3 text-3xl font-semibold tracking-tight tabular-nums", accent && "text-primary")}>{displayValue}</p>
      {detail && <p className="mt-1 text-xs text-muted-foreground">{detail}</p>}
      {to && <IconArrowRight className="absolute bottom-5 right-4 size-3.5 text-muted-foreground transition-transform group-hover:translate-x-1 group-hover:text-primary" />}
    </>
  );
  const className = "metric-cell relative min-w-0 px-4 py-5";
  return to ? (
    <Link to={to} search={search} className={cn(className, "group transition-colors hover:bg-primary/5 focus-visible:bg-primary/5")}>
      {content}
    </Link>
  ) : <div className={className}>{content}</div>;
}
