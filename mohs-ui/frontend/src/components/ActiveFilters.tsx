import { XIcon } from "lucide-react";
import { Button } from "@/components/ui/button";

export interface ActiveFilter {
  id: string;
  label: string;
  onRemove: () => void;
}

export function ActiveFilters({ filters, onClear }: { filters: ActiveFilter[]; onClear: () => void }) {
  if (filters.length === 0) return null;
  return (
    <div aria-label="Active filters" className="flex flex-wrap items-center gap-2">
      <span className="text-xs text-muted-foreground">Filtered by</span>
      {filters.map((filter) => (
        <Button key={filter.id} variant="secondary" size="sm" onClick={filter.onRemove}
          aria-label={`Remove filter: ${filter.label}`} title={filter.label} className="max-w-full gap-2 rounded-full text-xs">
          <span className="truncate">{filter.label}</span><XIcon className="size-3 shrink-0" />
        </Button>
      ))}
      <Button variant="ghost" size="sm" onClick={onClear} className="text-xs text-muted-foreground">Clear all</Button>
    </div>
  );
}
