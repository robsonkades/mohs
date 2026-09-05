import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { SearchIcon, XIcon } from "lucide-react";
import { fetchJobs } from "../lib/api";
import { queryKeys } from "../lib/queryKeys";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Command, CommandEmpty, CommandGroup, CommandInput, CommandItem, CommandList } from "@/components/ui/command";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";

const MAX_RESULTS = 8;

/**
 * Filters executions by job. The `jobKey` is already the readable identity — there is no opaque
 * id to hide from the user — so the chip shows the key itself instead of a name resolved through
 * an extra request.
 *
 * `GET /jobs` returns the whole list in one request (definitions declared at boot, not history),
 * so the search runs client-side: one cached query, shared with the Jobs page, instead of a
 * round-trip per keystroke.
 */
export function JobPicker({
  jobKey,
  onChange,
  placeholder = "Filter by job…",
}: {
  jobKey: string | undefined;
  onChange: (jobKey: string | undefined) => void;
  placeholder?: string;
}) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");

  const jobs = useQuery({ queryKey: queryKeys.jobs(), queryFn: fetchJobs, enabled: open && !jobKey });

  const results = useMemo(() => {
    const needle = query.trim().toLowerCase();
    const all = jobs.data ?? [];
    const matching = needle
      ? all.filter(
          (job) => job.jobKey.toLowerCase().includes(needle) || job.name.toLowerCase().includes(needle),
        )
      : all;
    return matching.slice(0, MAX_RESULTS);
  }, [jobs.data, query]);

  if (jobKey) {
    return (
      <Badge variant="secondary" className="h-8 gap-1.5 rounded-md pr-1 pl-3 font-mono text-sm font-medium">
        {jobKey}
        <Button
          variant="ghost"
          size="icon-xs"
          onClick={() => onChange(undefined)}
          aria-label="Clear job filter"
          className="size-5"
        >
          <XIcon />
        </Button>
      </Badge>
    );
  }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button variant="outline" className="w-64 justify-start font-normal text-muted-foreground">
          <SearchIcon className="size-4" />
          {placeholder}
        </Button>
      </PopoverTrigger>
      <PopoverContent align="start" className="w-72 p-0">
        <Command shouldFilter={false}>
          <CommandInput placeholder={placeholder} value={query} onValueChange={setQuery} />
          <CommandList>
            {jobs.isPending && <div className="px-3 py-2 text-sm text-muted-foreground">Loading…</div>}
            {jobs.data && results.length === 0 && <CommandEmpty>No jobs found</CommandEmpty>}
            {results.length > 0 && (
              <CommandGroup>
                {results.map((job) => (
                  <CommandItem
                    key={job.jobKey}
                    value={job.jobKey}
                    onSelect={() => {
                      onChange(job.jobKey);
                      setQuery("");
                      setOpen(false);
                    }}
                  >
                    <div className="flex flex-col items-start">
                      <span className="font-mono text-sm font-medium">{job.jobKey}</span>
                      <span className="truncate text-xs text-muted-foreground">{job.name}</span>
                    </div>
                  </CommandItem>
                ))}
              </CommandGroup>
            )}
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}
