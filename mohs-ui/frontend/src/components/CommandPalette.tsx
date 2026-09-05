import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";
import { fetchJobs } from "../lib/api";
import { queryKeys } from "../lib/queryKeys";
import { NAV_ITEMS } from "@/components/AppSidebar";
import { IconArrowRight, IconListChecks } from "@/components/Icons";
import { Button } from "@/components/ui/button";
import {
  Command,
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";

const MAX_JOB_RESULTS = 6;

/**
 * Global search: pages by name, jobs by key. Opened with / or Ctrl+K.
 *
 * Pages come from NAV_ITEMS rather than a list of its own: two route lists is how the palette ends
 * up still offering a page the sidebar no longer has.
 */
export function CommandPalette({ open, onClose }: { open: boolean; onClose: () => void }) {
  const navigate = useNavigate();
  const [query, setQuery] = useState("");

  // This component is always mounted (see site-header): open/close only toggles the dialog's
  // visibility, so without this the previous query would carry over to the next opening.
  useEffect(() => {
    if (open) setQuery("");
  }, [open]);

  const jobs = useQuery({ queryKey: queryKeys.jobs(), queryFn: fetchJobs, enabled: open });

  const pages = useMemo(() => {
    const needle = query.trim().toLowerCase();
    return NAV_ITEMS.filter((page) => !needle || page.label.toLowerCase().includes(needle));
  }, [query]);

  const matchingJobs = useMemo(() => {
    const needle = query.trim().toLowerCase();
    if (!needle) {
      return [];
    }
    return (jobs.data ?? [])
      .filter((job) => job.jobKey.toLowerCase().includes(needle) || job.name.toLowerCase().includes(needle))
      .slice(0, MAX_JOB_RESULTS);
  }, [jobs.data, query]);

  function goToPage(to: string) {
    onClose();
    void navigate({ to });
  }

  function goToJob(jobKey: string) {
    onClose();
    void navigate({ to: "/jobs", search: { jobKey } });
  }

  return (
    <CommandDialog
      open={open}
      onOpenChange={(next) => !next && onClose()}
      title="Global search"
      description="Search pages and jobs…"
    >
      <Command shouldFilter={false}>
        <CommandInput placeholder="Search pages and jobs…" value={query} onValueChange={setQuery} />
        <CommandList>
          {jobs.isError && (
            <div role="status" className="flex items-center justify-between gap-3 border-b px-3 py-3 text-xs text-muted-foreground">
              <span>Job search is unavailable. You can still navigate to pages.</span>
              <Button size="sm" variant="outline" disabled={jobs.isFetching} onClick={() => void jobs.refetch()}>Retry</Button>
            </div>
          )}
          {pages.length === 0 && matchingJobs.length === 0 && (
            <CommandEmpty>{jobs.isFetching ? "Searching…" : jobs.isError ? "No matching pages. Retry job search above." : `Nothing matches “${query}”.`}</CommandEmpty>
          )}
          {pages.length > 0 && (
            <CommandGroup heading="Pages">
              {pages.map((page) => (
                <CommandItem key={page.to} value={"page:" + page.to} onSelect={() => goToPage(page.to)} className="gap-3 py-3">
                  <page.icon className="size-4 text-muted-foreground" />
                  <span className="flex min-w-0 flex-col gap-0.5">
                    <span className="font-medium">{page.label}</span>
                    <span className="truncate text-xs text-muted-foreground">{page.subtitle}</span>
                  </span>
                  <IconArrowRight className="ml-auto size-3.5 text-muted-foreground" />
                </CommandItem>
              ))}
            </CommandGroup>
          )}
          {matchingJobs.length > 0 && (
            <CommandGroup heading="Jobs">
              {matchingJobs.map((job) => (
                <CommandItem key={job.jobKey} value={"job:" + job.jobKey} onSelect={() => goToJob(job.jobKey)} className="gap-3 py-3">
                  <IconListChecks className="size-4 text-muted-foreground" />
                  <span className="flex min-w-0 flex-col gap-0.5">
                    <span className="truncate font-medium">{job.name}</span>
                    <span className="truncate font-mono text-xs text-muted-foreground">{job.jobKey}</span>
                  </span>
                  <span className="ml-auto shrink-0 text-xs text-muted-foreground">{job.paused ? "Paused" : "Active"}</span>
                </CommandItem>
              ))}
            </CommandGroup>
          )}
        </CommandList>
        <div className="flex gap-4 border-t px-3 py-2.5 text-[11px] text-muted-foreground" aria-hidden="true">
          <span>↑ ↓ to navigate</span><span>↵ to open</span><span className="ml-auto">Esc to close</span>
        </div>
      </Command>
    </CommandDialog>
  );
}
