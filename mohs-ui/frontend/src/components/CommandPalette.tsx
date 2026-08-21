import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "@tanstack/react-router";
import { useQuery } from "@tanstack/react-query";
import { fetchJobs } from "../lib/api";
import { queryKeys } from "../lib/queryKeys";
import { NAV_ITEMS } from "@/components/app-sidebar";
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
          {pages.length === 0 && matchingJobs.length === 0 && (
            <CommandEmpty>{jobs.isFetching ? "Searching…" : `Nothing matches “${query}”.`}</CommandEmpty>
          )}
          {pages.length > 0 && (
            <CommandGroup heading="Pages">
              {pages.map((page) => (
                <CommandItem key={page.to} value={"page:" + page.to} onSelect={() => goToPage(page.to)}>
                  <span className="font-medium">{page.label}</span>
                </CommandItem>
              ))}
            </CommandGroup>
          )}
          {matchingJobs.length > 0 && (
            <CommandGroup heading="Jobs">
              {matchingJobs.map((job) => (
                <CommandItem key={job.jobKey} value={"job:" + job.jobKey} onSelect={() => goToJob(job.jobKey)}>
                  <span className="font-mono text-xs">{job.jobKey}</span>
                </CommandItem>
              ))}
            </CommandGroup>
          )}
        </CommandList>
      </Command>
    </CommandDialog>
  );
}
