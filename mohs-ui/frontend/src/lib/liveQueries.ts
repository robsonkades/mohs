import type { QueryClient } from "@tanstack/react-query";
import type { JobResponse } from "../types/api";
import type { StreamStatus } from "./streamWatchdog";

export function refreshExecutions(client: QueryClient, listLive: boolean) {
  return client.invalidateQueries({
    predicate: ({ queryKey: [resource] }) => resource === "execution" || resource === "execution-attention"
      || resource === "job-executions" || (resource === "executions" && listLive),
  }, { cancelRefetch: false });
}

export function refreshSupplemental(client: QueryClient, status: StreamStatus, listLive: boolean) {
  return client.invalidateQueries({
    predicate: ({ queryKey: [resource, window] }) => {
      if (resource === "executions" && !listLive) return false;
      return status !== "live" || resource === "rate-limits" || (resource === "overview" && window !== "PT1M");
    },
  }, { cancelRefetch: false });
}

export function seedJobs(client: QueryClient, jobs: JobResponse[]) {
  client.setQueryData(["jobs"], jobs);
  const byKey = new Map(jobs.map((job) => [job.jobKey, job]));
  for (const query of client.getQueryCache().findAll({ queryKey: ["job"] })) {
    const job = byKey.get(String(query.queryKey[1]));
    if (job) client.setQueryData(query.queryKey, job);
    else void client.invalidateQueries({ queryKey: query.queryKey, exact: true }, { cancelRefetch: false });
  }
}
