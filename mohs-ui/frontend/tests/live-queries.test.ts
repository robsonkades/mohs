import assert from "node:assert/strict";
import { test } from "node:test";
import { QueryClient, QueryObserver } from "@tanstack/react-query";
import { refreshExecutions, refreshSupplemental, seedJobs } from "../src/lib/liveQueries.ts";
import type { JobResponse } from "../src/types/api.ts";

test("pausing history leaves details and attention live; supplemental polling covers nonstreamed resources", async () => {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
  const reads = new Map<string, number>();
  const subscriptions: (() => void)[] = [];
  const cases = [
    ["history", ["executions", {}, undefined]],
    ["detail", ["execution", "id"]],
    ["attention", ["execution-attention"]],
    ["minute", ["overview", "PT1M"]],
    ["hour", ["overview", "PT1H"]],
    ["limits", ["rate-limits"]],
  ] as const;
  try {
    for (const [name, queryKey] of cases) {
      const queryFn = async () => { reads.set(name, (reads.get(name) ?? 0) + 1); return reads.get(name); };
      await client.fetchQuery({ queryKey, queryFn });
      subscriptions.push(new QueryObserver(client, { queryKey, queryFn }).subscribe(() => {}));
    }
    await refreshExecutions(client, false);
    assert.equal(reads.get("history"), 1);
    assert.equal(reads.get("detail"), 2);
    assert.equal(reads.get("attention"), 2);
    await refreshSupplemental(client, "live", false);
    assert.equal(reads.get("limits"), 2);
    assert.equal(reads.get("hour"), 2);
    assert.equal(reads.get("minute"), 1);
    await refreshSupplemental(client, "offline", false);
    assert.equal(reads.get("history"), 1);
    assert.equal(reads.get("detail"), 3);
    assert.equal(reads.get("minute"), 2);
    await refreshExecutions(client, true);
    assert.equal(reads.get("history"), 2);
  } finally { subscriptions.forEach((unsubscribe) => unsubscribe()); client.clear(); }
});

test("a jobs frame refreshes a previously loaded drawer without another read", () => {
  const client = new QueryClient();
  try {
    const job = { jobKey: "job-a", paused: false } as JobResponse;
    client.setQueryData(["job", "job-a"], job);
    seedJobs(client, [{ ...job, paused: true }]);
    assert.equal(client.getQueryData<JobResponse>(["job", "job-a"])?.paused, true);
  } finally { client.clear(); }
});

test("ticks do not restart a slow in-flight read", async () => {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
  const queryKey = ["execution", "slow"];
  client.setQueryData(queryKey, "old");
  let reads = 0;
  let complete!: (value: string) => void;
  const observer = new QueryObserver(client, { queryKey, queryFn: () => { reads++; return new Promise<string>((resolve) => { complete = resolve; }); } });
  const unsubscribe = observer.subscribe(() => {});
  try {
    const first = refreshExecutions(client, true);
    const second = refreshExecutions(client, true);
    assert.equal(reads, 1);
    complete("new");
    await Promise.all([first, second]);
    assert.equal(client.getQueryData(queryKey), "new");
  } finally { unsubscribe(); client.clear(); }
});
