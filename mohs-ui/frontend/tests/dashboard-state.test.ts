import assert from "node:assert/strict";
import { test } from "node:test";
import { validateExecutionsSearch } from "../src/lib/executionSearch.ts";
import { isTimeWindow } from "../src/lib/timeWindows.ts";
import { formatDuration } from "../src/lib/format.ts";
import { advanceCursor } from "../src/lib/cursorHistory.ts";
import { createStreamWatchdog, type StreamStatus } from "../src/lib/streamWatchdog.ts";
import { parseJobPayload, newSubmissionKey } from "../src/lib/manualSubmission.ts";
import { nodeFreshness } from "../src/lib/nodeStatus.ts";

test("invalid shared filters recover without passing invalid dates or inherited windows to the page", () => {
  for (const window of ["toString", "constructor", "__proto__", "bogus"]) {
    assert.equal(isTimeWindow(window), false);
    const search = validateExecutionsSearch({ window, from: "invalid", status: "DESTROYED" });
    assert.equal(search.window, "1h");
    assert.equal(search.from, undefined);
    assert.equal(search.status, undefined);
    assert.ok(search.filterWarning);
    assert.equal(validateExecutionsSearch({ ...search }).filterWarning, search.filterWarning);
  }
  for (const from of ["2026-02-30T12:00:00Z", "2026-09-19T24:00:00Z", "2026-09-19T10:00:00"]) {
    assert.equal(validateExecutionsSearch({ from }).from, undefined);
  }
  assert.equal(validateExecutionsSearch({ from: "2026-09-19T12:00:00Z", to: "2026-09-19T11:00:00Z" }).from, undefined);
  const precise = "2026-09-19T12:00:00.123456Z";
  assert.equal(validateExecutionsSearch({ from: precise, window: "all" }).from, precise);
  assert.equal(validateExecutionsSearch({ status: "FAILED", window: "all" }).filterWarning, undefined);
});

test("subsecond durations keep the precision of valid schedules", () => {
  assert.equal(formatDuration("PT0.5S"), "0.5s");
  assert.equal(formatDuration("PT1.5S"), "1.5s");
  assert.equal(formatDuration("PT0.000001S"), "0.000001s");
  assert.equal(formatDuration("PT1M0.25S"), "1m 0.25s");
  assert.equal(formatDuration("PT0S"), "0s");
});

test("a repeated next callback cannot duplicate a page and a changed continuation replaces its old branch", () => {
  const first = { cursors: [undefined], pageIndex: 0 };
  const second = advanceCursor(first, 0, "cursor-1");
  assert.equal(advanceCursor(second, 0, "cursor-1"), second);
  assert.equal(advanceCursor(second, 1, "cursor-1"), second);
  const returned = { cursors: [undefined, "old-cursor", "old-page-3"], pageIndex: 0 };
  assert.deepEqual(advanceCursor(returned, 0, "new-cursor"), { cursors: [undefined, "new-cursor"], pageIndex: 1 });
});

test("SSE timeout starts before open, repeated errors do not postpone it, and frames restore live", () => {
  let now = 0;
  let nextId = 0;
  const tasks = new Map<number, { at: number; callback: () => void }>();
  const states: StreamStatus[] = [];
  const watchdog = createStreamWatchdog((status) => states.push(status), {
    set(callback, delay) { const id = ++nextId; tasks.set(id, { at: now + delay, callback }); return id; },
    clear(id) { tasks.delete(id as number); },
  });
  const advance = (millis: number) => {
    now += millis;
    for (const [id, task] of tasks) if (task.at <= now) { tasks.delete(id); task.callback(); }
  };
  advance(2000); watchdog.reconnecting();
  advance(2000); watchdog.reconnecting();
  advance(2000);
  assert.equal(states.at(-1), "offline");
  watchdog.reconnecting();
  assert.equal(states.at(-1), "offline");
  watchdog.received();
  assert.equal(states.at(-1), "live");
  advance(6000);
  assert.equal(states.at(-1), "offline");
  watchdog.received();
  watchdog.close();
  advance(6000);
  assert.equal(states.at(-1), "live");
});

test("manual payload rejects nonobjects and new intents get distinct keys", () => {
  for (const json of ["[1]", "null", "4", "bad JSON"]) assert.throws(() => parseJobPayload(json));
  assert.deepEqual(parseJobPayload('{"customerId":"123","optional":null}'), { customerId: "123", optional: null });
  assert.notEqual(newSubmissionKey(), newSubmissionKey());
});

test("node freshness respects configured TTL and terminal lifecycle states", () => {
  const node = { state: "RUNNING" as const, lastHeartbeatAt: "2026-09-19T12:00:00Z" };
  const now = Date.parse("2026-09-19T12:00:20Z");
  assert.equal(nodeFreshness(node, now, 60).label, "Online");
  assert.equal(nodeFreshness(node, now, 15).label, "Heartbeat overdue");
  assert.equal(nodeFreshness({ ...node, state: "STOPPED" }, now, 60).label, "Stopped");
  assert.equal(nodeFreshness({ ...node, state: "FAILED" }, now, 60).label, "Failed");
});
