import assert from "node:assert/strict";
import { test } from "node:test";

Object.defineProperty(globalThis, "window", { value: { location: { origin: "https://example.test" } }, configurable: true });
Object.defineProperty(globalThis, "document", { value: {
  getElementById: () => ({ textContent: JSON.stringify({ apiBase: "/host/custom-api", uiBase: "/host/mohs-ui", csrfHeaderName: "X-CSRF-TOKEN", csrfToken: "sample-token", nodeLeaseTtlSeconds: 60 }) }),
}, configurable: true });
const api = await import("../src/lib/api.ts");

test("bootstrap paths, CSRF and one idempotency key survive a lost-response retry", async (context) => {
  assert.equal(api.API_BASE, "/host/custom-api");
  assert.equal(api.UI_BASE, "/host/mohs-ui");
  const requests: { url: string; headers: Headers; body: unknown }[] = [];
  context.mock.method(globalThis, "fetch", async (url: URL, init: RequestInit) => {
    requests.push({ url: String(url), headers: new Headers(init.headers), body: init.body });
    if (requests.length === 1) throw new TypeError("response lost");
    return new Response(JSON.stringify({ executionId: "same-receipt" }), { status: 202 });
  });
  const body = { payload: { customerId: "123" } };
  await assert.rejects(api.scheduleJob("job-a", body, "same-intent"), /response lost/);
  assert.equal((await api.scheduleJob("job-a", body, "same-intent")).executionId, "same-receipt");
  assert.equal(requests[0]?.url, "https://example.test/host/custom-api/jobs/job-a/schedule");
  for (const request of requests) {
    assert.equal(request.headers.get("X-CSRF-TOKEN"), "sample-token");
    assert.equal(request.headers.get("Idempotency-Key"), "same-intent");
    assert.equal(request.body, JSON.stringify(body));
  }
});

test("a cancelled query aborts its HTTP read", async (context) => {
  const controller = new AbortController();
  context.mock.method(globalThis, "fetch", (_url: URL, init: RequestInit) => {
    assert.equal(init.signal, controller.signal);
    return new Promise<Response>((_resolve, reject) => init.signal?.addEventListener("abort", () => reject(init.signal?.reason), { once: true }));
  });
  const request = api.fetchExecution("execution-id", controller.signal);
  controller.abort(new Error("query no longer needed"));
  await assert.rejects(request, /query no longer needed/);
});
