# Dashboard SSE stream

`GET /overview/stream` returns `text/event-stream`. It carries periodic snapshots for the
operational dashboard, without replay or durable event-delivery guarantees. Reconnect and read
current state; intermediate transitions may be missed. Use execution listing/detail for history.

## Frame contract

Every event uses `{ "asOf": "...", "data": ... }`. All events from a snapshot share one `asOf`.

| Event | Default payload |
| --- | --- |
| `overview` | `OverviewResponse`, default 60-second throughput window |
| `jobs` | `JobResponse[]` |
| `nodes` | `NodeResponse[]` |
| `runners` | `RunnerResponse[]` |
| `executions` | `ExecutionSummaryResponse[]`, first page of 50 |

Default subscribers retain the array contract. Clients may explicitly request
`GET /overview/stream?executionMode=invalidation`. For those subscribers only, executions use:

```text
event: executions
data: {"asOf":"2026-09-19T12:00:00Z","data":{"changed":true}}
```

This signal means refresh the client's current query; it is emitted periodically and is not proof
that a specific execution changed. The bundled dashboard uses it to refresh its filtered page.
When all subscribers opt in, snapshot construction omits the unfiltered executions query. A mixed
group still performs that query for clients expecting arrays, and converts only opt-in frames.

## Cadence and bounded work

| Property | Behavior |
| --- | --- |
| Cadence | Fixed delay of 2 seconds; slow ticks do not accumulate scheduled ticks |
| Reads | One snapshot shared across subscribers; no periodic reads when none are connected |
| New joins | A compatible snapshot younger than 2 seconds is reused; a lock serializes builds, including initial joins |
| Concurrent reads | At most five readers across builds/joins of this broadcaster |
| Read deadline | 2 seconds per build, followed by up to 1 second of cancellation grace |
| Build lock | Acquisition waits at most 2 seconds; this is additional to a build's deadline |
| Subscriber capacity | 64 seats, reserved before the initial snapshot |
| Slow receivers | A subscriber still writing a previous frame skips that tick; it does not queue frames |

A reader that ignores interruption retains its permit until the actual read returns. Later
snapshots fail when read capacity is exhausted instead of creating unbounded blocked database
readers. This bounds concurrent work; it cannot make an unresponsive driver stop. The store's
statement/pool timeouts remain necessary.

The independent reads run concurrently and do not form a single database transaction. A common
`asOf` is a frame observation stamp, not a promise of cross-query transactional consistency.

## Subscription and shutdown

The initial snapshot is obtained before registering the subscriber. It is sent while the new
subscriber is marked busy, preventing a periodic tick from interleaving with its initial frames.
A failed snapshot releases its reserved seat. A closed broadcaster or exhausted subscriber
capacity returns an already completed emitter without reading the database.

The stream has no emitter timeout. Disconnect/error callbacks remove the subscriber and release
its seat once. `MohsOverviewStreamLifecycle` closes streams before the web server's graceful
shutdown phase so idle dashboards do not keep that phase open. A servlet write already blocked
on a slow client can still delay completion on that connection.

## Default client example

```javascript
const es = new EventSource("/api/mohs/v1/overview/stream");
es.addEventListener("overview", e => {
  const { asOf, data } = JSON.parse(e.data);
  renderOverview(data, asOf);
});
es.addEventListener("executions", e => {
  const { data } = JSON.parse(e.data);
  renderTable(data); // array: this client did not request invalidations
});
```

Use the configured API base including the servlet context in a deployed client. Authentication,
authorization and proxy buffering/timeouts belong to the host. There is no `Last-Event-ID` replay,
per-client server filtering or exactly-once delivery guarantee. Snapshot/read limits do not
replace host access controls. See [dashboard deployment](../13-operations/dashboard.md).
