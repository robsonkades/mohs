# Dashboard

Status: Active · Last Reviewed: 2026-09-05 · Source of Truth: Repository

The optional dashboard is mounted at `/mohs-ui` on the host application's HTTP server. It reads and
mutates the operational REST API at `/api/mohs/v1`; `mohs.api.enabled=true` and the `mohs-ui`
dependency are both required.

## Pages

| Page | Use it for |
| --- | --- |
| Overview | Cluster health, queue and execution counts, throughput, recent attention items, nodes and upcoming firings |
| Jobs | Search definitions, inspect schedules and state, run manually, pause, resume or reschedule |
| Executions | Filter by job, status and time; inspect attempts; copy identifiers; cancel or retry eligible work |
| Rate Limits | Inspect cluster-wide token buckets and change their maximum and window |
| Runners | Inspect runner mode, capacity, activity and queueing on the node serving the request |

Select a row to open its detail drawer. Filters are reflected in the URL, so a filtered jobs or
executions view can be bookmarked and shared with another operator who has access.

## Live data and refresh

The header shows the SSE connection state. A healthy live connection updates the one-minute
overview and invalidates affected queries as events arrive. Other overview windows use polling.
Reconnection is automatic after an interruption, and the refresh button invalidates all dashboard
queries immediately.

The **Live list** switch on Executions controls only whether rows move while you inspect them. It
does not disconnect the dashboard, pause jobs or stop event delivery. A custom date range also
freezes the list so the selected interval remains stable.

## Operational actions

Mutating actions show a confirmation dialog with their consequence:

- Pausing stops automatic firing; manual scheduling and work already in flight continue.
- Resuming may materialise occurrences that became due during the pause according to the job's
  misfire policy.
- Cancelling running work is cooperative; the handler must observe cancellation.
- Retrying rearms the same failed execution and bypasses its exhausted automatic retry budget.
- Runtime schedule and rate-limit changes are cluster-wide but may be replaced at the next boot by
  the configured definition conflict policy. Read the notice returned by the action.

The UI enables actions only for compatible states, while the server remains the authority and can
reject a race with a 409 or 503.

## Navigation and accessibility

Press `/` outside an input to focus global search. Press `Ctrl+K` or `Command+K` to open the command
palette and navigate to a page or job. The sidebar collapses on smaller screens, dialogs trap focus,
and tables expose labelled controls for keyboard and assistive-technology users.

## Security

The dashboard has no login or permission model. Protect `/mohs-ui`, `/mohs-ui/**` and
`/api/mohs/**` with the host's Spring Security chain or an upstream gateway. Its requests do not add
an `X-Mohs-Actor` header, so the default resolver records mutations as `anonymous`; replace
`ActorResolver` with authenticated identity for an accountable audit trail.

The bundled client does not send a CSRF token. A host using cookie or other browser-managed
credentials must integrate the token into the SPA before allowing mutations. See the
[security guide](../08-security/security-overview.md) for a complete example.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Dashboard returns 404 | Confirm the `mohs-ui` dependency is present and open `/mohs-ui`, not the API path |
| Data requests return 404 | Confirm `mohs.api.enabled=true` and that the API base path matches the bundled client |
| Mutations return 403 | Check host authorization and CSRF configuration |
| Live indicator is interrupted | Check the `/api/mohs/v1/overview/stream` response and proxy buffering/timeouts |
| Values look stale | Use refresh, check the live indicator and confirm the browser tab is visible |

For server-side symptoms, continue with [troubleshooting](troubleshooting.md) and
[health and diagnostics](../09-observability/health-and-diagnostics.md).
