# 13. Operations

Status: Active · Last Reviewed: 2026-09-05 · Source of Truth: Repository

| Document | What it covers |
| --- | --- |
| [startup-and-shutdown.md](startup-and-shutdown.md) | The boot sequence and its ordering guarantees, boot-time checks, the shutdown sequence and why its order is load-bearing, crash behaviour, Kubernetes guidance |
| [deployment.md](deployment.md) | What the repository does and does not contain, runtime requirements, sizing, probes, migrations at deploy time, the deployment checklist |
| [runbook.md](runbook.md) | Task-oriented procedures: pause, resume, cancel, retry, throttle, drain, retire — plus incident procedures |
| [troubleshooting.md](troubleshooting.md) | Symptom-first diagnosis, with the evidence to collect |
| [dashboard.md](dashboard.md) | Dashboard navigation, live refresh, operational actions and security expectations |

## The five things an operator must know

1. **Recovery after a node dies is automatic**, and the latency floor is `node-lease-ttl` (15 s by
   default). Any non-zero `mohs.lease.reclaimed` means a node died or stopped.
2. **A resume produces a burst** proportional to the pause length. That is not misfire — occurrences
   within the misfire threshold fire late under any policy.
3. **Every `PATCH` reverts on the next boot** under the default conflict policy. The response says so
   in a `notice` field. Follow up with a code change.
4. **History retention is opt-in.** Configure `mohs.engine.history-retention` when execution payloads
   and attempts must expire. Idempotency records have their own automatic retention, seven days by
   default.
5. **`terminationGracePeriodSeconds` must exceed** `mohs.lifecycle.shutdown.grace-period` **plus** the
   web server's graceful-shutdown phase — otherwise pods are killed mid-drain and their work is
   reclaimed instead of finished.

## The three commands that answer most questions

```bash
jcmd <pid> Thread.print | grep -A5 mohs-        # is the engine loop alive?
curl https://app/api/mohs/v1/nodes              # what does the cluster believe?
grep -E "lease expired|clock moved|owns no shard|tick step" app.log
```
