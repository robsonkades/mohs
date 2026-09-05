# Soak: the long run nothing in this bench has ever done (PLAN.md, P2).
#
# The ten *Scenario classes drain in about a minute. Everything that only
# appears with uptime — a slow leak, clock drift, connection growth, a plan
# degrading as history grows — is invisible to them by construction. This
# script runs ONE node under modest, continuous load for hours and samples the
# four series that would show each of those.
#
# Acceptance (PLAN.md P2): throughput and latency stable from start to finish,
# heap with no trend, zero executions lost.
#
# Prerequisites, identical to write-amplification.ps1: the demo app running on
# the host against the target Postgres, container named 'postgres'
# (postgres/postgres), and the app's management port on 8090 (that is where
# the Prometheus scrape in the root docker-compose expects it).
#
# The load is deliberately modest. A soak at the throughput ceiling measures
# the ceiling; what this run is for is the SECOND derivative — whether a number
# that is fine at hour 1 is still fine at hour 12.

param(
    # Executions enqueued per wave. Small on purpose: the point is duration.
    [int]$WaveSize = 200,
    [int]$WaveIntervalSeconds = 10,
    [int]$DurationHours = 12,
    [int]$SampleIntervalSeconds = 60,
    [string]$JobKey = 'every-job',
    [string]$Container = 'postgres',
    [string]$Database = 'mohs',
    [string]$ActuatorBase = 'http://localhost:8090',
    [string]$OutputDirectory = "$PSScriptRoot/../target/soak"
)

$ErrorActionPreference = 'Stop'

function Invoke-Psql([string]$Sql) {
    docker exec $Container psql -U postgres -d $Database -t -A -c $Sql
}

function Get-ActuatorMetric([string]$Name) {
    try {
        $response = Invoke-RestMethod -Uri "$ActuatorBase/actuator/metrics/$Name" -TimeoutSec 5
        return [double]($response.measurements | Where-Object { $_.statistic -eq 'VALUE' } | Select-Object -First 1).value
    } catch {
        # A sample that could not be taken is recorded as NaN rather than skipped:
        # a gap in the series is itself a finding (the app was unreachable), and
        # dropping the row would hide it.
        return [double]::NaN
    }
}

New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$samplesPath = Join-Path $OutputDirectory "soak-$stamp.csv"
$summaryPath = Join-Path $OutputDirectory "soak-$stamp.md"

Write-Host "Soak: $DurationHours h, $WaveSize executions every $WaveIntervalSeconds s, sampling every $SampleIntervalSeconds s"
Write-Host "Samples -> $samplesPath"

# Baseline BEFORE the first wave: every delta below is against this row, and
# without it a leak that started before the script did would read as flat.
$startedAt = Get-Date
$endsAt = $startedAt.AddHours($DurationHours)
$baselineTerminal = [int](Invoke-Psql "SELECT count(*) FROM mohs_execution WHERE state IN ('SUCCEEDED','FAILED');")
$enqueued = 0
$samples = @()
$nextSample = $startedAt
$nextWave = $startedAt

while ((Get-Date) -lt $endsAt) {
    $now = Get-Date

    if ($now -ge $nextWave) {
        # Same enqueue unit as write-amplification.ps1: a PENDING history row
        # plus its mohs_ready entry, in one transaction. The prefix carries the
        # wave so a lost execution can be traced back to when it was born.
        $prefix = "soak-$stamp-$enqueued"
        Invoke-Psql @"
BEGIN;
INSERT INTO mohs_execution (execution_id, job_key, shard, priority, state, scheduled_at, created_at, actor, payload, payload_type)
SELECT '$prefix-' || g, '$JobKey', (g % 64), 20, 'PENDING', now(), now(), 'soak', '{}', 'java.lang.Object'
  FROM generate_series(1, $WaveSize) g;
INSERT INTO mohs_ready (execution_id, job_key, shard, priority, attempt, visible_at)
SELECT '$prefix-' || g, '$JobKey', (g % 64), 20, 1, now()
  FROM generate_series(1, $WaveSize) g;
COMMIT;
"@ | Out-Null
        $enqueued += $WaveSize
        $nextWave = $now.AddSeconds($WaveIntervalSeconds)
    }

    if ($now -ge $nextSample) {
        $elapsed = [int]($now - $startedAt).TotalSeconds
        $terminal = [int](Invoke-Psql "SELECT count(*) FROM mohs_execution WHERE state IN ('SUCCEEDED','FAILED');")
        $queued = [int](Invoke-Psql "SELECT count(*) FROM mohs_ready;")
        $running = [int](Invoke-Psql "SELECT count(*) FROM mohs_lease;")
        $history = [int](Invoke-Psql "SELECT count(*) FROM mohs_execution;")
        $connections = [int](Invoke-Psql "SELECT count(*) FROM pg_stat_activity WHERE datname = '$Database';")
        # Drift: the database's clock against this host's, the number
        # DatabaseClock would be correcting if mohs.time.mode=database
        $drift = [double](Invoke-Psql "SELECT EXTRACT(EPOCH FROM (now() - '$($now.ToUniversalTime().ToString('o'))'::timestamptz));")

        $sample = [PSCustomObject]@{
            elapsed_s     = $elapsed
            enqueued      = $enqueued
            terminal      = $terminal - $baselineTerminal
            queue_depth   = $queued
            running       = $running
            history_rows  = $history
            db_conns      = $connections
            clock_drift_s = $drift
            heap_used     = Get-ActuatorMetric 'jvm.memory.used'
            live_threads  = Get-ActuatorMetric 'jvm.threads.live'
            mohs_queue    = Get-ActuatorMetric 'mohs.queue.depth'
        }
        $samples += $sample
        $samples | Export-Csv -Path $samplesPath -NoTypeInformation
        Write-Host ("  {0,6}s  terminal={1,-8} queued={2,-6} heap={3:N0} threads={4} conns={5} drift={6:N3}s" -f `
            $sample.elapsed_s, $sample.terminal, $sample.queue_depth, $sample.heap_used, $sample.live_threads, `
            $sample.db_conns, $sample.clock_drift_s)
        $nextSample = $now.AddSeconds($SampleIntervalSeconds)
    }

    Start-Sleep -Seconds 1
}

# The verdict is read off the two halves. A soak has no single number: what it
# reports is whether the SECOND half looks like the first.
$half = [int][math]::Floor($samples.Count / 2)
$first = $samples | Select-Object -First $half
$second = $samples | Select-Object -Last $half
$firstRate = if ($first.Count -gt 1) { ($first[-1].terminal - $first[0].terminal) / [math]::Max(1, ($first[-1].elapsed_s - $first[0].elapsed_s)) } else { 0 }
$secondRate = if ($second.Count -gt 1) { ($second[-1].terminal - $second[0].terminal) / [math]::Max(1, ($second[-1].elapsed_s - $second[0].elapsed_s)) } else { 0 }
$lost = $enqueued - $samples[-1].terminal

@"
# Soak — $stamp

| | |
| --- | --- |
| Duration | $DurationHours h ($($samples[-1].elapsed_s) s observed) |
| Load | $WaveSize executions every $WaveIntervalSeconds s |
| Enqueued | $enqueued |
| Terminal | $($samples[-1].terminal) |
| **Unaccounted** | **$lost** (must be 0 net of what is still queued/running) |
| Throughput, first half | $([math]::Round($firstRate, 2))/s |
| Throughput, second half | $([math]::Round($secondRate, 2))/s |
| Heap, first sample | $($samples[0].heap_used) |
| Heap, last sample | $($samples[-1].heap_used) |
| Threads, first / last | $($samples[0].live_threads) / $($samples[-1].live_threads) |
| DB connections, first / last | $($samples[0].db_conns) / $($samples[-1].db_conns) |
| History rows at the end | $($samples[-1].history_rows) |

Acceptance (PLAN.md P2): throughput and latency stable start to finish, heap
with no trend, zero executions lost. The two throughput halves above are the
stability check; heap, threads and connections are read as TRENDS across
``$samplesPath``, not as endpoints — one high last sample is a GC that had not
run yet, a monotonic climb is the finding.
"@ | Set-Content -Path $summaryPath

Write-Host ""
Write-Host "Summary -> $summaryPath"
Get-Content $summaryPath
