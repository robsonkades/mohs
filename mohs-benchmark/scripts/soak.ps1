# Sustained SQL drain workload with scoped accounting and periodic metrics.
# Requires a disposable PostgreSQL 13+ database, current schema, running demo and
# PowerShell 7+. Disable history retention or retain longer than the run: deleted
# history cannot prove loss-free processing. This bypasses API acceptance;
# use api-load.ps1 for end-to-end submission.

param(
    # Executions enqueued per wave. Small on purpose: the point is duration.
    [int]$WaveSize = 200,
    [int]$WaveIntervalSeconds = 10,
    [int]$DurationHours = 12,
    [int]$SampleIntervalSeconds = 60,
    [string]$JobKey = 'every-job',
    [string]$Container = 'postgres',
    [string]$Database = 'postgres',
    [string]$DbUser = 'postgres',
    [string]$PayloadJson = '{}',
    [int]$DrainTimeoutSeconds = 300,
    [string]$ActuatorBase = 'http://localhost:8090',
    [string]$OutputDirectory = "$PSScriptRoot/../target/soak"
)

$ErrorActionPreference = 'Stop'

. "$PSScriptRoot/benchmark-support.ps1"
Assert-BenchmarkSchema
Assert-BenchmarkJob $JobKey
if ($WaveSize -lt 1 -or $WaveIntervalSeconds -lt 1 -or $DurationHours -lt 1 -or $SampleIntervalSeconds -lt 1 -or $DrainTimeoutSeconds -lt 1) { throw 'Counts and durations must be positive.' }
$cohort = New-BenchmarkCohort 'soak'
$cohortWhere = "actor LIKE '$cohort-%'"

function Get-ActuatorMetric([string]$Name, [string]$Query = '') {
    try {
        $response = Invoke-RestMethod -Uri "$ActuatorBase/actuator/metrics/$Name$Query" -TimeoutSec 5
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
$baselineTerminal = 0
$enqueued = 0
$samples = @()
$nextSample = $startedAt
$nextWave = $startedAt

while ((Get-Date) -lt $endsAt) {
    $now = Get-Date

    if ($now -ge $nextWave) {
        $prefix = "$cohort-$enqueued"
        Add-BenchmarkSeed -Cohort $prefix -Count $WaveSize -Key $JobKey -PayloadJson $PayloadJson
        $enqueued += $WaveSize
        $nextWave = $now.AddSeconds($WaveIntervalSeconds)
    }

    if ($now -ge $nextSample) {
        $elapsed = [int]($now - $startedAt).TotalSeconds
        $terminal = [int](Invoke-Psql "SELECT count(*) FROM mohs_execution WHERE $cohortWhere AND state IN ('SUCCEEDED','FAILED','CANCELLED');")
        $queued = [int](Invoke-Psql "SELECT count(*) FROM mohs_work w JOIN mohs_execution e USING (execution_id) WHERE e.$cohortWhere AND w.work_state = 'READY';")
        $running = [int](Invoke-Psql "SELECT count(*) FROM mohs_work w JOIN mohs_execution e USING (execution_id) WHERE e.$cohortWhere AND w.work_state = 'LEASED';")
        $history = [int](Invoke-Psql "SELECT count(*) FROM mohs_execution;")
        $connections = [int](Invoke-Psql "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database();")
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
            heap_used     = Get-ActuatorMetric 'jvm.memory.used' '?tag=area:heap'
            live_threads  = Get-ActuatorMetric 'jvm.threads.live'
            mohs_queue    = Get-ActuatorMetric 'mohs.queue.depth'
        }
        $samples += $sample
        $sample | Export-Csv -Path $samplesPath -NoTypeInformation -Append
        Write-Host ("  {0,6}s  terminal={1,-8} queued={2,-6} heap={3:N0} threads={4} conns={5} drift={6:N3}s" -f `
            $sample.elapsed_s, $sample.terminal, $sample.queue_depth, $sample.heap_used, $sample.live_threads, `
            $sample.db_conns, $sample.clock_drift_s)
        $nextSample = $now.AddSeconds($SampleIntervalSeconds)
    }

    Start-Sleep -Seconds 1
}

# Stop arrivals, then distinguish unfinished work from absent retained history.
$deadline = (Get-Date).AddSeconds($DrainTimeoutSeconds)
do {
    $pending = [long](Invoke-Psql "SELECT count(*) FROM mohs_execution WHERE $cohortWhere AND state NOT IN ('SUCCEEDED','FAILED','CANCELLED')")
    if ($pending -eq 0 -or (Get-Date) -ge $deadline) { break }
    Start-Sleep -Milliseconds 500
} while ($true)
$finalTerminal = [long](Invoke-Psql "SELECT count(*) FROM mohs_execution WHERE $cohortWhere AND state IN ('SUCCEEDED','FAILED','CANCELLED')")
$finalRetained = [long](Invoke-Psql "SELECT count(*) FROM mohs_execution WHERE $cohortWhere")
$unaccounted = $enqueued - $finalRetained

# The verdict is read off the two halves. A soak has no single number: what it
# reports is whether the SECOND half looks like the first.
$half = [int][math]::Floor($samples.Count / 2)
$first = $samples | Select-Object -First $half
$second = $samples | Select-Object -Last $half
$firstRate = if ($first.Count -gt 1) { ($first[-1].terminal - $first[0].terminal) / [math]::Max(1, ($first[-1].elapsed_s - $first[0].elapsed_s)) } else { 0 }
$secondRate = if ($second.Count -gt 1) { ($second[-1].terminal - $second[0].terminal) / [math]::Max(1, ($second[-1].elapsed_s - $second[0].elapsed_s)) } else { 0 }
$lost = $unaccounted

@"
# Soak — $stamp

| | |
| --- | --- |
| Duration | $DurationHours h ($($samples[-1].elapsed_s) s observed) |
| Load | $WaveSize executions every $WaveIntervalSeconds s |
| Enqueued | $enqueued |
| Terminal after final drain | $finalTerminal |
| Still pending after final drain | $pending |
| **Unaccounted** | **$lost** (submitted minus retained; retention must be disabled or exceed this run) |
| Throughput, first half | $([math]::Round($firstRate, 2))/s |
| Throughput, second half | $([math]::Round($secondRate, 2))/s |
| Heap, first sample | $($samples[0].heap_used) |
| Heap, last sample | $($samples[-1].heap_used) |
| Threads, first / last | $($samples[0].live_threads) / $($samples[-1].live_threads) |
| DB connections, first / last | $($samples[0].db_conns) / $($samples[-1].db_conns) |
| History rows at the end | $($samples[-1].history_rows) |

Acceptance: throughput and latency stable start to finish, heap
with no trend, zero executions lost. The two throughput halves above are the
stability check; heap, threads and connections are read as TRENDS across
``$samplesPath``, not as endpoints — one high last sample is a GC that had not
run yet, a monotonic climb is the finding.
"@ | Set-Content -Path $summaryPath

Write-Host ""
Write-Host "Summary -> $summaryPath"
Get-Content $summaryPath

if ($pending -gt 0 -or $unaccounted -ne 0) { throw "Soak accounting failed: $pending pending, $unaccounted unaccounted. See $summaryPath." }
