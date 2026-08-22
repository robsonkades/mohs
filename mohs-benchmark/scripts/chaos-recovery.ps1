# Chaos scenarios S6/S8 (ARCHITECTURE_REDESIGN_PLAN.md §20.2, Phase 0).
#
# Runs the two recovery scenarios against the CURRENT engine, on the same
# bench as BASELINE.md "Tuning fim a fim no Postgres": demo app on the host
# at the documented operating point, container 'postgres', seed by SQL with
# a unique prefix per round, measurement by mohs_attempts timestamps.
#
#   S6 — node kill -9 mid-drain. Pass: 100% of the seed reaches a terminal
#        state; re-executions (attempts > 1) only for executions that were
#        RUNNING at the kill. Reports the recovery timeline (lease expiry
#        is the floor: mohs.engine.lease-ttl, default 30s).
#   S8 — docker pause of the database for -PauseSeconds mid-drain. Pass:
#        no data loss, no exception storm in the app log, drain resumes
#        after unpause. Reports exception counts and the resume latency.
#
# The script boots the app itself (it must own the PID it kills) and aborts
# if port 8080 is already taken. Prerequisites: mohs-demo/target/classes and
# mohs-demo/target/cp.txt (mvnw -pl mohs-demo -am install, then
# dependency:build-classpath -DincludeScope=runtime -Dmdep.outputFile=target/cp.txt).

param(
    [Parameter(Mandatory)][ValidateSet('S6', 'S8')][string]$Scenario,
    [int]$SeedSize = 50000,
    [double]$TriggerAtFraction = 0.4,
    [int]$PauseSeconds = 30,
    [int]$DrainTimeoutSeconds = 300,
    [string]$Container = 'postgres',
    [string]$DbUser = 'postgres',
    [string]$RepoRoot = (Resolve-Path "$PSScriptRoot\..\..").Path
)

$ErrorActionPreference = 'Stop'
$demoDir = Join-Path $RepoRoot 'mohs-demo'
$logDir = Join-Path $RepoRoot 'mohs-benchmark\target\chaos'
New-Item -ItemType Directory -Force $logDir | Out-Null

function Invoke-Psql([string]$Sql) {
    $out = docker exec $Container psql -U $DbUser -tA -F '|' -v ON_ERROR_STOP=1 -c $Sql
    if ($LASTEXITCODE -ne 0) { throw "psql failed (exit $LASTEXITCODE): $Sql" }
    $out
}

function Get-DbNow { (Invoke-Psql "SELECT now()::text") }

function Start-App([string]$LogName) {
    if (Get-NetTCPConnection -LocalPort 8080 -State Listen -ErrorAction SilentlyContinue) {
        throw 'port 8080 already in use — stop whatever owns it first (the script must own the PID it kills)'
    }
    $cp = "target/classes;" + (Get-Content (Join-Path $demoDir 'target/cp.txt') -Raw).Trim()
    $javaArgs = @(
        '-Dspring.devtools.restart.enabled=false'
        '-cp', $cp
        'io.mohs.MohsApplication'
        '--spring.datasource.url=jdbc:postgresql://localhost:5432/postgres'
        '--spring.datasource.username=postgres'
        '--spring.datasource.password=postgres'
        '--spring.datasource.hikari.maximum-pool-size=300'
        '--mohs.jdbc.dialect=postgresql'
        '--mohs.engine.poll-interval=50ms'
        '--mohs.engine.batch-size=1000'
        '--mohs.engine.dispatch-concurrency=1024'
        '--mohs.engine.event-concurrency=256'
    )
    $proc = Start-Process java -ArgumentList $javaArgs -WorkingDirectory $demoDir -PassThru -WindowStyle Hidden `
        -RedirectStandardOutput (Join-Path $logDir "$LogName.log") `
        -RedirectStandardError (Join-Path $logDir "$LogName.err.log")
    $deadline = (Get-Date).AddSeconds(90)
    while (-not (Get-NetTCPConnection -LocalPort 8080 -State Listen -ErrorAction SilentlyContinue)) {
        if ($proc.HasExited) { throw "app exited during boot — see $logDir\$LogName.err.log" }
        if ((Get-Date) -gt $deadline) { throw 'app did not open port 8080 in 90s' }
        Start-Sleep -Milliseconds 500
    }
    $proc
}

function Get-Pending([string]$Prefix) {
    [int](Invoke-Psql "SELECT count(*) FROM mohs_executions WHERE id LIKE '$Prefix%' AND state NOT IN ('SUCCEEDED','FAILED','CANCELLED')")
}

function Wait-Fraction([string]$Prefix, [double]$Fraction) {
    $target = [int]($SeedSize * (1 - $Fraction))
    do {
        Start-Sleep -Milliseconds 300
        $pending = Get-Pending $Prefix
    } while ($pending -gt $target)
    $pending
}

function Wait-Drain([string]$Prefix) {
    $deadline = (Get-Date).AddSeconds($DrainTimeoutSeconds)
    do {
        Start-Sleep -Milliseconds 500
        $pending = Get-Pending $Prefix
        if ((Get-Date) -gt $deadline) { throw "drain timed out with $pending pending" }
    } while ($pending -gt 0)
}

function Show-ExceptionProfile([string]$LogName) {
    $lines = @()
    foreach ($f in "$LogName.log", "$LogName.err.log") {
        $path = Join-Path $logDir $f
        if (Test-Path $path) { $lines += Get-Content $path }
    }
    $found = $lines | Select-String -Pattern '([A-Za-z0-9_.]+(?:Exception|Error))' -AllMatches
    $total = ($found | Measure-Object).Count
    "exception-bearing log lines: $total"
    if ($total -gt 0) {
        $found | ForEach-Object { $_.Matches[0].Groups[1].Value } |
            Group-Object | Sort-Object Count -Descending | Select-Object -First 8 |
            ForEach-Object { '  {0,6}  {1}' -f $_.Count, $_.Name }
    }
}

function Seed([string]$Prefix) {
    Invoke-Psql ("INSERT INTO mohs_executions (id, job_key, state, scheduled_at, actor, priority, payload, payload_type, created_at) " +
        "SELECT '$Prefix'||lpad(n::text,7,'0'), 'every-job', 'ENQUEUED', now(), 'anonymous', 20, '{}', " +
        "'java.util.Collections`$UnmodifiableMap', now() FROM generate_series(1,$SeedSize) n") | Out-Null
}

# ── S6: node kill -9 mid-drain ───────────────────────────────────────────────
function Invoke-S6 {
    $prefix = 's6{0}-' -f (Get-Date -Format 'HHmmss')
    Write-Host "== S6: seeding $SeedSize as '$prefix*', kill -9 at $([int]($TriggerAtFraction*100))% drained =="
    $proc = Start-App "s6-$prefix-node1"
    Seed $prefix

    $pendingAtKill = Wait-Fraction $prefix $TriggerAtFraction
    $killTs = Get-DbNow
    $killClock = Get-Date
    Stop-Process -Id $proc.Id -Force        # TerminateProcess: no shutdown hooks, the Windows kill -9
    Write-Host ("killed pid {0} at {1} (pending {2})" -f $proc.Id, $killTs, $pendingAtKill)

    # process is dead, nothing mutates the seed rows until restart — the
    # snapshot taken now IS the state at the kill
    Invoke-Psql "DROP TABLE IF EXISTS chaos_s6_snapshot" | Out-Null
    Invoke-Psql ("CREATE TABLE chaos_s6_snapshot AS SELECT id, state FROM mohs_executions " +
        "WHERE id LIKE '$prefix%' AND state NOT IN ('SUCCEEDED','FAILED','CANCELLED')") | Out-Null
    Write-Host "--- state at kill ---"
    Invoke-Psql "SELECT state||': '||count(*) FROM chaos_s6_snapshot GROUP BY state ORDER BY state"

    $proc2 = Start-App "s6-$prefix-node2"
    $restartClock = Get-Date
    Write-Host "node2 up (pid $($proc2.Id)) — waiting full drain (lease-ttl 30s is the recovery floor)"
    Wait-Drain $prefix
    $endClock = Get-Date

    Write-Host "--- S6 results ---"
    Invoke-Psql "SELECT 'terminal '||state||': '||count(*) FROM mohs_executions WHERE id LIKE '$prefix%' GROUP BY state ORDER BY state"
    $lost = [int](Invoke-Psql "SELECT count(*) FROM mohs_executions WHERE id LIKE '$prefix%' AND state NOT IN ('SUCCEEDED','FAILED','CANCELLED')")
    $multi = [int](Invoke-Psql ("SELECT count(*) FROM (SELECT execution_id FROM mohs_attempts " +
        "WHERE execution_id LIKE '$prefix%' GROUP BY execution_id HAVING count(*) > 1) m"))
    $violations = [int](Invoke-Psql ("SELECT count(*) FROM (SELECT execution_id FROM mohs_attempts " +
        "WHERE execution_id LIKE '$prefix%' GROUP BY execution_id HAVING count(*) > 1) m " +
        "LEFT JOIN chaos_s6_snapshot s ON s.id = m.execution_id AND s.state = 'RUNNING' WHERE s.id IS NULL"))
    $reaped = [int](Invoke-Psql "SELECT count(*) FROM mohs_attempts WHERE execution_id LIKE '$prefix%' AND outcome = 'FAILED' AND error LIKE '%lease%'")
    $timeline = Invoke-Psql ("SELECT 'reclaim wave: '||min(started_at)||' -> '||max(started_at) FROM mohs_attempts a " +
        "JOIN chaos_s6_snapshot s ON s.id = a.execution_id AND s.state = 'RUNNING' WHERE a.number > 1")
    "seed fully terminal      : $(if ($lost -eq 0) { 'YES' } else { "NO — $lost non-terminal" })"
    "re-executed (attempts>1) : $multi (in-flight at kill: see RUNNING above — criterion: multi <= in-flight)"
    "multi-attempt NOT running at kill: $violations (criterion: 0)"
    "synthetic reaper attempts: $reaped"
    'kill -> drain end        : {0:N1}s (kill at {1})' -f ($endClock - $killClock).TotalSeconds, $killTs
    'restart -> drain end     : {0:N1}s' -f ($endClock - $restartClock).TotalSeconds
    $timeline
    Invoke-Psql "DROP TABLE chaos_s6_snapshot" | Out-Null

    Stop-Process -Id $proc2.Id -Force
    Show-ExceptionProfile "s6-$prefix-node2"
}

# ── S8: database pause mid-drain ─────────────────────────────────────────────
function Invoke-S8 {
    $prefix = 's8{0}-' -f (Get-Date -Format 'HHmmss')
    Write-Host "== S8: seeding $SeedSize as '$prefix*', docker pause ${PauseSeconds}s at $([int]($TriggerAtFraction*100))% drained =="
    $proc = Start-App "s8-$prefix"
    Seed $prefix

    $pendingAtPause = Wait-Fraction $prefix $TriggerAtFraction
    $pauseTsUtc = (Get-Date).ToUniversalTime().ToString('yyyy-MM-dd HH:mm:ss.fff')
    docker pause $Container | Out-Null
    Write-Host "paused at $pauseTsUtc UTC (pending $pendingAtPause)"
    Start-Sleep -Seconds $PauseSeconds
    docker unpause $Container | Out-Null
    $unpauseTsUtc = (Get-Date).ToUniversalTime().ToString('yyyy-MM-dd HH:mm:ss.fff')
    Write-Host "unpaused at $unpauseTsUtc UTC — waiting full drain"

    Wait-Drain $prefix

    Write-Host "--- S8 results ---"
    "app survived             : $(if ($proc.HasExited) { 'NO — process exited' } else { 'YES' })"
    Invoke-Psql "SELECT 'terminal '||state||': '||count(*) FROM mohs_executions WHERE id LIKE '$prefix%' GROUP BY state ORDER BY state"
    $lost = [int](Invoke-Psql "SELECT count(*) FROM mohs_executions WHERE id LIKE '$prefix%' AND state NOT IN ('SUCCEEDED','FAILED','CANCELLED')")
    $multi = [int](Invoke-Psql ("SELECT count(*) FROM (SELECT execution_id FROM mohs_attempts " +
        "WHERE execution_id LIKE '$prefix%' GROUP BY execution_id HAVING count(*) > 1) m"))
    $resume = Invoke-Psql ("SELECT 'first completion after unpause: '||min(finished_at)||' (unpause $unpauseTsUtc)' " +
        "FROM mohs_attempts WHERE execution_id LIKE '$prefix%' AND finished_at > '$unpauseTsUtc'")
    "seed fully terminal      : $(if ($lost -eq 0) { 'YES' } else { "NO — $lost non-terminal" })"
    "re-executed (attempts>1) : $multi (lease-ttl 30s vs pause ${PauseSeconds}s — self-reap race is the thing measured)"
    $resume
    Invoke-Psql ("SELECT 'completions in first 10s after unpause: '||count(*) FROM mohs_attempts " +
        "WHERE execution_id LIKE '$prefix%' AND finished_at BETWEEN '$unpauseTsUtc' AND ('$unpauseTsUtc'::timestamp + interval '10 seconds')")

    Stop-Process -Id $proc.Id -Force
    Show-ExceptionProfile "s8-$prefix"
}

if ($Scenario -eq 'S6') { Invoke-S6 } else { Invoke-S8 }
