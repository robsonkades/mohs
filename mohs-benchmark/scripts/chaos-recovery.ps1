# Process-death, database-pause and process-freeze experiments on a disposable database.
# Uses canonical UUIDv7 identities, current mohs_work READY/LEASED rows and an actor
# cohort per run. SQL seeding measures recovery after acceptance, not API acceptance.
# Latest-attempt storage can demonstrate terminal drain and unexpected redelivery;
# it cannot audit every reclaim or prove absence of duplicate side effects.
# Requires Windows/PowerShell 7+, PostgreSQL 13+, Docker and built demo classes/cp.txt.

param(
    [Parameter(Mandatory)][ValidateSet('S6', 'S8', 'SUSPEND')][string]$Scenario,
    [int]$SeedSize = 50000,
    [double]$TriggerAtFraction = 0.4,
    [int]$PauseSeconds = 30,
    [int]$SuspendSeconds = 25,
    [int]$DrainTimeoutSeconds = 300,
    [string]$Container = 'postgres',
    [string]$DbUser = 'postgres',
    [string]$Database = 'postgres',
    [string]$DbPassword = 'postgres',
    [string]$JobKey = 'every-job',
    [string]$PayloadJson = '{}',
    [string]$RepoRoot = (Resolve-Path "$PSScriptRoot\..\..").Path
)

$ErrorActionPreference = 'Stop'
$demoDir = Join-Path $RepoRoot 'mohs-demo'
$logDir = Join-Path $RepoRoot 'mohs-benchmark\target\chaos'
New-Item -ItemType Directory -Force $logDir | Out-Null

. "$PSScriptRoot/benchmark-support.ps1"
Assert-BenchmarkSchema
if ($SeedSize -lt 1 -or $DrainTimeoutSeconds -lt 1 -or $TriggerAtFraction -le 0 -or $TriggerAtFraction -ge 1 -or $PauseSeconds -lt 1 -or $SuspendSeconds -lt 1) { throw 'Counts/durations must be positive, and TriggerAtFraction between 0 and 1.' }
$processes = [Collections.Generic.List[object]]::new()
$snapshots = [Collections.Generic.List[string]]::new()
$script:databasePaused = $false
$script:frozenProcess = $null

function Get-DbNow { (Invoke-Psql "SELECT now()::text") }

function Start-App([string]$LogName, [int]$Port = 8080, [int]$PoolSize = 300) {
    if (Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue) {
        throw "port $Port already in use — stop whatever owns it first (the script must own the PID it kills)"
    }
    $cp = "target/classes;" + (Get-Content (Join-Path $demoDir 'target/cp.txt') -Raw).Trim()
    $javaArgs = @(
        '-Dspring.devtools.restart.enabled=false'
        '-cp', $cp
        'io.mohs.MohsApplication'
        "--server.port=$Port"
        "--spring.datasource.url=jdbc:postgresql://localhost:5432/$Database"
        "--spring.datasource.username=$DbUser"
        "--spring.datasource.password=$DbPassword"
        "--spring.datasource.hikari.maximum-pool-size=$PoolSize"
        '--mohs.jdbc.dialect=postgresql'
        '--mohs.engine.poll-interval=50ms'
        '--mohs.engine.batch-size=1000'
        '--mohs.engine.dispatch-concurrency=1024'
        '--mohs.engine.event-concurrency=256'
    )
    $proc = Start-Process java -ArgumentList $javaArgs -WorkingDirectory $demoDir -PassThru -WindowStyle Hidden `
        -RedirectStandardOutput (Join-Path $logDir "$LogName.log") `
        -RedirectStandardError (Join-Path $logDir "$LogName.err.log")
    $processes.Add($proc)
    $deadline = (Get-Date).AddSeconds(90)
    while (-not (Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue)) {
        if ($proc.HasExited) { throw "app exited during boot — see $logDir\$LogName.err.log" }
        if ((Get-Date) -gt $deadline) { throw "app did not open port $Port in 90s" }
        Start-Sleep -Milliseconds 500
    }
    $proc
}

# NtSuspendProcess/NtResumeProcess — o SIGSTOP do Windows: congela todas as
# threads do processo sem shutdown hooks, exatamente o "GC pause infinito"
# que o cenário SUSPEND precisa.
Add-Type -Namespace Chaos -Name Native -MemberDefinition @'
[DllImport("ntdll.dll")] public static extern int NtSuspendProcess(IntPtr processHandle);
[DllImport("ntdll.dll")] public static extern int NtResumeProcess(IntPtr processHandle);
'@

function Get-Pending([string]$Prefix) {
    [int](Invoke-Psql "SELECT count(*) FROM mohs_execution WHERE actor = '$Prefix' AND state NOT IN ('SUCCEEDED','FAILED','CANCELLED')")
}

function Wait-Fraction([string]$Prefix, [double]$Fraction) {
    $target = [int]($SeedSize * (1 - $Fraction))
    $deadline = (Get-Date).AddSeconds($DrainTimeoutSeconds)
    do {
        Start-Sleep -Milliseconds 300
        $pending = Get-Pending $Prefix
        if ((Get-Date) -gt $deadline) { throw "Trigger fraction not reached before timeout ($pending pending)." }
    } while ($pending -gt $target)
    if ($pending -eq 0) { throw 'Work finished before fault injection. Increase SeedSize or use slow-job; recovery was not exercised.' }
    $pending
}

function Wait-Drain([string]$Prefix) {
    Wait-BenchmarkDrain $Prefix $DrainTimeoutSeconds $SeedSize
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
    Add-BenchmarkSeed -Cohort $Prefix -Count $SeedSize -Key $JobKey -PayloadJson $PayloadJson
}

# ── S6: node kill -9 mid-drain ───────────────────────────────────────────────
function Invoke-S6 {
    $prefix = New-BenchmarkCohort 's6'
    Write-Host "== S6: seeding $SeedSize with actor '$prefix', kill -9 at $([int]($TriggerAtFraction*100))% drained =="
    $proc = Start-App "s6-$prefix-node1"
    Seed $prefix

    $pendingAtKill = Wait-Fraction $prefix $TriggerAtFraction
    $killTs = Get-DbNow
    $killClock = Get-Date
    Stop-Process -Id $proc.Id -Force        # TerminateProcess: no shutdown hooks, the Windows kill -9
    Write-Host ("killed pid {0} at {1} (pending {2})" -f $proc.Id, $killTs, $pendingAtKill)

    # process is dead, nothing mutates the seed rows until restart — the
    # snapshot taken now IS the state at the kill
    $snapshotTable = "chaos_s6_$([guid]::NewGuid().ToString('N'))"
    $snapshots.Add($snapshotTable)
    # RUNNING derives from LEASED work; history remains PENDING until terminal.
    Invoke-Psql ("CREATE TABLE $snapshotTable AS SELECT e.execution_id AS id, " +
        "CASE WHEN l.execution_id IS NOT NULL THEN 'RUNNING' ELSE 'ENQUEUED' END AS state " +
        "FROM mohs_execution e LEFT JOIN mohs_work l ON l.execution_id = e.execution_id AND l.work_state = 'LEASED' " +
        "WHERE e.actor = '$prefix' AND e.state NOT IN ('SUCCEEDED','FAILED','CANCELLED')") | Out-Null
    Write-Host "--- state at kill ---"
    Invoke-Psql "SELECT state||': '||count(*) FROM $snapshotTable GROUP BY state ORDER BY state"
    $runningAtKill = [long](Invoke-Psql "SELECT count(*) FROM $snapshotTable WHERE state = 'RUNNING'")
    if ($runningAtKill -eq 0) { throw 'No committed grant was running at kill; increase SeedSize or use slow-job to exercise recovery.' }

    $proc2 = Start-App "s6-$prefix-node2"
    $restartClock = Get-Date
    Write-Host "node2 up (pid $($proc2.Id)) — waiting full drain (node-lease-ttl 15s is the recovery floor)"
    Wait-Drain $prefix
    $endClock = Get-Date

    Write-Host "--- S6 results ---"
    Invoke-Psql "SELECT 'terminal '||state||': '||count(*) FROM mohs_execution WHERE actor = '$prefix' GROUP BY state ORDER BY state"
    $lost = [int](Invoke-Psql "SELECT count(*) FROM mohs_execution WHERE actor = '$prefix' AND state NOT IN ('SUCCEEDED','FAILED','CANCELLED')")
    $multi = [int](Invoke-Psql ("SELECT count(*) FROM (SELECT execution_id FROM mohs_execution " +
        "WHERE actor = '$prefix' AND last_attempt_number > 1) m"))
    $violations = [int](Invoke-Psql ("SELECT count(*) FROM (SELECT execution_id FROM mohs_execution " +
        "WHERE actor = '$prefix' AND last_attempt_number > 1) m " +
        "LEFT JOIN $snapshotTable s ON s.id = m.execution_id AND s.state = 'RUNNING' WHERE s.id IS NULL"))
    $reaped = [int](Invoke-Psql "SELECT count(*) FROM mohs_execution WHERE actor = '$prefix' AND last_attempt_outcome = 'FAILED' AND last_attempt_error LIKE '%lease%'")
    $timeline = Invoke-Psql ("SELECT 'reclaim wave: '||min(last_attempt_started_at)||' -> '||max(last_attempt_started_at) FROM mohs_execution a " +
        "JOIN $snapshotTable s ON s.id = a.execution_id AND s.state = 'RUNNING' WHERE a.last_attempt_number > 1")
    "seed fully terminal      : $(if ($lost -eq 0) { 'YES' } else { "NO — $lost non-terminal" })"
    "re-executed (attempts>1) : $multi (in-flight at kill: see RUNNING above — criterion: multi <= in-flight)"
    "multi-attempt NOT running at kill: $violations (criterion: 0)"
    "latest attempts still showing a lease failure: $reaped (earlier failures are overwritten)"
    'kill -> drain end        : {0:N1}s (kill at {1})' -f ($endClock - $killClock).TotalSeconds, $killTs
    'restart -> drain end     : {0:N1}s' -f ($endClock - $restartClock).TotalSeconds
    $timeline
    Invoke-Psql "DROP TABLE $snapshotTable" | Out-Null

    if ($violations -gt 0) { throw "Unexpected redelivery: $violations executions were not running at kill." }
    Stop-Process -Id $proc2.Id -Force
    Show-ExceptionProfile "s6-$prefix-node2"
}

# ── S8: database pause mid-drain ─────────────────────────────────────────────
function Invoke-S8 {
    $prefix = New-BenchmarkCohort 's8'
    Write-Host "== S8: seeding $SeedSize with actor '$prefix', docker pause ${PauseSeconds}s at $([int]($TriggerAtFraction*100))% drained =="
    $proc = Start-App "s8-$prefix"
    Seed $prefix

    $pendingAtPause = Wait-Fraction $prefix $TriggerAtFraction
    $pauseTsUtc = (Get-Date).ToUniversalTime().ToString('yyyy-MM-dd HH:mm:ss.fff')
    docker pause $Container | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "docker pause failed (exit $LASTEXITCODE)." }
    $script:databasePaused = $true
    Write-Host "paused at $pauseTsUtc UTC (pending $pendingAtPause)"
    Start-Sleep -Seconds $PauseSeconds
    docker unpause $Container | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "docker unpause failed (exit $LASTEXITCODE)." }
    $script:databasePaused = $false
    $unpauseTsUtc = (Get-Date).ToUniversalTime().ToString('yyyy-MM-dd HH:mm:ss.fff')
    Write-Host "unpaused at $unpauseTsUtc UTC — waiting full drain"

    Wait-Drain $prefix

    Write-Host "--- S8 results ---"
    "app survived             : $(if ($proc.HasExited) { 'NO — process exited' } else { 'YES' })"
    Invoke-Psql "SELECT 'terminal '||state||': '||count(*) FROM mohs_execution WHERE actor = '$prefix' GROUP BY state ORDER BY state"
    $lost = [int](Invoke-Psql "SELECT count(*) FROM mohs_execution WHERE actor = '$prefix' AND state NOT IN ('SUCCEEDED','FAILED','CANCELLED')")
    $multi = [int](Invoke-Psql ("SELECT count(*) FROM (SELECT execution_id FROM mohs_execution " +
        "WHERE actor = '$prefix' AND last_attempt_number > 1) m"))
    $resume = Invoke-Psql ("SELECT 'first completion after unpause: '||min(finished_at)||' (unpause $unpauseTsUtc)' " +
        "FROM mohs_execution WHERE actor = '$prefix' AND finished_at > '$unpauseTsUtc'")
    "seed fully terminal      : $(if ($lost -eq 0) { 'YES' } else { "NO — $lost non-terminal" })"
    "re-executed (attempts>1) : $multi (node-lease-ttl 15s vs pause ${PauseSeconds}s's heartbeat-first tick order should make this 0)"
    $resume
    Invoke-Psql ("SELECT 'completions in first 10s after unpause: '||count(*) FROM mohs_execution " +
        "WHERE actor = '$prefix' AND finished_at BETWEEN '$unpauseTsUtc' AND ('$unpauseTsUtc'::timestamp + interval '10 seconds')")

    if ($proc.HasExited) { throw "S8 failed: application exited during the pause." }
    Stop-Process -Id $proc.Id -Force
    Show-ExceptionProfile "s8-$prefix"
}

# ── SUSPEND: node frozen past the node lease, peers reclaim, zombie fenced ───
function Invoke-Suspend {
    $prefix = New-BenchmarkCohort 'suspend'
    Write-Host "== SUSPEND (E6): seeding $SeedSize with actor '$prefix', freezing node1 ${SuspendSeconds}s at $([int]($TriggerAtFraction*100))% drained =="
    # dois nodes no mesmo Postgres: metade do pool cada, senão 2×300 estoura max_connections
    $proc1 = Start-App "sp-$prefix-node1" 8080 200
    $proc2 = Start-App "sp-$prefix-node2" 8081 200
    Seed $prefix

    $pendingAtSuspend = Wait-Fraction $prefix $TriggerAtFraction
    $suspendTs = Get-DbNow
    if ([Chaos.Native]::NtSuspendProcess($proc1.Handle) -ne 0) { throw 'NtSuspendProcess failed.' }
    $script:frozenProcess = $proc1
    Write-Host ("froze node1 pid {0} at {1} (pending {2}) — node2 keeps draining and its reaper reclaims" -f $proc1.Id, $suspendTs, $pendingAtSuspend)
    Start-Sleep -Seconds $SuspendSeconds
    if ([Chaos.Native]::NtResumeProcess($proc1.Handle) -ne 0) { throw 'NtResumeProcess failed.' }
    $script:frozenProcess = $null
    $resumeClock = Get-Date
    Write-Host "node1 resumed — its zombies now race the reclaimed re-runs; the owner fence decides"

    Wait-Drain $prefix
    $endClock = Get-Date

    Write-Host "--- SUSPEND results ---"
    Invoke-Psql "SELECT 'terminal '||state||': '||count(*) FROM mohs_execution WHERE actor = '$prefix' GROUP BY state ORDER BY state"
    $lost = [int](Invoke-Psql "SELECT count(*) FROM mohs_execution WHERE actor = '$prefix' AND state NOT IN ('SUCCEEDED','FAILED','CANCELLED')")
    $reaped = [int](Invoke-Psql "SELECT count(*) FROM mohs_execution WHERE actor = '$prefix' AND last_attempt_outcome = 'FAILED' AND last_attempt_error LIKE '%lease%'")
    "seed fully terminal        : $(if ($lost -eq 0) { 'YES' } else { "NO — $lost non-terminal" })"
    "latest attempts still showing a lease failure: $reaped (not a count of all reclaims)"
    "double-completion audit     : unavailable from latest-attempt storage; run ClusterEngineTest for the live ownership-fence assertion"
    "expired sessions          : renewed only while valid; a resumed expired node must enter with a new session UUID"
    'resume -> drain end        : {0:N1}s' -f ($endClock - $resumeClock).TotalSeconds

    Stop-Process -Id $proc1.Id -Force
    Stop-Process -Id $proc2.Id -Force
    Show-ExceptionProfile "sp-$prefix-node1"
    Show-ExceptionProfile "sp-$prefix-node2"
}

try {
    switch ($Scenario) {
        'S6' { Invoke-S6 }
        'S8' { Invoke-S8 }
        'SUSPEND' { Invoke-Suspend }
    }
} finally {
    if ($script:databasePaused) {
        docker unpause $Container | Out-Null
        if ($LASTEXITCODE -ne 0) { Write-Warning "Cleanup could not unpause container '$Container'." }
    }
    if ($null -ne $script:frozenProcess -and -not $script:frozenProcess.HasExited) {
        if ([Chaos.Native]::NtResumeProcess($script:frozenProcess.Handle) -ne 0) { Write-Warning 'Cleanup could not resume the frozen process.' }
    }
    foreach ($process in $processes) {
        if (-not $process.HasExited) { Stop-Process -Id $process.Id -Force -ErrorAction Continue }
    }
    foreach ($snapshot in $snapshots) {
        try { Invoke-Psql "DROP TABLE IF EXISTS $snapshot" | Out-Null }
        catch { Write-Warning "Cleanup could not remove snapshot '$snapshot': $_" }
    }
}
