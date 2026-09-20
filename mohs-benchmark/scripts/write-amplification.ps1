# SQL drain microbenchmark against the current canonical PostgreSQL schema.
# Includes one bulk seed transaction per round. It does not measure API acceptance,
# payload validation or one enqueue transaction per execution. Use an isolated
# benchmark database: WAL statistics are instance-wide. Requires PowerShell 7+,
# PostgreSQL 14+ (pg_stat_wal), Docker and a running demo application.

param(
    [int]$SeedSize = 50000,
    [int]$Rounds = 2,
    [int]$IdleSeconds = 30,
    [int]$DrainTimeoutSeconds = 180,
    # slow-job deliberately spends 500 ms in the handler.
    [string]$JobKey = 'every-job',
    [string]$Container = 'postgres',
    [string]$DbUser = 'postgres',
    [string]$Database = 'postgres',
    [string]$PayloadJson = '{}',
    # uniform uses production FNV-1a shards; zero is a deliberate hotspot.
    [ValidateSet('uniform', 'zero')]
    [string]$ShardMode = 'uniform'
)

$ErrorActionPreference = 'Stop'

. "$PSScriptRoot/benchmark-support.ps1"
Assert-BenchmarkSchema
if ((Invoke-Psql "SELECT to_regclass('pg_stat_wal') IS NOT NULL") -ne 't') { throw 'This script requires PostgreSQL 14+ with pg_stat_wal.' }
Assert-BenchmarkJob $JobKey
if ($SeedSize -lt 1 -or $Rounds -lt 1 -or $IdleSeconds -lt 1 -or $DrainTimeoutSeconds -lt 1) { throw 'Counts and durations must be positive.' }

function Get-Snapshot {
    $sql = "SELECT 'ts', extract(epoch from clock_timestamp())::text " +
        "UNION ALL SELECT 'xact_commit', xact_commit::text FROM pg_stat_database WHERE datname = current_database() " +
        "UNION ALL SELECT 'wal_records', wal_records::text FROM pg_stat_wal " +
        "UNION ALL SELECT 'wal_fpi', wal_fpi::text FROM pg_stat_wal " +
        "UNION ALL SELECT 'wal_bytes', wal_bytes::text FROM pg_stat_wal " +
        "UNION ALL SELECT 'exec_ins', COALESCE(SUM(n_tup_ins),0)::text FROM pg_stat_user_tables WHERE relname ~ '^mohs_execution(_|$)' " +
        "UNION ALL SELECT 'exec_upd', COALESCE(SUM(n_tup_upd),0)::text FROM pg_stat_user_tables WHERE relname ~ '^mohs_execution(_|$)' " +
        "UNION ALL SELECT 'exec_hot', COALESCE(SUM(n_tup_hot_upd),0)::text FROM pg_stat_user_tables WHERE relname ~ '^mohs_execution(_|$)' " +
        "UNION ALL SELECT 'exec_dead', COALESCE(SUM(n_dead_tup),0)::text FROM pg_stat_user_tables WHERE relname ~ '^mohs_execution(_|$)' " +
        "UNION ALL SELECT 'work_ins', n_tup_ins::text FROM pg_stat_user_tables WHERE relname = 'mohs_work' " +
        "UNION ALL SELECT 'work_upd', n_tup_upd::text FROM pg_stat_user_tables WHERE relname = 'mohs_work' " +
        "UNION ALL SELECT 'work_del', n_tup_del::text FROM pg_stat_user_tables WHERE relname = 'mohs_work' " +
        "UNION ALL SELECT 'jobdef_upd', n_tup_upd::text FROM pg_stat_user_tables WHERE relname = 'mohs_job_definitions' " +
        "UNION ALL SELECT 'ratelimit_upd', n_tup_upd::text FROM pg_stat_user_tables WHERE relname = 'mohs_rate_limits' " +
        "UNION ALL SELECT 'nodes_upd', n_tup_upd::text FROM pg_stat_user_tables WHERE relname = 'mohs_nodes'"
    $snap = @{}
    Invoke-Psql $sql | ForEach-Object {
        $k, $v = $_ -split '\|', 2
        $snap[$k] = [double]$v
    }
    $snap
}

function Show-Delta([string]$Label, [hashtable]$Before, [hashtable]$After, [double]$PerExec) {
    $d = $After[$Label] - $Before[$Label]
    if ($PerExec -gt 0) {
        '{0,-14} {1,14:N0}   ({2,8:N3} /exec)' -f $Label, $d, ($d / $PerExec)
    } else {
        '{0,-14} {1,14:N0}' -f $Label, $d
    }
}

# ── idle calibration ─────────────────────────────────────────────────────────
Write-Host "== idle calibration ($IdleSeconds s; engine polling, every-job firing) =="
$idleBefore = Get-Snapshot
Start-Sleep -Seconds $IdleSeconds
$idleAfter = Get-Snapshot
$idleDur = $idleAfter['ts'] - $idleBefore['ts']
$idleCommitsPerSec = ($idleAfter['xact_commit'] - $idleBefore['xact_commit']) / $idleDur
$idleUpdPerSec = ($idleAfter['exec_upd'] - $idleBefore['exec_upd']) / $idleDur
$idleWalPerSec = ($idleAfter['wal_bytes'] - $idleBefore['wal_bytes']) / $idleDur
'idle: {0:N1} commits/s · {1:N1} exec-upd/s · {2:N0} wal bytes/s over {3:N1} s' -f `
    $idleCommitsPerSec, $idleUpdPerSec, $idleWalPerSec, $idleDur

# ── drain rounds ─────────────────────────────────────────────────────────────
foreach ($round in 1..$Rounds) {
    $prefix = New-BenchmarkCohort "wa-r$round"
    Write-Host ""
    Write-Host "== round $round : seeding $SeedSize with actor '$prefix' =="

    $before = Get-Snapshot

    Add-BenchmarkSeed -Cohort $prefix -Count $SeedSize -Key $JobKey -ShardMode $ShardMode -PayloadJson $PayloadJson
    Wait-BenchmarkDrain $prefix $DrainTimeoutSeconds $SeedSize

    # PG 15+ flushes backend stats within ~1 s of idle; settle before reading.
    Start-Sleep -Seconds 2
    $after = Get-Snapshot

    $window = Invoke-Psql ("SELECT count(*)||'|'||EXTRACT(EPOCH FROM max(last_attempt_finished_at) - min(scheduled_at)) " +
        "FROM mohs_execution WHERE actor = '$prefix'")
    $completed, $windowSec = $window -split '\|', 2
    $throughput = [double]$completed / [math]::Max(0.001, [double]$windowSec)

    $dur = $after['ts'] - $before['ts']
    'drained {0} in {1:N1} s wall · execution window {2:N1} s · {3:N1} exec/s' -f `
        $SeedSize, $dur, [double]$windowSec, $throughput
    'background during window (from idle rate): ~{0:N0} commits, ~{1:N0} exec-upd' -f `
        ($idleCommitsPerSec * $dur), ($idleUpdPerSec * $dur)
    Write-Host "--- deltas ---"
    foreach ($k in 'xact_commit', 'wal_records', 'wal_fpi', 'wal_bytes', 'exec_ins', 'exec_upd', 'exec_hot',
             'work_ins', 'work_upd', 'work_del') {
        Show-Delta $k $before $after $SeedSize
    }
    foreach ($k in 'exec_dead', 'jobdef_upd', 'ratelimit_upd', 'nodes_upd') {
        Show-Delta $k $before $after 0
    }
    # History tuple churn; work-table churn is reported separately above.
    $tupleVersions = (($after['exec_ins'] - $before['exec_ins']) + ($after['exec_upd'] - $before['exec_upd'])) / $SeedSize
    Write-Host "--- database deltas including bulk seed, observers and background work ---"
    'commits/execution        : {0:N3}' -f (($after['xact_commit'] - $before['xact_commit']) / $SeedSize)
    'tuple versions/execution : {0:N3}   (mohs_execution ins+upd, historia)' -f $tupleVersions
    'WAL bytes/execution      : {0:N0}' -f (($after['wal_bytes'] - $before['wal_bytes']) / $SeedSize)
}
