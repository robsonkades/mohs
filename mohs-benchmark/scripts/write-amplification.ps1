# Write-amplification measurement (ARCHITECTURE_REDESIGN_PLAN.md, Phase 0).
#
# Produces the three numbers the redesign is judged against, for the CURRENT
# engine: commits/execution, tuple versions/execution and WAL bytes/execution.
# Methodology identical to BASELINE.md "Tuning fim a fim no Postgres": the app
# runs on the host at the documented operating point, this script seeds N
# ENQUEUED rows by SQL (unique prefix per round), waits for the drain, and
# reads the deltas from pg_stat_database / pg_stat_user_tables / pg_stat_wal.
#
# The enqueue commit is NOT in these numbers (the seed bypasses REST); it is
# 1 per execution by construction (ADR-0003 §4). Report it separately.
#
# Prerequisites: the demo app running against the target Postgres, container
# named 'postgres' (postgres/postgres). The idle window quantifies background
# noise (heartbeat, empty claim rounds, the every-job PT1S trigger) so the
# drain deltas can be bounded honestly.

param(
    [int]$SeedSize = 50000,
    [int]$Rounds = 2,
    [int]$IdleSeconds = 30,
    [int]$DrainTimeoutSeconds = 180,
    [string]$Container = 'postgres',
    [string]$DbUser = 'postgres'
)

$ErrorActionPreference = 'Stop'

function Invoke-Psql([string]$Sql) {
    $out = docker exec $Container psql -U $DbUser -tA -F '|' -v ON_ERROR_STOP=1 -c $Sql
    if ($LASTEXITCODE -ne 0) { throw "psql failed (exit $LASTEXITCODE): $Sql" }
    $out
}

function Get-Snapshot {
    $sql = "SELECT 'ts', extract(epoch from clock_timestamp())::text " +
        "UNION ALL SELECT 'xact_commit', xact_commit::text FROM pg_stat_database WHERE datname = current_database() " +
        "UNION ALL SELECT 'wal_records', wal_records::text FROM pg_stat_wal " +
        "UNION ALL SELECT 'wal_fpi', wal_fpi::text FROM pg_stat_wal " +
        "UNION ALL SELECT 'wal_bytes', wal_bytes::text FROM pg_stat_wal " +
        "UNION ALL SELECT 'exec_ins', n_tup_ins::text FROM pg_stat_user_tables WHERE relname = 'mohs_executions' " +
        "UNION ALL SELECT 'exec_upd', n_tup_upd::text FROM pg_stat_user_tables WHERE relname = 'mohs_executions' " +
        "UNION ALL SELECT 'exec_hot', n_tup_hot_upd::text FROM pg_stat_user_tables WHERE relname = 'mohs_executions' " +
        "UNION ALL SELECT 'exec_dead', n_dead_tup::text FROM pg_stat_user_tables WHERE relname = 'mohs_executions' " +
        "UNION ALL SELECT 'att_ins', n_tup_ins::text FROM pg_stat_user_tables WHERE relname = 'mohs_attempts' " +
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
    $prefix = 'wa{0}r{1}-' -f (Get-Date -Format 'HHmmss'), $round
    Write-Host ""
    Write-Host "== round $round : seeding $SeedSize as '$prefix*' =="

    $before = Get-Snapshot

    $seed = "INSERT INTO mohs_executions (id, job_key, state, scheduled_at, actor, priority, payload, payload_type, created_at) " +
        "SELECT '$prefix'||lpad(n::text,7,'0'), 'every-job', 'ENQUEUED', now(), 'anonymous', 20, '{}', " +
        "'java.util.Collections`$UnmodifiableMap', now() FROM generate_series(1,$SeedSize) n"
    Invoke-Psql $seed | Out-Null

    $pending = "SELECT count(*) FROM mohs_executions WHERE id LIKE '$prefix%' AND state NOT IN ('SUCCEEDED','FAILED','CANCELLED')"
    $deadline = (Get-Date).AddSeconds($DrainTimeoutSeconds)
    do {
        Start-Sleep -Milliseconds 500
        $remaining = [int](Invoke-Psql $pending)
        if ((Get-Date) -gt $deadline) { throw "drain timed out with $remaining pending — is the app running?" }
    } while ($remaining -gt 0)

    # PG 15+ flushes backend stats within ~1 s of idle; settle before reading.
    Start-Sleep -Seconds 2
    $after = Get-Snapshot

    $window = Invoke-Psql ("SELECT count(*)||'|'||EXTRACT(EPOCH FROM max(finished_at) - min(started_at)) " +
        "FROM mohs_attempts WHERE execution_id LIKE '$prefix%'")
    $attempts, $windowSec = $window -split '\|', 2
    $throughput = [double]$attempts / [double]$windowSec

    $dur = $after['ts'] - $before['ts']
    'drained {0} in {1:N1} s wall · attempts window {2:N1} s · {3:N1} exec/s' -f `
        $SeedSize, $dur, [double]$windowSec, $throughput
    'background during window (from idle rate): ~{0:N0} commits, ~{1:N0} exec-upd' -f `
        ($idleCommitsPerSec * $dur), ($idleUpdPerSec * $dur)
    Write-Host "--- deltas ---"
    foreach ($k in 'xact_commit', 'wal_records', 'wal_fpi', 'wal_bytes', 'exec_ins', 'exec_upd', 'exec_hot', 'att_ins') {
        Show-Delta $k $before $after $SeedSize
    }
    foreach ($k in 'exec_dead', 'jobdef_upd', 'ratelimit_upd', 'nodes_upd') {
        Show-Delta $k $before $after 0
    }
    $tupleVersions = (($after['exec_ins'] - $before['exec_ins']) + ($after['exec_upd'] - $before['exec_upd'])) / $SeedSize
    Write-Host "--- the three numbers (engine-side; enqueue adds 1 commit by construction) ---"
    'commits/execution        : {0:N3}' -f (($after['xact_commit'] - $before['xact_commit']) / $SeedSize)
    'tuple versions/execution : {0:N3}   (mohs_executions ins+upd)' -f $tupleVersions
    'WAL bytes/execution      : {0:N0}' -f (($after['wal_bytes'] - $before['wal_bytes']) / $SeedSize)
}
