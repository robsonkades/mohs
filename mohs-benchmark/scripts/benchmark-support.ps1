# Shared PostgreSQL helpers for the disposable-database benchmark scripts.
# Dot-source after setting Container, DbUser and Database. Never runs a workload on import.

function ConvertTo-SqlLiteral([string]$Value) {
    "'" + $Value.Replace("'", "''") + "'"
}

function Invoke-Psql([string]$Sql) {
    # stdin avoids platform command-line length and quoting limits. -X ignores local psqlrc.
    $out = $Sql | docker exec -i $Container psql -X -q -U $DbUser -d $Database -tA -F '|' -v ON_ERROR_STOP=1
    if ($LASTEXITCODE -ne 0) { throw "psql failed (exit $LASTEXITCODE) in database '$Database'." }
    $out
}

function Assert-BenchmarkSchema {
    $valid = Invoke-Psql @'
SELECT to_regclass('mohs_work') IS NOT NULL
   AND to_regclass('mohs_execution') IS NOT NULL
   AND EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = current_schema() AND table_name = 'mohs_execution'
                 AND column_name = 'input_contract')
   AND EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = current_schema() AND table_name = 'mohs_job_definitions'
                 AND column_name = 'executable_key');
'@
    if ($valid -ne 't') { throw 'Install the current schema-postgresql.sql into a disposable benchmark database first.' }
}

function Assert-BenchmarkJob([string]$Key) {
    $literal = ConvertTo-SqlLiteral $Key
    $valid = Invoke-Psql "SELECT EXISTS (SELECT 1 FROM mohs_job_definitions WHERE job_key = $literal AND NOT retired AND NOT paused)"
    if ($valid -ne 't') { throw "Job '$Key' must exist, be active and be unpaused before seeding." }
}

function New-BenchmarkCohort([string]$Name) { "$Name-$([guid]::NewGuid().ToString('N'))" }

function Add-BenchmarkSeed([string]$Cohort, [int]$Count, [string]$Key, [string]$ShardMode = 'uniform', [string]$PayloadJson = '{}') {
    if ($Count -lt 1) { throw 'Seed count must be positive.' }
    $null = ConvertFrom-Json $PayloadJson -ErrorAction Stop
    Assert-BenchmarkJob $Key
    $keyLiteral = ConvertTo-SqlLiteral $Key
    $cohortLiteral = ConvertTo-SqlLiteral $Cohort
    $payloadLiteral = ConvertTo-SqlLiteral $PayloadJson
    $shard = if ($ShardMode -eq 'zero') { '0' } else { 'pg_temp.mohs_seed_shard(id)' }
    # This is a SQL drain microbenchmark, not an acceptance/API benchmark. The pair of
    # inserts is atomic; contract comes from the registered job. Validation/admission,
    # idempotency and application-side enqueue cost are intentionally outside this seed.
    # Low six FNV-1a bits equal Math.floorMod(the signed 32-bit hash, 64).
    $sql = @'
BEGIN;
CREATE FUNCTION pg_temp.mohs_seed_shard(id text) RETURNS smallint LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE hash integer := 5; i integer;
BEGIN
    FOR i IN 1..length(id) LOOP hash := ((hash # ascii(substr(id, i, 1))) * 19) % 64; END LOOP;
    RETURN hash::smallint;
END $$;
'@
    $sql += @"

WITH source AS MATERIALIZED (
    SELECT replace(gen_random_uuid()::text, '-', '') AS random_hex,
           lpad(to_hex(floor(extract(epoch FROM clock_timestamp()) * 1000)::bigint), 12, '0') AS millis
      FROM generate_series(1, $Count)
), ids AS MATERIALIZED (
    SELECT substr(millis, 1, 8) || '-' || substr(millis, 9, 4) || '-7' || substr(random_hex, 1, 3)
           || '-' || substr(random_hex, 17, 4) || '-' || substr(random_hex, 21, 12) AS id
      FROM source
), history AS (
    INSERT INTO mohs_execution (execution_id, job_key, shard, priority, state, scheduled_at, created_at, actor, payload, input_contract)
    SELECT id, j.job_key, $shard, 20, 'PENDING', now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC',
           $cohortLiteral, $payloadLiteral, j.input_contract
      FROM ids CROSS JOIN mohs_job_definitions j WHERE j.job_key = $keyLiteral AND NOT j.retired AND NOT j.paused
    RETURNING execution_id, job_key, shard, priority, scheduled_at
)
INSERT INTO mohs_work (execution_id, job_key, shard, priority, attempt, work_state, visible_at)
SELECT execution_id, job_key, shard, priority, 1, 'READY', scheduled_at FROM history;
COMMIT;
"@
    Invoke-Psql $sql | Out-Null
    $actual = [long](Invoke-Psql "SELECT count(*) FROM mohs_execution WHERE actor = $cohortLiteral")
    if ($actual -ne $Count) { throw "Seed cohort '$Cohort': expected $Count rows, found $actual. No further load will be submitted." }
}

function Get-BenchmarkPending([string]$Cohort) {
    $literal = ConvertTo-SqlLiteral $Cohort
    [long](Invoke-Psql "SELECT count(*) FROM mohs_execution WHERE actor = $literal AND state NOT IN ('SUCCEEDED','FAILED','CANCELLED')")
}

function Wait-BenchmarkDrain([string]$Cohort, [int]$TimeoutSeconds, [long]$ExpectedCount) {
    if ($ExpectedCount -lt 1) { throw 'ExpectedCount must be positive to detect missing history.' }
    $literal = ConvertTo-SqlLiteral $Cohort
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    do {
        $counts = Invoke-Psql "SELECT count(*), count(*) FILTER (WHERE state NOT IN ('SUCCEEDED','FAILED','CANCELLED')) FROM mohs_execution WHERE actor = $literal"
        $retained, $pending = $counts -split '\|', 2
        $remaining = [long]$pending
        if ([long]$retained -ne $ExpectedCount) { throw "Cohort '$Cohort' has $retained of $ExpectedCount expected rows. Disable retention for this experiment and investigate missing history." }
        if ($remaining -eq 0) { return }
        if ([DateTimeOffset]::UtcNow -ge $deadline) { throw "Drain timed out: $remaining pending in '$Cohort'." }
        Start-Sleep -Milliseconds 500
    } while ($true)
}
