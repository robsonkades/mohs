# Phase 6 (S6.4) — os tres numeros que os gates da fase pedem e nenhum
# script tinha: taxa de consulta OCIOSA por tamanho de cluster e latencia de
# dispatch (gates ADR-G), escala RELATIVA de vazao com N processos-no contra
# 1 (gate ADR-F).
#
#   -Mode Idle    — sobe N nos, PAUSA todas as definicoes (cluster de verdade
#                   ocioso: nada dispara), espera o backoff estacionar no teto
#                   e mede pg_stat_statements: consultas/s do cluster inteiro,
#                   com o top por chamadas pra atribuicao (quem paga o idle).
#   -Mode Drain   — sobe N nos, semeia SeedSize execucoes prontas com shard
#                   uniforme (a forma que os escritores reais produzem) e mede
#                   a vazao pela janela de mohs_attempt, como todo BASELINE.
#   -Mode Latency — sobe N nos ociosos e enfileira UMA execucao por vez pelo
#                   REST do no 0, espacadas o bastante pro backoff voltar ao
#                   teto: mede scheduled_at -> started_at e atribui por no
#                   quem despachou (hand-off local x poll do dono do shard).
#
# A comparacao honesta e' -Nodes 1/2/4 na MESMA sessao, com a MESMA config
# POR NO: o que escala e' o cluster, nao o dimensionamento de cada no. A
# primeira rodada de cada celula e' warmup e sai do registro (ritual da
# decisao 7 do PLAN.md).
#
# Pre-requisitos: Docker com o container 'postgres', mohs-demo/target/classes
# e mohs-demo/target/cp.txt (mvnw -pl mohs-demo -am install, depois
# dependency:build-classpath -DincludeScope=runtime -Dmdep.outputFile=target/cp.txt).
# Rodar com pwsh.

param(
    [Parameter(Mandatory)][ValidateSet('Idle', 'Drain', 'Latency')][string]$Mode,
    [int]$Nodes = 1,
    [int]$SeedSize = 50000,
    [int]$IdleSeconds = 60,
    [int]$Rounds = 2,
    # 100 por no cabe 4 nos dentro do max_connections=500 do container;
    # segurar a config POR NO e' o que faz a comparacao ser sobre o cluster.
    [int]$PoolSize = 100,
    [string]$PollInterval = '25ms',
    [string]$MaxPollInterval = '2s',
    [int]$DrainTimeoutSeconds = 300,
    # a historia retida cresce 50k por rodada e o custo do drain cresce com
    # ela: sem zerar entre CELULAS, a ultima celula da matriz mede uma base
    # maior que a primeira e a comparacao vira ordem de execucao
    [switch]$Reset,
    [int]$BasePort = 8080,
    [int]$Samples = 20,
    # 25ms dobrando ate' 2s leva ~5s: menos que isso e a sonda mede um loop
    # ainda acelerado pela sonda anterior, nao um cluster ocioso
    [int]$SettleSeconds = 7,
    [string]$JobKey = 'every-job',
    [string]$OnDemandJobKey = 'every-job2',
    [string]$Container = 'postgres',
    [string]$DbUser = 'postgres',
    [string]$RepoRoot = (Resolve-Path "$PSScriptRoot\..\..").Path
)

$ErrorActionPreference = 'Stop'
$demoDir = Join-Path $RepoRoot 'mohs-demo'
$logDir = Join-Path $RepoRoot 'mohs-benchmark\target\cluster'
New-Item -ItemType Directory -Force $logDir | Out-Null

function Invoke-Psql([string]$Sql) {
    $out = docker exec $Container psql -U $DbUser -tA -F '|' -v ON_ERROR_STOP=1 -c $Sql
    if ($LASTEXITCODE -ne 0) { throw "psql failed (exit $LASTEXITCODE): $Sql" }
    $out
}

function Start-Node([int]$Index) {
    $port = $BasePort + $Index
    if (Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue) {
        throw "port $port already in use — stop whatever owns it first"
    }
    $cp = "target/classes;" + (Get-Content (Join-Path $demoDir 'target/cp.txt') -Raw).Trim()
    $javaArgs = @(
        '-Dspring.devtools.restart.enabled=false'
        '-cp', $cp
        'io.mohs.MohsApplication'
        "--server.port=$port"
        '--spring.datasource.url=jdbc:postgresql://localhost:5432/postgres'
        '--spring.datasource.username=postgres'
        '--spring.datasource.password=postgres'
        "--spring.datasource.hikari.maximum-pool-size=$PoolSize"
        '--mohs.jdbc.dialect=postgresql'
        '--mohs.api.enabled=true'
        "--mohs.engine.poll-interval=$PollInterval"
        "--mohs.engine.max-poll-interval=$MaxPollInterval"
        '--mohs.engine.batch-size=1000'
        '--mohs.engine.dispatch-concurrency=1024'
        '--mohs.engine.event-concurrency=256'
    )
    $proc = Start-Process java -ArgumentList $javaArgs -WorkingDirectory $demoDir -PassThru -WindowStyle Hidden `
        -RedirectStandardOutput (Join-Path $logDir "node$Index.log") `
        -RedirectStandardError (Join-Path $logDir "node$Index.err.log")
    $deadline = (Get-Date).AddSeconds(120)
    while (-not (Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue)) {
        if ($proc.HasExited) { throw "node $Index exited during boot — see $logDir\node$Index.err.log" }
        if ((Get-Date) -gt $deadline) { throw "node $Index did not open port $port in 120s" }
        Start-Sleep -Milliseconds 500
    }
    $proc
}

function Stop-Nodes([object[]]$Procs) {
    foreach ($p in $Procs) {
        if (-not $p.HasExited) { Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue }
    }
    foreach ($p in $Procs) { $p.WaitForExit(30000) | Out-Null }
    # matar o processo nao fecha os backends do Postgres na hora: uma celula
    # de 4 nos deixa ate' 400 conexoes morrendo e a celula seguinte pediria
    # as suas por cima do max_connections — foi assim que uma passada inteira
    # da matriz saiu degradada antes deste wait existir
    $deadline = (Get-Date).AddSeconds(90)
    while ([int](Invoke-Psql "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()") -gt 5) {
        if ((Get-Date) -gt $deadline) { throw "backends from the killed nodes did not drain in 90s" }
        Start-Sleep -Seconds 1
    }
    # o purge de linhas mortas e' por tick de quem esta' vivo; a proxima
    # celula comeca sem heranca de nos parados
    Invoke-Psql "DELETE FROM mohs_nodes" | Out-Null
}

if ($Reset) {
    Invoke-Psql "TRUNCATE mohs_execution, mohs_attempt, mohs_ready, mohs_lease" | Out-Null
    Write-Host "== history/queue truncated =="
}

Write-Host "== booting $Nodes node(s) · pool $PoolSize each · poll $PollInterval..$MaxPollInterval =="
$procs = @(0..($Nodes - 1) | ForEach-Object { Start-Node $_ })
try {
    if ($Mode -eq 'Idle') {
        # cluster ocioso de verdade: o demo registra 'every-job' (PT1S) no boot,
        # e um trigger disparando a cada segundo mede outra coisa
        Invoke-Psql "UPDATE mohs_job_definitions SET paused = true" | Out-Null
        Write-Host "== all definitions paused; waiting for the backoff to reach the ceiling =="
        Start-Sleep -Seconds 20

        # pg_stat_statements e' CLUSTER-WIDE: sem o filtro de dbid o total
        # inclui qualquer outra aplicacao apontada pra mesma instancia — e o
        # container 'postgres' do compose da raiz e' compartilhado
        $dbFilter = "dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) AND query NOT LIKE '%pg_stat_%'"
        Invoke-Psql "SELECT pg_stat_statements_reset()" | Out-Null
        $t0 = [double](Invoke-Psql "SELECT extract(epoch from clock_timestamp())::text")
        $c0 = [double](Invoke-Psql "SELECT (xact_commit + xact_rollback)::text FROM pg_stat_database WHERE datname = current_database()")
        Start-Sleep -Seconds $IdleSeconds
        $c1 = [double](Invoke-Psql "SELECT (xact_commit + xact_rollback)::text FROM pg_stat_database WHERE datname = current_database()")
        # o psql do proprio script tambem conta; ler $calls ANTES de fechar a
        # janela, senao o denominador exclui round trips que o numerador inclui
        $calls = [double](Invoke-Psql "SELECT COALESCE(SUM(calls),0)::text FROM pg_stat_statements WHERE $dbFilter")
        $t1 = [double](Invoke-Psql "SELECT extract(epoch from clock_timestamp())::text")
        $dur = $t1 - $t0
        Write-Host ""
        Write-Host "--- idle over $('{0:N1}' -f $dur) s, $Nodes node(s) ---"
        '{0,-26} {1,10:N2}' -f 'queries/s (cluster)', ($calls / $dur)
        '{0,-26} {1,10:N2}' -f 'queries/s (per node)', ($calls / $dur / $Nodes)
        '{0,-26} {1,10:N2}' -f 'transactions/s (cluster)', (($c1 - $c0) / $dur)
        Write-Host ""
        Write-Host "--- top statements by calls (attribution) ---"
        Invoke-Psql ("SELECT calls || '|' || round((calls/$dur)::numeric, 2) || '|' || left(regexp_replace(query, '\s+', ' ', 'g'), 90) " +
            "FROM pg_stat_statements WHERE $dbFilter ORDER BY calls DESC LIMIT 12") |
            ForEach-Object {
                $n, $rate, $q = $_ -split '\|', 3
                '{0,10} {1,9}/s  {2}' -f $n, $rate, $q
            }
    }
    elseif ($Mode -eq 'Latency') {
        # latencia de dispatch de UM enqueue num cluster ocioso: com o tier
        # NOTIFY retirado (ADR-0054), so' o hand-off LOCAL acorda alguem — e
        # ele so' resolve se quem recebeu o POST for o dono do shard sorteado
        Invoke-Psql "UPDATE mohs_job_definitions SET paused = (job_key <> '$OnDemandJobKey')" | Out-Null
        $probeStart = Invoke-Psql "SELECT now()::text"
        Write-Host "== $Samples probes into node 0 (port $BasePort), $SettleSeconds s apart so the backoff is back at the ceiling =="
        foreach ($i in 1..$Samples) {
            Start-Sleep -Seconds $SettleSeconds
            Invoke-RestMethod -Method Post -Uri "http://localhost:$BasePort/api/mohs/v1/jobs/$OnDemandJobKey/schedule" `
                -ContentType 'application/json' -Body '{"payload":{}}' | Out-Null
        }
        Start-Sleep -Seconds 5
        $row = Invoke-Psql ("SELECT count(*)||'|'||" +
            "round((percentile_cont(0.5) WITHIN GROUP (ORDER BY lat) * 1000)::numeric, 1)||'|'||" +
            "round((percentile_cont(0.95) WITHIN GROUP (ORDER BY lat) * 1000)::numeric, 1)||'|'||" +
            "round((max(lat) * 1000)::numeric, 1) FROM (" +
            "  SELECT EXTRACT(EPOCH FROM a.started_at - e.scheduled_at) lat" +
            "    FROM mohs_execution e JOIN mohs_attempt a USING (execution_id)" +
            "   WHERE e.job_key = '$OnDemandJobKey' AND e.created_at > '$probeStart') t")
        $n, $p50, $p95, $max = $row -split '\|', 4
        Write-Host ""
        Write-Host "--- dispatch latency, $Nodes idle node(s), enqueue on node 0 (n=$n) ---"
        '{0,-22} {1,10} ms' -f 'p50', $p50
        '{0,-22} {1,10} ms' -f 'p95', $p95
        '{0,-22} {1,10} ms' -f 'max', $max
        Write-Host ""
        Write-Host "--- who dispatched (local hand-off only helps the shard owner) ---"
        Invoke-Psql ("SELECT node_id || ' dispatched ' || n || ' · p50 ' || p50 || ' ms' FROM (" +
            "  SELECT a.node_id, count(*) n," +
            "         round((percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM a.started_at - e.scheduled_at)) * 1000)::numeric, 1) p50" +
            "    FROM mohs_execution e JOIN mohs_attempt a USING (execution_id)" +
            "   WHERE e.job_key = '$OnDemandJobKey' AND e.created_at > '$probeStart'" +
            "   GROUP BY 1 ORDER BY 2 DESC) t")
    }
    else {
        # o job do seed precisa estar admissivel; uma rodada Idle anterior pode
        # ter deixado a definicao pausada na base
        Invoke-Psql "UPDATE mohs_job_definitions SET paused = (job_key <> '$JobKey')" | Out-Null
        Write-Host ("== retained history at start: {0} rows in mohs_execution ==" -f (Invoke-Psql "SELECT count(*) FROM mohs_execution"))
        foreach ($round in 1..$Rounds) {
            $prefix = 'cs{0}n{1}r{2}-' -f (Get-Date -Format 'HHmmss'), $Nodes, $round
            Write-Host ""
            Write-Host "== round $round : seeding $SeedSize as '$prefix*' =="
            Invoke-Psql ("INSERT INTO mohs_execution (execution_id, job_key, shard, priority, state, scheduled_at, created_at, actor, payload, payload_type) " +
                "SELECT '$prefix'||lpad(n::text,7,'0'), '$JobKey', (n % 64), 20, 'PENDING', now(), now(), 'anonymous', '{}', " +
                "'java.util.Collections`$UnmodifiableMap' FROM generate_series(1,$SeedSize) n") | Out-Null
            Invoke-Psql ("INSERT INTO mohs_ready (execution_id, job_key, shard, priority, attempt, visible_at) " +
                "SELECT '$prefix'||lpad(n::text,7,'0'), '$JobKey', (n % 64), 20, 1, now() FROM generate_series(1,$SeedSize) n") | Out-Null

            $pending = "SELECT count(*) FROM mohs_execution WHERE execution_id LIKE '$prefix%' AND state NOT IN ('SUCCEEDED','FAILED','CANCELLED')"
            $wall = [Diagnostics.Stopwatch]::StartNew()
            $deadline = (Get-Date).AddSeconds($DrainTimeoutSeconds)
            do {
                Start-Sleep -Milliseconds 500
                $remaining = [int](Invoke-Psql $pending)
                if ((Get-Date) -gt $deadline) { throw "drain timed out with $remaining pending" }
            } while ($remaining -gt 0)
            $wall.Stop()

            $row = Invoke-Psql ("SELECT count(*)||'|'||EXTRACT(EPOCH FROM max(finished_at) - min(started_at))||'|'||count(DISTINCT node_id) " +
                "FROM mohs_attempt WHERE execution_id LIKE '$prefix%'")
            $attempts, $windowSec, $distinctNodes = $row -split '\|', 3
            '{0} nodes · drained {1} in {2:N1} s wall · attempts window {3:N1} s · {4:N0} exec/s · {5} node(s) in attempts' -f `
                $Nodes, $SeedSize, $wall.Elapsed.TotalSeconds, [double]$windowSec, ([double]$attempts / [double]$windowSec), $distinctNodes
        }
    }
}
finally {
    Stop-Nodes $procs
}
