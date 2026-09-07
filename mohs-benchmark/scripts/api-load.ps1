#Requires -Version 7.0
<#
.SYNOPSIS
Enfileira execucoes de jobs existentes pela API Mohs, em ondas finitas.
.DESCRIPTION
Nao cria definicoes nem modifica recorrencias. Count e o total de execucoes,
distribuido entre JobKeys em round-robin. Cada onda espera todas as respostas;
WaveIntervalSeconds e uma pausa entre ondas, nao uma taxa garantida de chegada.
Requer PowerShell 7. O token Bearer pode vir de MOHS_API_TOKEN; Headers permite
autenticacao e cabecalhos adicionais (por exemplo, CSRF exigido pelo host).
O CSV registra aceites da API, nao conclusoes dos handlers. Nao ha retry automatico.
.EXAMPLE
./api-load.ps1 -JobKeys every-job2 -Count 10000 -WaveSize 500 -Concurrency 16
.EXAMPLE
./api-load.ps1 -JobKeys every-job,every-job2 -Count 2000 -WaveIntervalSeconds 2 -DryRun
.EXAMPLE
./api-load.ps1 -JobKeys send-invoice -PayloadJson '{"invoiceId":4711}' -Count 100
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][ValidateNotNullOrEmpty()][string[]]$JobKeys,
    [string]$BaseUrl = 'http://localhost:8080/api/mohs/v1',
    [ValidateRange(1, 2147483647)][int]$Count = 1000,
    [ValidateRange(1, 10000)][int]$WaveSize = 100,
    [ValidateRange(1, 256)][int]$Concurrency = 8,
    [ValidateRange(0, 86400)][double]$WaveIntervalSeconds = 1,
    [ValidateRange(1, 3600)][int]$TimeoutSeconds = 30,
    [string]$PayloadJson = '{}',
    [ValidateSet('CRITICAL', 'HIGH', 'NORMAL', 'LOW', 'BACKGROUND')][string]$Priority = 'NORMAL',
    [string]$Actor = 'api-volume-test',
    [hashtable]$Headers = @{},
    [ValidatePattern('^[a-zA-Z0-9_-]{1,80}$')][string]$RunId = [guid]::NewGuid().ToString('N'),
    [string]$OutputPath,
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$apiUri = $null
if (-not [uri]::TryCreate($BaseUrl, [UriKind]::Absolute, [ref]$apiUri) -or
    $apiUri.Scheme -notin @('http', 'https') -or $apiUri.Query -or $apiUri.Fragment -or $apiUri.UserInfo) {
    throw 'BaseUrl deve ser uma URL HTTP(S) da API, sem query, fragmento ou credenciais.'
}
$BaseUrl = $BaseUrl.TrimEnd('/')
foreach ($key in $JobKeys) {
    if ([string]::IsNullOrWhiteSpace($key)) { throw 'JobKeys nao pode conter uma chave vazia.' }
}
$payload = ConvertFrom-Json -InputObject $PayloadJson -AsHashtable
if ($payload -isnot [System.Collections.IDictionary]) {
    throw 'PayloadJson deve ser um objeto JSON, por exemplo {}.'
}
$body = @{ payload = $payload; priority = $Priority } | ConvertTo-Json -Depth 100 -Compress
$requestHeaders = @{} + $Headers
if ($env:MOHS_API_TOKEN -and -not $requestHeaders.ContainsKey('Authorization')) {
    $requestHeaders['Authorization'] = 'Bearer ' + $env:MOHS_API_TOKEN
}
$requestHeaders['X-Mohs-Actor'] = $Actor
$requestHeaders.Remove('Idempotency-Key')

if (-not $OutputPath) {
    $OutputPath = Join-Path $PSScriptRoot "../results/api-load-$RunId.csv"
}
$OutputPath = [IO.Path]::GetFullPath($OutputPath)
$waves = [long][Math]::Ceiling($Count / [double]$WaveSize)
Write-Host "RunId: $RunId | Total: $Count | Jobs: $($JobKeys -join ', ')"
Write-Host "Ondas: $waves | Tamanho: $WaveSize | Concorrencia: $Concurrency | Pausa: ${WaveIntervalSeconds}s"
Write-Host "API: $BaseUrl | CSV: $OutputPath"
if ($DryRun) {
    Write-Host 'Simulacao: nenhuma chamada HTTP ou escrita de arquivo realizada.'
    return
}
if (Test-Path -LiteralPath $OutputPath) { throw "O arquivo de resultados ja existe: $OutputPath" }

# Validacao somente de leitura antes de gerar carga. Uma chave inexistente nao deixa uma onda parcial.
foreach ($key in ($JobKeys | Select-Object -Unique)) {
    $encodedKey = [uri]::EscapeDataString($key)
    $null = Invoke-RestMethod -Uri "$BaseUrl/jobs/$encodedKey" -Method Get -Headers $requestHeaders -TimeoutSec $TimeoutSeconds
}
$null = New-Item -ItemType Directory -Path ([IO.Path]::GetDirectoryName($OutputPath)) -Force
$timer = [Diagnostics.Stopwatch]::StartNew()
$accepted = 0L
$unsuccessful = 0L
$attempted = 0L

for ($offset = 0L; $offset -lt $Count; $offset += $WaveSize) {
    $size = [int][Math]::Min($WaveSize, $Count - $offset)
    $wave = [long]($offset / $WaveSize) + 1
    $requests = for ($i = 0; $i -lt $size; $i++) {
        $ordinal = $offset + $i
        [pscustomobject]@{
            Ordinal = $ordinal + 1
            JobKey = $JobKeys[[int]($ordinal % $JobKeys.Length)]
            IdempotencyKey = "api-load-$RunId-$ordinal"
        }
    }

    $results = @($requests | ForEach-Object -ThrottleLimit $Concurrency -Parallel {
        $ErrorActionPreference = 'Stop'
        $request = $_
        $callHeaders = @{} + $using:requestHeaders
        $callHeaders['Idempotency-Key'] = $request.IdempotencyKey
        $uri = $using:BaseUrl + '/jobs/' + [uri]::EscapeDataString($request.JobKey) + '/schedule'
        $startedAt = [DateTimeOffset]::UtcNow.ToString('o')
        $watch = [Diagnostics.Stopwatch]::StartNew()
        $statusCode = 0
        $outcome = 'UNKNOWN'
        $executionId = ''
        $errorText = ''
        try {
            $receipt = Invoke-RestMethod -Uri $uri -Method Post -Headers $callHeaders -Body $using:body `
                -ContentType 'application/json; charset=utf-8' -TimeoutSec $using:TimeoutSeconds -StatusCodeVariable statusCode
            if ($statusCode -eq 202 -and $receipt.executionId) {
                $executionId = $receipt.executionId
                $outcome = 'ACCEPTED'
            } else {
                $errorText = 'Resposta inesperada: esperado HTTP 202 com executionId.'
            }
        } catch {
            if ($_.Exception.Response) {
                $statusCode = [int]$_.Exception.Response.StatusCode
                $outcome = 'HTTP_ERROR'
                $errorText = "HTTP $statusCode"
            } else {
                # Timeout/transporte pode ocorrer depois do commit. Nao repetir com outra chave.
                $errorText = 'Falha de transporte ou timeout; aceite pelo servidor desconhecido.'
            }
        }
        [pscustomobject]@{
            RunId = $using:RunId
            Wave = $using:wave
            Ordinal = $request.Ordinal
            JobKey = $request.JobKey
            IdempotencyKey = $request.IdempotencyKey
            StartedAtUtc = $startedAt
            HttpStatus = $statusCode
            Outcome = $outcome
            ExecutionId = $executionId
            LatencyMs = [Math]::Round($watch.Elapsed.TotalMilliseconds, 2)
            Error = $errorText
        }
    })
    $results | Sort-Object Ordinal | Export-Csv -LiteralPath $OutputPath -NoTypeInformation -Encoding utf8 -Append
    $waveAccepted = @($results | Where-Object Outcome -eq 'ACCEPTED').Count
    $accepted += $waveAccepted
    $unsuccessful += $size - $waveAccepted
    $attempted += $size
    Write-Host "Onda ${wave}/${waves}: $waveAccepted/$size aceitas | Total: $accepted/$Count"
    if ($waveAccepted -ne $size) {
        Write-Warning 'Carga interrompida apos esta onda. Consulte o CSV; UNKNOWN e erros 5xx podem ter sido persistidos.'
        break
    }
    if ($offset + $size -lt $Count -and $WaveIntervalSeconds -gt 0) {
        Start-Sleep -Seconds $WaveIntervalSeconds
    }
}
$timer.Stop()
[pscustomobject]@{
    RunId = $RunId
    Requested = $Count
    Attempted = $attempted
    Accepted = $accepted
    Unsuccessful = $unsuccessful
    ElapsedSeconds = [Math]::Round($timer.Elapsed.TotalSeconds, 2)
    AcceptedPerSecond = [Math]::Round($accepted / [Math]::Max(0.001, $timer.Elapsed.TotalSeconds), 2)
    ResultsPath = $OutputPath
}
if ($unsuccessful -gt 0) { throw 'A carga terminou com respostas sem aceite confirmado. Consulte o CSV.' }
