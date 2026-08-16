$uri = 'http://localhost:8080/api/mohs/v1/jobs/every-job/schedule'

$headers = @{
    'Content-Type' = 'application/json'
}

$body = '{"payload":{}}'

$totalRequests = 10000
$maxConcurrency = 1000

$results = 1..$totalRequests | ForEach-Object -Parallel {

    $sw = [System.Diagnostics.Stopwatch]::StartNew()

    try {
        $response = Invoke-RestMethod `
            -Uri $using:uri `
            -Method POST `
            -Headers $using:headers `
            -Body $using:body `
            -ErrorAction Stop

        $sw.Stop()

        [PSCustomObject]@{
            Success = $true
            Status  = 200
            Latency = $sw.ElapsedMilliseconds
        }
    }
    catch {
        $sw.Stop()

        $status = 0

        if ($_.Exception.Response) {
            try {
                $status = [int]$_.Exception.Response.StatusCode
            }
            catch {}
        }

        [PSCustomObject]@{
            Success = $false
            Status  = $status
            Latency = $sw.ElapsedMilliseconds
        }

    }

} -ThrottleLimit $maxConcurrency

$totalTime = ($results | Measure-Object Latency -Sum).Sum
$success = ($results | Where-Object Success).Count
$errors = $totalRequests - $success

Write-Host ""
Write-Host "=============================="
Write-Host "       LOAD TEST RESULT"
Write-Host "=============================="
Write-Host "Requests       : $totalRequests"
Write-Host "Concurrency    : $maxConcurrency"
Write-Host "Success        : $success"
Write-Host "Errors         : $errors"
Write-Host "Avg Latency    : $([math]::Round(($results | Measure-Object Latency -Average).Average, 2)) ms"
Write-Host "Min Latency    : $(($results | Measure-Object Latency -Minimum).Minimum) ms"
Write-Host "Max Latency    : $(($results | Measure-Object Latency -Maximum).Maximum) ms"
Write-Host "=============================="