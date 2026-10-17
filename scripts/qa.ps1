param(
    [ValidateRange(1, 100000)] [int]$Operations = 120,
    [string]$Concurrency = '1,4,8',
    [ValidateRange(100, 3600000)] [int]$P95Ms = 2000,
    [switch]$SkipBatch,
    [switch]$Faults
)
$ErrorActionPreference = 'Stop'
$qaDisk = (Get-Item -LiteralPath $PSScriptRoot).PSDrive
if ($null -ne $qaDisk.Free -and $qaDisk.Free -lt 5GB) {
    throw 'At least 5 GiB of free disk space is required before starting QA. Free space without deleting database volumes.'
}
$composeFile = Join-Path $PSScriptRoot '../compose.qa.yaml'
New-Item -ItemType Directory -Force (Join-Path $PSScriptRoot '../.work/qa') | Out-Null
function Invoke-QaCompose {
    & docker compose -p ledgerflow-qa -f $composeFile @args
    if ($LASTEXITCODE -ne 0) { throw "QA command failed (exit $LASTEXITCODE). See .work/qa reports and container logs." }
}
Invoke-QaCompose up -d --build --wait --wait-timeout 300 postgres kafka backend
Invoke-QaCompose build qa
$options = @('--operations', "$Operations", '--concurrency', $Concurrency, '--p95-ms', "$P95Ms")
if ($SkipBatch) { $options += '--skip-batch' }
Invoke-QaCompose run --rm --no-deps qa @options
if ($Faults) {
    try {
        Invoke-QaCompose stop kafka
        Invoke-QaCompose run --rm --no-deps qa --phase outage
    } finally {
        # Always attempt recovery, including if the outage assertion fails.
        Invoke-QaCompose start kafka
    }
    Invoke-QaCompose run --rm --no-deps qa --phase recovery
}
Write-Host 'Reports: .work/qa/<run-id>/report.html ; isolated API: http://localhost:18080'
Write-Host 'QA containers are left running. Stop safely: docker compose -p ledgerflow-qa -f compose.qa.yaml stop'
