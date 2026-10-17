param(
    [switch]$Apply,
    [ValidatePattern('^[a-z0-9-]{1,40}$')] [string]$Dataset = 'demo-20261016-v1',
    [ValidateRange(12,1000)] [int]$Accounts = 120,
    [ValidateRange(30,100000)] [int]$Operations = 6000,
    [ValidateRange(1,50)] [int]$MaxRps = 20
)
$ErrorActionPreference = 'Stop'
if ((Get-Item -LiteralPath $PSScriptRoot).PSDrive.Free -lt 5GB) {
    throw 'Free at least 5 GiB before generating demonstration data.'
}
New-Item -ItemType Directory -Force (Join-Path $PSScriptRoot '../.work/demo-data') | Out-Null
$demoOptions = @('--dataset', $Dataset, '--accounts', "$Accounts", '--operations', "$Operations", '--max-rps', "$MaxRps")
if ($Apply) { $demoOptions += @('--apply', '--confirm-database', 'ledgerflow') }
$demoAuthArgs=@()
if (Test-Path "$PSScriptRoot/../.env.auth") { $demoAuthArgs=@('--env-file', "$PSScriptRoot/../.env.auth") }
& docker compose @demoAuthArgs -p ledgerflow -f "$PSScriptRoot/../compose.yaml" `
    -f "$PSScriptRoot/../compose.pipeline.yaml" -f "$PSScriptRoot/../compose.batch.yaml" `
    -f "$PSScriptRoot/../compose.ops.yaml" -f "$PSScriptRoot/../compose.demo.yaml" `
    --profile app run --rm --no-deps demo-data @demoOptions
if ($LASTEXITCODE -ne 0) { throw 'Demo data command failed. Inspect the report and resume with the SAME dataset and parameters.' }
