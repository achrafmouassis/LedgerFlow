# Example: .\scripts\platform.ps1 up -d --build
# Stop without deleting data: .\scripts\platform.ps1 down
$platformAuthArgs = @()
if (Test-Path "$PSScriptRoot/../.env.auth") { $platformAuthArgs = @('--env-file', "$PSScriptRoot/../.env.auth") }
& docker compose @platformAuthArgs -p ledgerflow -f "$PSScriptRoot/../compose.yaml" `
    -f "$PSScriptRoot/../compose.pipeline.yaml" -f "$PSScriptRoot/../compose.batch.yaml" `
    -f "$PSScriptRoot/../compose.ops.yaml" --profile app @args
exit $LASTEXITCODE
