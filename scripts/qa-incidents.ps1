$ErrorActionPreference='Stop'
$incidentRoot=Split-Path $PSScriptRoot -Parent
New-Item -ItemType Directory -Force "$incidentRoot/.work/qa-private" | Out-Null
function Invoke-IncidentCompose {
    & docker compose -p ledgerflow-qa -f "$incidentRoot/compose.qa.yaml" @args
    if($LASTEXITCODE -ne 0){throw "QA incident command failed: $LASTEXITCODE"}
}
# Explicitly replace the default runner entrypoint for each phase.
function Run-Probe($phase){Invoke-IncidentCompose run --rm --no-deps --entrypoint python3 qa /qa/incidents.py $phase}
function Wait-QaBackend {
    $deadline=(Get-Date).AddSeconds(180)
    do {
        try { $health=Invoke-RestMethod 'http://localhost:18080/actuator/health' -TimeoutSec 3; if($health.status -eq 'UP'){return} }catch{}
        Start-Sleep -Seconds 2
    }while((Get-Date) -lt $deadline)
    throw 'QA backend did not recover'
}
# Downscale/recreate only QA; the main application is never in these commands.
Invoke-IncidentCompose up -d --build --wait --wait-timeout 300 --scale backend=1 postgres kafka backend
Invoke-IncidentCompose build qa interrupted-batch
Run-Probe prepare
try { Invoke-IncidentCompose kill -s SIGKILL backend } finally { Invoke-IncidentCompose start backend }
Wait-QaBackend
Run-Probe backend-recovered
try {
    Invoke-IncidentCompose stop postgres
    Run-Probe db-down
} finally { Invoke-IncidentCompose start postgres }
Wait-QaBackend
Run-Probe db-recovered
Run-Probe dlt
$stageMarker=Join-Path $incidentRoot '.work/qa/stage-lock-ready'
if(Test-Path $stageMarker){throw 'Stale stage-lock-ready marker exists; inspect the previous QA incident run before retrying.'}
try {
    Invoke-IncidentCompose run -d --name ledgerflow-qa-stage-lock --no-deps --entrypoint python3 qa /qa/incidents.py hold-stage
    $deadline=(Get-Date).AddSeconds(45)
    while(!(Test-Path $stageMarker) -and (Get-Date) -lt $deadline){Start-Sleep -Seconds 1}
    if(!(Test-Path $stageMarker)){throw 'QA stage lock did not become ready'}
    $incidentDay=[DateTime]::UtcNow.ToString('yyyy-MM-dd')
    Invoke-IncidentCompose run -d --name ledgerflow-qa-interrupted-batch --no-deps interrupted-batch --date $incidentDay
    Run-Probe batch-running
    & docker kill --signal SIGKILL ledgerflow-qa-interrupted-batch
    if($LASTEXITCODE -ne 0){throw 'Could not kill isolated batch'}
} finally {
    & docker stop --timeout 2 ledgerflow-qa-stage-lock
}
Run-Probe batch-recovered
Write-Host 'Incident reports are in .work/qa. Private credentials/state remain in .work/qa-private. Stopped diagnostic containers are retained.'
