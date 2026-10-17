param([ValidateRange(10,3600)][int]$Seconds=60,[ValidateRange(1,500)][int]$Rate=40)
$ErrorActionPreference='Stop'
$qaRoot=Split-Path $PSScriptRoot -Parent
function Invoke-Scale {
    & docker compose -p ledgerflow-qa -f "$qaRoot/compose.qa.yaml" -f "$qaRoot/compose.qa-scale.yaml" @args
    if($LASTEXITCODE -ne 0){throw "QA scalability command failed: $LASTEXITCODE"}
}
$qaScaleFailures=@()
Invoke-Scale build backend qa
foreach($replicas in 1,2,3){
    Invoke-Scale up -d --wait --wait-timeout 450 --scale "backend=$replicas" postgres kafka backend
    # Recreate the gateway so DNS resolves the current complete set of replicas.
    Invoke-Scale up -d --force-recreate gateway
    try { Invoke-Scale run --rm --no-deps --entrypoint python3 qa /qa/scalability.py --replicas $replicas --seconds $Seconds --rate $Rate }
    catch { $qaScaleFailures += $replicas; Write-Warning "Load assertions failed for $replicas replica(s); continuing comparison. $_" }
}
Write-Host 'Measurements: .work/qa/<run>/scalability.json. Compare latency, errors and drain time; do not infer unlimited scalability.'
if($qaScaleFailures.Count){throw "Failed load assertions for replica counts: $qaScaleFailures. Inspect reports."}
