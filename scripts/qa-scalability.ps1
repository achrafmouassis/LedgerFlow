param([ValidateRange(10,3600)][int]$Seconds=60,[ValidateRange(1,500)][int]$Rate=40,[switch]$SkipBuild,[ValidateSet(1,2,3)][int[]]$ReplicaOrder=@(1,2,3))
$ErrorActionPreference='Stop'
$qaRoot=Split-Path $PSScriptRoot -Parent
function Invoke-Scale {
    & docker compose -p ledgerflow-qa -f "$qaRoot/compose.qa.yaml" -f "$qaRoot/compose.qa-scale.yaml" @args
    if($LASTEXITCODE -ne 0){throw "QA scalability command failed: $LASTEXITCODE"}
}
$qaScaleFailures=@()
if(!$SkipBuild){Invoke-Scale build backend qa}
Invoke-Scale up -d --wait --wait-timeout 450 postgres kafka
foreach($replicas in $ReplicaOrder){
    # Do not restart the shared database/broker between measurements: it would
    # make later configurations use different cache and recovery conditions.
    Invoke-Scale up -d --wait --wait-timeout 450 --no-deps --scale "backend=$replicas" backend
    # Recreate the gateway so DNS resolves the current complete set of replicas.
    Invoke-Scale up -d --force-recreate gateway
    try { Invoke-Scale run --rm --no-deps --entrypoint python3 qa /qa/scalability.py --replicas $replicas --seconds $Seconds --rate $Rate }
    catch { $qaScaleFailures += $replicas; Write-Warning "Load assertions failed for $replicas replica(s); continuing comparison. $_" }
}
Write-Host 'Measurements: .work/qa/<run>/scalability.json. Compare latency, errors and drain time; do not infer unlimited scalability.'
if($qaScaleFailures.Count){throw "Failed load assertions for replica counts: $qaScaleFailures. Inspect reports."}
