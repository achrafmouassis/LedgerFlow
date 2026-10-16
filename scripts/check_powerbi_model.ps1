param([Parameter(Mandatory=$true)][string]$AssemblyDirectory)
$ErrorActionPreference='Stop'
# Microsoft.AnalysisServices NuGet package, lib/net8.0, with PowerShell 7.4+.
Add-Type -Path (Join-Path $AssemblyDirectory 'Microsoft.AnalysisServices.Core.dll')
Add-Type -Path (Join-Path $AssemblyDirectory 'Microsoft.AnalysisServices.Tabular.dll')
$definition=Join-Path $PSScriptRoot '../bi/powerbi/LedgerFlow.SemanticModel/definition'
$model=[Microsoft.AnalysisServices.Tabular.TmdlSerializer]::DeserializeModelFromFolder((Resolve-Path $definition).Path)
$pages=Join-Path $PSScriptRoot '../bi/powerbi/LedgerFlow.Report/definition/pages'
$references=0
Get-ChildItem $pages -Recurse -Filter visual.json | ForEach-Object {
    $visual=Get-Content -Raw -LiteralPath $_.FullName | ConvertFrom-Json
    foreach($role in $visual.visual.query.queryState.PSObject.Properties) {
        foreach($projection in $role.Value.projections) {
            $field=$projection.field
            if($field.Measure) { $entry=$field.Measure; $kind='Measures' }
            else { $entry=$field.Column; $kind='Columns' }
            $table=$model.Tables.Find($entry.Expression.SourceRef.Entity)
            if(-not $table -or -not $table.$kind.Find($entry.Property)) {
                throw "Unknown model reference: $($projection.queryRef)"
            }
            $references++
        }
    }
}
Write-Output "Microsoft TMDL parser: $($model.Tables.Count) tables, $($model.Expressions.Count) parameters; $references visual field references valid"
