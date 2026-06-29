$keyPath = Join-Path $PSScriptRoot "topdeck_key.local.txt"

function Get-ManualSheetPath {
    param([string]$FileName)

    $candidates = @(
        (Join-Path $PSScriptRoot $FileName),
        (Join-Path (Split-Path -Parent $PSScriptRoot) $FileName)
    )

    foreach ($candidate in $candidates) {
        if (Test-Path $candidate) {
            return (Resolve-Path $candidate).Path
        }
    }

    return $null
}

if (-not $env:TOPDECK_API_KEY) {
    if (Test-Path $keyPath) {
        $key = Get-Content -Path $keyPath -Raw
        $env:TOPDECK_API_KEY = $key.Trim()
    } else {
        throw "Defina TOPDECK_API_KEY no ambiente ou crie topdeck_key.local.txt na raiz do projeto."
    }
}

Push-Location $PSScriptRoot
try {
    foreach ($sheetName in @("2x2 online.xlsx", "2x2 presencial.xlsx")) {
        $sheetPath = Get-ManualSheetPath $sheetName
        if ($sheetPath) {
            Write-Host "Importando $sheetPath"
            python (Join-Path $PSScriptRoot "scripts/import_team_map_2x2.py") $sheetPath
        } else {
            Write-Warning "Planilha nao encontrada: $sheetName"
        }
    }

    python (Join-Path $PSScriptRoot "scripts/sync_topdeck.py")
} finally {
    Pop-Location
}
