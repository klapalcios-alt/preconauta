$keyPath = Join-Path $PSScriptRoot "topdeck_key.local.txt"

function Get-ManualSheetPath {
    param([string]$FileName)

    $baseName = [System.IO.Path]::GetFileNameWithoutExtension($FileName)
    $roots = @($PSScriptRoot, (Split-Path -Parent $PSScriptRoot))

    $candidates = @(
        (Join-Path $PSScriptRoot $FileName),
        (Join-Path $PSScriptRoot $baseName),
        (Join-Path (Split-Path -Parent $PSScriptRoot) $FileName),
        (Join-Path (Split-Path -Parent $PSScriptRoot) $baseName)
    )

    foreach ($candidate in $candidates) {
        if (Test-Path $candidate) {
            return (Resolve-Path $candidate).Path
        }
    }

    foreach ($root in $roots) {
        foreach ($ext in @(".xlsx", ".xlsm", ".csv", ".xls")) {
            $candidate = Join-Path $root "$baseName$ext"
            if (Test-Path $candidate) {
                return (Resolve-Path $candidate).Path
            }
        }

        $matches = Get-ChildItem -Path $root -File -ErrorAction SilentlyContinue |
            Where-Object {
                $name = $_.Name
                $base = $_.BaseName
                $name -eq $FileName -or $name -eq $baseName -or $name -like "$baseName*" -or $base -like "$baseName*"
            }

        if ($matches) {
            return (Resolve-Path $matches[0].FullName).Path
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
