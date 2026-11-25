param(
    # Reprocessa usando apenas os RAW locais (data/raw), sem chamar a API da TopDeck.
    [switch]$Offline
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    throw "Python nao encontrado no PATH. Instale o Python 3 (python.org) e rode de novo."
}
python -c "import pandas, requests" 2>$null
if ($LASTEXITCODE -ne 0) {
    throw "Faltam dependencias. Rode: python -m pip install pandas requests"
}

if ($Offline) {
    $env:TOPDECK_OFFLINE = "1"
} elseif (-not $env:TOPDECK_API_KEY) {
    $keyPath = Join-Path $PSScriptRoot "topdeck_key.local.txt"
    if (-not (Test-Path $keyPath)) {
        throw "Defina TOPDECK_API_KEY no ambiente ou crie topdeck_key.local.txt na raiz do projeto."
    }
    $env:TOPDECK_API_KEY = (Get-Content -Path $keyPath -Raw).Trim()
}

Push-Location $PSScriptRoot
try {
    foreach ($sheet in @("2x2 online.xlsx", "2x2 presencial.xlsx")) {
        if (Test-Path $sheet) {
            Write-Host "Importando $sheet"
            python scripts/import_team_map_2x2.py $sheet
            if ($LASTEXITCODE -ne 0) { throw "Falha ao importar $sheet" }
        } else {
            Write-Warning "Planilha nao encontrada: $sheet"
        }
    }

    python scripts/sync_topdeck.py
    if ($LASTEXITCODE -ne 0) { throw "Falha no sync_topdeck.py" }
} finally {
    if ($Offline) { Remove-Item Env:TOPDECK_OFFLINE -ErrorAction SilentlyContinue }
    Pop-Location
}
