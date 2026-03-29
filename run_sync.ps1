$keyPath = Join-Path $PSScriptRoot "topdeck_key.local.txt"

if (-not $env:TOPDECK_API_KEY) {
    if (Test-Path $keyPath) {
        $key = Get-Content -Path $keyPath -Raw
        $env:TOPDECK_API_KEY = $key.Trim()
    } else {
        throw "Defina TOPDECK_API_KEY no ambiente ou crie topdeck_key.local.txt na raiz do projeto."
    }
}

python (Join-Path $PSScriptRoot "scripts/sync_topdeck.py")
