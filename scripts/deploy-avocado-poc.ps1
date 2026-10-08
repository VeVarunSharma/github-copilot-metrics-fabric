[CmdletBinding()]
param(
    [string]$Config = "config\avocado-poc.yml",
    [string]$EnvFile = ".env"
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

if (-not (Test-Path $EnvFile)) {
    throw "Missing ignored environment file: $EnvFile"
}

$expected = @(
    "GITHUB_APP_ID",
    "GITHUB_APP_INSTALLATION_ID",
    "GITHUB_APP_PRIVATE_KEY_PATH"
)

foreach ($line in Get-Content $EnvFile) {
    if ($line -match '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$') {
        $name = $matches[1]
        if ($expected -contains $name) {
            $value = $matches[2].Trim().Trim('"').Trim("'")
            Set-Item -Path "Env:$name" -Value $value
        }
    }
}

foreach ($name in $expected) {
    if (-not (Get-Item "Env:$name" -ErrorAction SilentlyContinue)) {
        throw "Missing required variable $name in $EnvFile"
    }
}

$python = Join-Path $repoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) {
    python -m venv .venv
}

& $python -m pip install -e ".[dev]"
if ($LASTEXITCODE -ne 0) { throw "Dependency installation failed" }

$token = & $python scripts\mint_github_app_token.py
if ($LASTEXITCODE -ne 0 -or -not $token) {
    throw "GitHub App installation token generation failed"
}

try {
    $env:GITHUB_TOKEN = $token
    & $python scripts\apply_avocado_poc.py
    if ($LASTEXITCODE -ne 0) { throw "POC deployment failed" }
}
finally {
    Remove-Item Env:GITHUB_TOKEN -ErrorAction SilentlyContinue
    $token = $null
    Set-Clipboard ""
}
