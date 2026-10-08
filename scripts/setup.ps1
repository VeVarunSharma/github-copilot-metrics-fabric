[CmdletBinding()]
param(
    [string]$Config = "config\config.yml",
    [switch]$Yes,
    [switch]$PlanOnly,
    [switch]$ForceInit
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$python = Join-Path $repoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) {
    python -m venv .venv
}

& $python -m pip install --upgrade pip
& $python -m pip install -e ".[dev]"

if (-not (Test-Path $Config) -or $ForceInit) {
    $initArgs = @("-m", "copilot_metrics_fabric", "init", "--output", $Config)
    if ($ForceInit) {
        $initArgs += "--force"
    }
    & $python @initArgs
}

& $python -m copilot_metrics_fabric validate --config $Config
& $python -m copilot_metrics_fabric bootstrap plan --config $Config

if (-not $PlanOnly) {
    $applyArgs = @(
        "-m", "copilot_metrics_fabric", "bootstrap", "apply",
        "--config", $Config
    )
    if ($Yes) {
        $applyArgs += "--yes"
    }
    & $python @applyArgs
}
