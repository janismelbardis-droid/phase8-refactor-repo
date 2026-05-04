Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
if ([string]::IsNullOrWhiteSpace($repoRoot)) {
    $repoRoot = (Get-Location).Path
}

$venvScripts = Join-Path $repoRoot ".venv\Scripts"
$venvPython = Join-Path $venvScripts "python.exe"
$activateScript = Join-Path $venvScripts "Activate.ps1"

if (-not (Test-Path -LiteralPath $venvPython)) {
    throw "Missing .venv. Run .\bootstrap_dev.ps1 first."
}

$rawPathEntries = @()
foreach ($candidate in @(
    $venvScripts,
    [Environment]::GetEnvironmentVariable("Path", "User"),
    [Environment]::GetEnvironmentVariable("Path", "Machine"),
    $env:PATH
)) {
    if (-not [string]::IsNullOrWhiteSpace($candidate)) {
        $rawPathEntries += $candidate.Split(';', [System.StringSplitOptions]::RemoveEmptyEntries)
    }
}

$seen = New-Object "System.Collections.Generic.HashSet[string]" ([System.StringComparer]::OrdinalIgnoreCase)
$mergedPathEntries = foreach ($entry in $rawPathEntries) {
    $trimmed = $entry.Trim()
    if ($trimmed -and $seen.Add($trimmed)) {
        $trimmed
    }
}
$env:PATH = ($mergedPathEntries -join ';')

Set-Location -LiteralPath $repoRoot
. $activateScript

$pythonVersion = & $venvPython --version
$pytestVersion = & $venvPython -m pytest --version
$rgVersion = "rg not found"
$nodeVersion = "node not found"
$gitVersion = "git not found"

try {
    $rgVersion = (& rg --version | Select-Object -First 1)
} catch {
}

try {
    $nodeVersion = (& node --version)
} catch {
}

try {
    $gitVersion = (& git --version)
} catch {
}

Write-Host ""
Write-Host "Repo root : $repoRoot"
Write-Host "Python    : $pythonVersion"
Write-Host "Pytest    : $pytestVersion"
Write-Host "rg        : $rgVersion"
Write-Host "Node      : $nodeVersion"
Write-Host "Git       : $gitVersion"
Write-Host ""
Write-Host "Tip: dot-source this script to keep the environment in the current shell:"
Write-Host "  . .\dev_shell.ps1"
