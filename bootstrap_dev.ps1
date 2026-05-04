Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
if ([string]::IsNullOrWhiteSpace($repoRoot)) {
    $repoRoot = (Get-Location).Path
}

$venvDir = Join-Path $repoRoot ".venv"
$venvPython = Join-Path $venvDir "Scripts\python.exe"

Set-Location -LiteralPath $repoRoot

if (-not (Get-Command py -ErrorAction SilentlyContinue)) {
    throw "The Python launcher 'py' was not found. Install Python 3.12 and try again."
}

if (-not (Test-Path -LiteralPath $venvPython)) {
    Write-Host "Creating .venv with Python 3.12..."
    & py -3.12 -m venv $venvDir
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}

Write-Host "Upgrading pip in .venv..."
& $venvPython -m pip install --upgrade pip
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

Write-Host "Installing runtime and dev dependencies..."
& $venvPython -m pip install -r requirements.txt -r requirements-dev.txt
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

Write-Host ""
Write-Host "Bootstrap complete."
Write-Host "Next steps:"
Write-Host "  . .\dev_shell.ps1"
Write-Host "  .\run_tests.ps1"
