param(
    [switch]$Fast,
    [switch]$Cov,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$PytestArgs
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
if ([string]::IsNullOrWhiteSpace($repoRoot)) {
    $repoRoot = (Get-Location).Path
}

$venvPython = Join-Path $repoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $venvPython)) {
    throw "Missing .venv. Run .\bootstrap_dev.ps1 first."
}

Set-Location -LiteralPath $repoRoot

$pytestCommand = @("-m", "pytest")
if ($Fast) {
    $pytestCommand += @("-n", "auto")
}
if ($Cov) {
    $pytestCommand += @("--cov=app", "--cov-report=term-missing")
}
if ($PytestArgs) {
    $pytestCommand += $PytestArgs
}

& $venvPython @pytestCommand
exit $LASTEXITCODE
