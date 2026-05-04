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

Set-Location -LiteralPath $repoRoot

$testScript = Join-Path $repoRoot "run_tests.ps1"
if (-not (Test-Path -LiteralPath $testScript)) {
    throw "Missing run_tests.ps1."
}

$testArgs = @()
if ($Fast) {
    $testArgs += "-Fast"
}
if ($Cov) {
    $testArgs += "-Cov"
}
if ($PytestArgs) {
    $testArgs += $PytestArgs
}

& $testScript @testArgs
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

if (-not (Test-Path -LiteralPath (Join-Path $repoRoot "package.json"))) {
    Write-Host "No package.json found. Skipping UI checks."
    exit 0
}

if (-not (Get-Command npm -ErrorAction SilentlyContinue)) {
    Write-Warning "npm is not available. Skipping UI checks."
    exit 0
}

& npm run check:ui
exit $LASTEXITCODE
