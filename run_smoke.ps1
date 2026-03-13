# run_smoke.ps1
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

$env:PYTHONPATH = "src"
& ".\.venv\Scripts\python.exe" -m alpha_tracker2.pipelines.smoke
