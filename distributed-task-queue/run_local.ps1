$ErrorActionPreference = "Stop"

Set-Location $PSScriptRoot

if (-not (Test-Path ".venv")) {
    Write-Host "[local] creating virtual environment..."
    py -3 -m venv .venv
}

Write-Host "[local] activating virtual environment..."
. .\.venv\Scripts\Activate.ps1

Write-Host "[local] installing dependencies..."
pip install -r requirements.txt

Write-Host "[local] starting API + fault manager + 3 workers..."
python run_local.py --workers 3
