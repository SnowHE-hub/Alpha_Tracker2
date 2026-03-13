@echo off
cd /d %~dp0

set PYTHONPATH=src
.\.venv\Scripts\python.exe -m alpha_tracker2.pipelines.smoke

pause
