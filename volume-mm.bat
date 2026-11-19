@echo off
setlocal
for %%I in ("%~dp0.") do set "PROJECT=%%~fI"
set "UV=%PROJECT%\..\order-flow-robot\.tools\uv.exe"
if not exist "%UV%" set "UV=%PROJECT%\.tools\uv.exe"
set "UV_PROJECT_ENVIRONMENT=%PROJECT%\.venv"
"%UV%" run --project "%PROJECT%" volume-mm-robot %*
