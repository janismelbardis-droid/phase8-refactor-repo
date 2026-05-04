@echo off
setlocal EnableExtensions

set "ROOT=%~dp0"
set "PY312=C:\Users\Lenovo\AppData\Local\Programs\Python\Python312\python.exe"
set "PY314=C:\Python314\python.exe"

for %%L in (8765 8766) do (
  for /f "tokens=5" %%P in ('netstat -ano -p tcp ^| findstr /R /C:":%%L .*LISTENING"') do (
    if not "%%P"=="0" (
      echo Stopping process on port %%L ^(PID %%P^)^...
      taskkill /PID %%P /T /F >nul 2>&1
    )
  )
)

if exist "%PY312%" (
  "%PY312%" "%ROOT%run_app_v2.py"
  goto :end
)

echo Python 3.12 was not found at:
echo   %PY312%
echo.
echo Falling back to Python 3.14.
echo If this fails with missing packages, install the project dependencies for Python 3.14
echo or restore the Python 3.12 environment.
echo.

if exist "%PY314%" (
  "%PY314%" "%ROOT%run_app_v2.py"
  goto :end
)

echo No supported Python launcher was found.
echo Tried:
echo   %PY312%
echo   %PY314%

:end
echo.
pause
