@echo off
rem Runs meridian.ps1 with a process-scoped execution policy so the unsigned launcher works from a
rem downloaded zip without changing machine or user policy.
powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File "%~dp0meridian.ps1" %*
exit /b %ERRORLEVEL%
