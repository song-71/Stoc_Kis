@echo off
setlocal

REM Daily_fetch_auto_run.sh 실행 (WSL/Git Bash 등 bash 환경 필요)
set "BASE_DIR=%~dp0"
set "SCRIPT=%BASE_DIR%Daily_fetch_auto_run.sh"

REM WSL이 있으면 WSL bash로 실행
where wsl >nul 2>&1
if %errorlevel% equ 0 (
    wsl bash "%SCRIPT%"
    goto :done
)

REM bash가 PATH에 있으면 직접 실행 (Git Bash 등)
where bash >nul 2>&1
if %errorlevel% equ 0 (
    bash "%SCRIPT%"
    goto :done
)

echo [ERROR] bash 또는 WSL이 필요합니다. Daily_fetch_auto_run.sh를 직접 실행하세요.
exit /b 1

:done
endlocal
