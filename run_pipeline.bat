@echo off
setlocal enabledelayedexpansion

echo ==================================================
echo   FoxPro to SQLite Migration Pipeline
echo ==================================================

:: 1- Run Extraction
echo [1/3] Starting Extraction...
py extraction.py --all -w 21
if %ERRORLEVEL% NEQ 0 goto :error

echo.
:: 2- Run Update All Columns
echo [2/3] Updating all columns...
py update_all_columns.py
if %ERRORLEVEL% NEQ 0 goto :error

echo.
:: 3- Run Compare
echo [3/3] Running comparison check...
py compare.py
if %ERRORLEVEL% NEQ 0 goto :error

echo.
echo ==================================================
echo   Pipeline Execution Finished Successfully.
echo ==================================================
pause
exit /b

:error
echo.
echo !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
echo   ERROR: Pipeline failed at previous step.
echo !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
pause
exit /b 1