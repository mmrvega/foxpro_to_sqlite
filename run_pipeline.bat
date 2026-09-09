@echo off
setlocal enabledelayedexpansion

echo ==================================================
echo   FoxPro to SQLite Migration Pipeline
echo ==================================================

:: 1- Run Extraction
echo [1/4] Starting Extraction...
py extraction.py --all -w 21
if %ERRORLEVEL% NEQ 0 goto :error

echo.
:: 2- Run Update All Columns
echo [2/4] Updating all columns...
py update_all_columns.py
if %ERRORLEVEL% NEQ 0 goto :error

echo.
:: 3- Run Compare
echo [3/4] Running comparison check...
py compare.py
if %ERRORLEVEL% NEQ 0 goto :error

:: 4- run mssql compare 
echo.
echo [4/4] Running MSSQL comparison check...
py mssql_compare.py --trusted --workers 8
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