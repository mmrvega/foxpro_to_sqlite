@echo off
echo Starting FoxPro to SQLite Migration Pipeline...
py extraction.py --all -w 21
echo.
echo Pipeline Execution Finished.
pause
