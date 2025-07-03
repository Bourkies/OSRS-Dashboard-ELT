@echo off
REM This script runs the OSRS Clan Reporter

echo Running OSRS Clan Reporter...
echo.

REM Execute the python script located in the src folder
python src/1_fetch_data.py

echo.
echo Script has finished.
pause
