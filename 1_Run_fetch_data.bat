@echo off
REM This script runs the Discord data fetcher (1_fetch_data.py).

echo Running 1_fetch_data.py...
echo.

REM Execute the python script located in the src folder
python src/1_fetch_data.py

echo.
echo Script has finished.
pause
