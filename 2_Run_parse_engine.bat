@echo off
REM This script runs the raw log parser (2_parse_engine.py).

echo Running 2_parse_engine.py...
echo.

REM Execute the python script located in the src folder
python src/2_parse_engine.py

echo.
echo Script has finished.
pause
