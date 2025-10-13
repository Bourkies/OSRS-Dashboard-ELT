@echo off
REM This script runs the entire ETL pipeline in sequence.

echo Running the full ETL pipeline (run_all_etl.py)...
echo.

REM Execute the python script located in the src folder
python src/run_all_etl.py

echo.
echo Script has finished.
pause
