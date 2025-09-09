@echo off
REM This script runs the data transformation engine (3_transform_data.py).

echo Running 3_transform_data.py...
echo.

REM Execute the python script located in the src folder
python src/3_transform_data.py

echo.
echo Script has finished.
pause
