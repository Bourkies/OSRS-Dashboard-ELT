@echo off
REM This script validates the project's .toml configuration files.

echo Validating TOML files...
echo.

REM Execute the python script located in the src folder
python src/Validate_toml.py

echo.
echo Script has finished.
pause
