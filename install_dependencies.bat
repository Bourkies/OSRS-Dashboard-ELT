@echo off
REM This script installs the required Python libraries for the OSRS Clan Reporter.

echo Installing required Python libraries...
echo This may take a few moments.
echo.

REM Use pip to install all libraries listed in the requirements file.
python -m pip install -r src/requirements.txt

echo.
echo Installation complete. You can now run the fetch_data.bat script.
pause