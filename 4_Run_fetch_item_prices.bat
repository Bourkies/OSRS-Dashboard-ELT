@echo off
REM This script posts the Personal Bests to Discord (Running 4_fetch_item_prices.py).

echo Running 4_fetch_item_prices.py...
echo.

REM Execute the python script located in the src folder
python src/4_fetch_item_prices.py

echo.
echo Script has finished.
pause
