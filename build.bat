@echo off
echo Building SMT Production Data Entry System...
echo.

if exist dist_new rmdir /s /q dist_new
if exist build rmdir /s /q build

python -m PyInstaller -y --onedir --windowed --name "SMT_App" ^
  --add-data "columns.json;." ^
  --collect-all openpyxl ^
  --distpath "dist_new" ^
  smt_data_entry.py

if %errorlevel% neq 0 (
    echo.
    echo BUILD FAILED.
    exit /b 1
)

echo.
echo Copying databases...
if exist smt_rework.db  copy /y smt_rework.db  dist_new\SMT_App\smt_rework.db
if exist smt_quality.db copy /y smt_quality.db dist_new\SMT_App\smt_quality.db

echo.
echo BUILD COMPLETE. App is in dist_new\SMT_App\
