@echo off
REM ========================================
REM Intervals Generator - Windows Build Script
REM Creates portable .exe with PyInstaller
REM ========================================

echo ========================================
echo   Intervals Generator - Windows Build
echo ========================================
echo.

REM Check Python installation
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python nie jest zainstalowane lub nie jest w PATH!
    pause
    exit /b 1
)

echo [OK] Python znaleziono
echo.

REM Check PyInstaller
pip show pyinstaller >nul 2>&1
if errorlevel 1 (
    echo [INFO] Instalowanie PyInstaller...
    pip install pyinstaller
    if errorlevel 1 (
        echo [ERROR] Nie udało się zainstalować PyInstaller!
        pause
        exit /b 1
    )
    echo [OK] PyInstaller zainstalowany
) else (
    echo [OK] PyInstaller już zainstalowany
)
echo.

REM Check dependencies
echo [INFO] Sprawdzanie zależności...
pip install -r requirements.txt
if errorlevel 1 (
    echo [WARNING] Niektóre zależności mogły nie zostać zainstalowane
)
echo.

REM Clean previous builds
if exist build (
    echo [INFO] Czyszczenie poprzednich buildów...
    rmdir /s /q build
)
if exist dist (
    echo [INFO] Czyszczenie poprzednich dystrybucji...
    rmdir /s /q dist
)

REM Build with PyInstaller
echo.
echo ========================================
echo   Budowanie .exe...
echo ========================================
echo.

pyinstaller --clean MergeCSV.spec

if errorlevel 1 (
    echo.
    echo [ERROR] Budowanie nie powiodło się!
    pause
    exit /b 1
)

echo.
echo ========================================
echo   Budowanie zakończone pomyślnie!
echo ========================================
echo.
echo Lokalizacja .exe: dist\MergeCSV.exe
echo.
echo Użycie:
echo   1. Przeciągnij pliki CSV na MergeCSV.exe
echo   2. Albo kliknij dwukrotnie, aby wybrać pliki w oknie dialogowym
echo.
pause
