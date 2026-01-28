#!/bin/bash
# ========================================
# Intervals Generator - macOS Build Script
# Regenerates MergeCSV.app droplet
# ========================================

set -e

echo "========================================"
echo "  Intervals Generator - macOS Build"
echo "========================================"
echo

# Check Python installation
if ! command -v python3 &> /dev/null; then
    echo "[ERROR] Python3 nie jest zainstalowane!"
    exit 1
fi

echo "[OK] Python3 znaleziono"
echo

# Check PyInstaller
if ! pip3 show pyinstaller &> /dev/null; then
    echo "[INFO] Instalowanie PyInstaller..."
    pip3 install pyinstaller
    echo "[OK] PyInstaller zainstalowany"
else
    echo "[OK] PyInstaller już zainstalowany"
fi
echo

# Check dependencies
echo "[INFO] Sprawdzanie zależności..."
pip3 install -r requirements.txt
echo

# Clean previous builds
if [ -d "build" ]; then
    echo "[INFO] Czyszczenie poprzednich buildów..."
    rm -rf build
fi
if [ -d "dist" ]; then
    echo "[INFO] Czyszczenie poprzednich dystrybucji..."
    rm -rf dist
fi

# Note: The existing MergeCSV.app is an AppleScript droplet
# To regenerate it, you would need to use Script Editor on macOS
# This script is primarily for future PyInstaller-based builds

echo
echo "========================================"
echo "  Budowanie zakończone!"
echo "========================================"
echo
echo "UWAGA: MergeCSV.app jest istniejącym AppleScript dropletem."
echo "Jeśli chcesz go zregenerować, użyj Script Editor na macOS."
echo
