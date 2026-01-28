# 🏋️ Intervals Generator

> Automatyczny import i scalanie danych treningowych z wielu źródeł (TrainRed, Tymewear, Wahoo, Garmin) do jednego pliku CSV.

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## 🚀 Szybki Start

### macOS (Mac Mini)
1. Przeciągnij pliki CSV na `MergeCSV.app`
2. Poczekaj na przetwarzanie
3. Plik `Trening-*.csv` pojawi się w tym samym folderze

### Windows (Laptop)
**Sposób 1 - Drag & Drop:**
1. Przeciągnij pliki CSV na `MergeCSV.exe`
2. Poczekaj na przetwarzanie
3. Plik `Trening-*.csv` pojawi się w tym samym folderze

**Sposób 2 - GUI Dialog:**
1. Kliknij dwukrotnie na `MergeCSV.exe`
2. Wybierz pliki CSV w oknie dialogowym
3. Poczekaj na przetwarzanie

---

## 📖 Dokumentacja

Szczegółowa dokumentacja użycia i budowania na obu platformach znajdziesz w: **[CROSS_PLATFORM.md](CROSS_PLATFORM.md)**

---

## 🎯 Co to robi?

Scalanie danych z:
- **Wahoo** - Moc, kadencja, tętno (plik bazowy)
- **TrainRed** - SmO2, THb (saturacja mięśniowa)
- **Tymewear** - BR, VT, VE (wentylacja)
- **Garmin** - Temperatura skóry, HRV

**Wynik:** Jeden plik `Trening-DD.MM.YYYY-import.csv` zsynchronizowany czasowo.

---

## 📦 Instalacja

### Ze źródeł
```bash
git clone https://github.com/WielkiKrzych/IntervalsGenerator.git
cd IntervalsGenerator
pip install -r requirements.txt
```

### Z PyPI
```bash
pip install intervals-generator-csv
```

**Wymagania:** Python 3.10+

---

## 💻 Użycie CLI

```bash
# Pełny pipeline
python main.py

# Tylko import (bez merge)
python main.py --import-only

# Tylko merge (bez importu)
python main.py --merge-only

# Walidacja plików
python main.py --validate-only
```

---

## 🔧 Budowanie executabli

### Windows (.exe)
```batch
build_windows.bat
```

Plik wynikowy: `dist/MergeCSV.exe`

### macOS (.app)
Plik `MergeCSV.app` jest już gotowy. Aby zregenerować, użyj Script Editor.

---

## 📁 Struktura projektu

```
IntervalsGenerator/
├── quick_merge.py          # Szybki merge (CLI)
├── main.py                 # CLI entry point
├── app.py                  # Streamlit GUI
├── windows_launcher.py      # Windows .exe launcher
├── MergeCSV.spec           # PyInstaller spec
├── build_windows.bat        # Skrypt budowania Windows
├── build_macos.sh          # Skrypt budowania macOS
├── intervals/              # Logika aplikacji
├── MergeCSV.app/           # macOS droplet (gotowy)
└── CROSS_PLATFORM.md        # Dokumentacja krzyżowa
```

---

## 📄 Licencja

MIT License - patrz [LICENSE](LICENSE)

---

## 📞 Problemy?

Sprawdź: **[CROSS_PLATFORM.md](CROSS_PLATFORM.md)** - sekcja Rozwiązywanie problemów
