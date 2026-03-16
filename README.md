# Intervals Generator

> Automatyczny import i scalanie danych treningowych z wielu zrodel (TrainRed, Tymewear, Wahoo, Garmin FIT/CSV) do jednego pliku CSV kompatybilnego z Intervals.icu.

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## Szybki Start

### macOS
1. Przeciagnij pliki CSV i/lub FIT na `MergeCSV.app`
2. Poczekaj na przetwarzanie
3. Plik `Trening-*.csv` pojawi sie w tym samym folderze

### Windows
**Sposob 1 - Drag & Drop:**
1. Przeciagnij pliki CSV i/lub FIT na `MergeCSV.exe`
2. Poczekaj na przetwarzanie
3. Plik `Trening-*.csv` pojawi sie w tym samym folderze

**Sposob 2 - GUI Dialog:**
1. Kliknij dwukrotnie na `MergeCSV.exe`
2. Wybierz pliki CSV/FIT w oknie dialogowym
3. Poczekaj na przetwarzanie

---

## Dokumentacja

Szczegolowa dokumentacja uzycia i budowania na obu platformach: **[CROSS_PLATFORM.md](CROSS_PLATFORM.md)**

---

## Co to robi?

Scalanie danych z wielu zrodel treningowych do jednego pliku CSV:

| Zrodlo | Format | Dane |
|--------|--------|------|
| **Wahoo** | CSV | Moc, kadencja, tetno (plik bazowy) |
| **Garmin FIT** | FIT | Tetno, kadencja, predkosc, GPS, altitude, temperatura skory/rdzen, HRV (RMSSD), running dynamics |
| **Garmin CSV** | CSV | Temperatura skory, temperatura rdzenia, HRV |
| **TrainRed** | CSV | SmO2, THb (saturacja miesniowa) |
| **Tymewear** | CSV | BR, VT, VE (wentylacja) |

**Priorytet pliku bazowego:** Wahoo CSV > Garmin FIT > Garmin CSV

**Wynik:** Jeden plik `Trening-DD.MM.YYYY-import.csv` zsynchronizowany czasowo, gotowy do importu w Intervals.icu.

### Obsluga plikow FIT

Pliki `.fit` z zegarkow/komputerow Garmin sa parsowane bezposrednio (wymaga `fitparse`). Wyodrebniane dane:

- Tetno, kadencja (podwojona dla biegu), predkosc (m/s + km/h)
- GPS (lat/lng z konwersji semicircles), wysokosc, dystans
- Temperatura, vertical oscillation, stance time, step length
- SmO2/O2Hb/HHb (jesli podlaczony czujnik NIRS)
- Tymewear (breath rate, tidal volume, ventilation - jesli podlaczony)
- HRV jako per-second RMSSD obliczany z interwalow R-R

---

## Instalacja

### Ze zrodel
```bash
git clone https://github.com/WielkiKrzych/IntervalsGenerator.git
cd IntervalsGenerator
pip install -r requirements.txt
```

Dla obslugi plikow FIT:
```bash
pip install fitparse
```

### Z PyPI
```bash
pip install intervals-generator-csv
```

**Wymagania:** Python 3.10+

---

## Uzycie CLI

### Pelny pipeline (import + merge)
```bash
python main.py
```

### Szybki merge (bez importu)
```bash
python quick_merge.py file1.csv file2.fit garmin.FIT

# Lub bez argumentow - przetwarza wszystkie CSV/FIT w biezacym katalogu
python quick_merge.py
```

### Inne tryby
```bash
# Tylko import (bez merge)
python main.py --import-only

# Tylko merge (bez importu)
python main.py --merge-only

# Walidacja plikow
python main.py --validate-only
```

---

## Budowanie executabli

### Windows (.exe)
```batch
build_windows.bat
```

Plik wynikowy: `dist/MergeCSV.exe`

### macOS (.app)
Plik `MergeCSV.app` jest juz gotowy. Aby zregenerowac, uzyj Script Editor.

---

## Struktura projektu

```
IntervalsGenerator/
├── quick_merge.py           # Szybki merge CSV/FIT (CLI, drag & drop)
├── main.py                  # CLI entry point (pelny pipeline)
├── app.py                   # Streamlit GUI
├── windows_launcher.py      # Windows .exe launcher
├── intervals/               # Logika aplikacji
│   ├── loaders/             # Loadery per-zrodlo (Garmin, TrainRed, Tymewear)
│   ├── validators/          # Walidacja integralnosci danych
│   ├── merger.py            # Silnik scalania z synchronizacja czasowa
│   └── utils.py             # Narzedzia (parallel CSV read, gap detection)
├── tests/                   # Testy (pytest)
├── MergeCSV.app/            # macOS droplet (gotowy)
├── MergeCSV.spec            # PyInstaller spec
├── build_windows.bat        # Skrypt budowania Windows
└── CROSS_PLATFORM.md        # Dokumentacja krzyzowa
```

---

## Testy

```bash
python -m pytest tests/ -v
```

---

## Licencja

MIT License - patrz [LICENSE](LICENSE)

---

## Problemy?

Sprawdz: **[CROSS_PLATFORM.md](CROSS_PLATFORM.md)** - sekcja Rozwiazywanie problemow
