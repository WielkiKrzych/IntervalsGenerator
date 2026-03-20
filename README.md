# Intervals Generator

> Automatyczny import i scalanie danych treningowych z wielu zrodel (Intervals.icu, TrainRed, Tymewear, Wahoo, Garmin FIT/CSV) do jednego pliku CSV kompatybilnego z Intervals.icu i Analiza Biegowa.

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
| **Intervals.icu** | CSV (streams) | Pelne dane aktywnosci: tetno, kadencja, predkosc, dystans, HRV, wentylacja, VerticalOscillation — **wszystkie kolumny zachowane** |
| **Wahoo** | CSV | Moc, kadencja, tetno (plik bazowy) |
| **Garmin FIT** | FIT | Tetno, kadencja, predkosc, GPS, altitude, temperatura skory/rdzen, HRV (RMSSD), running dynamics |
| **Garmin sensor** | CSV | Temperatura skory, temperatura rdzenia, HeatStrainIndex, HRV |
| **TrainRed** | CSV | SmO2, THb (saturacja miesniowa) |
| **Tymewear** | CSV | BR, VT, VE (wentylacja) |
| **Merged** | CSV | Wczesniej zmergowany plik — dodatkowe kolumny z wielu zrodel |

### Priorytet pliku bazowego

```
Wahoo CSV > Garmin FIT > Intervals.icu streams > Garmin sensor > Merged
```

Plik bazowy definiuje os czasu i glowne kolumny. Pozostale pliki dostarczaja dodatkowe kolumny (deduplikacja — baza ma priorytet).

### Inteligentna detekcja typow

Pliki sa automatycznie rozpoznawane na podstawie naglowkow CSV:

- **Intervals.icu**: `hrv` + kolumny aktywnosci (`cadence`, `watts`, `distance`, `velocity_smooth`) — pelny export z Intervals.icu po synchronizacji z Garmin Connect
- **Garmin sensor**: `hrv` BEZ kolumn aktywnosci — dane z czujnikow zegarka (temperatura, heat strain)
- **Wahoo**: `secs`/`watts` bez `hrv`
- **Merged**: kolumny z 2+ roznych zrodel (np. `smo2` + `heartrate` + `skin_temperature`)
- **TrainRed**: `SmO2` + `THb` w tresci pliku
- **Tymewear**: `BR` + `VT` + `VE` w tresci pliku

**Wynik:** Jeden plik `Trening-DD.MM.YYYY-import.csv` zsynchronizowany czasowo, gotowy do importu w Intervals.icu lub analizy w Analiza Biegowa.

### Automatyczne przycinanie niekompletnych wierszy

Po scaleniu plik jest automatycznie przycinany od konca — usuwane sa wszystkie wiersze, w ktorych **jakakolwiek** kolumna nie ma wartosci. Zapobiega to sytuacji, gdy np. TrainRed (SmO2/THb) ma wiecej probek niz baza (Wahoo/Garmin/Intervals.icu), co skutkowaloby pustymi wierszami na koncu pliku wynikowego.

### Obsluga plikow FIT

Pliki `.fit` z zegarkow/komputerow Garmin sa parsowane bezposrednio (wymaga `fitparse`). Wyodrebniane dane:

- Tetno, kadencja (podwojona dla biegu), predkosc (m/s + km/h)
- GPS (lat/lng z konwersji semicircles), wysokosc, dystans
- Temperatura, vertical oscillation, stance time, step length
- SmO2/O2Hb/HHb (jesli podlaczony czujnik NIRS)
- Tymewear (breath rate, tidal volume, ventilation — jesli podlaczony)
- HRV jako per-second RMSSD obliczany z interwalow R-R

---

## Typowe scenariusze uzycia

### 1. Intervals.icu + pliki z czujnikow

Najczestszy workflow — dane z Garmin Connect synchronizowane do Intervals.icu, wzbogacone o dane z czujnikow:

```bash
python quick_merge.py i132707430_streams.csv trainred_session.csv tymewear_export.csv
```

Intervals.icu staje sie baza (ma kadencje, dystans, predkosc, VO), czujniki dodaja SmO2, THb, BR, VT, VE.

### 2. Intervals.icu + wczesniejszy merge

Polaczenie eksportu z Intervals.icu z wczesniej zmergowanym plikiem (np. SubT z danymi sensorowymi):

```bash
python quick_merge.py i132707430_streams.csv "SubT 17.03.2026.csv"
```

### 3. Wahoo + czujniki + Garmin

Klasyczny setup z komputerem rowerowym Wahoo jako baza:

```bash
python quick_merge.py wahoo_streams.csv garmin_streams.csv trainred.csv tymewear.csv
```

### 4. Garmin FIT + czujniki

Bezposrednie przetwarzanie pliku FIT z zegarka Garmin:

```bash
python quick_merge.py activity.fit trainred.csv tymewear.csv
```

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

### Szybki merge (przeciaganie plikow lub CLI)
```bash
# Jawne pliki wejsciowe
python quick_merge.py file1.csv file2.fit garmin.FIT

# Bez argumentow — przetwarza wszystkie CSV/FIT w biezacym katalogu
python quick_merge.py

# Wyjscie do konkretnego pliku
python quick_merge.py streams.csv trainred.csv -o output.csv
```

### Pelny pipeline (import z Downloads + merge)
```bash
python main.py
```

### Inne tryby
```bash
# Tylko import (bez merge)
python main.py --import-only

# Tylko merge (bez importu)
python main.py --merge-only

# Walidacja plikow
python main.py --validate-only

# Backup przed operacja
python main.py --with-backup

# Dry run (symulacja)
python main.py --dry-run
```

### Streamlit GUI
```bash
streamlit run app.py
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
├── intervals/               # Logika aplikacji (SOLID)
│   ├── config.py            # Konfiguracja sciezek i ustawien
│   ├── pipeline.py          # Orkiestracja pipeline'u
│   ├── merger.py            # Silnik scalania z synchronizacja czasowa
│   ├── loaders/             # Loadery per-zrodlo
│   │   ├── garmin.py        # Garmin sensor + Intervals.icu streams
│   │   ├── wahoo.py         # Wahoo ELEMNT (plik bazowy)
│   │   ├── trainred.py      # TrainRed SmO2/THb (10Hz -> 1Hz)
│   │   └── tymewear.py      # Tymewear BR/VT/VE
│   ├── validators/          # Walidacja integralnosci danych
│   └── utils.py             # Narzedzia (header scan, gap detection)
├── tests/                   # Testy (pytest, 106 testow)
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

106 testow pokrywajacych: detekcje typow plikow, merge edge cases, walidacje, pipeline, filesystem operations.

---

## Licencja

MIT License — patrz [LICENSE](LICENSE)

---

## Problemy?

Sprawdz: **[CROSS_PLATFORM.md](CROSS_PLATFORM.md)** — sekcja Rozwiazywanie problemow
