# 🏋️ Intervals Generator

> **Scal dane treningowe z wielu źródeł w jeden plik CSV — gotowy do importu w Intervals.icu i Analiza Biegowa.**

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-108_passing-brightgreen.svg)](tests/)

---

## 🚀 Jak używać?

### macOS — przeciągnij i upuść 🖱️

```
┌──────────────────────────────────────┐
│                                      │
│   📄 Wahoo.csv                       │
│   📄 trainred_session.csv     🏃 →  │  🌳 Trening-22.04.2026-import.csv
│   📄 tymewear_export.csv             │
│   📄 i59386392_streams.csv           │
│                                      │
└──────────────────────────────────────┘
         MergeCSV.app
```

1. **Zaznacz** pliki CSV/FIT
2. **Przeciągnij** na `MergeCSV.app`
3. Gotowe! Plik `Trening-DD.MM.YYYY-import.csv` pojawia się obok źródłowych

### CLI — zaawansowane użycie 🖥️

```bash
python quick_merge.py wahoo.csv trainred.csv tymewear.csv -o wynik.csv
```

### Streamlit — GUI w przeglądarce 🌐

```bash
streamlit run app.py
```

---

## 📡 Obsługiwane źródła

| Źródło | Dane |
|--------|------|
| **Wahoo ELEMNT** | Moc (L/R), kadencja, prędkość — plik bazowy |
| **Garmin FIT** | GPS, HR, HRV (RMSSD), temperatura, running dynamics, wysokość |
| **Garmin sensor CSV** | Temperatura skóry/rdzenia, HeatStrainIndex, HRV |
| **Intervals.icu** | Pełny stream — kadencja, dystans, VO, HRV — **wszystkie kolumny zachowane** |
| **TrainRed** | 🥇 SmO₂, THb (saturacja mięśniowa) |
| **Tymewear** | BR, VT, VE (wentylacja) |
| **Merged CSV** | Wcześniejszy merge — dodatkowe kolumny |

---

## 🥇 Priorytet TrainRed

Gdy plik bazowy (Wahoo/Intervals.icu) zawiera już kolumny `SmO₂`/`THb` z podłączonego czujnika NIRS, **dedykowany plik TrainRed ma priorytet** — jego wartości zastępują te z bazy. Żadnych duplikatów, żadnego bałaganu.

```
┌──────────────────────┐      ┌──────────────────────┐
│  Plik bazowy         │      │  TrainRed 🥇         │
│  SmO₂: 70–74  ❌     │  →   │  SmO₂: 65–69  ✅     │
│  THb:  12.0–12.4 ❌  │      │  THb:  11.5–11.9 ✅  │
└──────────────────────┘      └──────────────────────┘
```

---

## 🧠 Automatyczna detekcja typów

Pliki rozpoznawane są po nagłówkach — nie musisz nic oznaczać:

| Typ pliku | Sygnatura |
|-----------|-----------|
| **Wahoo** | kolumna `secs` + `watts`, brak `hrv` |
| **Intervals.icu** | `hrv` + kolumny aktywności (`cadence`, `distance`) |
| **Garmin sensor** | `hrv` BEZ kolumn aktywności |
| **TrainRed** | `SmO₂` + `THb` (≤5 kolumn) |
| **Tymewear** | `BR` + `VT` + `VE` |
| **Merged** | kolumny z 2+ różnych źródeł |

---

## ⚙️ Główne funkcje

- **Scalanie kolumnowe** — każdy plik dodaje swoje unikalne kolumny do wspólnej osi czasu
- **Priorytet bazy** — Wahoo > Garmin FIT > Intervals.icu > Garmin sensor > Merged
- **Priorytet TrainRed** — SmO₂/THb z TrainRed zastępuje te z bazy
- **Przycinanie ogona** — usuwa wiersze z NaN na końcu (tylko względem kolumn pliku bazowego — pliki uzupełniające mogą się kończyć wcześniej bez obcinania wyniku)
- **Obsługa FIT** — parsowanie Garmin `.fit` przez `fitparse`
- **108 testów** — detekcja, merge, walidacja, pipeline

---

## 📦 Instalacja

```bash
git clone https://github.com/WielkiKrzych/IntervalsGenerator.git
cd IntervalsGenerator
pip install -r requirements.txt
pip install fitparse    # do obsługi plików FIT
```

---

## 🧪 Uruchomienie testów

```bash
python -m pytest tests/ -v
```

---

## 🏗️ Struktura projektu

```
IntervalsGenerator/
├── quick_merge.py       # ⚡ Szybki merge — CLI i drag & drop
├── main.py              # 🔄 Pełny pipeline (import + merge)
├── app.py               # 🌐 Streamlit GUI
├── intervals/           # 🧱 Logika SOLID
│   ├── merger.py        #    Silnik scalania
│   ├── pipeline.py      #    Orkiestracja pipeline'u
│   ├── loaders/         #    Loadery: wahoo, garmin, trainred, tymewear
│   └── validators/      #    Walidacja danych
├── tests/               # 🧪 108 testów pytest
└── MergeCSV.app/        # 🍎 macOS droplet (gotowy do użycia)
```

---



---

## 📋 Historia zmian

### 2026-05-21 — Fix przycinania ogona (anchor columns)

Naprawiono błąd, w którym scalony plik był przycinany do długości najkrótszego pliku źródłowego.
Przyczyną było sprawdzanie **wszystkich kolumn** pod kątem NaN przy określaniu punktu cięcia ogona.
Pliki uzupełniające (TrainRed, Tymewear) mają legalne luki na końcu — plik bazowy (Wahoo/Garmin) 
może być dłuższy.

**Fix:** Funkcje `_trim_trailing_incomplete()` i `_validate_and_trim_tail()` przyjmują teraz 
opcjonalny `anchor_columns`. Gdy jest podany (z `base_df.columns`), tylko kolumny pliku bazowego 
decydują o punkcie cięcia. Pliki uzupełniające mogą się kończyć wcześniej bez wpływu na wynik.

Zmienione pliki: `quick_merge.py`, `intervals/merger.py`

## 📄 Licencja

MIT — patrz [LICENSE](LICENSE)
