# 📚 API Documentation - Intervals Generator

Dokumentacja API dla programistów chcących rozszerzyć lub zintegrować Intervals Generator.

---

## Spis treści

- [Przegląd architektury](#przegląd-architektury)
- [LoaderRegistry](#loaderregistry)
- [Loadery](#loadery)
  - [TrainRedLoader](#trainredloader)
  - [TymewearLoader](#tymewearloader)
  - [WahooLoader](#wahooloader)
  - [GarminLoader](#garminloader)
- [Walidatory](#walidatory)
- [Interpolacja](#interpolacja)
- [Pipeline](#pipeline)

---

## Przegląd architektury

```
┌─────────────────────────────────────────────────────────────────┐
│                         Pipeline                                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │ Cleanup  │→ │  Import  │→ │ Process  │→ │  Merge   │        │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘        │
│       ↓             ↓             ↓             ↓               │
│  ┌─────────────────────────────────────────────────────┐       │
│  │              LoaderRegistry                         │       │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐  │       │
│  │  │ Wahoo   │ │TrainRed │ │Tymewear │ │ Garmin  │  │       │
│  │  │ prio=1  │ │ prio=10 │ │ prio=20 │ │ prio=30 │  │       │
│  │  └─────────┘ └─────────┘ └─────────┘ └─────────┘  │       │
│  └─────────────────────────────────────────────────────┘       │
└─────────────────────────────────────────────────────────────────┘
```

---

## LoaderRegistry

Centralna rejestracja loaderów z wzorcem plugin.

### Rejestracja loadera

```python
from intervals.loaders import LoaderRegistry, BaseLoader

@LoaderRegistry.register(
    "my_loader",
    priority=15,
    description="My custom loader",
    file_patterns=["my_*.csv"]
)
class MyLoader(BaseLoader):
    ...
```

### API

| Metoda | Opis | Zwraca |
|--------|------|--------|
| `available_loaders()` | Lista nazw loaderów | `List[str]` |
| `get_loader(name)` | Klasa loadera | `Type[BaseLoader]` |
| `get_all_loaders(config, fs, ui)` | Instancje wszystkich loaderów | `List[BaseLoader]` |
| `get_metadata(name)` | Metadane loadera | `Dict[str, Any]` |
| `is_registered(name)` | Sprawdź rejestrację | `bool` |

### Przykład użycia

```python
from intervals.loaders import LoaderRegistry
from intervals.config import Config
from intervals.filesystem import RealFileSystem
from intervals.ui import ConsoleUI

config = Config.from_env()
fs = RealFileSystem()
ui = ConsoleUI()

# Pobierz wszystkie loadery
loaders = LoaderRegistry.get_all_loaders(config, fs, ui)

for loader in loaders:
    print(f"{loader.name}: {loader.source_dir}")
```

---

## Loadery

### TrainRedLoader

**Źródło**: Czujnik saturacji mięśniowej TrainRed

| Parametr | Wartość |
|----------|---------|
| **Priorytet** | 10 |
| **Wzorzec pliku** | `session_*.csv` |
| **Częstotliwość wejściowa** | 10 Hz |
| **Częstotliwość wyjściowa** | 1 Hz |

#### Wymagane kolumny wejściowe

| Kolumna | Typ | Opis |
|---------|-----|------|
| `Timestamp (seconds passed)` | `float` | Czas w sekundach (0.0, 0.1, 0.2...) |
| `SmO2` | `float` | Saturacja mięśniowa O2 (%) |
| `THb unfiltered` | `float` | Całkowita hemoglobina (g/dL) |

#### Mapowanie wyjściowe

```
SmO2 → smo2
THb unfiltered → THb
```

#### Przetwarzanie

1. **Normalizacja 10Hz → 1Hz**: Grupowanie po `floor(timestamp)`, średnia z próbek
2. **Ekstrakcja kolumn**: Tylko `smo2`, `THb`
3. **Brak interpolacji**: Dane muszą być kompletne

#### Synchronizacja czasu

- Używa indeksu wiersza do synchronizacji z plikiem bazowym Wahoo
- Dane są przycinane do długości najkrótszego pliku

---

### TymewearLoader

**Źródło**: Sensor oddechowy Tymewear

| Parametr | Wartość |
|----------|---------|
| **Priorytet** | 20 |
| **Detekcja** | Kolumny `BR`, `VT`, `VE` |
| **Częstotliwość** | 1 Hz |

#### Wymagane kolumny wejściowe

| Kolumna | Typ | Jednostka | Opis |
|---------|-----|-----------|------|
| `BR` | `int` | oddechy/min | Breathing Rate |
| `VT` | `float` | L | Tidal Volume |
| `VE` | `float` | L/min | Minute Ventilation |

#### Mapowanie wyjściowe

```
BR → TymeBreathRate
VT → tidal_volume
VE → TymeVentilation
```

#### Przetwarzanie

1. **Detekcja nagłówka**: Szuka wiersza z `BR`, `VT`, `VE`
2. **Pominięcie legendy**: Usuwa wiersz 2 (jednostki)
3. **Ekstrakcja kolumn**: Mapuje do nazw wyjściowych

---

### WahooLoader

**Źródło**: Komputer rowerowy Wahoo ELEMNT

| Parametr | Wartość |
|----------|---------|
| **Priorytet** | 1 (najniższy = przetwarzany pierwszy) |
| **Wzorzec pliku** | `*streams.csv` (bez kolumny `hrv`) |
| **Rola** | **Plik bazowy** dla wszystkich operacji merge |

#### Kolumny (wszystkie zachowane)

| Kolumna | Typ | Opis |
|---------|-----|------|
| `secs` | `int` | Czas od startu (s) |
| `watts` | `int` | Moc (W) |
| `cadence` | `int` | Kadencja (RPM) |
| `heartrate` | `int` | Tętno (BPM) |
| `distance` | `float` | Dystans (m) |
| `speed` | `float` | Prędkość (m/s) |
| `altitude` | `float` | Wysokość (m n.p.m.) |

#### API specjalne

```python
wahoo_loader.get_base_dataframe() -> pd.DataFrame
```

Zwraca DataFrame używany jako baza dla mergowania wszystkich źródeł.

---

### GarminLoader

**Źródło**: Zegarek Garmin

| Parametr | Wartość |
|----------|---------|
| **Priorytet** | 30 |
| **Detekcja** | `*streams.csv` z kolumną `hrv` |
| **Częstotliwość** | 1 Hz |

#### Kolumny do ekstrakcji

| Kolumna | Typ | Opis |
|---------|-----|------|
| `skin_temperature` | `float` | Temperatura skóry (°C) |
| `HeatStrainIndex` | `float` | Index obciążenia cieplnego (0-1) |
| `hrv` | `int` | Heart Rate Variability (ms) |

#### Przetwarzanie

1. **Usunięcie NaN z początku**: Max 30 wierszy z leadingNaN
2. **Zachowanie nazw**: Kolumny nie są przemapowywane

---

## Walidatory

### IntegrityValidator

Sprawdza ciągłość danych (luki NaN).

```python
from intervals.validators import IntegrityValidator
from intervals.ui import SilentUI

validator = IntegrityValidator(SilentUI(), gap_threshold=10)
issues = validator.validate(df, "TrainRed")
# issues: ["Kolumna 'smo2': 15 pustych wierszy z rzędu"]
```

### ColumnValidator

Waliduje obecność wymaganych kolumn z fuzzy matchingiem.

```python
from intervals.validators.column_validator import ColumnValidator

validator = ColumnValidator(ui, similarity_threshold=0.7)
result = validator.validate_columns(df, required=['SmO2', 'THb unfiltered'])
# result: {'is_valid': True, 'missing_columns': [], 'suggested_mappings': {}}
```

---

## Interpolacja

### Funkcje

```python
from intervals.interpolation import (
    interpolate_time_gaps,
    resample_to_frequency,
    align_time_series,
    detect_sampling_rate
)
```

#### interpolate_time_gaps

```python
df_filled, count = interpolate_time_gaps(
    df,
    time_col='secs',
    method='linear',  # 'linear', 'ffill', 'bfill', 'pad', 'none'
    max_gap=5,        # Max consecutive NaN to fill
    columns=None      # None = all numeric
)
```

#### resample_to_frequency

```python
df_1hz = resample_to_frequency(
    df_10hz,
    time_col='secs',
    target_freq=1,    # Hz
    current_freq=10,  # Hz (auto-detected if None)
    agg_method='mean' # 'mean', 'first', 'last', 'median'
)
```

#### detect_sampling_rate

```python
rate = detect_sampling_rate(df, time_col='secs')
# rate: 10.0 (Hz)
```

---

## Pipeline

### Pełny pipeline

```python
from intervals.config import Config
from intervals.pipeline import Pipeline

config = Config.from_env()
pipeline = Pipeline(config)

# Pełny pipeline
result = pipeline.run_full()
# result: Path to Trening-DD.MM.YYYY-import.csv

# Lub po krokach
pipeline.run_cleanup()
pipeline.run_import()
pipeline.run_processing()
pipeline.run_validation()
output = pipeline.run_merge()
```

### Tryb dry-run

```python
from intervals.filesystem import RealFileSystem

fs = RealFileSystem(dry_run=True)
pipeline = Pipeline(config, fs=fs)
pipeline.run_full()

# Sprawdź co zostałoby zrobione
operations = fs.get_operations_log()
for op in operations:
    print(op)
```

---

## Typy

Wszystkie typy zdefiniowane w `intervals/types.py`:

```python
from intervals.types import (
    LoaderSourceConfig,   # TypedDict dla konfiguracji loadera
    ValidationResult,     # Wynik walidacji
    ColumnValidationResult,
    LoaderProtocol,       # Protocol dla loaderów
    InterpolationMethod,  # Literal['none', 'linear', 'ffill', 'bfill', 'pad']
)
```
