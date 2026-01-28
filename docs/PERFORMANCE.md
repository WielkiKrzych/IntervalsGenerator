# ⚡ Performance Guide

Dokumentacja wydajności i skalowania Intervals Generator.

---

## Wyniki benchmarków

Testy przeprowadzone na MacBook Pro M1, Python 3.11.

### Operacje podstawowe

| Operacja | Rozmiar | Czas | Przepustowość |
|----------|---------|------|---------------|
| Merge | 10k wierszy, 5 plików | 0.02s | 500k rows/s |
| Normalizacja 10Hz→1Hz | 10k próbek | 0.05s | 200k samples/s |
| Interpolacja | 10k wierszy | 0.01s | 1M rows/s |
| Batch read | 50 plików | 0.3s | 166 files/s |

### Skalowanie

| Rozmiar | Wiersze | Pliki | Czas całkowity | Pamięć szczytowa |
|---------|---------|-------|----------------|------------------|
| Small | 1,000 | 10 | <1s | ~20 MB |
| Medium | 10,000 | 50 | ~2s | ~80 MB |
| Large | 100,000 | 200 | ~15s | ~500 MB |

---

## Optymalizacje w kodzie

### 1. Batch Concat (merger.py)

```python
# PRZED: O(n*f) - concat dla każdego pliku
for f in files:
    df = pd.concat([df, read(f)], axis=1)

# PO: O(n) - single concat
all_dfs = [base] + [read(f) for f in files]
df = pd.concat(all_dfs, axis=1)
```

**Speedup**: 3-5x dla wielu plików

### 2. Early-exit NaN checking (utils.py)

```python
# PRZED: zawsze pełne RLE
def check_nans(series):
    return rle_analysis(series)

# PO: early exit jeśli mało NaN
def check_nans_optimized(series, threshold=10):
    total_nans = series.isna().sum()
    if total_nans < threshold:
        return total_nans  # Skip expensive RLE
    return rle_analysis(series)
```

**Speedup**: 10x dla plików bez luk

### 3. Parallel file reading (validators/integrity.py)

```python
# Równoległe czytanie plików
from concurrent.futures import ThreadPoolExecutor

def read_csvs_parallel(paths, max_workers=4):
    with ThreadPoolExecutor(max_workers) as executor:
        return dict(executor.map(lambda p: (p, pd.read_csv(p)), paths))
```

**Speedup**: 2-4x dla wielu plików

### 4. Regex-free string ops (trainred.py)

```python
# PRZED: regex overhead
df['col'].str.replace(',', '.', regex=True)

# PO: string literal
df['col'].str.replace(',', '.', regex=False)
```

**Speedup**: 2x dla dużych kolumn tekstowych

---

## Zalecenia dla dużych zbiorów

### 1. Batch processing

Jeśli masz 100+ plików w tygodniu:

```bash
# Procesuj partiami
for batch in 1 2 3 4 5; do
    python main.py --base-dir ./batch_$batch
done
```

### 2. Chunked reading (dla plików >1GB)

```python
# Dodaj do loadera:
chunks = pd.read_csv(path, chunksize=10000)
df = pd.concat(chunks, ignore_index=True)
```

### 3. Memory optimization

```python
# Użyj kategorycznych dla powtarzających się wartości
df['Device'] = df['Device'].astype('category')

# Downcast numeric types
df['heartrate'] = pd.to_numeric(df['heartrate'], downcast='integer')
```

### 4. SSD vs HDD

| Storage | Read (50 files) | Write (merged) |
|---------|-----------------|----------------|
| SSD NVMe | 0.3s | 0.05s |
| HDD 7200 | 2.5s | 0.4s |

---

## Uruchamianie benchmarków

```bash
# Standardowy benchmark
python benchmarks/benchmark.py

# Duży zbiór testowy
python benchmarks/benchmark.py --size large

# Z profilowaniem pamięci
python benchmarks/benchmark.py --profile

# Output jako JSON
python benchmarks/benchmark.py --json > results.json
```

---

## Znane ograniczenia

1. **TrainRed 10Hz normalizacja** - wymaga ~10x więcej pamięci niż wynikowy plik
2. **Merge head/tail validation** - interaktywne, spowalnia automatyzację (użyj `validate_head=False`)
3. **Garmin leading NaN trim** - ograniczone do pierwszych 30 wierszy

---

## Profilowanie własnego kodu

```python
import cProfile
import pstats

with cProfile.Profile() as pr:
    pipeline.run_full()

stats = pstats.Stats(pr)
stats.sort_stats('cumulative')
stats.print_stats(20)
```

Memory profiling:

```python
from memory_profiler import profile

@profile
def my_function():
    ...
```
