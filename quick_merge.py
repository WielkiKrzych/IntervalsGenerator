#!/usr/bin/env python3
"""
MergeCSV - łączenie plików CSV z różnych urządzeń treningowych.

Obsługiwane źródła danych:
  - Intervals.icu streams CSV (pełne dane aktywności + sensory)
  - Wahoo ELEMNT CSV (dane bazowe: time, watts, cadence, HR, speed, altitude)
  - Garmin FIT (pełne dane aktywności z zegarka)
  - Garmin sensor CSV (hrv, skin_temperature, HeatStrainIndex, core_temperature)
  - TrainRed CSV (SmO2, THb)
  - Tymewear CSV (BR, VT, VE)
  - Wcześniej zmergowany plik (wszystkie dodatkowe kolumny)

Priorytet bazowy: Wahoo > FIT > Intervals.icu > Garmin sensor > merged
"""

import argparse
import math
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any
import pandas as pd
import numpy as np


# --- Column definitions ---

TRAINRED_COLUMNS = ["SmO2", "THb"]
TYMEWEAR_COLUMNS = ["BR", "VT", "VE"]
GARMIN_SENSOR_COLUMNS = ["skin_temperature", "HeatStrainIndex", "core_temperature", "hrv"]

# Columns that indicate a full activity stream (vs sensor-only)
ACTIVITY_INDICATOR_COLUMNS = {"cadence", "watts", "distance", "velocity_smooth"}

TYMEWEAR_MAPPING = {
    "BR": "TymeBreathRate",
    "VT": "tidal_volume",
    "VE": "TymeVentilation",
}

TRAINRED_MAPPING = {
    "SmO2": "smo2",
    "THb unfiltered": "THb",
    "THb": "THb",
}

# FIT file field mapping: fit_field_name -> output_column_name
FIT_FIELD_MAPPING = {
    "heart_rate": "heartrate",
    "cadence": "cadence",
    "fractional_cadence": "fractional_cadence",
    "enhanced_speed": "speed_m_s",
    "enhanced_altitude": "altitude",
    "distance": "distance",
    "position_lat": "lat",
    "position_long": "lng",
    "temperature": "temperature",
    "vertical_oscillation": "VerticalOscillation",
    "stance_time": "stance_time",
    "stance_time_percent": "stance_time_percent",
    "stance_time_balance": "stance_time_balance",
    "vertical_ratio": "vertical_ratio",
    "step_length": "step_length",
    "currHemoPerc": "smo2",
    "currO2Hb": "O2Hb",
    "currHHb": "HHb",
    "tyme_breath_rate": "TymeBreathRate",
    "tyme_tidal_volume": "tidal_volume",
    "tyme_minute_volume": "TymeVentilation",
}

# Key columns for head-trimming validation (ignore always-empty columns like torque)
HEAD_TRIM_KEY_COLUMNS = ["heartrate", "cadence", "velocity_smooth", "distance"]

SEMICIRCLE_TO_DEGREES = 180.0 / (2 ** 31)


# --- Utility functions ---

def find_header_row(
    path: Path, keywords: List[str], max_lines: int = 60
) -> Optional[int]:
    """Find the row index containing all keywords in a CSV file."""
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for i, line in enumerate(f):
                if i >= max_lines:
                    break
                line_lower = line.lower()
                if all(k.lower() in line_lower for k in keywords):
                    return i
    except (OSError, UnicodeDecodeError):
        pass
    return None


def _trim_leading_nan(df: pd.DataFrame, key_columns: List[str], limit: int = 30) -> pd.DataFrame:
    """
    Remove leading rows with NaN in KEY columns only.

    Unlike the old approach that checked ALL columns (causing torque/empty
    columns to trigger removal), this only checks activity-critical columns.
    """
    present_keys = [c for c in key_columns if c in df.columns]
    if not present_keys:
        return df

    head_n = min(limit, len(df))
    head_part = df.iloc[:head_n]
    idx_to_drop = head_part[head_part[present_keys].isna().any(axis=1)].index

    if len(idx_to_drop) > 0:
        df = df.drop(index=idx_to_drop).reset_index(drop=True)
        print(f"    -> Usunięto {len(idx_to_drop)} wierszy z początku (NaN w {present_keys})")

    return df


def _trim_trailing_incomplete(df: pd.DataFrame, anchor_columns: Optional[list[str]] = None) -> pd.DataFrame:
    """
    Trim trailing rows that don't have ALL values filled.

    Strict check: removes all consecutive rows from the end where ANY
    column has NaN/empty value. Prevents situations like 3000 rows of
    time/power/cadence but 3020 rows of SmO2/THb.
    """
    if df.empty:
        return df

    # Replace whitespace-only strings with NaN for consistent checking
    df_check = df.copy()
    obj_cols = df_check.select_dtypes(include=["object"]).columns
    if not obj_cols.empty:
        df_check[obj_cols] = df_check[obj_cols].replace(
            r"^\s*$", np.nan, regex=True
        )

    # Find rows where ALL columns have values (strict)
    if anchor_columns is not None:
        valid_cols = [c for c in anchor_columns if c in df_check.columns]
        if valid_cols:
            complete_mask = df_check[valid_cols].notna().all(axis=1)
        else:
            complete_mask = df_check.notna().all(axis=1)
    else:
        complete_mask = df_check.notna().all(axis=1)
    valid_positions = np.flatnonzero(complete_mask.values)

    if len(valid_positions) > 0:
        last_valid = valid_positions[-1]
        if last_valid < len(df) - 1:
            trimmed = len(df) - last_valid - 1
            df = df.iloc[:last_valid + 1].copy()
            print(f"    Przycięto {trimmed} niepełnych wierszy z końca (brak wartości w kolumnach bazowych)" if anchor_columns is not None else f"    Przycięto {trimmed} niepełnych wierszy z końca (brak wszystkich wartości)")
    else:
        print("    Uwaga: Brak wierszy z kompletnymi danymi w kolumnach bazowych!" if anchor_columns is not None else "    Uwaga: Brak wierszy z kompletnymi danymi we wszystkich kolumnach!")

    return df


# --- File type detection ---

def detect_file_type(filepath: Path) -> Optional[str]:
    """
    Detect the source type of a CSV file.

    Returns one of: 'intervals', 'wahoo', 'garmin', 'trainred',
    'tymewear', 'merged', or None.

    Key distinction:
      - 'intervals': Intervals.icu streams file — has hrv AND activity
        columns (cadence, watts, distance, velocity_smooth). This is
        a COMPLETE activity file and should be used as base.
      - 'garmin': Garmin watch sensor-only file — has hrv but NO
        activity columns. Only provides supplemental sensor data.
      - 'wahoo': Wahoo ELEMNT — has secs/watts but no hrv.
      - 'merged': Previously merged file — has columns from multiple
        sources (smo2 + heartrate, etc.). Used as additional data.
    """
    if filepath.suffix.lower() != ".csv":
        return None

    # Skip previously generated output files
    if filepath.name.startswith("Trening-") and filepath.name.endswith("-import.csv"):
        return None

    try:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            lines = [f.readline() for _ in range(60)]
            lines = [l for l in lines if l]

            content = "\n".join(lines).lower()
            first_line = lines[0].lower() if lines else ""

            # Strip BOM if present
            first_line = first_line.lstrip("\ufeff")

            header_fields = {f.strip() for f in first_line.split(",")}

            # Check for streams-like files (Intervals.icu, Wahoo, Garmin)
            # Must have time/secs AND at least one activity indicator OR
            # be named *streams.csv
            is_streams = (
                filepath.name.endswith("streams.csv")
                or "secs" in header_fields
                or first_line.startswith("time,watts")
                or ("time" in header_fields and bool(header_fields & ACTIVITY_INDICATOR_COLUMNS))
            )

            if is_streams:
                has_hrv = "hrv" in header_fields
                has_activity = bool(header_fields & ACTIVITY_INDICATOR_COLUMNS)

                if has_hrv and has_activity:
                    # Full activity stream from Intervals.icu
                    # Has both sensor (hrv) and activity (cadence, watts, etc.)
                    return "intervals"
                elif has_hrv and not has_activity:
                    # Garmin watch sensor-only export
                    return "garmin"
                elif "secs" in header_fields or "watts" in header_fields:
                    return "wahoo"

            # Previously merged file: has columns from multiple source types
            # Detect BEFORE individual sensor checks to avoid misclassification
            source_groups = 0
            if any(c in header_fields for c in ["smo2", "thb"]):
                source_groups += 1  # TrainRed data
            if any(c in header_fields for c in ["skin_temperature", "core_temperature", "heatstrainindex"]):
                source_groups += 1  # Garmin sensor data
            if any(c in header_fields for c in ["tymebreathrate", "tymeventilation"]):
                source_groups += 1  # Tymewear data
            if any(c in header_fields for c in ["heartrate", "altitude"]):
                source_groups += 1  # Base activity data

            if source_groups >= 2:
                return "merged"

            # TrainRed: SmO2 + THb in content (pure sensor file)
            if "smo2" in content and "thb" in content:
                return "trainred"

            # Tymewear: BR + VT + VE (pure sensor file)
            if all(col.lower() in content for col in ["br", "vt", "ve"]):
                return "tymewear"

    except (OSError, UnicodeDecodeError):
        pass

    return None


# --- File processors ---

def process_intervals(filepath: Path) -> pd.DataFrame:
    """
    Process an Intervals.icu streams CSV file.

    Keeps ALL columns — this is a complete activity file with all data
    from Garmin Connect merged by Intervals.icu. Includes cadence,
    heartrate, distance, velocity_smooth, VerticalOscillation, hrv, etc.

    Only performs:
      - BOM removal from column names
      - Leading NaN trimming on key activity columns
      - Trailing empty row removal
      - Dropping always-empty columns (e.g., torque if all NaN)
    """
    print(f"  [Intervals.icu] Przetwarzanie: {filepath.name}")

    df = pd.read_csv(filepath, encoding="utf-8-sig")
    df.columns = [str(c).strip() for c in df.columns]

    print(f"    -> {len(df)} wierszy, {len(df.columns)} kolumn: {list(df.columns)}")

    # Drop columns that are completely empty (e.g., torque)
    empty_cols = [c for c in df.columns if df[c].isna().all()]
    if empty_cols:
        df = df.drop(columns=empty_cols)
        print(f"    -> Usunięto puste kolumny: {empty_cols}")

    # Remove leading rows with NaN in KEY activity columns only
    df = _trim_leading_nan(df, HEAD_TRIM_KEY_COLUMNS)

    # Remove trailing empty rows
    df = _trim_trailing_incomplete(df)

    print(f"    -> Wynik: {len(df)} wierszy, {len(df.columns)} kolumn: {list(df.columns)}")
    return df


def process_merged(filepath: Path) -> pd.DataFrame:
    """
    Process a previously merged file (e.g., SubT).

    Keeps ALL columns — provides supplemental data not in the base.
    Duplicate columns are handled during merge (base wins).
    """
    print(f"  [Merged] Ładowanie: {filepath.name}")

    df = pd.read_csv(filepath, encoding="utf-8-sig")
    df.columns = [str(c).strip() for c in df.columns]

    # Drop completely empty columns
    empty_cols = [c for c in df.columns if df[c].isna().all()]
    if empty_cols:
        df = df.drop(columns=empty_cols)

    print(f"    -> {len(df)} wierszy, {len(df.columns)} kolumn: {list(df.columns)}")
    return df


def _compute_hrv_per_second(fitfile) -> Dict[int, float]:
    """Compute per-second HRV (RMSSD) from FIT HRV messages."""
    rr_intervals: List[float] = []
    for msg in fitfile.get_messages("hrv"):
        for field in msg.fields:
            if field.name == "time" and field.value is not None:
                for val in field.value:
                    if val is not None:
                        rr_intervals.append(val * 1000.0)  # s -> ms

    if len(rr_intervals) < 2:
        return {}

    # Assign each R-R interval to a cumulative second
    hrv_by_second: Dict[int, List[float]] = {}
    cumulative_ms = 0.0
    for i, rr in enumerate(rr_intervals):
        cumulative_ms += rr
        sec = int(cumulative_ms / 1000.0)
        if i > 0:
            diff_sq = (rr - rr_intervals[i - 1]) ** 2
            hrv_by_second.setdefault(sec, []).append(diff_sq)

    result: Dict[int, float] = {}
    for sec, diffs in hrv_by_second.items():
        rmssd = math.sqrt(sum(diffs) / len(diffs))
        result[sec] = round(rmssd, 2)
    return result


def process_fit(filepath: Path) -> pd.DataFrame:
    """Parse a Garmin .FIT file and return a per-second DataFrame with all available fields."""
    try:
        import fitparse
    except ImportError:
        print(f"  [FIT] Error: fitparse not installed. Run: pip3 install fitparse")
        return pd.DataFrame()

    print(f"  [FIT] Przetwarzanie: {filepath.name}")

    fitfile = fitparse.FitFile(str(filepath))
    records = list(fitfile.get_messages("record"))

    if not records:
        print(f"    -> Error: brak wiadomości record")
        return pd.DataFrame()

    # Extract per-record data
    rows: List[Dict[str, Any]] = []
    first_timestamp = None
    for record in records:
        row: Dict[str, Any] = {}
        for field in record.fields:
            if field.name == "timestamp":
                if first_timestamp is None:
                    first_timestamp = field.value
                row["_elapsed_sec"] = (field.value - first_timestamp).total_seconds()
            elif field.name in FIT_FIELD_MAPPING:
                value = field.value
                if value is None:
                    continue
                # Validate numeric fields before arithmetic
                if not isinstance(value, (int, float)):
                    try:
                        value = float(value)
                    except (ValueError, TypeError):
                        continue
                col_name = FIT_FIELD_MAPPING[field.name]
                # Convert semicircles to degrees for GPS
                if field.name in ("position_lat", "position_long"):
                    value = value * SEMICIRCLE_TO_DEGREES
                row[col_name] = value
        rows.append(row)

    df = pd.DataFrame(rows)

    if df.empty:
        print(f"    -> Error: brak danych")
        return pd.DataFrame()

    # Fix running cadence: Garmin stores half-cadence for running
    if "cadence" in df.columns:
        frac = df.get("fractional_cadence", 0)
        df["cadence"] = ((df["cadence"] + frac) * 2).round(0).astype(int)
    if "fractional_cadence" in df.columns:
        df = df.drop(columns=["fractional_cadence"])

    # Convert speed m/s -> km/h
    if "speed_m_s" in df.columns:
        df["velocity_smooth"] = (df["speed_m_s"] * 3.6).round(3)

    # Convert vertical oscillation from mm to cm
    if "VerticalOscillation" in df.columns:
        df["VerticalOscillation"] = (df["VerticalOscillation"] / 10.0).round(1)

    # Convert step_length from mm to m
    if "step_length" in df.columns:
        df["step_length"] = (df["step_length"] / 1000.0).round(3)

    # Add HRV data
    hrv_data = _compute_hrv_per_second(fitfile)
    if hrv_data:
        elapsed_secs = df["_elapsed_sec"].astype(int) if "_elapsed_sec" in df.columns else pd.Series(range(len(df)))
        df["hrv"] = elapsed_secs.map(hrv_data)
        print(f"    -> HRV: {len(hrv_data)} sekund z RMSSD")

    # Create time column (elapsed seconds) and drop internal column
    if "_elapsed_sec" in df.columns:
        df["time"] = df["_elapsed_sec"].astype(int)
        df = df.drop(columns=["_elapsed_sec"])

    # Remove leading rows with NaN in key columns
    df = _trim_leading_nan(df, HEAD_TRIM_KEY_COLUMNS)

    print(f"    -> {len(df)} wierszy, {len(df.columns)} kolumn: {list(df.columns)}")
    return df


def process_wahoo(filepath: Path) -> pd.DataFrame:
    """Load a Wahoo ELEMNT CSV file (used as-is as base)."""
    print(f"  [Wahoo] Ładowanie: {filepath.name}")
    df = pd.read_csv(filepath)
    print(f"    -> {len(df)} wierszy, {len(df.columns)} kolumn")
    return df


def process_trainred(filepath: Path) -> pd.DataFrame:
    """Process a TrainRed CSV file, extracting SmO2 and THb columns."""
    print(f"  [TrainRed] Przetwarzanie: {filepath.name}")

    header_idx = find_header_row(filepath, ["Timestamp", "SmO2"]) or find_header_row(
        filepath, ["SmO2", "THb"]
    )
    if header_idx is None:
        print(f"    -> Error: nie znaleziono nagłówka")
        return pd.DataFrame()

    df = pd.read_csv(filepath, skiprows=header_idx, engine="python")

    timestamp_col = next((c for c in df.columns if "timestamp" in str(c).lower()), None)
    if timestamp_col is None:
        print(f"    -> Error: brak kolumny Timestamp")
        return pd.DataFrame()

    df["_ts_float"] = pd.to_numeric(
        df[timestamp_col].astype(str).str.replace(",", ".", regex=False),
        errors="coerce",
    )
    df = df.dropna(subset=["_ts_float"])
    df["second"] = df["_ts_float"].astype(int)

    samples_per_sec = df.groupby("second").size().median()
    if samples_per_sec > 1:
        print(f"    -> Normalizacja {samples_per_sec:.0f}Hz -> 1Hz")
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        numeric_cols = [c for c in numeric_cols if c not in ["second", "_ts_float"]]
        df_agg = df.groupby("second")[numeric_cols].mean().reset_index()
    else:
        df_agg = df

    result_cols: Dict[str, pd.Series] = {}
    for col in df_agg.columns:
        col_lower = str(col).lower()
        if "smo2" in col_lower:
            result_cols["smo2"] = df_agg[col]
        elif "thb" in col_lower:
            result_cols["THb"] = df_agg[col]

    if not result_cols:
        print(f"    -> Error: brak kolumn SmO2/THb")
        return pd.DataFrame()

    df_out = pd.DataFrame(result_cols)
    print(f"    -> {len(df_out)} wierszy, kolumny: {list(df_out.columns)}")
    return df_out


def _find_tymewear_data_header(path: Path, max_lines: int = 60) -> Optional[int]:
    """
    Find the Tymewear data header row — must start with 'Time' and contain BR, VT, VE.

    This distinguishes the actual data header (e.g. 'Time,BR,VT,VE,...' with 34 columns)
    from summary tables that also contain BR/VT/VE keywords but have fewer columns.
    """
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for i, line in enumerate(f):
                if i >= max_lines:
                    break
                cols = [c.strip().lower() for c in line.split(",")]
                if len(cols) >= 4 and cols[0] == "time" and all(
                    k.lower() in cols for k in ["br", "vt", "ve"]
                ):
                    return i
    except (OSError, UnicodeDecodeError):
        pass
    return None


def process_tymewear(filepath: Path) -> pd.DataFrame:
    """Process a Tymewear CSV file, extracting BR/VT/VE columns."""
    print(f"  [Tymewear] Przetwarzanie: {filepath.name}")

    header_idx = _find_tymewear_data_header(filepath)
    if header_idx is None:
        # Fallback to simple keyword search
        header_idx = find_header_row(filepath, ["BR", "VT", "VE"])
    if header_idx is None:
        print(f"    -> Error: nie znaleziono nagłówka")
        return pd.DataFrame()

    # Read header row to check if next row is units (non-numeric)
    skip_rows = list(range(header_idx))
    try:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
        if header_idx + 1 < len(lines):
            next_line = lines[header_idx + 1]
            first_val = next_line.split(",")[0].strip()
            # If the row after header contains unit labels (e.g. "sec", "br/min"),
            # skip it — it's not data
            if first_val and not first_val.replace(".", "").replace("-", "").isdigit():
                skip_rows.append(header_idx + 1)
    except (OSError, UnicodeDecodeError):
        pass

    df = pd.read_csv(filepath, skiprows=skip_rows)
    df.columns = [str(c).strip() for c in df.columns]

    missing = [c for c in TYMEWEAR_COLUMNS if c not in df.columns]
    if missing:
        print(f"    -> Error: brakujące kolumny {missing}")
        return pd.DataFrame()

    df_out = df[TYMEWEAR_COLUMNS].copy()
    df_out = df_out.rename(columns=TYMEWEAR_MAPPING)

    for col in df_out.columns:
        df_out[col] = pd.to_numeric(df_out[col], errors="coerce")
    df_out = df_out.dropna(how="all")

    print(f"    -> {len(df_out)} wierszy, kolumny: {list(df_out.columns)}")
    return df_out


def process_garmin_sensor(filepath: Path) -> pd.DataFrame:
    """
    Process a Garmin watch sensor-only CSV file.

    Extracts only sensor columns: skin_temperature, HeatStrainIndex,
    core_temperature, hrv. Does NOT contain activity data.
    """
    print(f"  [Garmin sensor] Przetwarzanie: {filepath.name}")

    df = pd.read_csv(filepath, encoding="utf-8-sig")
    df.columns = [str(c).strip() for c in df.columns]

    present = [c for c in GARMIN_SENSOR_COLUMNS if c in df.columns]
    if not present:
        print(f"    -> Error: brak kolumn sensorowych {GARMIN_SENSOR_COLUMNS}")
        return pd.DataFrame()

    df_out = df[present].copy()
    df_out = df_out.replace(r"^\s*$", np.nan, regex=True)

    # Trim leading NaN rows (sensor warm-up period)
    df_out = _trim_leading_nan(df_out, present)

    print(f"    -> {len(df_out)} wierszy, kolumny: {list(df_out.columns)}")
    return df_out


# --- Merge logic ---

def merge_dataframes(
    base_df: pd.DataFrame, other_dfs: List[pd.DataFrame]
) -> pd.DataFrame:
    """
    Merge base DataFrame with additional DataFrames by column concatenation.

    TrainRed SmO2/THb data has priority: if TrainRed is present among other_dfs,
    those columns are stripped from the base and from non-TrainRed sources before merge.
    Duplicate columns are dropped (base version wins for non-priority columns).
    Trailing incomplete rows are trimmed (strict: all columns must have values).
    """
    if base_df.empty:
        return pd.DataFrame()

    # TrainRed priority columns (case-insensitive)
    _priority_lower = {'smo2', 'thb'}
    _non_priority_max_cols = 5

    def _is_trainred(df):
        cols_lower = {c.lower() for c in df.columns}
        return _priority_lower.issubset(cols_lower) and len(df.columns) <= _non_priority_max_cols

    def _strip_priority(df):
        cols = [c for c in df.columns if c.lower() in _priority_lower]
        return df.drop(columns=cols) if cols else df

    # Detect TrainRed among non-base DataFrames
    has_trainred = any(_is_trainred(df) for df in other_dfs if not df.empty)

    # If TrainRed is available, strip priority columns from base
    if has_trainred:
        stripped = _strip_priority(base_df)
        if len(stripped.columns) < len(base_df.columns):
            dropped = set(base_df.columns) - set(stripped.columns)
            print(f"    -> Priorytet TrainRed: usuwam {dropped} z bazy")
        base_df = stripped

    all_dfs = [base_df.reset_index(drop=True)]
    seen_columns = set(base_df.columns)

    for df in other_dfs:
        if df.empty:
            continue

        df_reset = df.reset_index(drop=True)

        # Strip priority cols from non-TrainRed sources
        if has_trainred and not _is_trainred(df_reset):
            stripped = _strip_priority(df_reset)
            if len(stripped.columns) < len(df_reset.columns):
                dropped = set(df_reset.columns) - set(stripped.columns)
                print(f"    -> Priorytet TrainRed: usuwam {dropped} z innych źródeł")
            df_reset = stripped

        duplicates = [col for col in df_reset.columns if col in seen_columns]
        if duplicates:
            print(f"    -> Pomijam duplikaty kolumn: {duplicates}")
            df_reset = df_reset.drop(columns=duplicates)

        if df_reset.empty or len(df_reset.columns) == 0:
            continue

        all_dfs.append(df_reset)
        seen_columns.update(df_reset.columns)

    print(f"\n  Łączenie {len(all_dfs)} DataFrame'ów...")
    df_merged = pd.concat(all_dfs, axis=1)

    # Trim trailing rows (strict: all columns must have values)
    df_merged = _trim_trailing_incomplete(df_merged, anchor_columns=list(base_df.columns))

    print(f"    Wynik łączenia: {len(df_merged)} wierszy, {len(df_merged.columns)} kolumn")
    return df_merged


# --- File discovery ---

def find_input_files(directory: Path) -> List[Path]:
    """Find all CSV and FIT files in a directory."""
    csv_files = list(directory.glob("*.csv"))
    fit_files = list(directory.glob("*.fit")) + list(directory.glob("*.FIT"))
    return sorted(set(csv_files + fit_files))


# --- Main entry point ---

def main():
    parser = argparse.ArgumentParser(
        description="MergeCSV - łączenie plików CSV z urządzeń treningowych.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("files", nargs="*", type=Path, help="Pliki CSV/FIT do połączenia")
    parser.add_argument("--output", "-o", type=Path, help="Ścieżka pliku wyjściowego")
    parser.add_argument("--verbose", "-v", action="store_true", help="Szczegółowy output")

    args = parser.parse_args()

    supported_extensions = {".csv", ".fit"}

    input_files = (
        [f for f in args.files if f.exists() and f.suffix.lower() in supported_extensions]
        if args.files
        else find_input_files(Path.cwd())
    )

    if not input_files:
        print("Error: Nie znaleziono plików CSV/FIT!")
        return 1

    print(f"\nZnaleziono {len(input_files)} plików wejściowych")

    files_by_type: Dict[str, List[Path]] = {
        "wahoo": [],
        "intervals": [],
        "garmin": [],
        "trainred": [],
        "tymewear": [],
        "merged": [],
        "fit": [],
        "unknown": [],
    }

    print("\nDetekcja typów plików:")
    for f in input_files:
        if f.suffix.lower() == ".fit":
            files_by_type["fit"].append(f)
            print(f"  {f.name} -> FIT (Garmin)")
        else:
            ftype = detect_file_type(f)
            if ftype:
                files_by_type[ftype].append(f)
                print(f"  {f.name} -> {ftype.upper()}")
            else:
                files_by_type["unknown"].append(f)
                print(f"  {f.name} -> NIEROZPOZNANY (pomijam)")

    # Determine base file priority: Wahoo > FIT > Intervals.icu > Garmin sensor > merged
    base_file = None
    base_type = None

    # Sort: prefer "real" Wahoo files (no Garmin extras) as base
    _garmin_cols = {"heat_strain_index", "heatstrainindex", "skin_temperature"}
    def _is_garmin_like(f):
        try:
            with open(f) as fh:
                header = fh.readline().lower().lstrip("\ufeff")
            return bool(_garmin_cols & {c.strip() for c in header.split(",")})
        except Exception:
            return True
    if files_by_type["wahoo"]:
        files_by_type["wahoo"].sort(key=_is_garmin_like)
        base_file = files_by_type["wahoo"][0]
        base_type = "wahoo"
        print(f"\nBaza: Wahoo ({base_file.name})")
    elif files_by_type["fit"]:
        base_file = files_by_type["fit"][0]
        base_type = "fit"
        print(f"\nBaza: FIT ({base_file.name})")
    elif files_by_type["intervals"]:
        base_file = files_by_type["intervals"][0]
        base_type = "intervals"
        print(f"\nBaza: Intervals.icu ({base_file.name})")
    elif files_by_type["garmin"]:
        base_file = files_by_type["garmin"][0]
        base_type = "garmin"
        print(f"\nBaza: Garmin sensor ({base_file.name}) - brak lepszego źródła")
    elif files_by_type["merged"]:
        base_file = files_by_type["merged"][0]
        base_type = "merged"
        print(f"\nBaza: Wcześniej zmergowany plik ({base_file.name})")

    if not base_file:
        print("\nError: Nie znaleziono pliku bazowego!")
        print("Potrzebny jest co najmniej jeden plik: Wahoo, FIT, Intervals.icu streams, lub Garmin.")
        return 1

    print("\n" + "=" * 60 + "\nPRZETWARZANIE PLIKÓW\n" + "=" * 60)

    # Process base file
    if base_type == "wahoo":
        base_df = process_wahoo(base_file)
    elif base_type == "fit":
        base_df = process_fit(base_file)
    elif base_type == "intervals":
        base_df = process_intervals(base_file)
    elif base_type == "garmin":
        # Garmin sensor as base — read ALL columns (fallback scenario)
        base_df = pd.read_csv(base_file, encoding="utf-8-sig")
        base_df.columns = [str(c).strip() for c in base_df.columns]
        empty_cols = [c for c in base_df.columns if base_df[c].isna().all()]
        if empty_cols:
            base_df = base_df.drop(columns=empty_cols)
        base_df = _trim_leading_nan(base_df, HEAD_TRIM_KEY_COLUMNS)
        print(f"  [Garmin base] {len(base_df)} wierszy, {len(base_df.columns)} kolumn")
    elif base_type == "merged":
        base_df = process_merged(base_file)
    else:
        base_df = pd.DataFrame()

    if base_df.empty:
        print("\nError: Plik bazowy jest pusty!")
        return 1

    # Merge additional Wahoo files (if Wahoo is base and there are extras)
    if base_type == "wahoo" and len(files_by_type["wahoo"]) > 1:
        for f in files_by_type["wahoo"][1:]:
            extra_df = process_wahoo(f)
            if extra_df.empty:
                continue
            new_cols = [c for c in extra_df.columns if c not in base_df.columns]
            if new_cols:
                print(f"    -> Dodaję kolumny z {f.name}: {new_cols}")
                extra_reset = extra_df[new_cols].reset_index(drop=True)
                base_df = pd.concat([base_df.reset_index(drop=True), extra_reset], axis=1)

    # Process all additional files
    other_dfs: List[pd.DataFrame] = []

    # Intervals.icu files (if not base)
    if base_type != "intervals":
        for f in files_by_type["intervals"]:
            df = process_intervals(f)
            if not df.empty:
                other_dfs.append(df)

    # TrainRed
    for f in files_by_type["trainred"]:
        df = process_trainred(f)
        if not df.empty:
            other_dfs.append(df)

    # Tymewear
    for f in files_by_type["tymewear"]:
        df = process_tymewear(f)
        if not df.empty:
            other_dfs.append(df)

    # Garmin sensor (if not base)
    if base_type != "garmin":
        for f in files_by_type["garmin"]:
            df = process_garmin_sensor(f)
            if not df.empty:
                other_dfs.append(df)

    # FIT files (if not base)
    if base_type == "fit" and len(files_by_type["fit"]) > 1:
        for f in files_by_type["fit"][1:]:
            df = process_fit(f)
            if not df.empty:
                other_dfs.append(df)
    elif base_type != "fit":
        for f in files_by_type["fit"]:
            df = process_fit(f)
            if not df.empty:
                other_dfs.append(df)

    # Previously merged files (if not base)
    if base_type != "merged":
        for f in files_by_type["merged"]:
            df = process_merged(f)
            if not df.empty:
                other_dfs.append(df)

    # HR priority: always from Wahoo when available
    if files_by_type["wahoo"]:
        strip_count = 0
        for i in range(len(other_dfs)):
            if "heartrate" in other_dfs[i].columns:
                other_dfs[i] = other_dfs[i].drop(columns=["heartrate"])
                strip_count += 1
        if strip_count > 0:
            print(f"  -> Priorytet HR: Wahoo zastępuje tętno z {strip_count} źródła/źródeł")

    print("\n" + "=" * 60 + "\nŁĄCZENIE DANYCH\n" + "=" * 60)
    df_merged = merge_dataframes(base_df, other_dfs)

    if df_merged.empty:
        print("\nError: Wynik łączenia jest pusty!")
        return 1

    # Normalize time column to start at 0
    for time_col in ("time", "secs"):
        if time_col in df_merged.columns:
            first_val = df_merged[time_col].iloc[0]
            if pd.notna(first_val) and first_val != 0:
                df_merged[time_col] = df_merged[time_col] - first_val
                print(f"  Znormalizowano '{time_col}' do startu od 0 (było {first_val})")

    # Determine output path
    if args.output:
        output_path = args.output
    else:
        today = datetime.now().strftime("%d.%m.%Y")
        output_filename = f"Trening-{today}-import.csv"

        if args.files:
            output_dir = args.files[0].parent
        elif base_file:
            output_dir = base_file.parent
        else:
            output_dir = Path.cwd()

        output_path = output_dir / output_filename

    df_merged.to_csv(output_path, index=False)

    print("\n" + "=" * 60 + "\nWYNIK\n" + "=" * 60)
    print(f"  Plik: {output_path}")
    print(f"  Wiersze: {len(df_merged)}")
    print(f"  Kolumny ({len(df_merged.columns)}): {list(df_merged.columns)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
