#!/usr/bin/env python3

import argparse
import math
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any
import pandas as pd
import numpy as np

TRAINRED_COLUMNS = ["SmO2", "THb"]
TYMEWEAR_COLUMNS = ["BR", "VT", "VE"]
GARMIN_COLUMNS = ["skin_temperature", "HeatStrainIndex", "core_temperature", "hrv"]

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

SEMICIRCLE_TO_DEGREES = 180.0 / (2 ** 31)


def find_header_row(
    path: Path, keywords: List[str], max_lines: int = 60
) -> Optional[int]:
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


def detect_file_type(filepath: Path) -> Optional[str]:
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

            if filepath.name.endswith("streams.csv") or "secs" in first_line or "time,watts" in first_line:
                if "hrv" in first_line:
                    return "garmin"
                elif "secs" in first_line or "watts" in first_line or "time,watts" in first_line:
                    return "wahoo"

            if "smo2" in content and "thb" in content:
                return "trainred"

            if all(col.lower() in content for col in ["br", "vt", "ve"]):
                return "tymewear"

    except (OSError, UnicodeDecodeError):
        pass

    return None


def _compute_hrv_per_second(fitfile) -> Dict[int, float]:
    """Compute per-second HRV (RMSSD) from FIT HRV messages."""
    rr_intervals = []
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

    result = {}
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

    print(f"  [FIT] Processing: {filepath.name}")

    fitfile = fitparse.FitFile(str(filepath))
    records = list(fitfile.get_messages("record"))

    if not records:
        print(f"    -> Error: no record messages found")
        return pd.DataFrame()

    # Extract per-record data
    rows = []
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
        print(f"    -> Error: no data extracted")
        return pd.DataFrame()

    # Fix running cadence: Garmin stores half-cadence for running
    if "cadence" in df.columns:
        frac = df.get("fractional_cadence", 0)
        df["cadence"] = ((df["cadence"] + frac) * 2).round(0).astype(int)
    if "fractional_cadence" in df.columns:
        df = df.drop(columns=["fractional_cadence"])

    # Convert speed m/s -> pace-friendly km/h
    if "speed_m_s" in df.columns:
        df["velocity_smooth"] = (df["speed_m_s"] * 3.6).round(3)

    # Convert vertical oscillation from mm to cm for readability
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
        print(f"    -> HRV data: {len(hrv_data)} seconds with RMSSD values")

    # Create time column (elapsed seconds) and drop internal column
    if "_elapsed_sec" in df.columns:
        df["time"] = df["_elapsed_sec"].astype(int)
        df = df.drop(columns=["_elapsed_sec"])

    # Remove leading rows with NaN in key columns
    head_n = min(30, len(df))
    head_part = df.iloc[:head_n]
    key_cols = [c for c in ["heartrate", "cadence", "speed_m_s"] if c in df.columns]
    if key_cols:
        idx_to_drop = head_part[head_part[key_cols].isna().any(axis=1)].index
        if len(idx_to_drop) > 0:
            df = df.drop(index=idx_to_drop).reset_index(drop=True)
            print(f"    -> Removed {len(idx_to_drop)} rows from start (NaN)")

    print(f"    -> {len(df)} rows, {len(df.columns)} columns: {list(df.columns)}")
    return df


def process_wahoo(filepath: Path) -> pd.DataFrame:
    print(f"  [Wahoo] Loading: {filepath.name}")
    df = pd.read_csv(filepath)
    print(f"    -> {len(df)} rows, {len(df.columns)} columns")
    return df


def process_trainred(filepath: Path) -> pd.DataFrame:
    print(f"  [TrainRed] Processing: {filepath.name}")

    header_idx = find_header_row(filepath, ["Timestamp", "SmO2"]) or find_header_row(
        filepath, ["SmO2", "THb"]
    )
    if header_idx is None:
        print(f"    -> Error: header not found")
        return pd.DataFrame()

    df = pd.read_csv(filepath, skiprows=header_idx, engine="python")

    timestamp_col = next((c for c in df.columns if "timestamp" in str(c).lower()), None)
    if timestamp_col is None:
        print(f"    -> Error: Timestamp column missing")
        return pd.DataFrame()

    df["_ts_float"] = pd.to_numeric(
        df[timestamp_col].astype(str).str.replace(",", ".", regex=False),
        errors="coerce",
    )
    df = df.dropna(subset=["_ts_float"])
    df["second"] = df["_ts_float"].astype(int)

    samples_per_sec = df.groupby("second").size().median()
    if samples_per_sec > 1:
        print(f"    -> Normalizing {samples_per_sec:.0f}Hz -> 1Hz")
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        numeric_cols = [c for c in numeric_cols if c not in ["second", "_ts_float"]]
        df_agg = df.groupby("second")[numeric_cols].mean().reset_index()
    else:
        df_agg = df

    result_cols = {}
    for col in df_agg.columns:
        col_lower = str(col).lower()
        if "smo2" in col_lower:
            result_cols["smo2"] = df_agg[col]
        elif "thb" in col_lower:
            result_cols["THb"] = df_agg[col]

    if not result_cols:
        print(f"    -> Error: SmO2/THb columns missing")
        return pd.DataFrame()

    df_out = pd.DataFrame(result_cols)
    print(f"    -> {len(df_out)} rows, columns: {list(df_out.columns)}")
    return df_out


def process_tymewear(filepath: Path) -> pd.DataFrame:
    print(f"  [Tymewear] Processing: {filepath.name}")

    header_idx = find_header_row(filepath, ["BR", "VT", "VE"])
    if header_idx is None:
        print(f"    -> Error: header not found")
        return pd.DataFrame()

    df = pd.read_csv(filepath, skiprows=header_idx)
    df.columns = [str(c).strip() for c in df.columns]

    missing = [c for c in TYMEWEAR_COLUMNS if c not in df.columns]
    if missing:
        print(f"    -> Error: missing columns {missing}")
        return pd.DataFrame()

    df_out = df[TYMEWEAR_COLUMNS].copy()
    df_out = df_out.rename(columns=TYMEWEAR_MAPPING)

    for col in df_out.columns:
        df_out[col] = pd.to_numeric(df_out[col], errors="coerce")
    df_out = df_out.dropna(how="all")

    print(f"    -> {len(df_out)} rows, columns: {list(df_out.columns)}")
    return df_out


def process_garmin(filepath: Path, include_secs: bool = True, keep_all_columns: bool = False) -> pd.DataFrame:
    print(f"  [Garmin] Processing: {filepath.name}")

    df = pd.read_csv(filepath)
    df.columns = [str(c).strip() for c in df.columns]

    present = [c for c in GARMIN_COLUMNS if c in df.columns]
    if not present:
        print(f"    -> Error: missing columns {GARMIN_COLUMNS}")
        return pd.DataFrame()

    if keep_all_columns:
        # When Garmin is the base, keep ALL columns from the original file
        # This preserves watts, cadence, heartrate, etc.
        df_out = df.copy()
    else:
        # When Garmin is an additional file, keep only GARMIN_COLUMNS
        cols_to_keep = present.copy()
        if include_secs and "secs" in df.columns and "secs" not in cols_to_keep:
            cols_to_keep.insert(0, "secs")
        df_out = df[cols_to_keep].copy()

    df_out = df_out.replace(r"^\s*$", np.nan, regex=True)

    head_n = min(30, len(df_out))
    head_part = df_out.iloc[:head_n]
    idx_to_drop = head_part[head_part.isna().any(axis=1)].index

    if len(idx_to_drop) > 0:
        df_out = df_out.drop(index=idx_to_drop)
        print(f"    -> Removed {len(idx_to_drop)} rows from start (NaN)")

    df_out = df_out.reset_index(drop=True)
    print(f"    -> {len(df_out)} rows, columns: {list(df_out.columns)}")
    return df_out


def merge_dataframes(
    base_df: pd.DataFrame, other_dfs: List[pd.DataFrame]
) -> pd.DataFrame:
    if base_df.empty:
        return pd.DataFrame()

    all_dfs = [base_df.reset_index(drop=True)]
    seen_columns = set(base_df.columns)

    for df in other_dfs:
        if df.empty:
            continue

        df_reset = df.reset_index(drop=True)
        duplicates = [col for col in df_reset.columns if col in seen_columns]
        if duplicates:
            df_reset = df_reset.drop(columns=duplicates)

        if df_reset.empty or len(df_reset.columns) == 0:
            continue

        all_dfs.append(df_reset)
        seen_columns.update(df_reset.columns)

    print(f"\n  Merging {len(all_dfs)} DataFrames...")
    df_merged = pd.concat(all_dfs, axis=1)

    mask = df_merged.notna().all(axis=1)
    valid_positions = np.flatnonzero(mask)

    if len(valid_positions) > 0:
        last_valid_pos = valid_positions[-1]
        df_merged = df_merged.iloc[: last_valid_pos + 1].copy()
        print(f"    Trimmed to last complete row: {len(df_merged)} rows")
    else:
        print("    Warning: No rows are fully complete!")

    return df_merged


def find_input_files(directory: Path) -> List[Path]:
    csv_files = list(directory.glob("*.csv"))
    fit_files = list(directory.glob("*.fit")) + list(directory.glob("*.FIT"))
    return sorted(set(csv_files + fit_files))


def main():
    parser = argparse.ArgumentParser(
        description="Prosty skrypt do laczenia plikow CSV z urzadzen treningowych.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("files", nargs="*", type=Path, help="CSV/FIT files to merge")
    parser.add_argument("--output", "-o", type=Path, help="Output file path")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    args = parser.parse_args()

    supported_extensions = {".csv", ".fit"}

    input_files = (
        [f for f in args.files if f.exists() and f.suffix.lower() in supported_extensions]
        if args.files
        else find_input_files(Path.cwd())
    )

    if not input_files:
        print("Error: No CSV/FIT files found!")
        return 1

    print(f"\nFound {len(input_files)} input files")

    files_by_type = {
        "wahoo": [],
        "garmin": [],
        "trainred": [],
        "tymewear": [],
        "fit": [],
        "unknown": [],
    }

    print("\nFile type detection:")
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

    # Determine base file priority: Wahoo CSV > FIT > Garmin CSV
    base_file = None
    base_type = None

    if files_by_type["wahoo"]:
        base_file = files_by_type["wahoo"][0]
        base_type = "wahoo"
        print(f"\nBase: Wahoo ({base_file.name})")
    elif files_by_type["fit"]:
        base_file = files_by_type["fit"][0]
        base_type = "fit"
        print(f"\nBase: FIT ({base_file.name})")
    elif files_by_type["garmin"]:
        base_file = files_by_type["garmin"][0]
        base_type = "garmin"
        print(f"\nBase: Garmin ({base_file.name}) - no Wahoo/FIT found")

    if not base_file:
        print("\nError: No base file found (Wahoo, FIT, or Garmin required)!")
        return 1

    print("\n" + "=" * 60 + "\nPROCESSING FILES\n" + "=" * 60)

    if base_type == "wahoo":
        base_df = process_wahoo(base_file)
    elif base_type == "fit":
        base_df = process_fit(base_file)
    else:
        base_df = process_garmin(base_file, keep_all_columns=True)

    if base_df.empty:
        return 1

    # Merge additional wahoo files into the base (fills missing columns like heartrate)
    if base_type == "wahoo" and len(files_by_type["wahoo"]) > 1:
        for f in files_by_type["wahoo"][1:]:
            extra_df = process_wahoo(f)
            if extra_df.empty:
                continue
            new_cols = [c for c in extra_df.columns if c not in base_df.columns]
            if new_cols:
                print(f"    -> Adding columns from {f.name}: {new_cols}")
                extra_reset = extra_df[new_cols].reset_index(drop=True)
                base_df = pd.concat([base_df.reset_index(drop=True), extra_reset], axis=1)

    other_dfs = []
    for f in files_by_type["trainred"]:
        df = process_trainred(f)
        if not df.empty:
            other_dfs.append(df)
    for f in files_by_type["tymewear"]:
        df = process_tymewear(f)
        if not df.empty:
            other_dfs.append(df)
    # Process Garmin CSV files only if they're not the base
    if base_type != "garmin":
        for f in files_by_type["garmin"]:
            df = process_garmin(f, include_secs=False)
            if not df.empty:
                other_dfs.append(df)
    # Process additional FIT files (not the base)
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

    print("\n" + "=" * 60 + "\nMERGING DATA\n" + "=" * 60)
    df_merged = merge_dataframes(base_df, other_dfs)

    if df_merged.empty:
        return 1

    # Normalize time column to start at 0 (required by Intervals.icu)
    for time_col in ("time", "secs"):
        if time_col in df_merged.columns:
            first_val = df_merged[time_col].iloc[0]
            if pd.notna(first_val) and first_val != 0:
                df_merged[time_col] = df_merged[time_col] - first_val
                print(f"  Adjusted '{time_col}' to start at 0 (was {first_val})")

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

    print("\n" + "=" * 60 + "\nRESULT\n" + "=" * 60)
    print(
        f"  File: {output_path}\n  Rows: {len(df_merged)}\n  Cols: {len(df_merged.columns)}"
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
