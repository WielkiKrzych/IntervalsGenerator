#!/usr/bin/env python3

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any
import pandas as pd
import numpy as np

TRAINRED_COLUMNS = ["SmO2", "THb"]
TYMEWEAR_COLUMNS = ["BR", "VT", "VE"]
GARMIN_COLUMNS = ["skin_temperature", "HeatStrainIndex", "hrv"]

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

    try:
        with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
            lines = [f.readline() for _ in range(60)]
            lines = [l for l in lines if l]

            content = "\n".join(lines).lower()
            first_line = lines[0].lower() if lines else ""

            if filepath.name.endswith("streams.csv") or "secs" in first_line:
                if "hrv" in first_line:
                    return "garmin"
                elif "secs" in first_line or "watts" in first_line:
                    return "wahoo"

            if "smo2" in content and "thb" in content:
                return "trainred"

            if all(col.lower() in content for col in ["br", "vt", "ve"]):
                return "tymewear"

    except (OSError, UnicodeDecodeError):
        pass

    return None


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


def process_garmin(filepath: Path) -> pd.DataFrame:
    print(f"  [Garmin] Processing: {filepath.name}")

    df = pd.read_csv(filepath)
    df.columns = [str(c).strip() for c in df.columns]

    present = [c for c in GARMIN_COLUMNS if c in df.columns]
    if not present:
        print(f"    -> Error: missing columns {GARMIN_COLUMNS}")
        return pd.DataFrame()

    df_out = df[present].copy()
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


def find_csv_files(directory: Path) -> List[Path]:
    return sorted(directory.glob("*.csv"))


def main():
    parser = argparse.ArgumentParser(
        description="Prosty skrypt do laczenia plikow CSV z urzadzen treningowych.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("files", nargs="*", type=Path, help="CSV files to merge")
    parser.add_argument("--output", "-o", type=Path, help="Output file path")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    args = parser.parse_args()

    csv_files = (
        [f for f in args.files if f.exists() and f.suffix.lower() == ".csv"]
        if args.files
        else find_csv_files(Path.cwd())
    )

    if not csv_files:
        print("Error: No CSV files found!")
        return 1

    print(f"\nFound {len(csv_files)} CSV files")

    files_by_type = {
        "wahoo": [],
        "garmin": [],
        "trainred": [],
        "tymewear": [],
        "unknown": [],
    }

    print("\nFile type detection:")
    for f in csv_files:
        ftype = detect_file_type(f)
        if ftype:
            files_by_type[ftype].append(f)
            print(f"  {f.name} -> {ftype.upper()}")
        else:
            files_by_type["unknown"].append(f)

    if not files_by_type["wahoo"]:
        print("\nError: Wahoo base file missing!")
        return 1

    print("\n" + "=" * 60 + "\nPROCESSING FILES\n" + "=" * 60)

    base_df = process_wahoo(files_by_type["wahoo"][0])
    if base_df.empty:
        return 1

    other_dfs = []
    for f in files_by_type["trainred"]:
        df = process_trainred(f)
        if not df.empty:
            other_dfs.append(df)
    for f in files_by_type["tymewear"]:
        df = process_tymewear(f)
        if not df.empty:
            other_dfs.append(df)
    for f in files_by_type["garmin"]:
        df = process_garmin(f)
        if not df.empty:
            other_dfs.append(df)

    print("\n" + "=" * 60 + "\nMERGING DATA\n" + "=" * 60)
    df_merged = merge_dataframes(base_df, other_dfs)

    if df_merged.empty:
        return 1

    if args.output:
        output_path = args.output
    else:
        today = datetime.now().strftime("%d.%m.%Y")
        output_filename = f"Trening-{today}-import.csv"

        if args.files:
            output_dir = args.files[0].parent
        elif files_by_type["wahoo"]:
            output_dir = files_by_type["wahoo"][0].parent
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
