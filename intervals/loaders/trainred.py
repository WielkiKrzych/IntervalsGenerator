"""
TrainRed data source loader.

Handles SmO2 and THb data from TrainRed muscle oxygen sensors.

Data Contract:
    Input:  10 Hz sampling rate, CSV with 'Timestamp (seconds passed)', 'SmO2', 'THb unfiltered'
    Output: 1 Hz normalized, CSV with 'smo2', 'THb' columns

Required Input Columns:
    - Timestamp (seconds passed): float - Time from start (0.0, 0.1, 0.2, ...)
    - SmO2: float - Muscle oxygen saturation (%)
    - THb unfiltered: float - Total hemoglobin (g/dL)

Optional Input Columns:
    - Device: str - Sensor identifier

Output Columns:
    - smo2: float - Renamed from SmO2
    - THb: float - Renamed from 'THb unfiltered'

Processing Steps:
    1. Find header row (may be preceded by metadata)
    2. Normalize 10Hz → 1Hz using mean aggregation
    3. Extract and rename required columns
    4. Move processed files to archive

Failure Modes:
    - ValueError if required columns are missing
    - Returns None from _normalize_to_1hz if parsing fails
    - Skips files with missing data
"""

from pathlib import Path
from typing import List, Optional, ClassVar, Dict, Any
import logging
import pandas as pd
import numpy as np

from .base import BaseLoader
from .registry import LoaderRegistry
from ..interfaces import (
    FileSystemOperations,
    UserInterface,
    LoaderSpec,
    LoaderColumnSpec,
    ValidationResult,
)
from ..config import Config
from ..exceptions import FileFormatError, MissingColumnError, IntervalsValidationError
from ..utils import find_header_row


logger = logging.getLogger(__name__)


@LoaderRegistry.register(
    "trainred",
    priority=10,
    description="TrainRed muscle oxygen sensor - SmO2, THb (10Hz → 1Hz)",
    file_patterns=["session_*.csv"],
)
class TrainRedLoader(BaseLoader):
    """
    Loader for TrainRed muscle oxygen sensor data.

    Attributes:
        LOADER_SPEC: Class-level specification of column requirements
        REQUIRED_COLUMNS: List of columns that must be present in raw data
        OUTPUT_COLUMNS: List of columns in clean output
        COLUMN_MAPPING: Dict mapping source → output column names

    Detection:
        - Filename contains 'trainred' (case-insensitive)
        - File extension is .csv

    Normalization:
        - Input: 10 samples per second (0.1s intervals)
        - Output: 1 sample per second (mean of 10 samples)
        - Aggregation: mean() for numeric, first() for categorical
    """

    # Class-level constants for column handling
    REQUIRED_COLUMNS: ClassVar[List[str]] = ["SmO2", "THb unfiltered"]
    OPTIONAL_COLUMNS: ClassVar[List[str]] = ["Device"]
    OUTPUT_COLUMNS: ClassVar[List[str]] = ["smo2", "THb"]

    COLUMN_MAPPING: ClassVar[Dict[str, str]] = {"SmO2": "smo2", "THb unfiltered": "THb"}

    # Loader specification for interface contract
    LOADER_SPEC: ClassVar[LoaderSpec] = LoaderSpec(
        name="TrainRed",
        priority=10,
        detection_method="filename",
        file_pattern="session_*.csv",
        input_frequency=10,
        output_frequency=1,
        required_columns=[
            LoaderColumnSpec(
                name="Muscle Oxygen Saturation",
                source_name="SmO2",
                output_name="smo2",
                dtype="float64",
                required=True,
                fallback=0.0,
            ),
            LoaderColumnSpec(
                name="Total Hemoglobin",
                source_name="THb unfiltered",
                output_name="THb",
                dtype="float64",
                required=True,
                fallback=0.0,
            ),
        ],
        optional_columns=[
            LoaderColumnSpec(
                name="Device Identifier",
                source_name="Device",
                output_name="Device",
                dtype="object",
                required=False,
                fallback=None,
            ),
        ],
        column_mapping={"SmO2": "smo2", "THb unfiltered": "THb"},
    )

    def __init__(
        self, config: Config, fs: FileSystemOperations, ui: UserInterface
    ) -> None:
        """
        Initialize TrainRed loader.

        Args:
            config: Application configuration
            fs: Filesystem operations interface
            ui: User interface for messages
        """
        super().__init__(config, fs, ui)

    def detect_in_downloads(self, filepath: Path) -> bool:
        """
        Check if file is a TrainRed CSV by checking for SmO2 and THb columns.
        """
        if filepath.suffix.lower() != ".csv":
            return False

        # Content-based check using shared utility
        try:
            return (
                find_header_row(
                    filepath,
                    ["SmO2", "THb"],
                    max_lines=self.config.HEADER_SCAN_MAX_LINES,
                )
                is not None
            )
        except Exception as e:
            logger.debug(f"Błąd odczytu przy detekcji {filepath.name}: {e}")

        return False

        # Content-based check: scan first 60 lines for SmO2 and THb columns
        # TrainRed files have ~40 lines of metadata before the header row
        try:
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                for i, line in enumerate(f):
                    if i >= 60:
                        break
                    line_upper = line.upper()
                    # Look for SmO2 and THb in the same line (header row)
                    if 'SMO2' in line_upper and ('THB' in line_upper):

                        return True
        except (OSError, UnicodeDecodeError) as e:
            logger.debug(f"Błąd odczytu przy detekcji {filepath.name}: {e}")

        return False

        if not imported:
            self.ui.print_warning(
                "Nie znaleziono pliku TrainRed (CSV z kolumnami SmO2 i THb) w folderze Downloads."
            )

        return imported

    def _normalize_to_1hz(self, path: Path) -> Optional[pd.DataFrame]:
        """
        Normalize high-frequency TrainRed data to 1Hz (1 sample per second).
        """
        header_idx: Optional[int] = find_header_row(
            path, ["Timestamp", "seconds passed"]
        )
        if header_idx is None:
            header_idx = find_header_row(path, ["Timestamp"])
        if header_idx is None:
            return None

        # Try to read CSV with fallback for malformed lines
        try:
            df: pd.DataFrame = pd.read_csv(path, engine="python", skiprows=header_idx)
        except (pd.errors.ParserError, pd.errors.EmptyDataError) as e:
            logger.warning(
                f"Błąd parsowania {path.name}, próbuję z pomijaniem błędnych linii: {e}"
            )
            try:
                df = pd.read_csv(
                    path, engine="python", skiprows=header_idx, on_bad_lines="skip"
                )
            except Exception as e2:
                logger.error(f"Krytyczny błąd parsowania {path.name}: {e2}")
                raise FileFormatError(
                    reason="parse_error", file_path=str(path), details=str(e2)
                )
        except OSError as e:
            logger.error(f"Błąd wejścia/wyjścia przy odczycie {path.name}: {e}")
            raise FileFormatError(
                reason="read_error", file_path=str(path), details=str(e)
            )

        # Find timestamp column
        timestamp_col: Optional[str] = None
        for c in df.columns:
            if 'timestamp' in str(c).lower():

                timestamp_col = c
                break

        if timestamp_col is None:
            return None

        # Convert timestamp to float (handle comma as decimal separator)
        df["_ts_float"] = pd.to_numeric(
            df[timestamp_col].astype(str).str.replace(",", ".", regex=False),
            errors="coerce",
        )
        df = df.dropna(subset=["_ts_float"])
        df["second"] = df["_ts_float"].astype(int)

        # Separate numeric and non-numeric columns
        numeric_cols: List[str] = df.select_dtypes(include=[np.number]).columns.tolist()
        numeric_cols = [c for c in numeric_cols if c not in ["second", "_ts_float"]]
        non_numeric_cols: List[str] = [
            c for c in df.columns if c not in numeric_cols + ["second", "_ts_float"]
        ]

        if not numeric_cols:
            return None

        # Aggregate by second
        def aggregate_group(group: pd.DataFrame) -> pd.Series:
            result: Dict[str, Any] = {}
            # Numeric columns: mean
            for col in numeric_cols:
                result[col] = group[col].mean()
            # Non-numeric columns: first valid value
            for col in non_numeric_cols:
                valid = group[col].dropna()
                result[col] = str(valid.iloc[0]) if len(valid) > 0 else ""
            # Add sample count for diagnostics
            result["samples_per_second"] = len(group)
            return pd.Series(result)

        return (
            df.groupby("second")
            .apply(aggregate_group, include_groups=False)
            .reset_index()
        )

    def process_files(self) -> List[Path]:
        """
        Process TrainRed files in two stages.
        """

        self.fs.mkdir(self.source_dir)
        self.fs.mkdir(self.old_dir)

        # Stage 1: Normalize to 1Hz
        # Look for session_*.csv files (TrainRed export format) or any CSV that isn't processed yet
        all_csvs: List[Path] = self.fs.glob(self.source_dir, "*.csv")
        session_files = [
            f for f in all_csvs if "_avg" not in f.name and "_clean" not in f.name
        ]

        self.ui.print_message(f"\n🔁 Normalizacja TrainRed do 1 Hz")
        self.ui.print_message(
            f"   Znaleziono {len(session_files)} plików CSV do przetworzenia"
        )

        avg_files: List[Path] = []
        for path in session_files:
            df_normalized: Optional[pd.DataFrame] = self._normalize_to_1hz(path)
            if df_normalized is not None:
                out_fname: Path = self.source_dir / (path.stem + "_avg.csv")
                self.fs.write_csv(df_normalized, out_fname, index=False)
                avg_files.append(out_fname)
                self.ui.print_success(
                    f"{out_fname.name} (wiersze: {len(df_normalized)})"
                )

                # Move original to archive
                try:
                    self.fs.move(path, self.old_dir / path.name)
                except OSError as e:
                    logger.warning(f"Błąd przenoszenia {path.name} do archiwum: {e}")
            else:
                self.ui.print_error(
                    f"{path.name}: nie można przetworzyć (brak Timestamp lub danych)"
                )

        # Stage 2: Extract SmO2 and THb
        self.ui.print_message(f"\n🧪 Ekstrakcja smo2 i THb z plików *_avg.csv")

        clean_files: List[Path] = []
        for path in avg_files:
            try:
                df: pd.DataFrame = self.fs.read_csv(path)
            except Exception as e:
                self.ui.print_error(f"{path.name}: błąd odczytu ({e})")
                continue

            # Check for available columns (flexible THb handling)
            current_cols = set(df.columns)

            # Determine THb column name
            thb_col = None
            if "THb unfiltered" in current_cols:
                thb_col = "THb unfiltered"
            elif "THb" in current_cols:
                thb_col = "THb"

            if "SmO2" not in current_cols or thb_col is None:
                self.ui.print_error(
                    f"{path.name}: brak wymaganych kolumn (SmO2, THb/THb unfiltered)"
                )
                continue

            # Extract and rename
            df_out: pd.DataFrame = df[["SmO2", thb_col]].copy()
            df_out = df_out.rename(columns={"SmO2": "smo2", thb_col: "THb"})

            out_clean: Path = self.source_dir / (path.stem + "_clean.csv")
            self.fs.write_csv(df_out, out_clean, index=False)
            clean_files.append(out_clean)
            self.ui.print_success(
                f"{out_clean.name} (kolumny: {', '.join(self.OUTPUT_COLUMNS)})"
            )

            # Move avg to archive
            try:
                self.fs.move(path, self.old_dir / path.name)
            except OSError as e:
                logger.warning(f"Błąd przenoszenia pliku tymczasowego {path.name}: {e}")

        return clean_files

    def get_clean_files(self) -> List[Path]:
        """
        Get list of clean TrainRed files ready for merging.
        """
        return self.fs.glob(self.source_dir, "*_clean.csv")
