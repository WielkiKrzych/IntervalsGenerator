"""
Garmin data source loader.

Handles Garmin smartwatch data with skin temperature, heat strain, and HRV.

Data Contract:
    Input:  1 Hz sampling rate, *streams.csv file WITH 'hrv' column
    Output: CSV with skin_temperature, HeatStrainIndex, hrv (whichever present)

Optional Input Columns (extracted if present):
    - skin_temperature: float - Skin temperature (°C)
    - HeatStrainIndex: float - Heat strain index (0-1)
    - hrv: int - Heart rate variability (ms)

Processing Steps:
    1. Detect streams.csv with 'hrv' column (distinguishes from Wahoo)
    2. Extract available columns from wanted list
    3. Remove leading NaN rows (up to 30)
    4. Save as *_clean.csv

Detection:
    Distinguishes from Wahoo by presence of 'hrv' column in header.
    Both use *streams.csv file pattern.

Failure Modes:
    - Skips files with no wanted columns
    - Logs warning for missing columns
"""

from pathlib import Path
from typing import List, ClassVar
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
from ..exceptions import FileFormatError


logger = logging.getLogger(__name__)


@LoaderRegistry.register(
    "garmin",
    priority=30,
    description="Garmin smartwatch - skin_temperature, HeatStrainIndex, hrv",
    file_patterns=["*streams.csv"],
)
class GarminLoader(BaseLoader):
    """
    Loader for Garmin smartwatch data.

    Attributes:
        LOADER_SPEC: Class-level specification of column requirements
        WANTED_COLUMNS: Columns to extract if present
        LEADING_NAN_LIMIT: Max rows to check for leading NaN removal

    Detection:
        - Filename ends with 'streams.csv'
        - First line contains 'hrv' column

    Note:
        All columns are optional - extracts whichever are present.
        File is skipped if no wanted columns exist.
    """

    # Class-level constants
    WANTED_COLUMNS: ClassVar[List[str]] = [
        'skin_temperature', 'HeatStrainIndex', 'core_temperature', 'hrv'
    ]

    LEADING_NAN_LIMIT: ClassVar[int] = 30

    # Loader specification for interface contract
    LOADER_SPEC: ClassVar[LoaderSpec] = LoaderSpec(
        name="Garmin",
        priority=30,
        detection_method="header_presence",
        file_pattern="*streams.csv",
        input_frequency=1,
        output_frequency=1,
        required_columns=[],  # All optional
        optional_columns=[
            LoaderColumnSpec(
                name="Skin Temperature",
                source_name="skin_temperature",
                output_name="skin_temperature",
                dtype="float64",
                required=False,
                fallback=None,
            ),
            LoaderColumnSpec(
                name="Heat Strain Index",
                source_name="HeatStrainIndex",
                output_name="HeatStrainIndex",
                dtype="float64",
                required=False,
                fallback=0.0
            ),
            LoaderColumnSpec(
                name="Core Temperature",
                source_name="core_temperature",
                output_name="core_temperature",
                dtype="float64",
                required=False,
                fallback=None

            ),
            LoaderColumnSpec(
                name="Heart Rate Variability",
                source_name="hrv",
                output_name="hrv",
                dtype="int64",
                required=False,
                fallback=None,
            ),
        ],
        column_mapping={},  # No renaming needed
    )

    def __init__(
        self, config: Config, fs: FileSystemOperations, ui: UserInterface
    ) -> None:
        """
        Initialize Garmin loader.

        Args:
            config: Application configuration
            fs: Filesystem operations interface
            ui: User interface for messages
        """
        super().__init__(config, fs, ui)

    def detect_in_downloads(self, filepath: Path) -> bool:
        """
        Check if file is a Garmin streams.csv (has 'hrv' column).

        This distinguishes Garmin from Wahoo - both use streams.csv
        but only Garmin has HRV data.

        Args:
            filepath: Path to the file to check

        Returns:
            bool: True if file is Garmin (streams.csv with hrv)
        """
        if not filepath.name.endswith("streams.csv"):
            return False

        try:
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                first_line = f.readline().lower()
                return "hrv" in first_line
        except (OSError, UnicodeDecodeError) as e:
            logger.debug(f"Błąd odczytu nagłówka Garmin w {filepath.name}: {e}")
            return False

        return imported

    def validate_dataframe(self, df: pd.DataFrame) -> ValidationResult:
        """
        Garmin specific: at least one wanted column must be present.
        """
        result = ValidationResult()
        existing_cols = set(df.columns)
        present = [c for c in self.WANTED_COLUMNS if c in existing_cols]

        if not present:
            result.add_error(f"Brak jakichkolwiek kolumn z {self.WANTED_COLUMNS}")

        return result

    def process_files(self) -> List[Path]:
        """
        Process Garmin files: extract columns and remove leading NaN.

        Processing steps:
            1. Find wanted columns that are present
            2. Extract those columns
            3. Remove leading NaN rows (first 30 rows)
            4. Save as *_clean.csv
            5. Archive original

        Returns:
            List[Path]: List of clean file paths

        Note:
            Files with no wanted columns are skipped with a message.
        """
        garmin_files: List[Path] = self.fs.glob(self.source_dir, "*.csv")
        garmin_files = [f for f in garmin_files if "_clean" not in f.name]

        self.ui.print_message(
            f"\n🧪 Ekstrakcja Garmin streams (skin_temperature, HeatStrainIndex, hrv)"
        )
        self.ui.print_message(
            f"   Znaleziono {len(garmin_files)} plików CSV w {self.source_dir.name}"
        )

        self.fs.mkdir(self.old_dir)
        clean_files: List[Path] = []

        for path in garmin_files:
            try:
                df: pd.DataFrame = self.fs.read_csv(path)
            except (OSError, pd.errors.ParserError) as e:
                logger.error(f"Błąd odczytu pliku Garmin {path.name}: {e}")
                self.ui.print_error(f"{path.name}: błąd odczytu ({e})")
                continue

            df.columns = [str(c).strip() for c in df.columns]

            # Find which wanted columns are present
            present: List[str] = [c for c in self.WANTED_COLUMNS if c in df.columns]
            if not present:
                self.ui.print_message(
                    f"   ⏭️ {path.name}: brak jakichkolwiek kolumn z {self.WANTED_COLUMNS}"
                )
                continue

            # Extract present columns
            df_out: pd.DataFrame = df[present].copy()
            df_out = df_out.replace(r"^\s*$", np.nan, regex=True)

            # Remove leading rows with NaN (up to 30)
            head_n: int = min(self.LEADING_NAN_LIMIT, len(df_out))
            head_part = df_out.iloc[:head_n]
            idx_to_drop = head_part[head_part.isna().any(axis=1)].index

            rows_dropped: int = 0
            if len(idx_to_drop) > 0:
                df_out = df_out.drop(index=idx_to_drop)
                rows_dropped = len(idx_to_drop)

            out_clean: Path = self.source_dir / (path.stem + "_clean.csv")
            self.fs.write_csv(df_out, out_clean, index=False)
            self.ui.print_success(
                f"{out_clean.name} (kolumny: {', '.join(present)}, usunięto {rows_dropped} wierszy z góry)"
            )
            clean_files.append(out_clean)

            # Move original to archive
            try:
                self.fs.move(path, self.old_dir / path.name)
                self.ui.print_message(
                    f"   ↪ przeniesiono oryginalny Garmin: {path.name} -> {self.old_dir.name}"
                )
            except OSError as e:
                logger.warning(f"Błąd archiwizacji pliku Garmin {path.name}: {e}")
                self.ui.print_error(f"błąd przenoszenia {path.name}: {e}")

        return clean_files

    def get_clean_files(self) -> List[Path]:
        """
        Get list of clean Garmin files ready for merging.

        Returns:
            List[Path]: Paths to *_clean.csv files
        """
        return self.fs.glob(self.source_dir, "*_clean.csv")
