"""
Garmin data source loader.

Handles TWO types of Garmin-related files:

1. **Garmin sensor CSV** (from Garmin Connect watch export):
   - Has hrv, skin_temperature, HeatStrainIndex, core_temperature
   - Does NOT have activity data (cadence, watts, distance, velocity_smooth)
   - Processed as supplemental sensor data

2. **Intervals.icu streams CSV** (full activity export):
   - Has ALL data: hrv + cadence + watts + distance + velocity_smooth + ...
   - Downloaded from Intervals.icu after Garmin Connect sync
   - Processed as complete activity file (base candidate)

Detection:
    Both have 'hrv' in header. Distinguished by presence of activity
    indicator columns (cadence, watts, distance, velocity_smooth).

Processing Steps (sensor-only):
    1. Detect streams.csv with 'hrv' but WITHOUT activity columns
    2. Extract available sensor columns from wanted list
    3. Remove leading NaN rows (up to 30)
    4. Save as *_clean.csv

Processing Steps (Intervals.icu):
    1. Detect streams.csv with 'hrv' AND activity columns
    2. Keep ALL columns (no filtering)
    3. Drop completely empty columns (e.g., torque)
    4. Remove leading NaN in key activity columns only
    5. Save as *_clean.csv
"""

from pathlib import Path
from typing import List, ClassVar, Set
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


# Columns that indicate a full activity stream (vs sensor-only)
ACTIVITY_INDICATOR_COLUMNS: Set[str] = {"cadence", "watts", "distance", "velocity_smooth"}

# Key columns for smart head-trimming (only check these, not ALL columns)
HEAD_TRIM_KEY_COLUMNS: List[str] = ["heartrate", "cadence", "velocity_smooth", "distance"]


@LoaderRegistry.register(
    "garmin",
    priority=30,
    description="Garmin smartwatch / Intervals.icu streams",
    file_patterns=["*streams.csv"],
)
class GarminLoader(BaseLoader):
    """
    Loader for Garmin-origin data files.

    Handles both sensor-only Garmin CSVs and full Intervals.icu streams.
    When file is a full activity stream (has cadence/watts/distance),
    ALL columns are preserved. When sensor-only, extracts WANTED_COLUMNS.

    Attributes:
        WANTED_COLUMNS: Sensor columns to extract when file is sensor-only
        LEADING_NAN_LIMIT: Max rows to check for leading NaN removal
    """

    # Sensor-only columns (extracted when file is NOT a full activity stream)
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
        super().__init__(config, fs, ui)

    @staticmethod
    def _is_full_activity_stream(filepath: Path) -> bool:
        """
        Check if file is a full activity stream (Intervals.icu) vs sensor-only.

        Full activity streams have both hrv AND activity columns like
        cadence, watts, distance, or velocity_smooth.
        """
        try:
            with open(filepath, "r", encoding="utf-8-sig", errors="ignore") as f:
                first_line = f.readline().lower().strip()
                header_fields = {field.strip() for field in first_line.split(",")}
                return bool(header_fields & ACTIVITY_INDICATOR_COLUMNS)
        except (OSError, UnicodeDecodeError):
            return False

    def detect_in_downloads(self, filepath: Path) -> bool:
        """
        Check if file is a Garmin-origin file (has 'hrv' column).

        This matches both sensor-only and full Intervals.icu streams.
        """
        if not filepath.name.endswith("streams.csv"):
            return False

        try:
            with open(filepath, "r", encoding="utf-8-sig", errors="ignore") as f:
                first_line = f.readline().lower()
                return "hrv" in first_line
        except (OSError, UnicodeDecodeError) as e:
            logger.debug(f"Błąd odczytu nagłówka Garmin w {filepath.name}: {e}")
            return False

    def validate_dataframe(self, df: pd.DataFrame) -> ValidationResult:
        """At least one useful column must be present."""
        result = ValidationResult()
        existing_cols = set(df.columns)

        # For full activity streams, check for activity columns
        has_activity = bool(existing_cols & ACTIVITY_INDICATOR_COLUMNS)
        has_sensor = any(c in existing_cols for c in self.WANTED_COLUMNS)

        if not has_activity and not has_sensor:
            result.add_error(
                f"Brak kolumn aktywności ({ACTIVITY_INDICATOR_COLUMNS}) "
                f"ani sensorowych ({self.WANTED_COLUMNS})"
            )

        return result

    def process_files(self) -> List[Path]:
        """
        Process Garmin/Intervals.icu files.

        Full activity streams: keep ALL columns, drop empty ones,
        trim leading NaN on key activity columns only.

        Sensor-only files: extract only WANTED_COLUMNS, trim leading NaN.
        """
        garmin_files: List[Path] = self.fs.glob(self.source_dir, "*.csv")
        garmin_files = [f for f in garmin_files if "_clean" not in f.name]

        self.ui.print_message(
            f"\n🧪 Przetwarzanie plików Garmin/Intervals.icu"
        )
        self.ui.print_message(
            f"   Znaleziono {len(garmin_files)} plików CSV w {self.source_dir.name}"
        )

        self.fs.mkdir(self.old_dir)
        clean_files: List[Path] = []

        for path in garmin_files:
            try:
                is_full = self._is_full_activity_stream(path)

                if is_full:
                    clean_path = self._process_full_activity(path)
                else:
                    clean_path = self._process_sensor_only(path)

                if clean_path:
                    clean_files.append(clean_path)

                # Move original to archive
                try:
                    self.fs.move(path, self.old_dir / path.name)
                    self.ui.print_message(
                        f"   ↪ przeniesiono: {path.name} -> {self.old_dir.name}"
                    )
                except OSError as e:
                    logger.warning(f"Błąd archiwizacji {path.name}: {e}")

            except (OSError, pd.errors.ParserError) as e:
                logger.error(f"Błąd odczytu {path.name}: {e}")
                self.ui.print_error(f"{path.name}: błąd odczytu ({e})")

        return clean_files

    def _process_full_activity(self, path: Path) -> Path | None:
        """
        Process a full Intervals.icu activity stream.

        Keeps ALL columns. Only drops completely empty columns and
        trims leading NaN on key activity columns.
        """
        self.ui.print_message(f"   📊 [Intervals.icu] {path.name}")

        df: pd.DataFrame = self.fs.read_csv(path)
        df.columns = [str(c).strip() for c in df.columns]

        # Drop completely empty columns (e.g., torque when always NaN)
        empty_cols = [c for c in df.columns if df[c].isna().all()]
        if empty_cols:
            df = df.drop(columns=empty_cols)
            self.ui.print_message(f"      Usunięto puste kolumny: {empty_cols}")

        # Smart head trimming: only check KEY activity columns
        present_keys = [c for c in HEAD_TRIM_KEY_COLUMNS if c in df.columns]
        if present_keys:
            head_n = min(self.LEADING_NAN_LIMIT, len(df))
            head_part = df.iloc[:head_n]
            idx_to_drop = head_part[head_part[present_keys].isna().any(axis=1)].index

            rows_dropped = 0
            if len(idx_to_drop) > 0:
                df = df.drop(index=idx_to_drop).reset_index(drop=True)
                rows_dropped = len(idx_to_drop)

        out_clean: Path = self.source_dir / (path.stem + "_clean.csv")
        self.fs.write_csv(df, out_clean, index=False)
        self.ui.print_success(
            f"{out_clean.name} (Intervals.icu: {len(df.columns)} kolumn, "
            f"{len(df)} wierszy)"
        )
        return out_clean

    def _process_sensor_only(self, path: Path) -> Path | None:
        """
        Process a Garmin sensor-only file.

        Extracts only WANTED_COLUMNS (hrv, skin_temperature, etc.).
        """
        self.ui.print_message(f"   🔬 [Garmin sensor] {path.name}")

        df: pd.DataFrame = self.fs.read_csv(path)
        df.columns = [str(c).strip() for c in df.columns]

        present: List[str] = [c for c in self.WANTED_COLUMNS if c in df.columns]
        if not present:
            self.ui.print_message(
                f"   ⏭️ {path.name}: brak kolumn sensorowych z {self.WANTED_COLUMNS}"
            )
            return None

        df_out: pd.DataFrame = df[present].copy()
        df_out = df_out.replace(r"^\s*$", np.nan, regex=True)

        # Remove leading NaN rows (sensor warm-up period)
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
            f"{out_clean.name} (sensor: {', '.join(present)}, "
            f"usunięto {rows_dropped} wierszy z góry)"
        )
        return out_clean

    def get_clean_files(self) -> List[Path]:
        """Get list of clean files ready for merging."""
        return self.fs.glob(self.source_dir, "*_clean.csv")

    def get_base_dataframe(self) -> pd.DataFrame:
        """
        Load Garmin/Intervals.icu file as the base DataFrame for merging.

        Checks both original files AND _clean files (original may be archived).
        When used as base, ALL columns are preserved.
        """
        # First try _clean files (available after processing)
        clean_files = self.fs.glob(self.source_dir, "*_clean.csv")

        # Then try original files
        all_files = self.fs.glob(self.source_dir, "*.csv")
        original_files = [f for f in all_files if "_clean" not in f.name]

        # Prefer original (has all columns), fall back to clean
        candidates = original_files or clean_files

        if not candidates:
            return pd.DataFrame()

        target_file = candidates[0]
        if not self.fs.exists(target_file):
            self.ui.print_error(f"BŁĄD KRYTYCZNY: Nie znaleziono pliku {target_file}!")
            return pd.DataFrame()

        try:
            df = self.fs.read_csv(target_file)
            # When used as base, keep ALL columns
            return df.copy()
        except (OSError, pd.errors.ParserError) as e:
            logger.error(f"Błąd odczytu bazy Garmin {target_file}: {e}")
            self.ui.print_error(f"Błąd odczytu bazy Garmin: {e}")
            return pd.DataFrame()
