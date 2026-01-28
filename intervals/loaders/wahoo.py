"""
Wahoo data source loader.

Handles Wahoo ELEMNT bike computer data - serves as the BASE file for merging.

Data Contract:
    Input:  1 Hz sampling rate, *streams.csv file WITHOUT 'hrv' column
    Output: Unchanged - all columns preserved as-is

Required Input Columns:
    - secs: int - Time from start in seconds (required, used as time index)

Optional Input Columns (all preserved):
    - watts: int - Power (W)
    - cadence: int - Cadence (RPM)
    - heartrate: int - Heart rate (BPM)
    - distance: float - Cumulative distance (m)
    - speed: float - Speed (m/s)
    - altitude: float - Elevation (m)

Processing Steps:
    1. Detect streams.csv without 'hrv' column (Garmin has 'hrv')
    2. Rename to Wahoo.csv
    3. No column transformation needed

Special Role:
    Wahoo.csv is the BASE file for all merge operations.
    Its row count determines the output length.
    Its 'secs' column provides the time index.

Failure Modes:
    - Critical error if Wahoo.csv not found during merge
    - Empty DataFrame returned if file cannot be read
"""

from pathlib import Path
from typing import List, ClassVar
import logging
import pandas as pd

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


logger = logging.getLogger(__name__)


@LoaderRegistry.register(
    "wahoo",
    priority=1,  # Lowest priority = processed first (base file)
    description="Wahoo ELEMNT bike computer - base file for merging",
    file_patterns=["*streams.csv"],
)
class WahooLoader(BaseLoader):
    """
    Loader for Wahoo ELEMNT bike computer data.

    This is the BASE loader - Wahoo.csv serves as the primary time axis
    for merging all other data sources.

    Attributes:
        LOADER_SPEC: Class-level specification of column requirements
        REQUIRED_COLUMNS: secs (time index)
        OPTIONAL_COLUMNS: watts, cadence, heartrate, distance, speed, altitude

    Detection:
        - Filename ends with 'streams.csv'
        - First line does NOT contain 'hrv' (that would be Garmin)
    """

    # Class-level constants
    REQUIRED_COLUMNS: ClassVar[List[str]] = ["secs"]
    OPTIONAL_COLUMNS: ClassVar[List[str]] = [
        "watts",
        "cadence",
        "heartrate",
        "distance",
        "speed",
        "altitude",
    ]

    # Loader specification for interface contract
    LOADER_SPEC: ClassVar[LoaderSpec] = LoaderSpec(
        name="Wahoo",
        priority=1,
        detection_method="header_presence",
        file_pattern="*streams.csv",
        input_frequency=1,
        output_frequency=1,
        required_columns=[
            LoaderColumnSpec(
                name="Time Index",
                source_name="secs",
                output_name="secs",
                dtype="int64",
                required=True,
                fallback=None,  # Cannot have fallback - critical
            ),
        ],
        optional_columns=[
            LoaderColumnSpec(
                name="Power",
                source_name="watts",
                output_name="watts",
                dtype="int64",
                required=False,
                fallback=None,
            ),
            LoaderColumnSpec(
                name="Cadence",
                source_name="cadence",
                output_name="cadence",
                dtype="int64",
                required=False,
                fallback=None,
            ),
            LoaderColumnSpec(
                name="Heart Rate",
                source_name="heartrate",
                output_name="heartrate",
                dtype="int64",
                required=False,
                fallback=None,
            ),
            LoaderColumnSpec(
                name="Distance",
                source_name="distance",
                output_name="distance",
                dtype="float64",
                required=False,
                fallback=None,
            ),
            LoaderColumnSpec(
                name="Speed",
                source_name="speed",
                output_name="speed",
                dtype="float64",
                required=False,
                fallback=None,
            ),
            LoaderColumnSpec(
                name="Altitude",
                source_name="altitude",
                output_name="altitude",
                dtype="float64",
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
        Initialize Wahoo loader.

        Args:
            config: Application configuration
            fs: Filesystem operations interface
            ui: User interface for messages
        """
        super().__init__(config, fs, ui)

    def detect_in_downloads(self, filepath: Path) -> bool:
        """
        Check if file is a Wahoo streams.csv (NOT Garmin).
        """
        if not filepath.name.endswith("streams.csv"):
            return False

        try:
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                first_line = f.readline().lower()
                if "hrv" in first_line:
                    return False
                # Ensure it's actually a streams file by checking other keys
                return "secs" in first_line or "watts" in first_line
        except (OSError, UnicodeDecodeError) as e:
            logger.debug(f"Błąd odczytu nagłówka Wahoo w {filepath.name}: {e}")
            return False

    def import_from_downloads(self, downloads_dir: Path) -> List[Path]:
        """
        Specialized import for Wahoo to handle renaming to Wahoo.csv.
        """
        if not self.fs.exists(downloads_dir):
            self.ui.print_error(f"Downloads: {downloads_dir} nie istnieje")
            return []

        self.ui.print_message(f"\n📅 Szukam plików Wahoo w Downloads...")

        csv_files: List[Path] = self.fs.glob(downloads_dir, "*streams.csv")
        self.fs.mkdir(self.source_dir)
        imported: List[Path] = []

        for src in csv_files:
            if self.detect_in_downloads(src):
                dst: Path = self.source_dir / "Wahoo.csv"
                if self._copy_and_remove_from_downloads(src, dst):
                    self.ui.print_success(
                        f"Wykryto i skopiowano Wahoo: {src.name} -> Wahoo.csv"
                    )
                    imported.append(dst)
                    break  # Only one base file needed

        return imported

    def process_files(self) -> List[Path]:
        """
        Process Wahoo files.

        Wahoo files don't need additional processing.
        Wahoo.csv is used as-is as the base for merging.

        Returns:
            List[Path]: Same as get_clean_files()
        """
        return self.get_clean_files()

    def get_clean_files(self) -> List[Path]:
        """
        Get the Wahoo.csv file (base for merging).

        Returns:
            List[Path]: [Wahoo.csv] if exists, else empty list
        """
        wahoo_file: Path = self.source_dir / "Wahoo.csv"
        if self.fs.exists(wahoo_file):
            return [wahoo_file]
        return []

    def get_base_dataframe(self) -> pd.DataFrame:
        """
        Load Wahoo.csv as the base DataFrame for merging.

        This is the PRIMARY data for the merge operation.
        All other data sources align to this DataFrame's row count.

        Returns:
            pd.DataFrame: Wahoo data, or empty DataFrame if not found

        Failure Modes:
            - Returns empty DataFrame if file not found
            - Logs critical error to UI
        """
        wahoo_file: Path = self.source_dir / "Wahoo.csv"
        if not self.fs.exists(wahoo_file):
            self.ui.print_error(f"BŁĄD KRYTYCZNY: Nie znaleziono pliku {wahoo_file}!")
            return pd.DataFrame()

        try:
            return self.fs.read_csv(wahoo_file)
        except (OSError, pd.errors.ParserError) as e:
            logger.error(f"Błąd odczytu bazy Wahoo {wahoo_file}: {e}")
            self.ui.print_error(f"Błąd odczytu bazy Wahoo: {e}")
            return pd.DataFrame()
