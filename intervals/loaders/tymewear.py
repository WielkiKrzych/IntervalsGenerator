"""
Tymewear data source loader.

Handles breathing rate, tidal volume, and ventilation data from Tymewear sensors.

Data Contract:
    Input:  1 Hz sampling rate, CSV with 'BR', 'VT', 'VE' columns
    Output: 1 Hz, CSV with 'TymeBreathRate', 'tidal_volume', 'TymeVentilation'

Required Input Columns:
    - BR: int - Breathing rate (breaths/min)
    - VT: float - Tidal volume (L)
    - VE: float - Minute ventilation (L/min)

Output Columns:
    - TymeBreathRate: int - Renamed from BR
    - tidal_volume: float - Renamed from VT
    - TymeVentilation: float - Renamed from VE

Processing Steps:
    1. Detect header row containing BR, VT, VE
    2. Extract and rename columns
    3. Remove legend row (row 2 with units)
    4. Move processed files to archive

Failure Modes:
    - ValueError if required columns are missing
    - Skips files with parsing errors
"""

from pathlib import Path
from typing import List, Optional, ClassVar, Dict
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
from ..utils import find_header_row


logger = logging.getLogger(__name__)


@LoaderRegistry.register(
    "tymewear",
    priority=20,
    description="Tymewear breathing sensor - BR, VT, VE",
    file_patterns=["*.csv"],
)
class TymewearLoader(BaseLoader):
    """
    Loader for Tymewear breathing sensor data.

    Attributes:
        LOADER_SPEC: Class-level specification of column requirements
        REQUIRED_COLUMNS: List of columns that must be present
        OUTPUT_COLUMNS: List of columns in clean output
        COLUMN_MAPPING: Dict mapping source → output column names

    Detection:
        - File header contains BR, VT, VE columns (content-based detection)
        - Checks first 10 lines for optimization
    """

    # Class-level constants for column handling
    REQUIRED_COLUMNS: ClassVar[List[str]] = ["BR", "VT", "VE"]
    OUTPUT_COLUMNS: ClassVar[List[str]] = [
        "TymeBreathRate",
        "tidal_volume",
        "TymeVentilation",
    ]

    COLUMN_MAPPING: ClassVar[Dict[str, str]] = {
        "BR": "TymeBreathRate",
        "VT": "tidal_volume",
        "VE": "TymeVentilation",
    }

    # Loader specification for interface contract
    LOADER_SPEC: ClassVar[LoaderSpec] = LoaderSpec(
        name="Tymewear",
        priority=20,
        detection_method="header_columns",
        file_pattern="*.csv",
        input_frequency=1,
        output_frequency=1,
        required_columns=[
            LoaderColumnSpec(
                name="Breathing Rate",
                source_name="BR",
                output_name="TymeBreathRate",
                dtype="int64",
                required=True,
                fallback=0,
            ),
            LoaderColumnSpec(
                name="Tidal Volume",
                source_name="VT",
                output_name="tidal_volume",
                dtype="float64",
                required=True,
                fallback=0.0,
            ),
            LoaderColumnSpec(
                name="Minute Ventilation",
                source_name="VE",
                output_name="TymeVentilation",
                dtype="float64",
                required=True,
                fallback=0.0,
            ),
        ],
        optional_columns=[],
        column_mapping={
            "BR": "TymeBreathRate",
            "VT": "tidal_volume",
            "VE": "TymeVentilation",
        },
    )

    def __init__(
        self, config: Config, fs: FileSystemOperations, ui: UserInterface
    ) -> None:
        """
        Initialize Tymewear loader.

        Args:
            config: Application configuration
            fs: Filesystem operations interface
            ui: User interface for messages
        """
        super().__init__(config, fs, ui)

    def detect_in_downloads(self, filepath: Path) -> bool:
        """
        Check if file is a Tymewear CSV by content analysis.
        """
        if filepath.suffix.lower() != ".csv":
            return False

        try:
            return (
                find_header_row(
                    filepath,
                    ["BR", "VT", "VE"],
                    max_lines=self.config.HEADER_SCAN_MAX_LINES,
                )
                is not None
            )
        except Exception as e:
            logger.debug(f"Błąd odczytu nagłówka Tymewear w {filepath.name}: {e}")
            return False

    def process_files(self) -> List[Path]:
        """
        Process Tymewear files: extract and rename columns.
        """
        csv_files: List[Path] = self.fs.glob(self.source_dir, "*.csv")
        csv_files = [f for f in csv_files if "_clean" not in f.name]

        self.ui.print_message(
            f"\n🧪 Ekstrakcja TymeBreathRate, tidal_volume, TymeVentilation z plików Tymewear"
        )
        self.ui.print_message(
            f"   Znaleziono {len(csv_files)} plików CSV w {self.source_dir.name}"
        )

        self.fs.mkdir(self.old_dir)
        clean_files: List[Path] = []

        for path in csv_files:
            header_row: int = (
                find_header_row(
                    path,
                    ["BR", "VT", "VE"],
                    max_lines=self.config.HEADER_SCAN_MAX_LINES,
                )
                or 0
            )

            try:
                df: pd.DataFrame = self.fs.read_csv(path, skiprows=header_row)

            except (OSError, pd.errors.ParserError) as e:
                logger.error(f"Błąd odczytu pliku Tymewear {path.name}: {e}")
                self.ui.print_error(f"{path.name}: błąd odczytu ({e})")
                continue

            df.columns = [str(c).strip() for c in df.columns]

            # Validate required columns
            validation = self.validate_dataframe(df)
            if not validation.is_valid:
                self.ui.print_error(f"{path.name}: {validation.errors}")
                continue

            # Extract and rename columns
            df_out: pd.DataFrame = df[self.REQUIRED_COLUMNS].copy()
            df_out = df_out.rename(columns=self.COLUMN_MAPPING)

            # Remove empty rows
            df_out = df_out.dropna(how="all")
            df_out = df_out[~(df_out == "").all(axis=1)]

            # Save as clean
            self.fs.write_csv(df_out, out_clean, index=False)
            self.ui.print_success(
                f"{out_clean.name} (kolumny: {', '.join(self.OUTPUT_COLUMNS)})"
            )

            clean_files.append(out_clean)

            # Move original to archive
            try:
                self.fs.move(path, self.old_dir / path.name)
                self.ui.print_message(
                    f"   ↪ przeniesiono oryginalny Tymewear: {path.name} -> {self.old_dir.name}"
                )
            except OSError as e:
                logger.warning(f"Błąd archiwizacji pliku Tymewear {path.name}: {e}")
                self.ui.print_error(f"błąd przenoszenia {path.name}: {e}")

        return clean_files

    def get_clean_files(self) -> List[Path]:
        """
        Get list of clean Tymewear files ready for merging.
        """
        return self.fs.glob(self.source_dir, "*_clean.csv")
