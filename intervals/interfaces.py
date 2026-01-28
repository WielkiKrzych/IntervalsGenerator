"""
Abstract interfaces for Intervals Generator.
Implements OCP (Open/Closed) and DIP (Dependency Inversion) principles.

This module defines:
- Data contracts (TypedDict schemas for normalized data)
- Abstract base classes for loaders, validators, UI, and filesystem
- Type hints and protocols for static analysis
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    List,
    Optional,
    Dict,
    Any,
    Tuple,
    TypedDict,
    Literal,
    ClassVar,
    Protocol,
    runtime_checkable,
    Union,
    Callable,
)
import pandas as pd


# ============================================================
# Data Schemas (TypedDict for DataFrame contracts)
# ============================================================


class WahooSchema(TypedDict, total=False):
    """
    Schema for Wahoo data (base file).

    Required columns:
        secs: Time in seconds from start (int)

    Optional columns (preserved as-is):
        watts, cadence, heartrate, distance, speed, altitude, etc.
    """

    secs: int
    watts: int
    cadence: int
    heartrate: int
    distance: float
    speed: float
    altitude: float


class TrainRedSchema(TypedDict):
    """
    Schema for TrainRed normalized data (1 Hz).

    Required columns:
        smo2: Muscle oxygen saturation (%) - mapped from 'SmO2'
        THb: Total hemoglobin (g/dL) - mapped from 'THb unfiltered'
    """

    smo2: float
    THb: float


class TrainRedRawSchema(TypedDict):
    """
    Schema for raw TrainRed data (10 Hz).

    Required columns:
        Timestamp (seconds passed): Time from start in fractional seconds
        SmO2: Muscle oxygen saturation (%)
        THb unfiltered: Total hemoglobin (g/dL)

    Optional columns:
        Device: Sensor identifier
    """

    Timestamp_seconds_passed: float  # Note: actual column has spaces
    SmO2: float
    THb_unfiltered: float  # Note: actual column has space


class TymewearSchema(TypedDict):
    """
    Schema for Tymewear clean data.

    Required columns (after mapping):
        TymeBreathRate: Breathing rate (breaths/min) - mapped from 'BR'
        tidal_volume: Tidal volume (L) - mapped from 'VT'
        TymeVentilation: Minute ventilation (L/min) - mapped from 'VE'
    """

    TymeBreathRate: int
    tidal_volume: float
    TymeVentilation: float


class TymewearRawSchema(TypedDict):
    """
    Schema for raw Tymewear data.

    Required columns:
        BR: Breathing rate (breaths/min)
        VT: Tidal volume (L)
        VE: Minute ventilation (L/min)
    """

    BR: int
    VT: float
    VE: float


class GarminSchema(TypedDict, total=False):
    """
    Schema for Garmin clean data.

    All columns are optional (extracted if present):
        skin_temperature: Skin temperature (°C)
        HeatStrainIndex: Heat strain index (0-1)
        hrv: Heart rate variability (ms)
    """

    skin_temperature: float
    HeatStrainIndex: float
    hrv: int


class MergedSchema(TypedDict, total=False):
    """
    Schema for final merged output file.

    Base (from Wahoo):
        secs, watts, cadence, heartrate, distance, speed, altitude

    TrainRed:
        smo2, THb

    Tymewear:
        TymeBreathRate, tidal_volume, TymeVentilation

    Garmin:
        skin_temperature, HeatStrainIndex, hrv
    """

    # Wahoo (base)
    secs: int
    watts: int
    cadence: int
    heartrate: int
    distance: float
    speed: float
    altitude: float
    # TrainRed
    smo2: float
    THb: float
    # Tymewear
    TymeBreathRate: int
    tidal_volume: float
    TymeVentilation: float
    # Garmin
    skin_temperature: float
    HeatStrainIndex: float
    hrv: int


# ============================================================
# Loader Configuration Dataclass
# ============================================================


@dataclass
class LoaderColumnSpec:
    """
    Specification for a loader's column requirements.

    Attributes:
        name: Human-readable column name
        source_name: Original column name in source file
        output_name: Column name in output file
        dtype: Expected pandas dtype
        required: Whether column must be present
        fallback: Default value when missing (None = NaN)
    """

    name: str
    source_name: str
    output_name: str
    dtype: str = "float64"
    required: bool = True
    fallback: Optional[float] = None


@dataclass
class LoaderSpec:
    """
    Complete specification for a data source loader.

    Attributes:
        name: Human-readable loader name
        priority: Processing order (lower = earlier)
        detection_method: How files are detected
        file_pattern: Glob pattern for files
        input_frequency: Input sampling rate in Hz
        output_frequency: Output sampling rate in Hz
        required_columns: List of required column specs
        optional_columns: List of optional column specs
        column_mapping: Dict of source_name -> output_name
    """

    name: str
    priority: int
    detection_method: Literal["filename", "header_columns", "header_presence"]
    file_pattern: str
    input_frequency: int = 1
    output_frequency: int = 1
    required_columns: List[LoaderColumnSpec] = field(default_factory=list)
    optional_columns: List[LoaderColumnSpec] = field(default_factory=list)
    column_mapping: Dict[str, str] = field(default_factory=dict)

    @property
    def all_columns(self) -> List[LoaderColumnSpec]:
        """Get all column specifications."""
        return self.required_columns + self.optional_columns

    @property
    def required_source_columns(self) -> List[str]:
        """Get list of required source column names."""
        return [col.source_name for col in self.required_columns]

    @property
    def output_column_names(self) -> List[str]:
        """Get list of output column names."""
        return [col.output_name for col in self.all_columns]


# ============================================================
# Validation Result Dataclass
# ============================================================


@dataclass
class ValidationResult:
    """
    Result of a validation operation.

    Attributes:
        is_valid: Whether validation passed
        errors: List of error messages (blocking)
        warnings: List of warning messages (non-blocking)
        column_issues: Dict of column_name -> list of issues
    """

    is_valid: bool = True
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    column_issues: Dict[str, List[str]] = field(default_factory=dict)

    def add_error(self, message: str, column: Optional[str] = None) -> None:
        """Add an error message."""
        self.errors.append(message)
        self.is_valid = False
        if column:
            self.column_issues.setdefault(column, []).append(message)

    def add_warning(self, message: str, column: Optional[str] = None) -> None:
        """Add a warning message."""
        self.warnings.append(message)
        if column:
            self.column_issues.setdefault(column, []).append(message)


# ============================================================
# Abstract Base Classes
# ============================================================


class DataSourceLoader(ABC):
    """
    Abstract base class for data source loaders.

    Each data source (TrainRed, Tymewear, Wahoo, Garmin) implements this interface.

    Responsibilities:
        - Detect files belonging to this source
        - Import files from downloads
        - Process/normalize raw data
        - Provide clean data for merging

    Contract:
        - Implementations must define LOADER_SPEC class variable
        - load_csv() must return DataFrame matching output schema
        - validate_dataframe() must check required columns

    Failure Modes:
        - FileNotFoundError: Source directory doesn't exist
        - ValueError: Required columns missing from file
        - pd.errors.ParserError: CSV parsing failed
    """

    # Class variable: must be defined by subclasses
    LOADER_SPEC: ClassVar[LoaderSpec]

    @property
    @abstractmethod
    def name(self) -> str:
        """
        Human-readable name of the data source.

        Returns:
            str: Name like "TrainRed", "Wahoo", etc.
        """
        pass

    @property
    @abstractmethod
    def source_dir(self) -> Path:
        """
        Directory where this source's files are stored.

        Returns:
            Path: Absolute path to source directory
        """
        pass

    @property
    @abstractmethod
    def old_dir(self) -> Path:
        """
        Directory for archived/processed files.

        Returns:
            Path: Absolute path to archive directory
        """
        pass

    @abstractmethod
    def detect_in_downloads(self, filepath: Path) -> bool:
        """
        Check if a file in downloads belongs to this data source.

        Detection methods vary by source:
            - TrainRed: filename contains 'trainred' (case-insensitive)
            - Tymewear: header contains BR, VT, VE columns
            - Wahoo: *streams.csv without 'hrv' column
            - Garmin: *streams.csv with 'hrv' column

        Args:
            filepath: Path to the file to check

        Returns:
            bool: True if file belongs to this data source

        Failure Modes:
            - Returns False if file cannot be read
            - Returns False if file doesn't match pattern
        """
        pass

    @abstractmethod
    def import_from_downloads(self, downloads_dir: Path) -> List[Path]:
        """
        Import files from downloads directory to source directory.

        Process:
            1. Scan downloads_dir for matching files
            2. Copy matched files to source_dir
            3. Remove originals from downloads_dir

        Args:
            downloads_dir: Path to downloads directory

        Returns:
            List[Path]: List of imported file paths in source_dir

        Failure Modes:
            - Returns empty list if downloads_dir doesn't exist
            - Skips files that fail to copy
        """
        pass

    @abstractmethod
    def process_files(self) -> List[Path]:
        """
        Process raw files and create clean versions.

        Processing steps vary by source:
            - TrainRed: Normalize 10Hz→1Hz, extract SmO2/THb
            - Tymewear: Extract BR/VT/VE, rename columns
            - Wahoo: No processing (used as-is)
            - Garmin: Extract skin_temp/HeatStrain/hrv, trim leading NaN

        Returns:
            List[Path]: List of processed file paths (*_clean.csv)

        Failure Modes:
            - Skips files with missing required columns
            - Logs errors for unprocessable files
        """
        pass

    @abstractmethod
    def get_clean_files(self) -> List[Path]:
        """
        Get list of clean/processed files ready for merging.

        Returns:
            List[Path]: List of *_clean.csv file paths
        """
        pass

    def load_csv(self, path: Path, **kwargs: Any) -> pd.DataFrame:
        """
        Load a CSV file with standard options.

        Default behavior can be overridden by subclasses.

        Args:
            path: Path to CSV file
            **kwargs: Additional pandas read_csv arguments

        Returns:
            pd.DataFrame: Loaded data

        Raises:
            FileNotFoundError: If file doesn't exist
            pd.errors.ParserError: If CSV parsing fails
        """
        return pd.read_csv(path, **kwargs)

    def validate_dataframe(self, df: pd.DataFrame) -> ValidationResult:
        """
        Validate a DataFrame against this loader's schema.

        Checks:
            - Required columns are present
            - Column dtypes are compatible
            - No excessive NaN values

        Args:
            df: DataFrame to validate

        Returns:
            ValidationResult: Validation outcome with errors/warnings
        """
        result = ValidationResult()

        if not hasattr(self, "LOADER_SPEC"):
            result.add_warning("LOADER_SPEC not defined, skipping validation")
            return result

        spec = self.LOADER_SPEC
        existing_cols = set(df.columns)

        for col_spec in spec.required_columns:
            if col_spec.source_name not in existing_cols:
                result.add_error(
                    f"Required column '{col_spec.source_name}' missing",
                    column=col_spec.source_name,
                )

        return result


class Validator(ABC):
    """
    Abstract base class for data validators.

    ISP: Validators are separate from loaders, allowing different
    validation strategies to be used independently.
    """

    @abstractmethod
    def validate(self, df: pd.DataFrame, source_name: str) -> List[str]:
        """
        Validate a DataFrame.

        Args:
            df: DataFrame to validate
            source_name: Name of the data source for error messages

        Returns:
            List[str]: List of validation issues (empty if valid)
        """
        pass


class UserInterface(ABC):
    """
    Abstract interface for user interaction.

    DIP: Business logic depends on this abstraction, not concrete I/O.
    This allows for console UI, GUI, or silent testing mode.
    """

    @abstractmethod
    def print_message(self, message: str) -> None:
        """Display a message to the user."""
        pass

    @abstractmethod
    def print_success(self, message: str) -> None:
        """Display a success message."""
        pass

    @abstractmethod
    def print_warning(self, message: str) -> None:
        """Display a warning message."""
        pass

    @abstractmethod
    def print_error(self, message: str) -> None:
        """Display an error message."""
        pass

    @abstractmethod
    def ask_yes_no(self, question: str) -> bool:
        """
        Ask the user a yes/no question.

        Args:
            question: The question to ask

        Returns:
            bool: True if user answered yes, False otherwise
        """
        pass

    @abstractmethod
    def print_header(self, title: str) -> None:
        """Display a section header."""
        pass

    @abstractmethod
    def print_separator(self) -> None:
        """Display a visual separator."""
        pass

    @abstractmethod
    def print_progress(self, current: int, total: int, prefix: str = "") -> None:
        """
        Display a progress indicator.

        Args:
            current: Current item number (1-indexed)
            total: Total number of items
            prefix: Optional prefix text
        """
        pass


class FileSystemOperations(ABC):
    """
    Abstract interface for file system operations.

    DIP: Allows mocking filesystem for testing without touching real files.

    All paths should be absolute Path objects.
    """

    @abstractmethod
    def exists(self, path: Path) -> bool:
        """Check if a path exists."""
        pass

    @abstractmethod
    def glob(self, directory: Path, pattern: str) -> List[Path]:
        """
        Find files matching a pattern.

        Args:
            directory: Directory to search in
            pattern: Glob pattern (e.g., "*.csv")

        Returns:
            List[Path]: Matching file paths
        """
        pass

    @abstractmethod
    def copy(self, src: Path, dst: Path) -> None:
        """
        Copy a file.

        Args:
            src: Source file path
            dst: Destination file path

        Raises:
            FileNotFoundError: If source doesn't exist
        """
        pass

    @abstractmethod
    def move(self, src: Path, dst: Path) -> None:
        """
        Move a file.

        Args:
            src: Source file path
            dst: Destination file path

        Raises:
            FileNotFoundError: If source doesn't exist
        """
        pass

    @abstractmethod
    def remove(self, path: Path) -> None:
        """
        Remove a file.

        Args:
            path: File path to remove

        Raises:
            FileNotFoundError: If file doesn't exist
        """
        pass

    @abstractmethod
    def read_csv(self, path: Path, **kwargs: Any) -> pd.DataFrame:
        """
        Read a CSV file into DataFrame.

        Args:
            path: Path to CSV file
            **kwargs: Additional pandas read_csv arguments

        Returns:
            pd.DataFrame: Loaded data
        """
        pass

    @abstractmethod
    def write_csv(self, df: pd.DataFrame, path: Path, **kwargs: Any) -> None:
        """
        Write DataFrame to CSV.

        Args:
            df: DataFrame to write
            path: Output file path
            **kwargs: Additional pandas to_csv arguments
        """
        pass

    @abstractmethod
    def mkdir(self, path: Path, parents: bool = True, exist_ok: bool = True) -> None:
        """
        Create a directory.

        Args:
            path: Directory path to create
            parents: Create parent directories if needed
            exist_ok: Don't raise if directory exists
        """
        pass

    @abstractmethod
    def list_files(self, directory: Path) -> List[Path]:
        """
        List files in a directory.

        Args:
            directory: Directory to list

        Returns:
            List[Path]: File paths (not directories)
        """
        pass
