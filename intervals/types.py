"""
Type definitions for Intervals Generator.
Provides TypedDict, Protocol, and type aliases for better type safety.
"""

from typing import (
    TypedDict, Protocol, List, Dict, Optional, 
    Callable, Any, Literal, runtime_checkable
)
from pathlib import Path
import pandas as pd


# ============================================================
# Configuration Types
# ============================================================

class NormalizationConfig(TypedDict, total=False):
    """Configuration for data normalization."""
    enabled: bool
    target_hz: int
    timestamp_column: str


class LoaderSourceConfig(TypedDict, total=False):
    """Configuration for a single data source loader."""
    detection_pattern: Optional[str]
    detection_columns: List[str]
    detection_require_columns: List[str]
    detection_exclude_columns: List[str]
    file_pattern: str
    required_columns: List[str]
    extract_columns: List[str]
    output_mapping: Dict[str, str]
    fallback_values: Dict[str, float]
    normalization: NormalizationConfig
    skip_legend_row: bool
    rename_to: str
    trim_leading_nans: bool
    trim_max_rows: int


class ValidationConfig(TypedDict):
    """Global validation settings."""
    max_consecutive_nans: int
    require_wahoo_base: bool


class OutputConfig(TypedDict):
    """Output file settings."""
    filename_pattern: str
    date_format: str


class ColumnConfig(TypedDict):
    """Full column_config.yaml structure."""
    trainred: LoaderSourceConfig
    tymewear: LoaderSourceConfig
    wahoo: LoaderSourceConfig
    garmin: LoaderSourceConfig
    validation: ValidationConfig
    output: OutputConfig


# ============================================================
# Validation Result Types
# ============================================================

class ValidationIssue(TypedDict):
    """Single validation issue."""
    column: str
    issue_type: Literal['consecutive_nans', 'missing_column', 'invalid_data']
    message: str
    severity: Literal['error', 'warning', 'info']


class ValidationResult(TypedDict):
    """Result of validation operation."""
    is_valid: bool
    errors: List[ValidationIssue]
    warnings: List[ValidationIssue]


class ColumnValidationResult(TypedDict):
    """Result of column validation."""
    is_valid: bool
    missing_columns: List[str]
    suggested_mappings: Dict[str, str]
    error: Optional[str]


# ============================================================
# Processing Result Types
# ============================================================

class ProcessingResult(TypedDict):
    """Result of file processing."""
    success: bool
    input_path: Path
    output_path: Optional[Path]
    rows_processed: int
    error: Optional[str]


class MergeResult(TypedDict):
    """Result of merge operation."""
    success: bool
    output_path: Optional[Path]
    total_columns: int
    total_rows: int
    sources_merged: List[str]
    issues: List[str]


# ============================================================
# Protocol Definitions
# ============================================================

@runtime_checkable
class LoaderProtocol(Protocol):
    """Protocol that all data source loaders must implement."""
    
    @property
    def name(self) -> str:
        """Human-readable name of the data source."""
        ...
    
    @property
    def source_dir(self) -> Path:
        """Directory where this source's files are stored."""
        ...
    
    @property
    def old_dir(self) -> Path:
        """Directory for archived files."""
        ...
    
    def detect_in_downloads(self, filepath: Path) -> bool:
        """Check if a file in downloads belongs to this data source."""
        ...
    
    def import_from_downloads(self, downloads_dir: Path) -> List[Path]:
        """Import files from downloads directory to source directory."""
        ...
    
    def process_files(self) -> List[Path]:
        """Process raw files and create clean versions."""
        ...
    
    def get_clean_files(self) -> List[Path]:
        """Get list of clean/processed files ready for merging."""
        ...


@runtime_checkable
class ValidatorProtocol(Protocol):
    """Protocol for data validators."""
    
    def validate(self, df: pd.DataFrame, source_name: str) -> List[str]:
        """Validate a DataFrame and return list of issues."""
        ...


@runtime_checkable
class UserInterfaceProtocol(Protocol):
    """Protocol for user interaction."""
    
    def print_message(self, message: str) -> None:
        """Display a message."""
        ...
    
    def print_success(self, message: str) -> None:
        """Display a success message."""
        ...
    
    def print_warning(self, message: str) -> None:
        """Display a warning message."""
        ...
    
    def print_error(self, message: str) -> None:
        """Display an error message."""
        ...
    
    def ask_yes_no(self, question: str) -> bool:
        """Ask yes/no question."""
        ...


@runtime_checkable  
class FileSystemProtocol(Protocol):
    """Protocol for file system operations."""
    
    def exists(self, path: Path) -> bool:
        """Check if path exists."""
        ...
    
    def read_csv(self, path: Path, **kwargs: Any) -> pd.DataFrame:
        """Read CSV file."""
        ...
    
    def write_csv(self, df: pd.DataFrame, path: Path, **kwargs: Any) -> None:
        """Write DataFrame to CSV."""
        ...
    
    def copy(self, src: Path, dst: Path) -> None:
        """Copy file."""
        ...
    
    def move(self, src: Path, dst: Path) -> None:
        """Move file."""
        ...
    
    def remove(self, path: Path) -> None:
        """Remove file."""
        ...


# ============================================================
# Type Aliases
# ============================================================

# CSV reader function type
CSVReader = Callable[[Path], pd.DataFrame]

# File-source tuple for validation
FileSourceTuple = tuple[Path, str]

# Column mapping dictionary
ColumnMapping = Dict[str, str]

# Fallback values dictionary
FallbackValues = Dict[str, float]

# Interpolation methods
InterpolationMethod = Literal['none', 'linear', 'ffill', 'bfill', 'pad']

# Sampling frequency
SamplingFrequency = Literal[1, 10, 100]  # Hz
