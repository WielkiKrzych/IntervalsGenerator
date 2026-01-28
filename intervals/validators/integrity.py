"""
Data integrity validator.

Provides comprehensive validation for CSV files including:
- Column presence validation
- Data type validation (numeric columns)
- Timestamp validation (monotonic, no duplicates, no gaps)
- Sampling frequency detection
- Gap detection (consecutive NaN values)

OPTIMIZED: Early exit, efficient RLE, parallel file reading.
"""

from pathlib import Path
from typing import List, Tuple, Optional, Set, Any, Callable
import pandas as pd
import numpy as np

from ..interfaces import Validator, UserInterface, LoaderSpec
from ..exceptions import (
    IntervalsValidationError,
    MissingColumnError,
    InvalidDataTypeError,
    TimestampError,
    SamplingFrequencyError,
    DataGapError,
    FileFormatError,
)
from ..utils import check_consecutive_nans_optimized, read_csvs_parallel


class IntegrityValidator(Validator):
    """
    Validates data integrity with comprehensive checks.

    Validation checks:
        1. Required columns present
        2. Numeric columns contain valid numbers
        3. Timestamps are monotonic (non-decreasing)
        4. No duplicate timestamps
        5. Sampling frequency within tolerance
        6. No excessive data gaps (consecutive NaN)

    Attributes:
        ui: UserInterface for messages
        gap_threshold: Max consecutive NaN before error
        fail_fast: If True, raise exception on first error
        strict_mode: If True, treat warnings as errors
    """

    def __init__(
        self,
        ui: UserInterface,
        gap_threshold: int = None,
        fail_fast: bool = False,
        strict_mode: bool = False,
    ):
        # Use default from config if not provided
        if gap_threshold is None:
            from ..config import Config

            gap_threshold = Config.DEFAULT_GAP_THRESHOLD
        """
        Initialize validator.

        Args:
            ui: UserInterface for messages
            gap_threshold: Max consecutive NaN before error (default: 10)
            fail_fast: Raise exception on first error (default: False)
            strict_mode: Treat warnings as errors (default: False)
        """
        self._ui = ui
        self.gap_threshold = gap_threshold
        self.fail_fast = fail_fast
        self.strict_mode = strict_mode

    @property
    def ui(self) -> UserInterface:
        return self._ui

    def validate(self, df: pd.DataFrame, source_name: str) -> List[str]:
        """
        Validate a DataFrame for data gaps.

        Legacy interface - returns list of issue strings.
        For new code, use validate_full() instead.

        Args:
            df: DataFrame to validate
            source_name: Name of the data source for error messages

        Returns:
            List[str]: List of validation issues (empty if valid)
        """
        issues: List[str] = []

        for col in df.columns:
            max_gap = check_consecutive_nans_optimized(df[col], self.gap_threshold)
            if max_gap >= self.gap_threshold:
                issues.append(f"Kolumna '{col}': {max_gap} pustych wierszy z rzędu")

        return issues

    def validate_columns(
        self,
        df: pd.DataFrame,
        required_columns: List[str],
        file_path: Optional[str] = None,
    ) -> None:
        """
        Validate that required columns are present.

        Args:
            df: DataFrame to validate
            required_columns: List of required column names
            file_path: Path to file (for error messages)

        Raises:
            MissingColumnError: If any required column is missing
        """
        existing: Set[str] = set(df.columns)
        missing: List[str] = [col for col in required_columns if col not in existing]

        if missing:
            raise MissingColumnError(
                columns=missing, file_path=file_path, available_columns=list(df.columns)
            )

    def validate_numeric_columns(
        self,
        df: pd.DataFrame,
        numeric_columns: List[str],
        file_path: Optional[str] = None,
    ) -> List[str]:
        """
        Validate that numeric columns contain valid numbers.

        Args:
            df: DataFrame to validate
            numeric_columns: List of columns that should be numeric
            file_path: Path to file (for error messages)

        Returns:
            List[str]: Warnings for recoverable issues

        Raises:
            InvalidDataTypeError: If column has too many invalid values (>50%)
        """
        warnings: List[str] = []

        for col in numeric_columns:
            if col not in df.columns:
                continue

            # Try to convert to numeric
            numeric_values = pd.to_numeric(df[col], errors="coerce")
            invalid_mask = numeric_values.isna() & df[col].notna()
            invalid_count = invalid_mask.sum()

            if invalid_count > 0:
                invalid_pct = invalid_count / len(df) * 100
                invalid_samples = df.loc[invalid_mask, col].head(5).tolist()

                if invalid_pct > 50:
                    # More than 50% invalid = error
                    raise InvalidDataTypeError(
                        column=col,
                        expected_type="numeric",
                        invalid_values=[str(v) for v in invalid_samples],
                        invalid_count=invalid_count,
                        file_path=file_path,
                    )
                else:
                    # Less than 50% = warning
                    warning = (
                        f"Kolumna '{col}': {invalid_count} wartości nienumerycznych "
                        f"({invalid_pct:.1f}%) - zostaną zamienione na NaN"
                    )
                    warnings.append(warning)
                    self.ui.print_warning(f"   ⚠️ {warning}")

        return warnings

    def validate_timestamps(
        self,
        df: pd.DataFrame,
        time_column: str = "secs",
        file_path: Optional[str] = None,
        check_monotonic: bool = True,
        check_duplicates: bool = True,
        check_negative: bool = True,
    ) -> List[str]:
        """
        Validate timestamp column for common issues.

        Args:
            df: DataFrame to validate
            time_column: Name of time column
            file_path: Path to file (for error messages)
            check_monotonic: Check if values are non-decreasing
            check_duplicates: Check for duplicate values
            check_negative: Check for negative values

        Returns:
            List[str]: Warnings for recoverable issues

        Raises:
            TimestampError: For critical timestamp issues
        """
        warnings: List[str] = []

        if time_column not in df.columns:
            return warnings

        time_series = pd.to_numeric(df[time_column], errors="coerce")

        # Check for negative values
        if check_negative:
            negative_count = (time_series < 0).sum()
            if negative_count > 0:
                if self.fail_fast:
                    raise TimestampError(
                        error_type="negative",
                        column=time_column,
                        file_path=file_path,
                        details=f"{negative_count} wartości ujemnych",
                    )
                else:
                    warnings.append(
                        f"Kolumna '{time_column}': {negative_count} wartości ujemnych"
                    )

        # Check for monotonic (non-decreasing)
        if check_monotonic:
            diff = time_series.diff()
            decreasing_count = (diff < 0).sum()
            if decreasing_count > 0:
                first_decrease_idx = diff[diff < 0].index[0]
                if self.fail_fast:
                    raise TimestampError(
                        error_type="non_monotonic",
                        column=time_column,
                        file_path=file_path,
                        details=f"Wartość maleje w wierszu {first_decrease_idx}",
                    )
                else:
                    warnings.append(
                        f"Kolumna '{time_column}': wartości maleją {decreasing_count}x "
                        f"(pierwsze w wierszu {first_decrease_idx})"
                    )

        # Check for duplicates
        if check_duplicates:
            duplicate_count = time_series.duplicated().sum()
            if duplicate_count > 0:
                warnings.append(
                    f"Kolumna '{time_column}': {duplicate_count} zduplikowanych wartości"
                )

        return warnings

    def validate_sampling_frequency(
        self,
        df: pd.DataFrame,
        time_column: str,
        expected_freq: int,
        tolerance: float = 0.2,
        file_path: Optional[str] = None,
    ) -> Optional[float]:
        """
        Validate that sampling frequency matches expected value.

        Args:
            df: DataFrame to validate
            time_column: Name of time column
            expected_freq: Expected frequency in Hz
            tolerance: Acceptable deviation (e.g., 0.2 = ±20%)
            file_path: Path to file (for error messages)

        Returns:
            float: Detected frequency, or None if cannot detect

        Raises:
            SamplingFrequencyError: If frequency outside tolerance
        """
        if time_column not in df.columns or len(df) < 2:
            return None

        time_series = pd.to_numeric(df[time_column], errors="coerce")
        time_diff = time_series.diff().dropna()

        if len(time_diff) == 0:
            return None

        median_diff = time_diff.median()
        if median_diff <= 0:
            return None

        detected_freq = 1.0 / median_diff

        # Check if within tolerance
        lower_bound = expected_freq * (1 - tolerance)
        upper_bound = expected_freq * (1 + tolerance)

        if not (lower_bound <= detected_freq <= upper_bound):
            if self.fail_fast:
                raise SamplingFrequencyError(
                    expected_freq=expected_freq,
                    detected_freq=detected_freq,
                    file_path=file_path,
                    tolerance=tolerance,
                )
            else:
                self.ui.print_warning(
                    f"   ⚠️ Częstotliwość {detected_freq:.1f} Hz (oczekiwano {expected_freq} Hz)"
                )

        return detected_freq

    def validate_data_gaps(
        self,
        df: pd.DataFrame,
        columns: Optional[List[str]] = None,
        file_path: Optional[str] = None,
    ) -> List[str]:
        """
        Validate that data doesn't have excessive gaps.

        Args:
            df: DataFrame to validate
            columns: Columns to check (None = all)
            file_path: Path to file (for error messages)

        Returns:
            List[str]: Warnings for gaps found

        Raises:
            DataGapError: If fail_fast and large gap found
        """
        warnings: List[str] = []
        cols_to_check = columns if columns else df.columns.tolist()

        for col in cols_to_check:
            if col not in df.columns:
                continue

            max_gap = check_consecutive_nans_optimized(df[col], self.gap_threshold)

            if max_gap >= self.gap_threshold:
                if self.fail_fast:
                    raise DataGapError(
                        column=col,
                        gap_size=max_gap,
                        threshold=self.gap_threshold,
                        file_path=file_path,
                    )
                else:
                    warnings.append(
                        f"Kolumna '{col}': {max_gap} pustych wierszy z rzędu"
                    )

        return warnings

    def validate_full(
        self,
        df: pd.DataFrame,
        source_name: str,
        file_path: Optional[str] = None,
        loader_spec: Optional[LoaderSpec] = None,
        time_column: Optional[str] = None,
    ) -> Tuple[bool, List[str], List[str]]:
        """
        Perform comprehensive validation on a DataFrame.

        Args:
            df: DataFrame to validate
            source_name: Name of data source
            file_path: Path to file
            loader_spec: LoaderSpec with column requirements
            time_column: Name of time column (auto-detect if None)

        Returns:
            Tuple of (is_valid, errors, warnings)

        Raises:
            IntervalsValidationError: If fail_fast and critical error found
        """
        errors: List[str] = []
        warnings: List[str] = []

        file_str = str(file_path) if file_path else None

        # 1. Check for empty DataFrame
        if len(df) == 0:
            if self.fail_fast:
                raise FileFormatError(reason="empty_file", file_path=file_str)
            errors.append("Plik jest pusty")
            return False, errors, warnings

        # 2. Validate required columns (if spec provided)
        if loader_spec:
            required_cols = [c.source_name for c in loader_spec.required_columns]
            try:
                self.validate_columns(df, required_cols, file_str)
            except MissingColumnError as e:
                if self.fail_fast:
                    raise
                errors.append(str(e))

            # Check optional columns (warning only)
            optional_cols = [c.source_name for c in loader_spec.optional_columns]
            missing_optional = [c for c in optional_cols if c not in df.columns]
            if missing_optional:
                warnings.append(
                    f"Brak opcjonalnych kolumn: {', '.join(missing_optional)}"
                )

            # Validate numeric columns
            numeric_cols = [
                c.source_name
                for c in loader_spec.all_columns
                if c.dtype in ("int64", "float64")
            ]
            numeric_cols = [c for c in numeric_cols if c in df.columns]
            try:
                type_warnings = self.validate_numeric_columns(
                    df, numeric_cols, file_str
                )
                warnings.extend(type_warnings)
            except InvalidDataTypeError as e:
                if self.fail_fast:
                    raise
                errors.append(str(e))

        # 3. Validate timestamps
        time_col = time_column
        if not time_col:
            # Auto-detect time column
            for candidate in ["secs", "Timestamp (seconds passed)", "time", "second"]:
                if candidate in df.columns:
                    time_col = candidate
                    break

        if time_col:
            try:
                ts_warnings = self.validate_timestamps(df, time_col, file_str)
                warnings.extend(ts_warnings)
            except TimestampError as e:
                if self.fail_fast:
                    raise
                errors.append(str(e))

        # 4. Validate data gaps
        try:
            gap_warnings = self.validate_data_gaps(df, file_path=file_str)
            warnings.extend(gap_warnings)
        except DataGapError as e:
            if self.fail_fast:
                raise
            errors.append(str(e))

        is_valid = len(errors) == 0
        if self.strict_mode and warnings:
            is_valid = False
            errors.extend(warnings)

        return is_valid, errors, warnings

    def validate_files(
        self,
        files: List[Tuple[Path, str]],
        read_func: Callable[[Path], pd.DataFrame],
        parallel: bool = True,
        max_workers: int = None,
    ) -> bool:
        # Use default from config if not provided
        if max_workers is None:
            from ..config import Config

            max_workers = Config.DEFAULT_MAX_WORKERS
        """
        Validate multiple files and report issues.

        Args:
            files: List of (path, source_name) tuples
            read_func: Function to read CSV files
            parallel: Whether to use parallel reading
            max_workers: Number of parallel workers

        Returns:
            bool: True if all files valid, False if issues found
        """
        self.ui.print_message(
            f"\n🛡️  WALIDACJA DANYCH (Szukanie dziur > {self.gap_threshold} wierszy)"
        )
        self.ui.print_separator()

        issues_found = False

        if parallel and len(files) > 1:
            paths = [f[0] for f in files]
            path_to_source = {f[0]: f[1] for f in files}

            self.ui.print_message(f"   ⚡ Czytanie {len(paths)} plików równolegle...")
            dfs = read_csvs_parallel(paths, read_func, max_workers)

            for path, df in dfs.items():
                source_name = path_to_source[path]
                issues = self.validate(df, source_name)

                if issues:
                    issues_found = True
                    self.ui.print_message(f"   🚩 {source_name} / {path.name}:")
                    for issue in issues:
                        self.ui.print_warning(f"      {issue}")
        else:
            for file_path, source_name in files:
                try:
                    df = read_func(file_path)
                except Exception as e:
                    self.ui.print_error(f"Błąd odczytu {file_path.name}: {e}")
                    continue

                issues = self.validate(df, source_name)

                if issues:
                    issues_found = True
                    self.ui.print_message(f"   🚩 {source_name} / {file_path.name}:")
                    for issue in issues:
                        self.ui.print_warning(f"      {issue}")

        if issues_found:
            self.ui.print_warning("\nZNALEZIONO DUŻE LUKI W DANYCH!")
            return False
        else:
            self.ui.print_success(
                f"Walidacja OK: Brak ciągłych luk powyżej {self.gap_threshold} wierszy."
            )
            return True
