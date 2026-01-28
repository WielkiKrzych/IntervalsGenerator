"""
Custom exceptions for Intervals Generator validation.

These exceptions provide explicit, actionable error messages
with context about the file and column that caused the error.
"""

from typing import Optional, List


class IntervalsValidationError(Exception):
    """
    Base exception for all validation errors.
    
    Attributes:
        message: Human-readable error message
        file_path: Path to the file that caused the error
        column: Column name (if applicable)
        details: Additional context
    """
    
    def __init__(
        self,
        message: str,
        file_path: Optional[str] = None,
        column: Optional[str] = None,
        details: Optional[str] = None
    ):
        self.message = message
        self.file_path = file_path
        self.column = column
        self.details = details
        
        # Build full error message
        parts = [message]
        if file_path:
            parts.append(f"File: {file_path}")
        if column:
            parts.append(f"Column: {column}")
        if details:
            parts.append(f"Details: {details}")
        
        super().__init__(" | ".join(parts))


class MissingColumnError(IntervalsValidationError):
    """
    Raised when a required column is missing from the CSV.
    
    Example:
        >>> raise MissingColumnError(
        ...     columns=['SmO2', 'THb'],
        ...     file_path='trainred.csv',
        ...     available_columns=['Timestamp', 'Device']
        ... )
    """
    
    def __init__(
        self,
        columns: List[str],
        file_path: Optional[str] = None,
        available_columns: Optional[List[str]] = None
    ):
        self.missing_columns = columns
        self.available_columns = available_columns
        
        message = f"Brak wymaganych kolumn: {', '.join(columns)}"
        details = None
        if available_columns:
            details = f"Dostępne kolumny: {', '.join(available_columns[:10])}"
            if len(available_columns) > 10:
                details += f" ... (+{len(available_columns) - 10} więcej)"
        
        super().__init__(
            message=message,
            file_path=file_path,
            details=details
        )


class InvalidDataTypeError(IntervalsValidationError):
    """
    Raised when a column contains non-numeric values where numeric is expected.
    
    Example:
        >>> raise InvalidDataTypeError(
        ...     column='watts',
        ...     expected_type='numeric',
        ...     invalid_values=['N/A', 'error', '---'],
        ...     file_path='wahoo.csv'
        ... )
    """
    
    def __init__(
        self,
        column: str,
        expected_type: str,
        invalid_values: Optional[List[str]] = None,
        invalid_count: int = 0,
        file_path: Optional[str] = None
    ):
        self.expected_type = expected_type
        self.invalid_values = invalid_values
        self.invalid_count = invalid_count
        
        message = f"Kolumna '{column}' zawiera nieprawidłowe wartości (oczekiwano: {expected_type})"
        details = None
        if invalid_values:
            sample = invalid_values[:5]
            details = f"Przykłady: {sample}"
            if invalid_count > 5:
                details += f" ... (+{invalid_count - 5} więcej)"
        elif invalid_count > 0:
            details = f"Liczba nieprawidłowych wartości: {invalid_count}"
        
        super().__init__(
            message=message,
            file_path=file_path,
            column=column,
            details=details
        )


class TimestampError(IntervalsValidationError):
    """
    Raised when timestamp column has issues (non-monotonic, duplicates, gaps).
    
    Example:
        >>> raise TimestampError(
        ...     error_type='non_monotonic',
        ...     column='secs',
        ...     file_path='wahoo.csv',
        ...     details='Values decrease at row 150'
        ... )
    """
    
    def __init__(
        self,
        error_type: str,  # 'non_monotonic', 'duplicates', 'gaps', 'negative'
        column: str,
        file_path: Optional[str] = None,
        details: Optional[str] = None
    ):
        self.error_type = error_type
        
        messages = {
            'non_monotonic': f"Kolumna czasu '{column}' nie jest monotoniczna (wartości maleją)",
            'duplicates': f"Kolumna czasu '{column}' zawiera duplikaty",
            'gaps': f"Kolumna czasu '{column}' zawiera luki czasowe",
            'negative': f"Kolumna czasu '{column}' zawiera wartości ujemne"
        }
        
        message = messages.get(error_type, f"Błąd w kolumnie czasu '{column}'")
        
        super().__init__(
            message=message,
            file_path=file_path,
            column=column,
            details=details
        )


class SamplingFrequencyError(IntervalsValidationError):
    """
    Raised when sampling frequency doesn't match expected value.
    
    Example:
        >>> raise SamplingFrequencyError(
        ...     expected_freq=10,
        ...     detected_freq=5,
        ...     file_path='trainred.csv'
        ... )
    """
    
    def __init__(
        self,
        expected_freq: int,
        detected_freq: float,
        file_path: Optional[str] = None,
        tolerance: float = 0.1
    ):
        self.expected_freq = expected_freq
        self.detected_freq = detected_freq
        self.tolerance = tolerance
        
        message = (
            f"Nieoczekiwana częstotliwość próbkowania: "
            f"wykryto {detected_freq:.1f} Hz, oczekiwano {expected_freq} Hz"
        )
        
        super().__init__(
            message=message,
            file_path=file_path,
            details=f"Tolerancja: ±{tolerance * 100:.0f}%"
        )


class DataGapError(IntervalsValidationError):
    """
    Raised when data contains unacceptable gaps (consecutive NaN values).
    
    Example:
        >>> raise DataGapError(
        ...     column='smo2',
        ...     gap_size=25,
        ...     threshold=10,
        ...     file_path='trainred.csv'
        ... )
    """
    
    def __init__(
        self,
        column: str,
        gap_size: int,
        threshold: int,
        file_path: Optional[str] = None,
        row_start: Optional[int] = None
    ):
        self.gap_size = gap_size
        self.threshold = threshold
        self.row_start = row_start
        
        message = f"Zbyt duża luka w danych: {gap_size} pustych wierszy (próg: {threshold})"
        details = None
        if row_start is not None:
            details = f"Luka zaczyna się w wierszu {row_start}"
        
        super().__init__(
            message=message,
            file_path=file_path,
            column=column,
            details=details
        )


class FileFormatError(IntervalsValidationError):
    """
    Raised when file format is invalid or unrecognized.
    
    Example:
        >>> raise FileFormatError(
        ...     reason='empty_file',
        ...     file_path='empty.csv'
        ... )
    """
    
    def __init__(
        self,
        reason: str,  # 'empty_file', 'no_header', 'encoding_error', 'parse_error'
        file_path: Optional[str] = None,
        details: Optional[str] = None
    ):
        self.reason = reason
        
        reasons = {
            'empty_file': "Plik jest pusty",
            'no_header': "Nie znaleziono nagłówka",
            'encoding_error': "Błąd kodowania pliku",
            'parse_error': "Błąd parsowania CSV"
        }
        
        message = reasons.get(reason, f"Nieprawidłowy format pliku")
        
        super().__init__(
            message=message,
            file_path=file_path,
            details=details
        )
