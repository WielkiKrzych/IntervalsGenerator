"""
Tests for data validation with custom exceptions.

Tests cover:
- Missing required columns
- Corrupted numeric columns
- Timestamp issues (non-monotonic, duplicates, negative)
- Sampling frequency validation
- Data gaps (consecutive NaN)
"""

import pytest
import pandas as pd
import numpy as np
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from intervals.validators.integrity import IntegrityValidator
from intervals.exceptions import (
    IntervalsValidationError,
    MissingColumnError,
    InvalidDataTypeError,
    TimestampError,
    SamplingFrequencyError,
    DataGapError,
    FileFormatError
)


class TestMissingColumnValidation:
    """Tests for missing required column detection."""
    
    def test_missing_single_column_raises_error(self, silent_ui):
        """Missing single required column should raise MissingColumnError."""
        df = pd.DataFrame({
            'timestamp': [0, 1, 2],
            'THb': [12.1, 12.0, 11.9]
        })
        
        validator = IntegrityValidator(silent_ui, fail_fast=True)
        
        with pytest.raises(MissingColumnError) as exc_info:
            validator.validate_columns(df, ['SmO2', 'THb'], 'trainred.csv')
        
        assert 'SmO2' in exc_info.value.missing_columns
        assert 'THb' not in exc_info.value.missing_columns
        assert 'trainred.csv' in str(exc_info.value)
    
    def test_missing_multiple_columns_raises_error(self, silent_ui):
        """Missing multiple required columns listed in error."""
        df = pd.DataFrame({
            'timestamp': [0, 1, 2]
        })
        
        validator = IntegrityValidator(silent_ui, fail_fast=True)
        
        with pytest.raises(MissingColumnError) as exc_info:
            validator.validate_columns(df, ['SmO2', 'THb'], 'trainred.csv')
        
        assert 'SmO2' in exc_info.value.missing_columns
        assert 'THb' in exc_info.value.missing_columns
    
    def test_all_columns_present_no_error(self, silent_ui):
        """No error when all required columns present."""
        df = pd.DataFrame({
            'SmO2': [65.0, 64.5, 64.0],
            'THb': [12.1, 12.0, 11.9]
        })
        
        validator = IntegrityValidator(silent_ui)
        
        # Should not raise
        validator.validate_columns(df, ['SmO2', 'THb'])
    
    def test_available_columns_in_error_message(self, silent_ui):
        """Available columns should be listed in error message."""
        df = pd.DataFrame({
            'column_a': [1, 2, 3],
            'column_b': [4, 5, 6]
        })
        
        validator = IntegrityValidator(silent_ui)
        
        with pytest.raises(MissingColumnError) as exc_info:
            validator.validate_columns(df, ['missing_col'], 'test.csv')
        
        assert 'column_a' in exc_info.value.available_columns
        assert 'column_b' in exc_info.value.available_columns


class TestNumericColumnValidation:
    """Tests for numeric column validation."""
    
    def test_valid_numeric_column_no_error(self, silent_ui):
        """Valid numeric column should not raise error."""
        df = pd.DataFrame({
            'watts': [100, 150, 200, 180, 160]
        })
        
        validator = IntegrityValidator(silent_ui)
        warnings = validator.validate_numeric_columns(df, ['watts'])
        
        assert len(warnings) == 0
    
    def test_non_numeric_values_warning(self, silent_ui):
        """Non-numeric values below 50% should generate warning."""
        df = pd.DataFrame({
            'watts': [100, 150, 'error', 180, 160]  # 20% invalid
        })
        
        validator = IntegrityValidator(silent_ui)
        warnings = validator.validate_numeric_columns(df, ['watts'])
        
        assert len(warnings) == 1
        assert 'watts' in warnings[0]
        assert 'nienumeryczn' in warnings[0].lower()
    
    def test_mostly_invalid_raises_error(self, silent_ui):
        """Column with >50% invalid values should raise error."""
        df = pd.DataFrame({
            'watts': ['error', 'N/A', '---', 180, 160]  # 60% invalid
        })
        
        validator = IntegrityValidator(silent_ui, fail_fast=True)
        
        with pytest.raises(InvalidDataTypeError) as exc_info:
            validator.validate_numeric_columns(df, ['watts'], 'wahoo.csv')
        
        assert exc_info.value.column == 'watts'
        assert exc_info.value.expected_type == 'numeric'
    
    def test_nan_not_counted_as_invalid(self, silent_ui):
        """NaN values should not be counted as invalid types."""
        df = pd.DataFrame({
            'watts': [100, np.nan, np.nan, 180, 160]  # NaN is valid
        })
        
        validator = IntegrityValidator(silent_ui)
        warnings = validator.validate_numeric_columns(df, ['watts'])
        
        assert len(warnings) == 0


class TestTimestampValidation:
    """Tests for timestamp validation."""
    
    def test_monotonic_timestamps_no_warning(self, silent_ui):
        """Monotonically increasing timestamps should not warn."""
        df = pd.DataFrame({
            'secs': [0, 1, 2, 3, 4, 5]
        })
        
        validator = IntegrityValidator(silent_ui)
        warnings = validator.validate_timestamps(df, 'secs')
        
        assert len(warnings) == 0
    
    def test_non_monotonic_timestamps_warning(self, silent_ui):
        """Non-monotonic timestamps should generate warning."""
        df = pd.DataFrame({
            'secs': [0, 1, 2, 1, 3, 4]  # Decreases at index 3
        })
        
        validator = IntegrityValidator(silent_ui)
        warnings = validator.validate_timestamps(df, 'secs')
        
        assert len(warnings) >= 1
        assert any('malej' in w.lower() for w in warnings)
    
    def test_non_monotonic_fail_fast_raises(self, silent_ui):
        """Non-monotonic with fail_fast should raise TimestampError."""
        df = pd.DataFrame({
            'secs': [0, 1, 2, 1, 3, 4]
        })
        
        validator = IntegrityValidator(silent_ui, fail_fast=True)
        
        with pytest.raises(TimestampError) as exc_info:
            validator.validate_timestamps(df, 'secs', 'wahoo.csv')
        
        assert exc_info.value.error_type == 'non_monotonic'
        assert exc_info.value.column == 'secs'
    
    def test_duplicate_timestamps_warning(self, silent_ui):
        """Duplicate timestamps should generate warning."""
        df = pd.DataFrame({
            'secs': [0, 1, 1, 2, 3]  # Duplicate at index 2
        })
        
        validator = IntegrityValidator(silent_ui)
        warnings = validator.validate_timestamps(df, 'secs')
        
        assert any('duplik' in w.lower() for w in warnings)
    
    def test_negative_timestamps_fail_fast(self, silent_ui):
        """Negative timestamps with fail_fast should raise."""
        df = pd.DataFrame({
            'secs': [-2, -1, 0, 1, 2]
        })
        
        validator = IntegrityValidator(silent_ui, fail_fast=True)
        
        with pytest.raises(TimestampError) as exc_info:
            validator.validate_timestamps(df, 'secs')
        
        assert exc_info.value.error_type == 'negative'


class TestSamplingFrequencyValidation:
    """Tests for sampling frequency validation."""
    
    def test_correct_frequency_no_error(self, silent_ui):
        """Correct frequency should return detected value without error."""
        df = pd.DataFrame({
            'secs': [0.0, 0.1, 0.2, 0.3, 0.4, 0.5]  # 10 Hz
        })
        
        validator = IntegrityValidator(silent_ui)
        freq = validator.validate_sampling_frequency(df, 'secs', expected_freq=10)
        
        assert 9.0 <= freq <= 11.0
    
    def test_wrong_frequency_warning(self, silent_ui):
        """Wrong frequency should warn but not raise by default."""
        df = pd.DataFrame({
            'secs': [0, 1, 2, 3, 4, 5]  # 1 Hz, not 10 Hz
        })
        
        validator = IntegrityValidator(silent_ui)
        freq = validator.validate_sampling_frequency(df, 'secs', expected_freq=10)
        
        assert freq == 1.0
    
    def test_wrong_frequency_fail_fast_raises(self, silent_ui):
        """Wrong frequency with fail_fast should raise."""
        df = pd.DataFrame({
            'secs': [0, 1, 2, 3, 4, 5]  # 1 Hz, not 10 Hz
        })
        
        validator = IntegrityValidator(silent_ui, fail_fast=True)
        
        with pytest.raises(SamplingFrequencyError) as exc_info:
            validator.validate_sampling_frequency(df, 'secs', expected_freq=10)
        
        assert exc_info.value.expected_freq == 10
        assert exc_info.value.detected_freq == 1.0


class TestDataGapValidation:
    """Tests for consecutive NaN (data gap) validation."""
    
    def test_no_gaps_no_warning(self, silent_ui):
        """Data without gaps should not warn."""
        df = pd.DataFrame({
            'smo2': [65.0, 64.5, 64.0, 63.5, 63.0]
        })
        
        validator = IntegrityValidator(silent_ui, gap_threshold=3)
        warnings = validator.validate_data_gaps(df)
        
        assert len(warnings) == 0
    
    def test_small_gap_no_warning(self, silent_ui):
        """Gaps smaller than threshold should not warn."""
        df = pd.DataFrame({
            'smo2': [65.0, np.nan, np.nan, 63.5, 63.0]  # 2 consecutive NaN
        })
        
        validator = IntegrityValidator(silent_ui, gap_threshold=3)
        warnings = validator.validate_data_gaps(df)
        
        assert len(warnings) == 0
    
    def test_large_gap_warning(self, silent_ui):
        """Gaps equal or larger than threshold should warn."""
        df = pd.DataFrame({
            'smo2': [65.0] + [np.nan] * 5 + [63.0]  # 5 consecutive NaN
        })
        
        validator = IntegrityValidator(silent_ui, gap_threshold=3)
        warnings = validator.validate_data_gaps(df)
        
        assert len(warnings) == 1
        assert 'smo2' in warnings[0]
        assert '5' in warnings[0]
    
    def test_large_gap_fail_fast_raises(self, silent_ui):
        """Large gap with fail_fast should raise DataGapError."""
        df = pd.DataFrame({
            'smo2': [65.0] + [np.nan] * 5 + [63.0]
        })
        
        validator = IntegrityValidator(silent_ui, gap_threshold=3, fail_fast=True)
        
        with pytest.raises(DataGapError) as exc_info:
            validator.validate_data_gaps(df, file_path='trainred.csv')
        
        assert exc_info.value.column == 'smo2'
        assert exc_info.value.gap_size == 5
        assert exc_info.value.threshold == 3


class TestFullValidation:
    """Tests for comprehensive validate_full method."""
    
    def test_valid_data_returns_true(self, silent_ui):
        """Valid data should return is_valid=True."""
        df = pd.DataFrame({
            'secs': [0, 1, 2, 3, 4],
            'watts': [100, 150, 160, 170, 180],
            'smo2': [65.0, 64.5, 64.0, 63.5, 63.0]
        })
        
        validator = IntegrityValidator(silent_ui)
        is_valid, errors, warnings = validator.validate_full(df, 'test')
        
        assert is_valid is True
        assert len(errors) == 0
    
    def test_empty_dataframe_invalid(self, silent_ui):
        """Empty DataFrame should be invalid."""
        df = pd.DataFrame()
        
        validator = IntegrityValidator(silent_ui)
        is_valid, errors, warnings = validator.validate_full(df, 'test')
        
        assert is_valid is False
        assert len(errors) == 1
        assert 'pusty' in errors[0].lower()
    
    def test_strict_mode_treats_warnings_as_errors(self, silent_ui):
        """Strict mode should treat warnings as errors."""
        df = pd.DataFrame({
            'secs': [0, 1, 1, 2, 3],  # Duplicate timestamps = warning
            'watts': [100, 150, 160, 170, 180]
        })
        
        validator = IntegrityValidator(silent_ui, strict_mode=True)
        is_valid, errors, warnings = validator.validate_full(df, 'test')
        
        assert is_valid is False  # Warning treated as error in strict mode


class TestExceptionMessages:
    """Tests for exception message formatting."""
    
    def test_missing_column_error_format(self):
        """MissingColumnError should have clear format."""
        error = MissingColumnError(
            columns=['SmO2', 'THb'],
            file_path='trainred.csv',
            available_columns=['Timestamp', 'Device']
        )
        
        message = str(error)
        assert 'SmO2' in message
        assert 'THb' in message
        assert 'trainred.csv' in message
    
    def test_invalid_data_type_error_format(self):
        """InvalidDataTypeError should include examples."""
        error = InvalidDataTypeError(
            column='watts',
            expected_type='numeric',
            invalid_values=['N/A', 'error', '---'],
            invalid_count=10,
            file_path='wahoo.csv'
        )
        
        message = str(error)
        assert 'watts' in message
        assert 'numeric' in message
        assert 'N/A' in message
    
    def test_timestamp_error_format(self):
        """TimestampError should have clear error type."""
        error = TimestampError(
            error_type='non_monotonic',
            column='secs',
            file_path='wahoo.csv',
            details='Value decreases at row 150'
        )
        
        message = str(error)
        assert 'secs' in message
        assert 'monoton' in message.lower() or 'malej' in message.lower()
    
    def test_data_gap_error_format(self):
        """DataGapError should include gap size and threshold."""
        error = DataGapError(
            column='smo2',
            gap_size=25,
            threshold=10,
            file_path='trainred.csv',
            row_start=150
        )
        
        message = str(error)
        assert 'smo2' in message
        assert '25' in message
        assert '10' in message
