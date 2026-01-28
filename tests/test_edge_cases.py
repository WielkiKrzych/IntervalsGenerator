"""
Edge case tests for Intervals Generator.
Tests unusual but realistic scenarios that the tool should handle gracefully.
"""

import pytest
import pandas as pd
import numpy as np
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from intervals.validators.column_validator import ColumnValidator
from intervals.validators.integrity import IntegrityValidator
from intervals.interpolation import (
    interpolate_time_gaps,
    resample_to_frequency,
    detect_sampling_rate,
    align_time_series
)
from intervals.loaders import LoaderRegistry


class TestMissingColumns:
    """Tests for handling missing required columns."""
    
    def test_trainred_missing_smo2(self, silent_ui):
        """TrainRed file without SmO2 column should fail validation gracefully."""
        df = pd.DataFrame({
            'Timestamp (seconds passed)': [0.0, 0.1, 0.2],
            'THb unfiltered': [12.1, 12.0, 11.9],
            'Device': ['Sensor1', 'Sensor1', 'Sensor1']
        })
        
        validator = ColumnValidator(silent_ui)
        result = validator.validate_columns(df, ['SmO2', 'THb unfiltered'])
        
        assert not result['is_valid']
        assert 'SmO2' in result['missing_columns']
        assert 'THb unfiltered' not in result['missing_columns']
    
    def test_wahoo_completely_empty_file(self, silent_ui):
        """Empty CSV should be handled gracefully."""
        df = pd.DataFrame()
        
        validator = ColumnValidator(silent_ui)
        result = validator.validate_columns(df, ['secs', 'watts'])
        
        assert not result['is_valid']
        assert 'secs' in result['missing_columns']
        assert 'watts' in result['missing_columns']
    
    def test_fuzzy_column_matching(self, silent_ui):
        """Similar column names should be suggested."""
        df = pd.DataFrame({
            'SmO_2': [65.2, 65.1],  # Close to SmO2
            'THB_unfiltered': [12.1, 12.0]  # Close to THb unfiltered
        })
        
        validator = ColumnValidator(silent_ui)
        result = validator.validate_columns(df, ['SmO2', 'THb unfiltered'])
        
        # Should suggest mappings
        assert len(result['suggested_mappings']) > 0
    
    def test_case_insensitive_matching(self, silent_ui):
        """Column matching should be case-insensitive."""
        df = pd.DataFrame({
            'SMO2': [65.2, 65.1],  # Uppercase
            'thb UNFILTERED': [12.1, 12.0]  # Mixed case
        })
        
        validator = ColumnValidator(silent_ui)
        result = validator.validate_columns(df, ['SmO2', 'THb unfiltered'])
        
        # Should find columns despite case difference
        assert result['is_valid'] or len(result['suggested_mappings']) > 0


class TestTimestampErrors:
    """Tests for handling timestamp issues."""
    
    def test_non_numeric_timestamp(self):
        """Handle non-numeric values in timestamp column."""
        df = pd.DataFrame({
            'secs': ['start', '1', '2', 'end'],
            'watts': [0, 150, 165, 0]
        })
        
        # Convert to numeric, coercing errors
        df['secs_numeric'] = pd.to_numeric(df['secs'], errors='coerce')
        
        assert df['secs_numeric'].isna().sum() == 2  # 'start' and 'end' become NaN
    
    def test_negative_timestamps(self):
        """Handle negative timestamp values."""
        df = pd.DataFrame({
            'secs': [-2, -1, 0, 1, 2],
            'watts': [0, 0, 100, 150, 165]
        })
        
        # Should be able to filter negative values
        df_positive = df[df['secs'] >= 0]
        assert len(df_positive) == 3
    
    def test_out_of_order_timestamps(self):
        """Handle timestamps that aren't monotonic."""
        df = pd.DataFrame({
            'secs': [0, 2, 1, 3, 5, 4],  # Out of order
            'watts': [0, 165, 150, 180, 200, 190]
        })
        
        # Sort by timestamp
        df_sorted = df.sort_values('secs').reset_index(drop=True)
        
        assert df_sorted['secs'].tolist() == [0, 1, 2, 3, 4, 5]
        assert df_sorted['watts'].tolist() == [0, 150, 165, 180, 190, 200]
    
    def test_duplicate_timestamps(self):
        """Handle duplicate timestamp values."""
        df = pd.DataFrame({
            'secs': [0, 1, 1, 2, 3, 3, 3],
            'watts': [0, 150, 155, 165, 180, 182, 185]
        })
        
        # Aggregate duplicates
        df_dedup = df.groupby('secs').agg({'watts': 'mean'}).reset_index()
        
        assert len(df_dedup) == 4
        assert df_dedup.loc[df_dedup['secs'] == 1, 'watts'].values[0] == 152.5


class TestSamplingRates:
    """Tests for different sampling frequencies."""
    
    def test_detect_10hz_sampling(self):
        """Detect 10Hz sampling rate."""
        df = pd.DataFrame({
            'secs': [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
        })
        
        rate = detect_sampling_rate(df, 'secs')
        
        assert 9.5 <= rate <= 10.5  # Allow small tolerance
    
    def test_detect_1hz_sampling(self):
        """Detect 1Hz sampling rate."""
        df = pd.DataFrame({
            'secs': [0, 1, 2, 3, 4, 5]
        })
        
        rate = detect_sampling_rate(df, 'secs')
        
        assert rate == 1.0
    
    def test_resample_10hz_to_1hz(self):
        """Resample 10Hz data to 1Hz."""
        # Create 10Hz data (10 samples per second)
        df_10hz = pd.DataFrame({
            'secs': [i * 0.1 for i in range(20)],
            'watts': [100 + i for i in range(20)]
        })
        
        df_1hz = resample_to_frequency(df_10hz, target_freq=1, current_freq=10)
        
        assert len(df_1hz) == 2  # 2 seconds of data
    
    def test_variable_sampling_rate(self):
        """Handle data with inconsistent sampling rate."""
        df = pd.DataFrame({
            'secs': [0, 0.1, 0.3, 0.5, 1.0, 1.1, 2.0],  # Irregular intervals
            'watts': [100, 110, 130, 150, 200, 210, 300]
        })
        
        rate = detect_sampling_rate(df, 'secs')
        
        # Should return a reasonable estimate
        assert rate > 0
    
    def test_merge_different_lengths(self):
        """Merge files of different lengths should use common range."""
        df1 = pd.DataFrame({'secs': [0, 1, 2, 3, 4, 5], 'watts': [100, 150, 160, 170, 180, 190]})
        df2 = pd.DataFrame({'secs': [1, 2, 3], 'smo2': [65, 64, 63]})  # Shorter
        
        aligned = align_time_series([df1, df2], 'secs')
        
        assert len(aligned[0]) == 3  # Trimmed to common range
        assert len(aligned[1]) == 3


class TestDataFormats:
    """Tests for inconsistent data formats."""
    
    def test_comma_decimal_separator(self):
        """Handle European decimal format (comma instead of period)."""
        # Simulate European format
        values = ['65,2', '65,1', '64,8']
        
        # Convert comma to period
        cleaned = [float(v.replace(',', '.')) for v in values]
        
        assert cleaned == [65.2, 65.1, 64.8]
    
    def test_whitespace_in_column_names(self, silent_ui):
        """Handle whitespace in column names."""
        df = pd.DataFrame({
            '  SmO2  ': [65.2, 65.1],
            ' THb unfiltered': [12.1, 12.0]
        })
        
        # Normalize column names
        df.columns = [str(c).strip() for c in df.columns]
        
        assert 'SmO2' in df.columns
        assert 'THb unfiltered' in df.columns
    
    def test_mixed_types_in_column(self):
        """Handle columns with mixed data types."""
        df = pd.DataFrame({
            'watts': [100, '150', 160.5, None, 'error']
        })
        
        # Convert to numeric with coercion
        df['watts_clean'] = pd.to_numeric(df['watts'], errors='coerce')
        
        assert df['watts_clean'].isna().sum() == 2  # None and 'error'
        assert df['watts_clean'].iloc[0] == 100
        assert df['watts_clean'].iloc[1] == 150


class TestInterpolation:
    """Tests for time gap interpolation."""
    
    def test_linear_interpolation_small_gap(self):
        """Linear interpolation fills small gaps."""
        df = pd.DataFrame({
            'secs': [0, 1, 2, 3, 4],
            'watts': [100, np.nan, np.nan, 160, 170]
        })
        
        df_filled, count = interpolate_time_gaps(df, max_gap=3)
        
        assert count == 2
        assert not df_filled['watts'].isna().any()
        # Check linear interpolation values
        assert df_filled['watts'].iloc[1] == 120  # (100 + 160) / 2 midway doesn't apply, linear interp
    
    def test_no_interpolation_large_gap(self):
        """Large gaps should not be interpolated."""
        df = pd.DataFrame({
            'secs': range(10),
            'watts': [100, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, 170, 180, 190]
        })
        
        df_filled, count = interpolate_time_gaps(df, max_gap=3)
        
        # Gap of 6 should not be fully filled
        assert df_filled['watts'].isna().sum() > 0
    
    def test_ffill_interpolation(self):
        """Forward fill should propagate last value."""
        df = pd.DataFrame({
            'secs': [0, 1, 2, 3, 4],
            'watts': [100, 110, np.nan, np.nan, 140]
        })
        
        df_filled, count = interpolate_time_gaps(df, method='ffill', max_gap=3)
        
        assert count == 2
        assert df_filled['watts'].iloc[2] == 110  # Forward filled
        assert df_filled['watts'].iloc[3] == 110


class TestLoaderRegistry:
    """Tests for the plugin loader registry."""
    
    def test_all_loaders_registered(self):
        """All built-in loaders should be registered."""
        available = LoaderRegistry.available_loaders()
        
        assert 'trainred' in available
        assert 'tymewear' in available
        assert 'wahoo' in available
        assert 'garmin' in available
    
    def test_loader_priority_order(self):
        """Loaders should be sorted by priority."""
        available = LoaderRegistry.available_loaders()
        
        # Wahoo (priority=1) should come first
        assert available[0] == 'wahoo'
    
    def test_get_loader_metadata(self):
        """Can retrieve loader metadata."""
        meta = LoaderRegistry.get_metadata('trainred')
        
        assert 'priority' in meta
        assert 'description' in meta
        assert meta['priority'] == 10
    
    def test_get_nonexistent_loader_raises(self):
        """Getting non-existent loader should raise KeyError."""
        with pytest.raises(KeyError):
            LoaderRegistry.get_loader('nonexistent_loader')


class TestValidatorIntegration:
    """Integration tests for validators."""
    
    def test_validate_file_with_gaps(self, silent_ui):
        """Validator detects gaps in real-world-like data."""
        # Simulate sensor dropout
        data = {
            'secs': list(range(100)),
            'smo2': [65.0] * 40 + [np.nan] * 15 + [60.0] * 45
        }
        df = pd.DataFrame(data)
        
        validator = IntegrityValidator(silent_ui, gap_threshold=10)
        issues = validator.validate(df, 'TrainRed')
        
        assert len(issues) == 1
        assert 'smo2' in issues[0]
        assert '15' in issues[0]  # 15 consecutive NaNs
    
    def test_validate_file_without_gaps(self, silent_ui):
        """Clean data passes validation."""
        df = pd.DataFrame({
            'secs': range(100),
            'watts': [100 + i for i in range(100)]
        })
        
        validator = IntegrityValidator(silent_ui, gap_threshold=10)
        issues = validator.validate(df, 'Wahoo')
        
        assert len(issues) == 0
