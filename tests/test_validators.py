"""
Unit tests for data validators.
"""

import pytest
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from intervals.validators.integrity import IntegrityValidator
from intervals.utils import check_consecutive_nans_optimized


class TestIntegrityValidator:
    """Tests for IntegrityValidator class."""
    
    def test_validate_clean_data(self, silent_ui, sample_wahoo_df):
        """Test validation of clean data without gaps."""
        validator = IntegrityValidator(silent_ui, gap_threshold=10)
        issues = validator.validate(sample_wahoo_df, "Wahoo")
        
        assert len(issues) == 0
    
    def test_validate_data_with_gaps(self, silent_ui, df_with_gaps):
        """Test validation detects large gaps."""
        validator = IntegrityValidator(silent_ui, gap_threshold=10)
        issues = validator.validate(df_with_gaps, "Test")
        
        # col1 has 10 consecutive NaNs
        assert len(issues) == 1
        assert "col1" in issues[0]
    
    def test_validate_below_threshold(self, silent_ui):
        """Test that gaps below threshold don't trigger issues."""
        df = pd.DataFrame({
            'col1': [1, None, None, None, 5, 6, 7]  # 3 consecutive NaNs
        })
        validator = IntegrityValidator(silent_ui, gap_threshold=10)
        issues = validator.validate(df, "Test")
        
        assert len(issues) == 0
    
    def test_custom_threshold(self, silent_ui):
        """Test custom gap threshold."""
        df = pd.DataFrame({
            'col1': [1, None, None, None, None, None, 7]  # 5 consecutive NaNs
        })
        validator = IntegrityValidator(silent_ui, gap_threshold=5)
        issues = validator.validate(df, "Test")
        
        assert len(issues) == 1


class TestConsecutiveNansOptimized:
    """Tests for optimized NaN checking function."""
    
    def test_no_nans(self):
        """Test series with no NaN values."""
        series = pd.Series([1, 2, 3, 4, 5])
        assert check_consecutive_nans_optimized(series) == 0
    
    def test_single_nan(self):
        """Test series with single NaN."""
        series = pd.Series([1, None, 3, 4, 5])
        assert check_consecutive_nans_optimized(series) == 1
    
    def test_consecutive_nans(self):
        """Test series with consecutive NaNs."""
        series = pd.Series([1, None, None, None, 5])
        assert check_consecutive_nans_optimized(series) == 3
    
    def test_multiple_gaps(self):
        """Test series with multiple gaps - returns max."""
        # Gap 1: positions 1-2 (2 NaNs)
        # Gap 2: positions 4-6 (3 NaNs)
        # Total: 5 NaNs, threshold=3 triggers full RLE analysis
        # Expected: 3 (max consecutive gap)
        series = pd.Series([1, None, None, 4, None, None, None, 8])
        # With threshold=3, total NaNs (5) >= threshold, so full RLE runs
        assert check_consecutive_nans_optimized(series, threshold=3) == 3
    
    def test_early_exit_optimization(self):
        """Test early exit when total NaNs < threshold."""
        series = pd.Series([1, None, None, 4, 5, 6, 7, 8, 9])  # 2 NaNs total
        result = check_consecutive_nans_optimized(series, threshold=10)
        assert result == 2  # Early exit returns total count
    
    def test_empty_string_as_nan(self):
        """Test that empty strings are treated as NaN."""
        series = pd.Series([1, '', '', 4, 5])
        assert check_consecutive_nans_optimized(series) == 2
