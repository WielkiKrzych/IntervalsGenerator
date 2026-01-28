"""
Unit tests for data merger.
"""

import pytest
import pandas as pd
import numpy as np
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from intervals.merger import DataMerger


class TestDataMerger:
    """Tests for DataMerger class."""
    
    def test_merge_single_file(self, test_config, real_fs, silent_ui, temp_dir, sample_wahoo_df, sample_trainred_df):
        """Test merging a single clean file into base."""
        # Setup
        clean_file = temp_dir / "trainred_clean.csv"
        sample_trainred_df[['SmO2', 'THb unfiltered']].rename(columns={
            'SmO2': 'smo2', 'THb unfiltered': 'THb'
        }).to_csv(clean_file, index=False)
        
        merger = DataMerger(test_config, real_fs, silent_ui)
        
        # Execute
        result = merger.merge_files(
            sample_wahoo_df,
            [clean_file],
            validate_head=False,
            validate_tail=False
        )
        
        # Assert
        assert 'smo2' in result.columns
        assert 'THb' in result.columns
        assert 'watts' in result.columns  # From base
        assert len(result) == len(sample_wahoo_df)
    
    def test_merge_multiple_files(self, test_config, real_fs, silent_ui, temp_dir, sample_wahoo_df):
        """Test merging multiple clean files."""
        # Create two clean files
        clean1 = temp_dir / "clean1.csv"
        pd.DataFrame({'smo2': [1, 2, 3, 4, 5]}).to_csv(clean1, index=False)
        
        clean2 = temp_dir / "clean2.csv"
        pd.DataFrame({'THb': [10, 20, 30, 40, 50]}).to_csv(clean2, index=False)
        
        merger = DataMerger(test_config, real_fs, silent_ui)
        
        result = merger.merge_files(
            sample_wahoo_df,
            [clean1, clean2],
            validate_head=False,
            validate_tail=False
        )
        
        assert 'smo2' in result.columns
        assert 'THb' in result.columns
        assert len(result.columns) == len(sample_wahoo_df.columns) + 2
    
    def test_merge_ignores_duplicate_columns(self, test_config, real_fs, silent_ui, temp_dir, sample_wahoo_df):
        """Test that duplicate columns in clean files are ignored."""
        # Create clean file with column that exists in base
        clean_file = temp_dir / "clean.csv"
        pd.DataFrame({
            'watts': [999, 999, 999, 999, 999],  # Duplicate
            'new_col': [1, 2, 3, 4, 5]  # New
        }).to_csv(clean_file, index=False)
        
        merger = DataMerger(test_config, real_fs, silent_ui)
        
        result = merger.merge_files(
            sample_wahoo_df,
            [clean_file],
            validate_head=False,
            validate_tail=False
        )
        
        # Original watts should be preserved, not overwritten
        assert result['watts'].iloc[1] == 150  # Original value
        assert 'new_col' in result.columns
    
    def test_merge_empty_file_list(self, test_config, real_fs, silent_ui, sample_wahoo_df):
        """Test merge with no clean files returns base unchanged."""
        merger = DataMerger(test_config, real_fs, silent_ui)
        
        result = merger.merge_files(
            sample_wahoo_df,
            [],
            validate_head=False,
            validate_tail=False
        )
        
        pd.testing.assert_frame_equal(result, sample_wahoo_df.reset_index(drop=True))


class TestMergerValidation:
    """Tests for head/tail validation during merge."""
    
    def test_trim_tail_removes_incomplete_rows(self, test_config, real_fs, silent_ui):
        """Test that incomplete rows at tail are detected."""
        # Create base with NaN at end
        base_df = pd.DataFrame({
            'secs': [0, 1, 2, 3, 4],
            'watts': [100, 150, 175, np.nan, np.nan]
        })
        
        merger = DataMerger(test_config, real_fs, silent_ui)
        
        # With SilentUI (default_yes_no=True), it should trim
        result = merger.merge_files(
            base_df,
            [],
            validate_head=False,
            validate_tail=True
        )
        
        # Last valid row is index 2 (value 175)
        assert len(result) == 3
