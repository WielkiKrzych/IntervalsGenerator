"""
Edge case tests for CSV merge pipeline.

Tests cover critical edge cases:
1. CSV with missing optional columns (should not crash)
2. CSV with missing required columns (should fail validation)
3. CSV with time gaps (verify handling)
4. Mixed sampling rates (1 Hz + 10 Hz)
5. One vendor file present, others missing

All tests use small synthetic CSV fixtures for clarity.
"""

import pytest
import pandas as pd
import numpy as np
import tempfile
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from intervals.merger import DataMerger
from intervals.loaders import TrainRedLoader, TymewearLoader, WahooLoader, GarminLoader
from intervals.validators.integrity import IntegrityValidator
from intervals.config import Config
from intervals.exceptions import MissingColumnError


# ============================================================
# Fixtures: Synthetic CSV Data
# ============================================================

@pytest.fixture
def wahoo_base_df():
    """Standard Wahoo base DataFrame (10 seconds of data)."""
    return pd.DataFrame({
        'secs': list(range(10)),
        'watts': [100, 120, 140, 160, 180, 175, 170, 165, 160, 155],
        'cadence': [80, 82, 85, 87, 90, 88, 86, 84, 82, 80],
        'heartrate': [100, 110, 120, 130, 140, 138, 136, 134, 132, 130],
        'distance': np.cumsum([0, 2.5, 2.6, 2.7, 2.8, 2.7, 2.6, 2.5, 2.4, 2.3]),
        'speed': [0.0, 2.5, 2.6, 2.7, 2.8, 2.7, 2.6, 2.5, 2.4, 2.3],
        'altitude': [200.0, 200.5, 201.0, 201.5, 202.0, 201.8, 201.6, 201.4, 201.2, 201.0],
    })


@pytest.fixture
def wahoo_minimal_df():
    """Wahoo with only required column (secs)."""
    return pd.DataFrame({
        'secs': list(range(10))
    })


@pytest.fixture
def trainred_clean_df():
    """TrainRed clean data (already normalized to 1 Hz)."""
    return pd.DataFrame({
        'smo2': [65.0, 64.5, 64.0, 63.5, 63.0, 62.5, 62.0, 61.5, 61.0, 60.5],
        'THb': [12.1, 12.0, 11.9, 11.8, 11.7, 11.8, 11.9, 12.0, 12.1, 12.2]
    })


@pytest.fixture
def tymewear_clean_df():
    """Tymewear clean data."""
    return pd.DataFrame({
        'TymeBreathRate': [15, 16, 18, 20, 22, 21, 20, 19, 18, 17],
        'tidal_volume': [0.5, 0.55, 0.6, 0.65, 0.7, 0.68, 0.66, 0.64, 0.62, 0.6],
        'TymeVentilation': [7.5, 8.8, 10.8, 13.0, 15.4, 14.3, 13.2, 12.2, 11.2, 10.2]
    })


@pytest.fixture
def garmin_clean_df():
    """Garmin clean data (optional columns)."""
    return pd.DataFrame({
        'skin_temperature': [32.0, 32.1, 32.2, 32.3, 32.4, 32.5, 32.4, 32.3, 32.2, 32.1],
        'HeatStrainIndex': [0.1, 0.12, 0.14, 0.16, 0.18, 0.17, 0.16, 0.15, 0.14, 0.13],
        'hrv': [45, 48, 42, 50, 47, 44, 46, 49, 51, 48]
    })


# ============================================================
# Test 1: Missing Optional Columns (Should Not Crash)
# ============================================================

class TestMissingOptionalColumns:
    """Tests for handling missing optional columns gracefully."""
    
    def test_wahoo_without_watts_column(self, temp_dir, silent_ui, real_fs, test_config):
        """Wahoo file without watts column should merge successfully."""
        # Wahoo with secs but missing watts
        wahoo_df = pd.DataFrame({
            'secs': [0, 1, 2, 3, 4],
            'cadence': [80, 82, 84, 86, 88],
            'heartrate': [100, 110, 120, 130, 140]
            # watts is missing - optional
        })
        
        wahoo_path = temp_dir / "Wahoo.csv"
        wahoo_df.to_csv(wahoo_path, index=False)
        
        merger = DataMerger(test_config, real_fs, silent_ui)
        result = merger.merge_files(wahoo_df, [], validate_head=False, validate_tail=False)
        
        # Should succeed with available columns
        assert len(result) == 5
        assert 'secs' in result.columns
        assert 'cadence' in result.columns
        assert 'heartrate' in result.columns
        # watts is missing but that's OK
        assert 'watts' not in result.columns
    
    def test_garmin_without_hrv_column(self, temp_dir, silent_ui, real_fs, test_config):
        """Garmin file without hrv column should merge successfully."""
        wahoo_df = pd.DataFrame({
            'secs': [0, 1, 2, 3, 4],
            'watts': [100, 120, 140, 160, 180]
        })
        
        # Garmin with only skin_temperature (hrv and HeatStrainIndex missing)
        garmin_df = pd.DataFrame({
            'skin_temperature': [32.0, 32.1, 32.2, 32.3, 32.4]
        })
        garmin_path = temp_dir / "garmin_clean.csv"
        garmin_df.to_csv(garmin_path, index=False)
        
        merger = DataMerger(test_config, real_fs, silent_ui)
        result = merger.merge_files(
            wahoo_df, 
            [garmin_path], 
            validate_head=False, 
            validate_tail=False
        )
        
        # Should have merged Garmin data
        assert len(result) == 5
        assert 'skin_temperature' in result.columns
        # hrv is optional, should not crash
    
    def test_merge_with_no_additional_files(self, silent_ui, real_fs, test_config, wahoo_base_df):
        """Merge with only Wahoo base should work."""
        merger = DataMerger(test_config, real_fs, silent_ui)
        result = merger.merge_files(wahoo_base_df, [], validate_head=False, validate_tail=False)
        
        assert len(result) == 10
        assert list(result.columns) == list(wahoo_base_df.columns)


# ============================================================
# Test 2: Missing Required Columns (Should Fail Validation)
# ============================================================

class TestMissingRequiredColumns:
    """Tests for proper failure when required columns are missing."""
    
    def test_trainred_missing_smo2_fails_validation(self, silent_ui):
        """TrainRed without SmO2 should fail column validation."""
        df = pd.DataFrame({
            'THb unfiltered': [12.1, 12.0, 11.9],
            'Device': ['Sensor1', 'Sensor1', 'Sensor1']
            # SmO2 is required but missing
        })
        
        validator = IntegrityValidator(silent_ui, fail_fast=True)
        
        with pytest.raises(MissingColumnError) as exc_info:
            validator.validate_columns(df, ['SmO2', 'THb unfiltered'], 'trainred.csv')
        
        assert 'SmO2' in exc_info.value.missing_columns
        assert 'THb unfiltered' not in exc_info.value.missing_columns
    
    def test_tymewear_missing_all_required_fails(self, silent_ui):
        """Tymewear without BR/VT/VE should fail validation."""
        df = pd.DataFrame({
            'timestamp': [0, 1, 2],
            'device_id': ['dev1', 'dev1', 'dev1']
            # BR, VT, VE are all missing
        })
        
        validator = IntegrityValidator(silent_ui, fail_fast=True)
        
        with pytest.raises(MissingColumnError) as exc_info:
            validator.validate_columns(df, ['BR', 'VT', 'VE'], 'tymewear.csv')
        
        assert 'BR' in exc_info.value.missing_columns
        assert 'VT' in exc_info.value.missing_columns
        assert 'VE' in exc_info.value.missing_columns
    
    def test_wahoo_missing_secs_is_critical(self, silent_ui):
        """Wahoo without secs column is a critical error."""
        df = pd.DataFrame({
            'watts': [100, 120, 140],
            'cadence': [80, 82, 84]
            # secs is THE required column
        })
        
        validator = IntegrityValidator(silent_ui, fail_fast=True)
        
        with pytest.raises(MissingColumnError) as exc_info:
            validator.validate_columns(df, ['secs'], 'wahoo.csv')
        
        assert 'secs' in exc_info.value.missing_columns


# ============================================================
# Test 3: CSV with Time Gaps (Verify Handling)
# ============================================================

class TestTimeGaps:
    """Tests for proper handling of time gaps in data."""
    
    def test_small_gap_preserved_as_nan(self, temp_dir, silent_ui, real_fs, test_config):
        """Small gaps should be preserved as NaN in merged output."""
        wahoo_df = pd.DataFrame({
            'secs': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            'watts': [100, 120, np.nan, np.nan, 160, 180, 175, 170, 165, 160]
        })
        
        trainred_df = pd.DataFrame({
            'smo2': [65.0, 64.5, 64.0, 63.5, 63.0, 62.5, 62.0, 61.5, 61.0, 60.5],
            'THb': [12.1, 12.0, 11.9, 11.8, 11.7, 11.8, 11.9, 12.0, 12.1, 12.2]
        })
        trainred_path = temp_dir / "trainred_clean.csv"
        trainred_df.to_csv(trainred_path, index=False)
        
        merger = DataMerger(test_config, real_fs, silent_ui)
        result = merger.merge_files(
            wahoo_df, 
            [trainred_path], 
            validate_head=False, 
            validate_tail=False
        )
        
        # Gaps in watts should be preserved
        assert pd.isna(result.loc[2, 'watts'])
        assert pd.isna(result.loc[3, 'watts'])
        # TrainRed data should still be present
        assert result.loc[2, 'smo2'] == 64.0
        assert result.loc[3, 'smo2'] == 63.5
    
    def test_large_gap_detected_by_validator(self, silent_ui):
        """Large gaps (>threshold) should be detected by validator."""
        df = pd.DataFrame({
            'secs': list(range(20)),
            'watts': [100] * 5 + [np.nan] * 12 + [150] * 3  # 12 consecutive NaN
        })
        
        validator = IntegrityValidator(silent_ui, gap_threshold=10)
        issues = validator.validate(df, 'wahoo')
        
        assert len(issues) == 1
        assert 'watts' in issues[0]
        assert '12' in issues[0]
    
    def test_gap_in_middle_of_file(self, temp_dir, silent_ui, real_fs, test_config):
        """Gap in middle of file should not affect other columns."""
        wahoo_df = pd.DataFrame({
            'secs': list(range(10)),
            'watts': [100, 120, 140, 160, 180, 175, 170, 165, 160, 155],
            'cadence': [80, 82, np.nan, np.nan, np.nan, 88, 86, 84, 82, 80]  # Gap in cadence
        })
        
        merger = DataMerger(test_config, real_fs, silent_ui)
        result = merger.merge_files(wahoo_df, [], validate_head=False, validate_tail=False)
        
        # Watts should be unaffected
        assert not result['watts'].isna().any()
        # Cadence gaps preserved
        assert result['cadence'].isna().sum() == 3


# ============================================================
# Test 4: Mixed Sampling Rates
# ============================================================

class TestMixedSamplingRates:
    """Tests for handling different sampling rates."""
    
    def test_merge_1hz_with_normalized_10hz(self, temp_dir, silent_ui, real_fs, test_config):
        """
        Merge 1 Hz Wahoo with pre-normalized TrainRed (originally 10 Hz).
        Both should be at 1 Hz at merge time.
        """
        # Wahoo at 1 Hz (10 seconds)
        wahoo_df = pd.DataFrame({
            'secs': list(range(10)),
            'watts': [100 + i * 10 for i in range(10)]
        })
        
        # TrainRed already normalized to 1 Hz (from 10 Hz)
        trainred_df = pd.DataFrame({
            'smo2': [65.0 - i * 0.5 for i in range(10)],
            'THb': [12.0 + i * 0.1 for i in range(10)]
        })
        trainred_path = temp_dir / "trainred_clean.csv"
        trainred_df.to_csv(trainred_path, index=False)
        
        merger = DataMerger(test_config, real_fs, silent_ui)
        result = merger.merge_files(
            wahoo_df, 
            [trainred_path], 
            validate_head=False, 
            validate_tail=False
        )
        
        # Should have 10 rows (1 Hz × 10 seconds)
        assert len(result) == 10
        # Check alignment - row 0 should have first values
        assert result.loc[0, 'watts'] == 100
        assert result.loc[0, 'smo2'] == 65.0
        # Row 9 should have last values
        assert result.loc[9, 'watts'] == 190
        assert result.loc[9, 'smo2'] == 60.5
    
    def test_different_length_files_trimmed_to_shortest(
        self, temp_dir, silent_ui, real_fs, test_config
    ):
        """Files of different lengths should be aligned to shortest common length."""
        # Wahoo: 10 seconds
        wahoo_df = pd.DataFrame({
            'secs': list(range(10)),
            'watts': [100 + i * 10 for i in range(10)]
        })
        
        # TrainRed: only 7 seconds (shorter)
        trainred_df = pd.DataFrame({
            'smo2': [65.0 - i * 0.5 for i in range(7)],
            'THb': [12.0 + i * 0.1 for i in range(7)]
        })
        trainred_path = temp_dir / "trainred_clean.csv"
        trainred_df.to_csv(trainred_path, index=False)
        
        merger = DataMerger(test_config, real_fs, silent_ui)
        result = merger.merge_files(
            wahoo_df, 
            [trainred_path], 
            validate_head=False, 
            validate_tail=False
        )
        
        # Result should have 10 rows (Wahoo length)
        # but TrainRed columns will have NaN for rows 7-9
        assert len(result) == 10
        # First 7 rows should have TrainRed data
        assert not pd.isna(result.loc[0, 'smo2'])
        assert not pd.isna(result.loc[6, 'smo2'])
        # Rows 7-9 will have NaN for TrainRed columns
        assert pd.isna(result.loc[7, 'smo2'])
        assert pd.isna(result.loc[9, 'smo2'])
    
    def test_sampling_rate_detection(self, silent_ui):
        """Validator should detect sampling rate."""
        # 10 Hz data
        df_10hz = pd.DataFrame({
            'secs': [i * 0.1 for i in range(20)],  # 0.0, 0.1, 0.2, ...
            'value': list(range(20))
        })
        
        # 1 Hz data
        df_1hz = pd.DataFrame({
            'secs': list(range(10)),
            'value': list(range(10))
        })
        
        validator = IntegrityValidator(silent_ui)
        
        freq_10hz = validator.validate_sampling_frequency(df_10hz, 'secs', expected_freq=10)
        freq_1hz = validator.validate_sampling_frequency(df_1hz, 'secs', expected_freq=1)
        
        assert 9.5 <= freq_10hz <= 10.5
        assert freq_1hz == 1.0


# ============================================================
# Test 5: One Vendor File Present, Others Missing
# ============================================================

class TestPartialVendorData:
    """Tests for when only some vendor files are present."""
    
    def test_only_wahoo_present(self, silent_ui, real_fs, test_config, wahoo_base_df):
        """Pipeline should succeed with only Wahoo file."""
        merger = DataMerger(test_config, real_fs, silent_ui)
        result = merger.merge_files(
            wahoo_base_df, 
            [],  # No other files
            validate_head=False, 
            validate_tail=False
        )
        
        assert len(result) == 10
        assert 'secs' in result.columns
        assert 'watts' in result.columns
        # No TrainRed/Tymewear/Garmin columns
        assert 'smo2' not in result.columns
        assert 'TymeBreathRate' not in result.columns
        assert 'hrv' not in result.columns
    
    def test_wahoo_plus_trainred_only(
        self, temp_dir, silent_ui, real_fs, test_config, 
        wahoo_base_df, trainred_clean_df
    ):
        """Merge with Wahoo + TrainRed (no Tymewear/Garmin)."""
        trainred_path = temp_dir / "trainred_clean.csv"
        trainred_clean_df.to_csv(trainred_path, index=False)
        
        merger = DataMerger(test_config, real_fs, silent_ui)
        result = merger.merge_files(
            wahoo_base_df, 
            [trainred_path], 
            validate_head=False, 
            validate_tail=False
        )
        
        # Should have Wahoo + TrainRed columns
        assert 'secs' in result.columns
        assert 'watts' in result.columns
        assert 'smo2' in result.columns
        assert 'THb' in result.columns
        # No Tymewear/Garmin
        assert 'TymeBreathRate' not in result.columns
        assert 'hrv' not in result.columns
    
    def test_all_vendors_present(
        self, temp_dir, silent_ui, real_fs, test_config,
        wahoo_base_df, trainred_clean_df, tymewear_clean_df, garmin_clean_df
    ):
        """Merge with all four vendor files."""
        # Save clean files
        trainred_path = temp_dir / "trainred_clean.csv"
        tymewear_path = temp_dir / "tymewear_clean.csv"
        garmin_path = temp_dir / "garmin_clean.csv"
        
        trainred_clean_df.to_csv(trainred_path, index=False)
        tymewear_clean_df.to_csv(tymewear_path, index=False)
        garmin_clean_df.to_csv(garmin_path, index=False)
        
        merger = DataMerger(test_config, real_fs, silent_ui)
        result = merger.merge_files(
            wahoo_base_df, 
            [trainred_path, tymewear_path, garmin_path], 
            validate_head=False, 
            validate_tail=False
        )
        
        # Should have all columns from all sources
        expected_columns = [
            # Wahoo
            'secs', 'watts', 'cadence', 'heartrate', 'distance', 'speed', 'altitude',
            # TrainRed
            'smo2', 'THb',
            # Tymewear
            'TymeBreathRate', 'tidal_volume', 'TymeVentilation',
            # Garmin
            'skin_temperature', 'HeatStrainIndex', 'hrv'
        ]
        
        for col in expected_columns:
            assert col in result.columns, f"Missing column: {col}"
        
        # Verify correct number of rows
        assert len(result) == 10
    
    def test_order_independence(
        self, temp_dir, silent_ui, real_fs, test_config,
        wahoo_base_df, trainred_clean_df, tymewear_clean_df
    ):
        """Merge order should not affect result values."""
        trainred_path = temp_dir / "trainred_clean.csv"
        tymewear_path = temp_dir / "tymewear_clean.csv"
        
        trainred_clean_df.to_csv(trainred_path, index=False)
        tymewear_clean_df.to_csv(tymewear_path, index=False)
        
        merger = DataMerger(test_config, real_fs, silent_ui)
        
        # Order 1: TrainRed first
        result1 = merger.merge_files(
            wahoo_base_df.copy(), 
            [trainred_path, tymewear_path], 
            validate_head=False, 
            validate_tail=False
        )
        
        # Order 2: Tymewear first
        result2 = merger.merge_files(
            wahoo_base_df.copy(), 
            [tymewear_path, trainred_path], 
            validate_head=False, 
            validate_tail=False
        )
        
        # Values should be identical
        assert result1['smo2'].tolist() == result2['smo2'].tolist()
        assert result1['TymeBreathRate'].tolist() == result2['TymeBreathRate'].tolist()


# ============================================================
# Additional Edge Cases
# ============================================================

class TestAdditionalEdgeCases:
    """Additional edge case tests."""
    
    def test_empty_additional_file(self, temp_dir, silent_ui, real_fs, test_config, wahoo_base_df):
        """Empty additional file should be handled gracefully."""
        empty_path = temp_dir / "empty.csv"
        pd.DataFrame().to_csv(empty_path, index=False)
        
        merger = DataMerger(test_config, real_fs, silent_ui)
        # Should not crash, empty file ignored
        result = merger.merge_files(
            wahoo_base_df, 
            [empty_path], 
            validate_head=False, 
            validate_tail=False
        )
        
        assert len(result) == 10
    
    def test_duplicate_column_names_uses_base(
        self, temp_dir, silent_ui, real_fs, test_config, wahoo_base_df
    ):
        """When merging files with same column, base should be kept."""
        # File with duplicate 'watts' column (different values)
        duplicate_df = pd.DataFrame({
            'watts': [999, 998, 997, 996, 995, 994, 993, 992, 991, 990]
        })
        duplicate_path = temp_dir / "duplicate.csv"
        duplicate_df.to_csv(duplicate_path, index=False)
        
        merger = DataMerger(test_config, real_fs, silent_ui)
        result = merger.merge_files(
            wahoo_base_df, 
            [duplicate_path], 
            validate_head=False, 
            validate_tail=False
        )
        
        # Should keep base values, not the duplicate
        assert result.loc[0, 'watts'] == 100  # From wahoo, not 999
        assert result.loc[9, 'watts'] == 155  # From wahoo, not 990
    
    def test_single_row_files(self, temp_dir, silent_ui, real_fs, test_config):
        """Single-row files should merge without errors."""
        wahoo_df = pd.DataFrame({
            'secs': [0],
            'watts': [100]
        })
        
        trainred_df = pd.DataFrame({
            'smo2': [65.0],
            'THb': [12.0]
        })
        trainred_path = temp_dir / "trainred_clean.csv"
        trainred_df.to_csv(trainred_path, index=False)
        
        merger = DataMerger(test_config, real_fs, silent_ui)
        result = merger.merge_files(
            wahoo_df, 
            [trainred_path], 
            validate_head=False, 
            validate_tail=False
        )
        
        assert len(result) == 1
        assert result.loc[0, 'watts'] == 100
        assert result.loc[0, 'smo2'] == 65.0
