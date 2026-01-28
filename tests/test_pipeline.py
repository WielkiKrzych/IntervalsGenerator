"""
Integration tests for the full pipeline.
"""

import pytest
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from intervals.config import Config
from intervals.pipeline import Pipeline
from intervals.filesystem import RealFileSystem
from intervals.ui import SilentUI


class TestPipelineIntegration:
    """Integration tests for Pipeline class."""
    
    def test_full_pipeline_with_wahoo_only(self, test_config, sample_wahoo_df):
        """Test pipeline runs with just Wahoo file."""
        # Setup: Create Wahoo file
        wahoo_dir = test_config.wahoo_dir
        wahoo_dir.mkdir(parents=True, exist_ok=True)
        wahoo_file = wahoo_dir / "Wahoo.csv"
        sample_wahoo_df.to_csv(wahoo_file, index=False)
        
        # Create pipeline
        fs = RealFileSystem()
        ui = SilentUI()
        pipeline = Pipeline(test_config, fs=fs, ui=ui)
        
        # Run merge only (skip import)
        result = pipeline.run_merge()
        
        assert result is not None
        assert result.exists()
        assert "Trening-" in result.name
    
    def test_validation_detects_gaps(self, test_config, df_with_gaps):
        """Test pipeline validation detects data gaps."""
        # Setup: Create file with gaps
        trainred_dir = test_config.trainred_dir
        trainred_dir.mkdir(parents=True, exist_ok=True)
        gap_file = trainred_dir / "gap_clean.csv"
        df_with_gaps.to_csv(gap_file, index=False)
        
        # Create pipeline
        fs = RealFileSystem()
        ui = SilentUI()
        pipeline = Pipeline(test_config, fs=fs, ui=ui)
        
        # Run validation
        is_valid = pipeline.run_validation()
        
        # Should detect gaps
        assert is_valid is False
    
    def test_cleanup_moves_files(self, test_config, sample_wahoo_df):
        """Test cleanup moves files to old directories."""
        # Setup: Create file in source dir
        wahoo_dir = test_config.wahoo_dir
        wahoo_dir.mkdir(parents=True, exist_ok=True)
        wahoo_file = wahoo_dir / "test.csv"
        sample_wahoo_df.to_csv(wahoo_file, index=False)
        
        # Create pipeline
        fs = RealFileSystem()
        ui = SilentUI()
        pipeline = Pipeline(test_config, fs=fs, ui=ui)
        
        # Run cleanup
        moved = pipeline.run_cleanup()
        
        assert moved >= 1
        assert not wahoo_file.exists()
        assert (test_config.wahoo_old_dir / "test.csv").exists()


class TestPipelineDryRun:
    """Tests for pipeline in dry-run mode."""
    
    def test_dry_run_no_files_modified(self, test_config, sample_wahoo_df):
        """Test dry-run doesn't modify any files."""
        # Setup
        wahoo_dir = test_config.wahoo_dir
        wahoo_dir.mkdir(parents=True, exist_ok=True)
        wahoo_file = wahoo_dir / "Wahoo.csv"
        sample_wahoo_df.to_csv(wahoo_file, index=False)
        
        original_content = wahoo_file.read_text()
        
        # Run in dry-run mode
        fs = RealFileSystem(dry_run=True)
        ui = SilentUI()
        pipeline = Pipeline(test_config, fs=fs, ui=ui)
        
        pipeline.run_cleanup()
        
        # File should still be there unchanged
        assert wahoo_file.exists()
        assert wahoo_file.read_text() == original_content
