"""
Unit tests for filesystem operations.
"""

import pytest
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from intervals.filesystem import RealFileSystem, DryRunFileSystem


class TestRealFileSystem:
    """Tests for RealFileSystem class."""
    
    def test_exists_true(self, temp_dir):
        """Test exists returns True for existing path."""
        fs = RealFileSystem()
        assert fs.exists(temp_dir) is True
    
    def test_exists_false(self, temp_dir):
        """Test exists returns False for non-existing path."""
        fs = RealFileSystem()
        assert fs.exists(temp_dir / "nonexistent") is False
    
    def test_glob_finds_files(self, temp_dir):
        """Test glob finds matching files."""
        # Create some files
        (temp_dir / "test1.csv").touch()
        (temp_dir / "test2.csv").touch()
        (temp_dir / "other.txt").touch()
        
        fs = RealFileSystem()
        results = fs.glob(temp_dir, "*.csv")
        
        assert len(results) == 2
        assert all(p.suffix == ".csv" for p in results)
    
    def test_copy_creates_file(self, temp_dir):
        """Test copy creates destination file."""
        src = temp_dir / "source.txt"
        src.write_text("test content")
        dst = temp_dir / "dest.txt"
        
        fs = RealFileSystem()
        fs.copy(src, dst)
        
        assert dst.exists()
        assert dst.read_text() == "test content"
    
    def test_write_and_read_csv(self, temp_dir, sample_wahoo_df):
        """Test CSV write and read roundtrip."""
        path = temp_dir / "test.csv"
        
        fs = RealFileSystem()
        fs.write_csv(sample_wahoo_df, path, index=False)
        
        result = fs.read_csv(path)
        
        pd.testing.assert_frame_equal(result, sample_wahoo_df)


class TestDryRunFileSystem:
    """Tests for dry-run mode."""
    
    def test_dry_run_copy_no_file_created(self, temp_dir):
        """Test dry-run copy doesn't create file."""
        src = temp_dir / "source.txt"
        src.write_text("test")
        dst = temp_dir / "dest.txt"
        
        fs = DryRunFileSystem()
        fs.copy(src, dst)
        
        assert not dst.exists()
    
    def test_dry_run_logs_operation(self, temp_dir):
        """Test dry-run logs the operation."""
        src = temp_dir / "source.txt"
        src.write_text("test")
        dst = temp_dir / "dest.txt"
        
        fs = DryRunFileSystem()
        fs.copy(src, dst)
        
        operations = fs.get_operations_log()
        assert len(operations) == 1
        assert "COPY" in operations[0]
    
    def test_dry_run_move_no_file_moved(self, temp_dir):
        """Test dry-run move doesn't actually move."""
        src = temp_dir / "source.txt"
        src.write_text("test")
        dst = temp_dir / "dest.txt"
        
        fs = DryRunFileSystem()
        fs.move(src, dst)
        
        assert src.exists()  # Still there
        assert not dst.exists()
    
    def test_dry_run_mkdir_no_dir_created(self, temp_dir):
        """Test dry-run mkdir doesn't create directory."""
        new_dir = temp_dir / "new_folder"
        
        fs = DryRunFileSystem()
        fs.mkdir(new_dir)
        
        assert not new_dir.exists()
    
    def test_dry_run_write_csv_no_file(self, temp_dir, sample_wahoo_df):
        """Test dry-run write_csv doesn't create file."""
        path = temp_dir / "test.csv"
        
        fs = DryRunFileSystem()
        fs.write_csv(sample_wahoo_df, path, index=False)
        
        assert not path.exists()
        
        operations = fs.get_operations_log()
        assert any("WRITE CSV" in op for op in operations)
