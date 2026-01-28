"""
Pytest configuration and fixtures for Intervals Generator tests.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from intervals.config import Config
from intervals.filesystem import RealFileSystem
from intervals.ui import SilentUI


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    tmp = Path(tempfile.mkdtemp())
    yield tmp
    shutil.rmtree(tmp, ignore_errors=True)


@pytest.fixture
def test_config(temp_dir):
    """Create a test configuration with temporary directories."""
    config = Config.for_testing(temp_dir)
    config.ensure_directories()
    return config


@pytest.fixture
def silent_ui():
    """Create a silent UI for testing."""
    return SilentUI(default_yes_no=True)


@pytest.fixture
def real_fs():
    """Create a real filesystem instance."""
    return RealFileSystem(dry_run=False)


@pytest.fixture
def dry_run_fs():
    """Create a dry-run filesystem instance."""
    return RealFileSystem(dry_run=True)


@pytest.fixture
def sample_trainred_df():
    """Sample TrainRed DataFrame after normalization."""
    return pd.DataFrame({
        'second': [0, 1, 2, 3, 4],
        'SmO2': [65.2, 65.1, 64.8, 64.5, 64.3],
        'THb unfiltered': [12.1, 12.0, 11.9, 11.8, 11.7],
        'samples_per_second': [10, 10, 10, 10, 10]
    })


@pytest.fixture
def sample_tymewear_df():
    """Sample Tymewear DataFrame."""
    return pd.DataFrame({
        'BR': [14, 15, 16, 17, 18],
        'VT': [0.5, 0.6, 0.7, 0.8, 0.9],
        'VE': [7.0, 9.0, 11.2, 13.6, 16.2]
    })


@pytest.fixture
def sample_wahoo_df():
    """Sample Wahoo DataFrame (base)."""
    return pd.DataFrame({
        'secs': [0, 1, 2, 3, 4],
        'watts': [0, 150, 165, 180, 175],
        'cadence': [0, 78, 82, 85, 83],
        'heartrate': [85, 92, 98, 105, 108],
        'distance': [0, 2.5, 5.1, 7.8, 10.5]
    })


@pytest.fixture
def sample_garmin_df():
    """Sample Garmin DataFrame."""
    return pd.DataFrame({
        'skin_temperature': [32.1, 32.2, 32.3, 32.4, 32.5],
        'HeatStrainIndex': [0.1, 0.12, 0.15, 0.18, 0.2],
        'hrv': [45, 48, 42, 50, 47]
    })


@pytest.fixture
def trainred_csv_file(temp_dir, sample_trainred_df):
    """Create a sample TrainRed CSV file."""
    file_path = temp_dir / "session_test_avg.csv"
    sample_trainred_df.to_csv(file_path, index=False)
    return file_path


@pytest.fixture
def wahoo_csv_file(temp_dir, sample_wahoo_df):
    """Create a sample Wahoo CSV file."""
    file_path = temp_dir / "Wahoo.csv"
    sample_wahoo_df.to_csv(file_path, index=False)
    return file_path


@pytest.fixture
def df_with_gaps():
    """DataFrame with consecutive NaN gaps for validation testing."""
    data = {
        'col1': [1, 2, None, None, None, None, None, None, None, None, None, None, 13, 14],
        'col2': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]
    }
    return pd.DataFrame(data)
