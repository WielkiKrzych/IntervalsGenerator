import subprocess
import sys
from pathlib import Path
import pandas as pd
import pytest


@pytest.fixture
def project_root():
    return Path(__file__).parent.parent


@pytest.fixture
def quick_merge_script(project_root):
    return project_root / "quick_merge.py"


def test_quick_merge_cli_help(quick_merge_script):
    result = subprocess.run(
        [sys.executable, str(quick_merge_script), "--help"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "Uzycie" in result.stdout or "usage" in result.stdout


def test_quick_merge_no_wahoo(quick_merge_script, temp_dir):
    (temp_dir / "random.csv").touch()
    result = subprocess.run(
        [sys.executable, str(quick_merge_script)],
        cwd=temp_dir,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert "Error: No base file found" in result.stdout


def test_quick_merge_success(quick_merge_script, temp_dir):
    wahoo_df = pd.DataFrame({"secs": [0, 1, 2], "watts": [100, 110, 120]})
    wahoo_path = temp_dir / "activity_streams.csv"
    wahoo_df.to_csv(wahoo_path, index=False)

    trainred_df = pd.DataFrame(
        {
            "Timestamp (seconds passed)": [0.0, 0.1, 1.0, 1.1, 2.0],
            "SmO2": [60, 61, 62, 63, 64],
            "THb": [12, 12, 12, 12, 12],
        }
    )
    trainred_path = temp_dir / "session_test.csv"
    trainred_df.to_csv(trainred_path, index=False)

    result = subprocess.run(
        [sys.executable, str(quick_merge_script)],
        cwd=temp_dir,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0

    output_files = list(temp_dir.glob("Trening-*.csv"))
    assert len(output_files) == 1

    merged_df = pd.read_csv(output_files[0])
    assert "secs" in merged_df.columns
    assert "watts" in merged_df.columns
    assert "smo2" in merged_df.columns
    assert "THb" in merged_df.columns
    assert len(merged_df) == 3


def test_quick_merge_garmin_as_base(quick_merge_script, temp_dir):
    """Test merging with Garmin as base when Wahoo is not available."""
    # Garmin with all typical columns (like real Garmin streams.csv)
    garmin_df = pd.DataFrame({
        "secs": [0, 1, 2],
        "watts": [100, 110, 120],
        "cadence": [80, 82, 84],
        "heartrate": [140, 145, 150],
        "speed": [8.5, 8.6, 8.7],
        "distance": [100.0, 108.6, 117.2],
        "hrv": [45, 48, 42],
        "skin_temperature": [32.0, 32.1, 32.2]
    })
    garmin_path = temp_dir / "garmin_streams.csv"
    garmin_df.to_csv(garmin_path, index=False)

    trainred_df = pd.DataFrame({
        "Timestamp (seconds passed)": [0.0, 0.1, 1.0, 1.1, 2.0],
        "SmO2": [60, 61, 62, 63, 64],
        "THb": [12, 12, 12, 12, 12],
    })
    trainred_path = temp_dir / "session_test.csv"
    trainred_df.to_csv(trainred_path, index=False)

    result = subprocess.run(
        [sys.executable, str(quick_merge_script)],
        cwd=temp_dir,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "Base: Garmin" in result.stdout

    output_files = list(temp_dir.glob("Trening-*.csv"))
    assert len(output_files) == 1

    merged_df = pd.read_csv(output_files[0])
    # All Garmin columns should be preserved
    assert "secs" in merged_df.columns
    assert "watts" in merged_df.columns
    assert "cadence" in merged_df.columns
    assert "heartrate" in merged_df.columns
    assert "speed" in merged_df.columns
    assert "distance" in merged_df.columns
    assert "hrv" in merged_df.columns
    assert "skin_temperature" in merged_df.columns
    # TrainRed columns should be added
    assert "smo2" in merged_df.columns
    assert "THb" in merged_df.columns
    # Verify data integrity
    assert merged_df["watts"].tolist() == [100, 110, 120]
    assert merged_df["cadence"].tolist() == [80, 82, 84]


def test_quick_merge_trim_nan_tail(quick_merge_script, temp_dir):
    wahoo_df = pd.DataFrame({"secs": range(100), "watts": range(100)})
    wahoo_path = temp_dir / "activity_streams.csv"
    wahoo_df.to_csv(wahoo_path, index=False)

    hrv = [800] * 100
    hrv[99] = None
    garmin_df = pd.DataFrame({"secs": range(100), "hrv": hrv})
    garmin_path = temp_dir / "garmin_streams.csv"
    garmin_df.to_csv(garmin_path, index=False)

    result = subprocess.run(
        [sys.executable, str(quick_merge_script)],
        cwd=temp_dir,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0

    output_files = list(temp_dir.glob("Trening-*.csv"))
    merged_df = pd.read_csv(output_files[0])

    assert len(merged_df) == 99
    assert merged_df.isna().sum().sum() == 0
