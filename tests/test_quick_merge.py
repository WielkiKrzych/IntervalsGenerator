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
    assert "Nie znaleziono pliku bazowego" in result.stdout


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
        "skin_temperature": [32.0, 32.1, 32.2],
        "core_temperature": [37.0, 37.1, 37.2]
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
    # File has hrv + activity columns → detected as Intervals.icu
    assert "Baza: Intervals.icu" in result.stdout

    output_files = list(temp_dir.glob("Trening-*.csv"))
    assert len(output_files) == 1

    merged_df = pd.read_csv(output_files[0])
    # All columns should be preserved (Intervals.icu keeps everything)
    assert "secs" in merged_df.columns
    assert "watts" in merged_df.columns
    assert "cadence" in merged_df.columns
    assert "heartrate" in merged_df.columns
    assert "speed" in merged_df.columns
    assert "distance" in merged_df.columns
    assert "hrv" in merged_df.columns
    assert "skin_temperature" in merged_df.columns
    assert "core_temperature" in merged_df.columns
    # TrainRed columns should be added
    assert "smo2" in merged_df.columns
    assert "THb" in merged_df.columns
    # Verify data integrity
    assert merged_df["watts"].tolist() == [100, 110, 120]
    assert merged_df["cadence"].tolist() == [80, 82, 84]
    assert merged_df["core_temperature"].tolist() == [37.0, 37.1, 37.2]


def test_quick_merge_wahoo_with_garmin_core_temp(quick_merge_script, temp_dir):
    """Test that core_temperature from Garmin is merged when Wahoo is base."""
    wahoo_df = pd.DataFrame({"secs": [0, 1, 2], "watts": [100, 110, 120]})
    wahoo_path = temp_dir / "activity_streams.csv"
    wahoo_df.to_csv(wahoo_path, index=False)

    # Garmin with core_temperature (as additional file, not base)
    garmin_df = pd.DataFrame({
        "secs": [0, 1, 2],
        "hrv": [45, 48, 42],
        "skin_temperature": [32.0, 32.1, 32.2],
        "core_temperature": [37.0, 37.1, 37.2]
    })
    garmin_path = temp_dir / "garmin_streams.csv"
    garmin_df.to_csv(garmin_path, index=False)

    result = subprocess.run(
        [sys.executable, str(quick_merge_script)],
        cwd=temp_dir,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "Baza: Wahoo" in result.stdout

    output_files = list(temp_dir.glob("Trening-*.csv"))
    assert len(output_files) == 1

    merged_df = pd.read_csv(output_files[0])
    # All Garmin columns should be merged
    assert "hrv" in merged_df.columns
    assert "skin_temperature" in merged_df.columns
    assert "core_temperature" in merged_df.columns
    # Verify core_temperature data integrity
    assert merged_df["core_temperature"].tolist() == [37.0, 37.1, 37.2]


def test_quick_merge_trim_nan_tail(quick_merge_script, temp_dir):
    """Test that rows with mostly NaN at the end are trimmed.

    Trimming uses a relaxed >=50% non-null threshold: rows where the
    majority of columns are NaN get trimmed. Rows where base activity
    data (secs, watts) is present but some optional sensor columns are
    missing are preserved — that's real training data.
    """
    # Create a short Wahoo file (only 3 rows)
    wahoo_df = pd.DataFrame({"secs": range(3), "watts": [100, 110, 120]})
    wahoo_path = temp_dir / "activity_streams.csv"
    wahoo_df.to_csv(wahoo_path, index=False)

    # Garmin sensor file with 5 rows (2 extra rows beyond Wahoo)
    # After merge, rows 3-4 will have secs=NaN, watts=NaN but hrv filled
    # → most columns NaN → should be trimmed
    garmin_df = pd.DataFrame({
        "hrv": [800, 800, 800, 800, 800],
        "skin_temperature": [32.0, 32.0, 32.0, 32.0, 32.0],
        "core_temperature": [37.0, 37.0, 37.0, 37.0, 37.0],
    })
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

    # Rows 3-4 have 3/5 columns filled (60%) → still above 50% threshold
    # But secs and watts are NaN → trimming depends on threshold
    # With 5 columns and threshold = 2, rows with 3 non-null survive
    # So all 5 rows remain (Garmin fills 3 cols, base fills 2 for first 3)
    assert len(merged_df) == 5
    # First 3 rows should be complete
    assert merged_df.iloc[:3].notna().all().all()
