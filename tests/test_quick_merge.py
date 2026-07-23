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
    """Test that trailing rows with ANY NaN are trimmed (strict).

    merge_dataframes() now calls _trim_trailing_incomplete which strips
    trailing rows where ANY column has NaN. After merge, rows 3-4 have
    NaN in secs/watts (base was only 3 rows) — they get trimmed.
    """
    # Create a short Wahoo file (only 3 rows)
    wahoo_df = pd.DataFrame({"secs": range(3), "watts": [100, 110, 120]})
    wahoo_path = temp_dir / "activity_streams.csv"
    wahoo_df.to_csv(wahoo_path, index=False)

    # Garmin sensor file with 5 rows (2 extra rows beyond Wahoo)
    # After merge, rows 3-4 will have secs=NaN, watts=NaN but hrv filled
    # → strict trimming removes these trailing incomplete rows
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

    # Strict trim: rows 3-4 have NaN in secs/watts → trimmed
    # Only the first 3 rows (where ALL columns have values) survive
    assert len(merged_df) == 3
    # All remaining rows should be complete (no NaN anywhere)
    assert merged_df.notna().all().all()


# --- enhance_running_data ---

import importlib.util


@pytest.fixture
def quick_merge_module(quick_merge_script):
    spec = importlib.util.spec_from_file_location("quick_merge", quick_merge_script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_enhance_running_data_nan_cadence(quick_merge_module):
    """Dropout czujnika kadencji (NaN w srodku) nie moze crashowac."""
    df = pd.DataFrame({
        "VerticalOscillation": [8.1, 8.3, 8.2],
        "velocity_smooth": [3.0, 3.1, 3.2],
        "cadence": [85.0, None, 88.0],
    })
    out = quick_merge_module.enhance_running_data(df)
    # Half-cadence podwojona; NaN zachowany (nullable Int64)
    assert out["cadence"].dropna().tolist() == [170, 176]
    assert out["cadence"].isna().sum() == 1
    # Pace nie moze byc dodane jako kolumna (Intervals.icu go nie przyjmuje)
    assert "pace" not in out.columns


def test_enhance_running_data_skips_cycling(quick_merge_module):
    """Plik rowerowy (brak VerticalOscillation/stance_time) — kadencja nietknieta."""
    df = pd.DataFrame({
        "velocity_smooth": [8.0, 8.1],
        "cadence": [90.0, 92.0],
        "watts": [250, 255],
    })
    out = quick_merge_module.enhance_running_data(df)
    assert out["cadence"].tolist() == [90.0, 92.0]


def test_enhance_running_data_real_full_cadence_not_doubled(quick_merge_module):
    """Bieg z prawdziwa pelna kadencja (mediana >= 110) nie jest podwajany."""
    df = pd.DataFrame({
        "VerticalOscillation": [8.0, 8.0, 8.0],
        "velocity_smooth": [3.0, 3.1, 3.2],
        "cadence": [170.0, 172.0, 174.0],
    })
    out = quick_merge_module.enhance_running_data(df)
    assert out["cadence"].tolist() == [170.0, 172.0, 174.0]


def test_enhance_running_data_zero_speed_no_crash(quick_merge_module):
    """Same zera predkosci (postoj) — brak crasha na pace."""
    df = pd.DataFrame({
        "VerticalOscillation": [8.0, 8.0],
        "velocity_smooth": [0.0, 0.0],
        "cadence": [80.0, 82.0],
    })
    out = quick_merge_module.enhance_running_data(df)
    assert out["cadence"].dropna().tolist() == [160, 164]


# --- Wyrownanie po czasie (merge_dataframes) ---

def test_merge_time_align_interior_gap(quick_merge_module):
    """Luka w srodku zrodla: SmO2 nie moze sie przesunac (cichy shift)."""
    base = pd.DataFrame({"time": [0, 1, 2, 3, 4], "watts": [100, 110, 120, 130, 140]})
    aux = pd.DataFrame({"second": [0, 1, 3, 4], "smo2": [60, 61, 63, 64]})
    res = quick_merge_module.merge_dataframes(base, [aux])
    assert len(res) == 5
    assert pd.isna(res.loc[2, "smo2"])      # brak sekundy 2 -> uczciwy NaN
    assert res.loc[3, "smo2"] == 63.0        # sekunda 3 zostaje przy 3
    assert res.loc[4, "smo2"] == 64.0
    assert "second" not in res.columns       # klucz aux nie wycieka


def test_merge_time_align_offset_start(quick_merge_module):
    """Zrodlo z przesunietym wlasnym zegarem — normalizacja do 0 wyrownuje."""
    base = pd.DataFrame({"time": [0, 1, 2, 3], "watts": [100, 110, 120, 130]})
    # TrainRed liczy czas od 1000; po normalizacji -> 0,1,2,3
    aux = pd.DataFrame({"second": [1000, 1001, 1002, 1003], "smo2": [60, 61, 62, 63]})
    res = quick_merge_module.merge_dataframes(base, [aux])
    assert res["smo2"].tolist() == [60.0, 61.0, 62.0, 63.0]


def test_merge_positional_fallback_no_key(quick_merge_module):
    """Zrodlo bez klucza czasu -> laczenie pozycyjne (zachowanie jak dawniej)."""
    base = pd.DataFrame({"time": [0, 1, 2], "watts": [100, 110, 120]})
    aux = pd.DataFrame({"smo2": [60, 61, 62]})  # brak time/secs/second
    res = quick_merge_module.merge_dataframes(base, [aux])
    assert res["smo2"].tolist() == [60.0, 61.0, 62.0]


def test_merge_base_without_time_key_positional(quick_merge_module):
    """Baza bez klucza czasu -> pelny fallback pozycyjny, klucz aux odrzucony."""
    base = pd.DataFrame({"watts": [100, 110, 120]})  # brak time/secs
    aux = pd.DataFrame({"second": [0, 1, 2], "smo2": [60, 61, 62]})
    res = quick_merge_module.merge_dataframes(base, [aux])
    assert res["smo2"].tolist() == [60.0, 61.0, 62.0]
    assert "second" not in res.columns


def test_norm_seconds_zero_based(quick_merge_module):
    """_norm_seconds sprowadza do 0-based int, odporne na float i NaN."""
    out = quick_merge_module._norm_seconds(pd.Series([10.0, 11.0, 13.0]))
    assert out.tolist() == [0, 1, 3]
