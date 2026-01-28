#!/usr/bin/env python3
"""
Benchmark suite for Intervals Generator.
Measures performance for different file sizes and counts.

Usage:
    python benchmarks/benchmark.py
    python benchmarks/benchmark.py --size large
    python benchmarks/benchmark.py --profile
"""

import argparse
import time
import tempfile
import shutil
from pathlib import Path
from typing import Dict, List, Tuple
import sys

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import numpy as np

from intervals.config import Config
from intervals.pipeline import Pipeline
from intervals.filesystem import RealFileSystem
from intervals.ui import SilentUI
from intervals.loaders import TrainRedLoader, TymewearLoader, WahooLoader
from intervals.merger import DataMerger
from intervals.interpolation import interpolate_time_gaps, resample_to_frequency


# ============================================================
# Test Data Generators
# ============================================================

def generate_wahoo_data(rows: int) -> pd.DataFrame:
    """Generate synthetic Wahoo data."""
    return pd.DataFrame({
        'secs': range(rows),
        'watts': np.random.randint(100, 350, rows),
        'cadence': np.random.randint(60, 100, rows),
        'heartrate': np.random.randint(80, 180, rows),
        'distance': np.cumsum(np.random.uniform(2, 5, rows)),
        'speed': np.random.uniform(5, 15, rows),
        'altitude': np.cumsum(np.random.uniform(-1, 1, rows)) + 200,
    })


def generate_trainred_data(rows: int, frequency: int = 10) -> pd.DataFrame:
    """Generate synthetic TrainRed data at given frequency."""
    total_samples = rows * frequency
    return pd.DataFrame({
        'Timestamp (seconds passed)': np.arange(0, rows, 1/frequency)[:total_samples],
        'SmO2': np.random.uniform(50, 80, total_samples),
        'THb unfiltered': np.random.uniform(10, 14, total_samples),
        'Device': ['Sensor1'] * total_samples,
    })


def generate_tymewear_data(rows: int) -> pd.DataFrame:
    """Generate synthetic Tymewear data."""
    return pd.DataFrame({
        'BR': np.random.randint(10, 40, rows),
        'VT': np.random.uniform(0.3, 1.5, rows),
        'VE': np.random.uniform(5, 50, rows),
    })


def generate_garmin_data(rows: int) -> pd.DataFrame:
    """Generate synthetic Garmin data."""
    return pd.DataFrame({
        'secs': range(rows),
        'skin_temperature': np.random.uniform(30, 38, rows),
        'HeatStrainIndex': np.random.uniform(0, 0.5, rows),
        'hrv': np.random.randint(20, 100, rows),
    })


# ============================================================
# Benchmark Functions
# ============================================================

def benchmark_merge(rows: int, num_files: int = 3) -> Dict[str, float]:
    """Benchmark merge operation."""
    base_df = generate_wahoo_data(rows)
    clean_files = []
    
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # Create clean files
        for i in range(num_files):
            df = pd.DataFrame({
                f'col_{i}_a': np.random.random(rows),
                f'col_{i}_b': np.random.random(rows),
            })
            path = tmpdir / f'clean_{i}.csv'
            df.to_csv(path, index=False)
            clean_files.append(path)
        
        config = Config.for_testing(tmpdir)
        fs = RealFileSystem()
        ui = SilentUI()
        merger = DataMerger(config, fs, ui)
        
        # Benchmark
        start = time.perf_counter()
        result = merger.merge_files(base_df, clean_files, validate_head=False, validate_tail=False)
        end = time.perf_counter()
        
        return {
            'rows': rows,
            'files': num_files,
            'columns': len(result.columns),
            'time_seconds': end - start,
            'rows_per_second': rows / (end - start),
        }


def benchmark_normalization(rows: int, frequency: int = 10) -> Dict[str, float]:
    """Benchmark TrainRed normalization (10Hz -> 1Hz)."""
    df = generate_trainred_data(rows, frequency)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        path = tmpdir / 'session_test.csv'
        df.to_csv(path, index=False)
        
        config = Config.for_testing(tmpdir)
        config._trainred_dir = tmpdir
        fs = RealFileSystem()
        ui = SilentUI()
        loader = TrainRedLoader(config, fs, ui)
        
        start = time.perf_counter()
        result = loader._normalize_to_1hz(path)
        end = time.perf_counter()
        
        input_rows = len(df)
        output_rows = len(result) if result is not None else 0
        
        return {
            'input_rows': input_rows,
            'output_rows': output_rows,
            'frequency': frequency,
            'time_seconds': end - start,
            'samples_per_second': input_rows / (end - start),
        }


def benchmark_interpolation(rows: int, gap_size: int = 5) -> Dict[str, float]:
    """Benchmark time gap interpolation."""
    df = generate_wahoo_data(rows)
    
    # Add gaps
    gap_positions = np.random.choice(range(10, rows - 10), size=rows // 100, replace=False)
    for pos in gap_positions:
        df.loc[pos:pos+gap_size, 'watts'] = np.nan
    
    start = time.perf_counter()
    result, filled = interpolate_time_gaps(df, max_gap=gap_size + 1)
    end = time.perf_counter()
    
    return {
        'rows': rows,
        'gaps_added': len(gap_positions),
        'values_filled': filled,
        'time_seconds': end - start,
    }


def benchmark_resampling(rows: int, from_freq: int = 10, to_freq: int = 1) -> Dict[str, float]:
    """Benchmark frequency resampling."""
    df = generate_trainred_data(rows, from_freq)
    
    start = time.perf_counter()
    result = resample_to_frequency(
        df, 
        time_col='Timestamp (seconds passed)',
        target_freq=to_freq,
        current_freq=from_freq
    )
    end = time.perf_counter()
    
    return {
        'input_rows': len(df),
        'output_rows': len(result),
        'from_freq': from_freq,
        'to_freq': to_freq,
        'time_seconds': end - start,
    }


def benchmark_full_pipeline(rows: int) -> Dict[str, float]:
    """Benchmark full pipeline execution."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # Create directory structure
        wahoo_dir = tmpdir / '3_Wahoo_files'
        wahoo_dir.mkdir()
        
        # Create Wahoo file
        wahoo_df = generate_wahoo_data(rows)
        wahoo_df.to_csv(wahoo_dir / 'Wahoo.csv', index=False)
        
        config = Config.for_testing(tmpdir)
        fs = RealFileSystem()
        ui = SilentUI()
        pipeline = Pipeline(config, fs, ui)
        
        start = time.perf_counter()
        result = pipeline.run_merge()
        end = time.perf_counter()
        
        return {
            'rows': rows,
            'time_seconds': end - start,
            'output_file': str(result) if result else None,
        }


# ============================================================
# Memory Profiling
# ============================================================

def profile_memory(rows: int = 10000) -> Dict[str, float]:
    """Profile memory usage for merge operation."""
    try:
        import tracemalloc
    except ImportError:
        return {'error': 'tracemalloc not available'}
    
    tracemalloc.start()
    
    base_df = generate_wahoo_data(rows)
    trainred_df = generate_trainred_data(rows, 10)
    tymewear_df = generate_tymewear_data(rows)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        trainred_df.to_csv(tmpdir / 'trainred.csv', index=False)
        tymewear_df.to_csv(tmpdir / 'tymewear.csv', index=False)
        
        config = Config.for_testing(tmpdir)
        fs = RealFileSystem()
        ui = SilentUI()
        merger = DataMerger(config, fs, ui)
        
        result = merger.merge_files(
            base_df,
            [tmpdir / 'trainred.csv', tmpdir / 'tymewear.csv'],
            validate_head=False,
            validate_tail=False
        )
    
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    
    return {
        'rows': rows,
        'current_mb': current / 1024 / 1024,
        'peak_mb': peak / 1024 / 1024,
        'mb_per_1000_rows': peak / 1024 / 1024 / (rows / 1000),
    }


# ============================================================
# Batch Processing Benchmark
# ============================================================

def benchmark_batch_processing(file_count: int, rows_per_file: int = 1000) -> Dict[str, float]:
    """Benchmark processing multiple files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        # Create multiple Wahoo files
        files = []
        for i in range(file_count):
            df = generate_wahoo_data(rows_per_file)
            path = tmpdir / f'wahoo_{i}.csv'
            df.to_csv(path, index=False)
            files.append(path)
        
        # Time reading all files
        start = time.perf_counter()
        dfs = [pd.read_csv(f) for f in files]
        read_time = time.perf_counter() - start
        
        # Time concatenating
        start = time.perf_counter()
        combined = pd.concat(dfs, ignore_index=True)
        concat_time = time.perf_counter() - start
        
        return {
            'file_count': file_count,
            'rows_per_file': rows_per_file,
            'total_rows': len(combined),
            'read_time': read_time,
            'concat_time': concat_time,
            'total_time': read_time + concat_time,
            'files_per_second': file_count / (read_time + concat_time),
        }


# ============================================================
# Main Runner
# ============================================================

def run_benchmarks(size: str = 'medium', profile: bool = False) -> List[Dict]:
    """Run all benchmarks."""
    sizes = {
        'small': {'rows': 1000, 'files': 10},
        'medium': {'rows': 10000, 'files': 50},
        'large': {'rows': 100000, 'files': 200},
    }
    
    params = sizes.get(size, sizes['medium'])
    rows = params['rows']
    files = params['files']
    
    results = []
    
    print(f"\n{'='*60}")
    print(f"BENCHMARK SUITE - Size: {size.upper()}")
    print(f"{'='*60}\n")
    
    # Merge benchmark
    print("📊 Merge benchmark...")
    result = benchmark_merge(rows, num_files=5)
    results.append({'name': 'merge', **result})
    print(f"   {rows:,} rows, 5 files: {result['time_seconds']:.3f}s ({result['rows_per_second']:,.0f} rows/s)")
    
    # Normalization benchmark
    print("📊 Normalization benchmark (10Hz → 1Hz)...")
    result = benchmark_normalization(rows // 10)  # Smaller due to 10x expansion
    results.append({'name': 'normalization', **result})
    print(f"   {result['input_rows']:,} samples: {result['time_seconds']:.3f}s ({result['samples_per_second']:,.0f} samples/s)")
    
    # Interpolation benchmark
    print("📊 Interpolation benchmark...")
    result = benchmark_interpolation(rows)
    results.append({'name': 'interpolation', **result})
    print(f"   {rows:,} rows: {result['time_seconds']:.3f}s, {result['values_filled']} values filled")
    
    # Resampling benchmark
    print("📊 Resampling benchmark...")
    result = benchmark_resampling(rows // 10)
    results.append({'name': 'resampling', **result})
    print(f"   {result['input_rows']:,} → {result['output_rows']:,} rows: {result['time_seconds']:.3f}s")
    
    # Batch processing
    print(f"📊 Batch processing ({files} files)...")
    result = benchmark_batch_processing(files, rows_per_file=rows // 10)
    results.append({'name': 'batch', **result})
    print(f"   {files} files: {result['total_time']:.3f}s ({result['files_per_second']:.1f} files/s)")
    
    # Memory profiling
    if profile:
        print("📊 Memory profiling...")
        result = profile_memory(rows)
        results.append({'name': 'memory', **result})
        if 'error' not in result:
            print(f"   Peak memory: {result['peak_mb']:.1f} MB ({result['mb_per_1000_rows']:.2f} MB per 1000 rows)")
    
    print(f"\n{'='*60}")
    print("BENCHMARK COMPLETE")
    print(f"{'='*60}\n")
    
    return results


def main():
    parser = argparse.ArgumentParser(description='Benchmark Intervals Generator')
    parser.add_argument('--size', choices=['small', 'medium', 'large'], default='medium',
                       help='Benchmark size (default: medium)')
    parser.add_argument('--profile', action='store_true',
                       help='Include memory profiling')
    parser.add_argument('--json', action='store_true',
                       help='Output results as JSON')
    
    args = parser.parse_args()
    
    results = run_benchmarks(args.size, args.profile)
    
    if args.json:
        import json
        print(json.dumps(results, indent=2))


if __name__ == '__main__':
    main()
