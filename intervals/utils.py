"""
Utilities for parallel I/O operations.
Provides optimized concurrent file reading for better performance.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Callable, TypeVar
import pandas as pd

from .config import Config

logger = logging.getLogger(__name__)


T = TypeVar("T")


def find_header_row(
    path: Path, keywords: List[str], max_lines: int = None
) -> Optional[int]:
    # Use default from config if not provided
    if max_lines is None:
        max_lines = Config.HEADER_SCAN_MAX_LINES
    """
    Find the row index containing the header by searching for keywords.

    Args:
        path: Path to CSV file
        keywords: List of strings to search for (all must be present in the line)
        max_lines: Maximum number of lines to scan

    Returns:
        Optional[int]: Row index of header, or None if not found

    Raises:
        FileFormatError: If file cannot be read
    """
    from .exceptions import FileFormatError

    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for i, line in enumerate(f):
                if i >= max_lines:
                    break

                line_lower = line.lower()
                if all(k.lower() in line_lower for k in keywords):
                    return i
    except (OSError, UnicodeDecodeError) as e:
        logger.error(f"Błąd odczytu nagłówka w {path.name}: {e}")
        raise FileFormatError(reason="read_error", file_path=str(path), details=str(e))

    return None


def read_csvs_parallel(
    paths: List[Path],
    read_func: Callable[[Path], pd.DataFrame],
    max_workers: int = None,
) -> Dict[Path, pd.DataFrame]:
    # Use default from config if not provided
    if max_workers is None:
        max_workers = Config.DEFAULT_MAX_WORKERS
    """
    Read multiple CSV files in parallel using ThreadPoolExecutor.

    OPTIMIZATION: Reduces I/O time from O(f×io) to O(io) with parallelization.

    Args:
        paths: List of file paths to read
        read_func: Function to read a single CSV (e.g., pd.read_csv or custom)
        max_workers: Maximum number of parallel threads

    Returns:
        Dictionary mapping path to DataFrame (excludes failed reads)
    """
    if not paths:
        return {}

    def safe_read(path: Path) -> tuple[Path, Optional[pd.DataFrame]]:
        try:
            return (path, read_func(path))
        except Exception as e:
            logger.warning(f"Błąd równoległego odczytu CSV {path}: {e}")
            return (path, None)

    results = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(safe_read, path): path for path in paths}

        for future in as_completed(futures):
            path, df = future.result()
            if df is not None:
                results[path] = df

    return results


def check_consecutive_nans_optimized(series: pd.Series, threshold: int = None) -> int:
    # Use default from config if not provided
    if threshold is None:
        threshold = Config.DEFAULT_GAP_THRESHOLD
    """
    Find the maximum length of consecutive NaN/empty values.
    OPTIMIZED: Early exit and efficient RLE implementation.

    Args:
        series: Pandas Series to check
        threshold: Threshold for early exit optimization

    Returns:
        Maximum number of consecutive NaN/empty values
    """
    # Fast check for NaN/empty
    is_null = series.isna() | (series == "")

    if not is_null.any():
        return 0

    # Early exit: if total NaN count < threshold, can't have gap >= threshold
    null_count = is_null.sum()
    if null_count < threshold:
        return null_count

    # RLE (Run Length Encoding) - find consecutive groups
    changes = is_null.ne(is_null.shift())
    groups = changes.cumsum()

    # Only count runs of True (null values)
    null_groups = groups[is_null]
    if null_groups.empty:
        return 0

    return int(null_groups.value_counts().max())


def process_files_parallel(
    paths: List[Path], process_func: Callable[[Path], T], max_workers: int = None
) -> List[T]:
    # Use default from config if not provided
    if max_workers is None:
        max_workers = Config.DEFAULT_MAX_WORKERS
    """
    Process multiple files in parallel.

    Args:
        paths: List of file paths to process
        process_func: Function to process a single file
        max_workers: Maximum number of parallel threads

    Returns:
        List of results (in arbitrary order)
    """
    if not paths:
        return []

    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_func, path): path for path in paths}

        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                path = futures[future]
                logger.error(
                    f"Wyjątek podczas równoległego przetwarzania pliku {path}: {e}"
                )

    return results
