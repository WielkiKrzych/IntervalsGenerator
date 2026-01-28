"""
Time series interpolation module.
Handles gaps in temporal data and different sampling frequencies.
"""

from typing import Literal, Optional, List, Tuple
import pandas as pd
import numpy as np

from .types import InterpolationMethod


def interpolate_time_gaps(
    df: pd.DataFrame,
    time_col: str = 'secs',
    method: InterpolationMethod = 'linear',
    max_gap: int = 5,
    columns: Optional[List[str]] = None
) -> Tuple[pd.DataFrame, int]:
    """
    Interpolate missing values in time series data.
    
    Args:
        df: DataFrame with time series data
        time_col: Name of the time column
        method: Interpolation method ('linear', 'ffill', 'bfill', 'pad', 'none')
        max_gap: Maximum consecutive missing values to interpolate
        columns: Specific columns to interpolate (None = all numeric)
        
    Returns:
        Tuple of (interpolated DataFrame, number of values filled)
        
    Example:
        >>> df = pd.DataFrame({'secs': [0, 1, 2, 3, 4], 'watts': [100, np.nan, np.nan, 160, 170]})
        >>> df_filled, count = interpolate_time_gaps(df, max_gap=3)
        >>> print(count)  # 2 values filled
    """
    if method == 'none':
        return df.copy(), 0
    
    df_copy = df.copy()
    
    # Determine columns to interpolate
    if columns is None:
        columns = df_copy.select_dtypes(include=[np.number]).columns.tolist()
        # Exclude time column
        columns = [c for c in columns if c != time_col]
    
    total_filled = 0
    
    for col in columns:
        if col not in df_copy.columns:
            continue
        
        # Count NaN before
        nan_before = df_copy[col].isna().sum()
        
        if nan_before == 0:
            continue
        
        # Check for gaps exceeding max_gap
        nan_mask = df_copy[col].isna()
        gap_lengths = _get_consecutive_lengths(nan_mask)
        
        # Only interpolate gaps <= max_gap
        if max(gap_lengths) <= max_gap if gap_lengths else True:
            if method == 'linear':
                df_copy[col] = df_copy[col].interpolate(method='linear', limit=max_gap)
            elif method in ('ffill', 'pad'):
                df_copy[col] = df_copy[col].ffill(limit=max_gap)
            elif method == 'bfill':
                df_copy[col] = df_copy[col].bfill(limit=max_gap)
        else:
            # Only interpolate small gaps
            df_copy[col] = _interpolate_small_gaps(
                df_copy[col],
                max_gap=max_gap,
                method=method
            )
        
        # Count filled values
        nan_after = df_copy[col].isna().sum()
        total_filled += (nan_before - nan_after)
    
    return df_copy, total_filled


def _get_consecutive_lengths(mask: pd.Series) -> List[int]:
    """
    Get lengths of consecutive True values in a boolean series.
    
    Args:
        mask: Boolean series
        
    Returns:
        List of consecutive True lengths
    """
    if not mask.any():
        return []
    
    # Find consecutive groups
    groups = (mask != mask.shift()).cumsum()
    
    # Get lengths of True groups
    lengths = []
    for _, group in mask.groupby(groups):
        if group.iloc[0]:  # Only True groups
            lengths.append(len(group))
    
    return lengths


def _interpolate_small_gaps(
    series: pd.Series,
    max_gap: int,
    method: str
) -> pd.Series:
    """
    Interpolate only gaps smaller than max_gap.
    
    Args:
        series: Numeric series with gaps
        max_gap: Maximum gap size to interpolate
        method: Interpolation method
        
    Returns:
        Series with small gaps filled
    """
    result = series.copy()
    nan_mask = series.isna()
    
    if not nan_mask.any():
        return result
    
    # Find gap boundaries
    groups = (nan_mask != nan_mask.shift()).cumsum()
    
    for group_id, group in nan_mask.groupby(groups):
        if not group.iloc[0]:  # Skip non-NaN groups
            continue
        
        gap_length = len(group)
        if gap_length <= max_gap:
            # Interpolate this small gap
            start_idx = group.index[0]
            end_idx = group.index[-1]
            
            # Get surrounding values for interpolation
            if method == 'linear':
                result.loc[start_idx:end_idx] = result.loc[start_idx:end_idx].interpolate(
                    method='linear'
                )
            elif method in ('ffill', 'pad'):
                result.loc[start_idx:end_idx] = result.loc[start_idx:end_idx].ffill()
            elif method == 'bfill':
                result.loc[start_idx:end_idx] = result.loc[start_idx:end_idx].bfill()
    
    return result


def resample_to_frequency(
    df: pd.DataFrame,
    time_col: str = 'secs',
    target_freq: int = 1,
    current_freq: Optional[int] = None,
    agg_method: Literal['mean', 'first', 'last', 'median'] = 'mean'
) -> pd.DataFrame:
    """
    Resample DataFrame to target frequency (samples per second).
    
    Useful for normalizing different sampling rates (e.g., 10Hz → 1Hz).
    
    Args:
        df: DataFrame with time series data
        time_col: Name of the time column
        target_freq: Target samples per second (Hz)
        current_freq: Current frequency (auto-detected if None)
        agg_method: Aggregation method for downsampling
        
    Returns:
        Resampled DataFrame
        
    Example:
        >>> # Convert 10Hz data to 1Hz
        >>> df_1hz = resample_to_frequency(df_10hz, target_freq=1)
    """
    if time_col not in df.columns:
        raise ValueError(f"Time column '{time_col}' not found in DataFrame")
    
    df_copy = df.copy()
    
    # Detect current frequency if not provided
    if current_freq is None:
        time_diff = df_copy[time_col].diff().median()
        if time_diff > 0:
            current_freq = int(round(1 / time_diff))
        else:
            current_freq = 1
    
    if current_freq == target_freq:
        return df_copy
    
    # Create integer second column for grouping
    df_copy['_second'] = (df_copy[time_col] * target_freq).astype(int)
    
    # Aggregate numeric columns
    numeric_cols = df_copy.select_dtypes(include=[np.number]).columns.tolist()
    numeric_cols = [c for c in numeric_cols if c not in [time_col, '_second']]
    
    non_numeric_cols = [c for c in df_copy.columns if c not in numeric_cols + [time_col, '_second']]
    
    # Build aggregation dict
    agg_dict = {}
    for col in numeric_cols:
        agg_dict[col] = agg_method
    for col in non_numeric_cols:
        agg_dict[col] = 'first'
    
    # Group and aggregate
    result = df_copy.groupby('_second').agg(agg_dict).reset_index()
    result = result.rename(columns={'_second': time_col})
    
    return result


def align_time_series(
    dfs: List[pd.DataFrame],
    time_col: str = 'secs',
    fill_method: InterpolationMethod = 'ffill'
) -> List[pd.DataFrame]:
    """
    Align multiple DataFrames to the same time index.
    
    Args:
        dfs: List of DataFrames to align
        time_col: Name of the time column
        fill_method: Method to fill missing values after alignment
        
    Returns:
        List of aligned DataFrames
    """
    if not dfs:
        return []
    
    # Find common time range
    min_time = max(df[time_col].min() for df in dfs if time_col in df.columns)
    max_time = min(df[time_col].max() for df in dfs if time_col in df.columns)
    
    aligned = []
    for df in dfs:
        if time_col not in df.columns:
            aligned.append(df)
            continue
        
        # Filter to common range
        df_filtered = df[(df[time_col] >= min_time) & (df[time_col] <= max_time)].copy()
        aligned.append(df_filtered)
    
    return aligned


def detect_sampling_rate(
    df: pd.DataFrame,
    time_col: str = 'secs'
) -> float:
    """
    Detect sampling rate (Hz) from time column.
    
    Args:
        df: DataFrame with time data
        time_col: Name of time column
        
    Returns:
        Detected sampling rate in Hz
    """
    if time_col not in df.columns or len(df) < 2:
        return 1.0
    
    time_diff = df[time_col].diff().dropna()
    
    if len(time_diff) == 0:
        return 1.0
    
    median_diff = time_diff.median()
    
    if median_diff <= 0:
        return 1.0
    
    return 1.0 / median_diff
