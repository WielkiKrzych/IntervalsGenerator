"""
Column validator for pre-validation of CSV files.
Validates column presence and suggests fuzzy mappings.
"""

from typing import List, Dict, Optional, Set
from difflib import SequenceMatcher
import pandas as pd

from ..interfaces import UserInterface
from ..types import ColumnValidationResult


class ColumnValidator:
    """
    Validates columns in CSV files before processing.

    Features:
    - Check for required columns
    - Suggest fuzzy matches for missing columns
    - Case-insensitive matching
    - Whitespace normalization
    """

    def __init__(self, ui: UserInterface, similarity_threshold: float = None):
        # Use default from config if not provided
        if similarity_threshold is None:
            from ..config import Config

            similarity_threshold = Config.DEFAULT_SIMILARITY_THRESHOLD
        """
        Initialize validator.
        
        Args:
            ui: UserInterface for messages
            similarity_threshold: Minimum similarity ratio for fuzzy matching (0-1)
        """
        self.ui = ui
        self.similarity_threshold = similarity_threshold

    def validate_columns(
        self,
        df: pd.DataFrame,
        required: List[str],
        optional: Optional[List[str]] = None,
    ) -> ColumnValidationResult:
        """
        Validate that required columns exist in DataFrame.

        Args:
            df: DataFrame to validate
            required: List of required column names
            optional: List of optional column names (for logging only)

        Returns:
            ColumnValidationResult with validation status and suggestions
        """
        # Normalize existing columns (strip whitespace)
        existing_columns = [str(c).strip() for c in df.columns]
        existing_lower = {c.lower(): c for c in existing_columns}

        missing_columns: List[str] = []
        suggested_mappings: Dict[str, str] = {}

        for req_col in required:
            req_lower = req_col.lower().strip()

            # Exact match (case-insensitive)
            if req_lower in existing_lower:
                # Column exists, possibly with different case
                actual = existing_lower[req_lower]
                if actual != req_col:
                    suggested_mappings[actual] = req_col
                continue

            # Fuzzy match
            best_match = self._find_best_match(req_col, existing_columns)
            if best_match:
                suggested_mappings[best_match] = req_col
                self.ui.print_warning(
                    f"Kolumna '{req_col}' nie znaleziona, "
                    f"ale '{best_match}' wygląda podobnie"
                )
            else:
                missing_columns.append(req_col)

        # Check optional columns (just for logging)
        if optional:
            found_optional = [c for c in optional if c.lower() in existing_lower]
            if found_optional:
                self.ui.print_message(
                    f"   Znaleziono opcjonalne kolumny: {', '.join(found_optional)}"
                )

        is_valid = len(missing_columns) == 0
        error = None

        if not is_valid:
            error = f"Brak wymaganych kolumn: {', '.join(missing_columns)}"

        return ColumnValidationResult(
            is_valid=is_valid,
            missing_columns=missing_columns,
            suggested_mappings=suggested_mappings,
            error=error,
        )

    def _find_best_match(self, target: str, candidates: List[str]) -> Optional[str]:
        """
        Find the best fuzzy match for a column name.

        Args:
            target: Column name to match
            candidates: List of existing column names

        Returns:
            Best matching column name, or None if no good match
        """
        best_ratio = 0.0
        best_match = None

        target_lower = target.lower()

        for candidate in candidates:
            # Calculate similarity ratio
            ratio = SequenceMatcher(None, target_lower, candidate.lower()).ratio()

            if ratio > best_ratio and ratio >= self.similarity_threshold:
                best_ratio = ratio
                best_match = candidate

        return best_match

    def normalize_columns(
        self, df: pd.DataFrame, mapping: Dict[str, str]
    ) -> pd.DataFrame:
        """
        Apply column name normalization based on mapping.

        Args:
            df: DataFrame to normalize
            mapping: Dict of {existing_name: target_name}

        Returns:
            DataFrame with renamed columns
        """
        if not mapping:
            return df

        df_copy = df.copy()
        df_copy = df_copy.rename(columns=mapping)

        renamed = [f"{k} → {v}" for k, v in mapping.items()]
        self.ui.print_message(f"   Przemapowano kolumny: {', '.join(renamed)}")

        return df_copy

    def get_column_info(self, df: pd.DataFrame) -> Dict[str, str]:
        """
        Get information about columns in a DataFrame.

        Args:
            df: DataFrame to analyze

        Returns:
            Dict of {column_name: dtype_string}
        """
        return {str(col): str(df[col].dtype) for col in df.columns}

    def detect_timestamp_column(
        self, df: pd.DataFrame, patterns: Optional[List[str]] = None
    ) -> Optional[str]:
        """
        Detect timestamp/time column in DataFrame.

        Args:
            df: DataFrame to analyze
            patterns: Optional list of patterns to match (default: common time patterns)

        Returns:
            Detected timestamp column name, or None
        """
        if patterns is None:
            patterns = [
                "timestamp",
                "time",
                "secs",
                "seconds",
                "timer.s",
                "elapsed",
                "duration",
            ]

        for col in df.columns:
            col_lower = str(col).lower()
            for pattern in patterns:
                if pattern in col_lower:
                    return str(col)

        return None
