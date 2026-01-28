"""
Data merger module.
Combines data from all sources into a single training file.
"""

from pathlib import Path
from typing import List, Optional
import pandas as pd
import numpy as np

from .interfaces import UserInterface, FileSystemOperations
from .config import Config


# Time-related column names (case-insensitive)
TIME_KEYWORDS = ["secs", "seconds", "time", "timestamp", "timer.s"]


class DataMerger:
    """
    Merges data from all sources into a single training file.
    Uses Wahoo.csv as the base and appends columns from other sources.
    """

    def __init__(self, config: Config, fs: FileSystemOperations, ui: UserInterface):
        self.config = config
        self.fs = fs
        self.ui = ui

    def merge_files(
        self,
        base_df: pd.DataFrame,
        clean_files: List[Path],
        validate_head: bool = True,
        validate_tail: bool = True,
    ) -> pd.DataFrame:
        """
        Merge all clean files into the base DataFrame.
        OPTIMIZED: Batch concat instead of N sequential concats.

        Args:
            base_df: Base DataFrame (from Wahoo.csv)
            clean_files: List of clean file paths to merge
            validate_head: Whether to validate and trim head
            validate_tail: Whether to validate and trim tail

        Returns:
            Merged DataFrame
        """
        self.ui.print_message(f"\n🔗 MERGING WSZYSTKICH DANYCH (Baza: Wahoo.csv)")
        self.ui.print_separator()

        # OPTIMIZATION: Batch concat - collect all DataFrames first, then concat once
        # Instead of O(f*n) for f files and n rows, we get O(n)
        all_dfs = [base_df.reset_index(drop=True)]
        seen_columns = set(base_df.columns)

        for clean_path in clean_files:
            try:
                df_new = self.fs.read_csv(clean_path)
                new_reset = df_new.reset_index(drop=True)

                # Find and remove duplicate columns (keep base)
                duplicates = [col for col in new_reset.columns if col in seen_columns]

                if duplicates:
                    self.ui.print_message(
                        f"      🛡️  Ignoruję kolumny z {clean_path.name}: {duplicates}"
                    )
                    new_reset = new_reset.drop(columns=duplicates)

                if new_reset.empty or len(new_reset.columns) == 0:
                    self.ui.print_warning(
                        f"Plik {clean_path.name} nie wnosi żadnych nowych kolumn."
                    )
                    continue

                # Add to batch (don't concat yet!)
                all_dfs.append(new_reset)
                seen_columns.update(new_reset.columns)
                self.ui.print_success(f"Przygotowano dane z {clean_path.name}")
                self.ui.print_message(f"      ✅ Dodane kolumny: {list(new_reset.columns)}")

            except Exception as e:
                self.ui.print_error(f"Błąd mergowania {clean_path}: {e}")

        # SINGLE concat at the end - much more efficient
        self.ui.print_message(
            f"\n   ⚡ Wykonuję batch concat ({len(all_dfs)} DataFrames)..."
        )
        df_merged = pd.concat(all_dfs, axis=1)
        self.ui.print_success(
            f"Połączono wszystkie dane: {len(df_merged.columns)} kolumn"
        )

        if validate_head:
            df_merged = self._validate_and_trim_head(df_merged)

        if validate_tail:
            df_merged = self._validate_and_trim_tail(df_merged)

        return df_merged

    def _validate_and_trim_head(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate start of file for incomplete rows.
        Shifts data up while keeping time columns intact.
        """
        self.ui.print_message("\n✂️  WALIDACJA POCZĄTKU PLIKU (Synchronizacja startu)")

        # OPTIMIZATION: only run on string/object columns to avoid slow regex on floats/ints
        df_check = df.copy()
        obj_cols = df_check.select_dtypes(include=["object"]).columns
        if not obj_cols.empty:
            df_check[obj_cols] = df_check[obj_cols].replace(
                r"^\s*$", np.nan, regex=True
            )

        complete_mask = ~df_check.isna().any(axis=1)
        complete_indices = np.where(complete_mask)[0]

        if len(complete_indices) == 0:
            self.ui.print_warning(
                "UWAGA: Nie znaleziono ani jednego w pełni kompletnego wiersza!"
            )
            return df

        first_valid_pos = complete_indices[0]

        if first_valid_pos == 0:
            self.ui.print_success(
                "Pierwszy wiersz jest kompletny. Brak linii do usunięcia z początku."
            )
            return df

        self.ui.print_warning(
            f"Znaleziono {first_valid_pos} niepełnych linii na POCZĄTKU pliku."
        )

        if not self.ui.ask_yes_no(
            f"Czy usunąć {first_valid_pos} pierwszych linii, zachowując licznik czasu?"
        ):
            self.ui.print_message("⏭️  Pozostawiono plik bez zmian.")
            return df

        # Separate time and data columns
        time_cols = [c for c in df.columns if str(c).lower() in TIME_KEYWORDS]
        data_cols = [c for c in df.columns if c not in time_cols]

        self.ui.print_message(
            f"      🕒 Kolumny czasu (zostają nienaruszone): {time_cols}"
        )
        self.ui.print_message(
            f"      📉 Kolumny danych (przesuwane o {first_valid_pos} w górę): {len(data_cols)} kolumn"
        )

        # Shift data columns up
        df_new = df.copy()
        df_new[data_cols] = df_new[data_cols].shift(-first_valid_pos)

        self.ui.print_success("Przesunięto dane. Licznik czasu pozostał bez zmian.")
        return df_new

    def _validate_and_trim_tail(self, df: pd.DataFrame) -> pd.DataFrame:
        self.ui.print_message("\n✂️  WALIDACJA KOŃCÓWKI PLIKU (Synchronizacja długości)")

        # OPTIMIZATION: same as in _validate_and_trim_head
        df_check = df.copy()
        obj_cols = df_check.select_dtypes(include=["object"]).columns
        if not obj_cols.empty:
            df_check[obj_cols] = df_check[obj_cols].replace(
                r"^\s*$", np.nan, regex=True
            )

        complete_mask = df_check.notna().all(axis=1)
        complete_indices = np.where(complete_mask)[0]
        total_rows = len(df)

        if len(complete_indices) == 0:
            self.ui.print_warning(
                "UWAGA: Nie znaleziono ani jednego w pełni kompletnego wiersza!"
            )
            return df

        last_valid_pos = complete_indices[-1]
        rows_to_keep = last_valid_pos + 1
        to_remove = total_rows - rows_to_keep

        if to_remove == 0:
            self.ui.print_success(
                "Wszystkie wiersze są kompletne do końca. Brak linii do usunięcia."
            )
            return df

        self.ui.print_warning(
            f"Automatyczne przycinanie: Znaleziono {to_remove} niepełnych linii na KOŃCU pliku."
        )
        self.ui.print_message(
            f"   (Całkowita długość: {total_rows}, Ostatni w pełni wypełniony wiersz: {last_valid_pos})"
        )

        df_trimmed = df.iloc[:rows_to_keep].copy()
        self.ui.print_success(
            f"✂️  Usunięto {to_remove} linii dla lepszej kompatybilności. Nowa długość: {len(df_trimmed)}"
        )

        return df_trimmed

    def save_output(self, df: pd.DataFrame) -> Path:
        """
        Save the merged DataFrame to the output file.

        Returns:
            Path to the saved file
        """
        output_path = self.config.base_dir / self.config.output_filename
        self.fs.write_csv(df, output_path, index=False)

        self.ui.print_message(f"\n🎉 UTWORZONO: {output_path}")
        self.ui.print_message(f"   📈 Kolumny: {len(df.columns)}")
        self.ui.print_message(f"   📊 Wiersze:  {len(df)}")

        self.ui.print_message("\n📋 PRZYKŁADOWE KOLUMNY:")
        sample_cols = df.columns[:10].tolist()
        self.ui.print_message("   " + ", ".join(sample_cols) + " ...")

        return output_path
