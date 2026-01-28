"""
Base loader class with common functionality.
"""

from abc import ABC
from pathlib import Path
from typing import List, Optional
import pandas as pd

from ..interfaces import (
    DataSourceLoader,
    FileSystemOperations,
    UserInterface,
    ValidationResult,
)
from ..config import Config


class BaseLoader(DataSourceLoader, ABC):
    """
    Base class for all data source loaders.
    Provides common functionality for file operations.
    """

    def __init__(self, config: Config, fs: FileSystemOperations, ui: UserInterface):
        self._config = config
        self._fs = fs
        self._ui = ui

    @property
    def name(self) -> str:
        """Name from LOADER_SPEC."""
        return self.LOADER_SPEC.name

    @property
    def source_dir(self) -> Path:
        """Dynamically resolve source directory from config."""
        attr_name = f"{self.LOADER_SPEC.name.lower()}_dir"
        return getattr(self.config, attr_name)

    @property
    def old_dir(self) -> Path:
        """Dynamically resolve old directory from config."""
        attr_name = f"{self.LOADER_SPEC.name.lower()}_old_dir"
        return getattr(self.config, attr_name)

    @property
    def config(self) -> Config:
        return self._config

    @property
    def fs(self) -> FileSystemOperations:
        return self._fs

    @property
    def ui(self) -> UserInterface:
        return self._ui

    def archive_existing_files(self) -> int:
        """
        Move all existing files from source_dir to old_dir.

        Returns:
            Number of files moved
        """
        self.fs.mkdir(self.old_dir)

        files = self.fs.list_files(self.source_dir)
        moved_count = 0

        for src_file in files:
            try:
                dst = self.old_dir / src_file.name
                self.fs.move(src_file, dst)
                moved_count += 1
            except Exception as e:
                self.ui.print_error(f"{self.name}/{src_file.name}: {e}")

        if moved_count > 0:
            self.ui.print_message(f"   📁 {self.name}: {moved_count} plików -> *_old")

        return moved_count

    def import_from_downloads(self, downloads_dir: Path) -> List[Path]:
        """
        Generic implementation of file import from downloads.
        Scans downloads_dir for files matching this source's pattern.
        """
        if not self.fs.exists(downloads_dir):
            self.ui.print_error(f"Downloads: {downloads_dir} nie istnieje")
            return []

        self.ui.print_message(f"\n📅 Szukam plików {self.name} w Downloads...")

        # Use case-insensitive glob for extensions
        csv_files: List[Path] = self.fs.glob(downloads_dir, "*.[cC][sS][vV]")
        self.fs.mkdir(self.source_dir)
        imported: List[Path] = []

        for src in csv_files:
            if self.detect_in_downloads(src):
                dst: Path = self.source_dir / src.name
                if self._copy_and_remove_from_downloads(src, dst):
                    self.ui.print_success(
                        f"Wykryto i skopiowano {self.name}: {src.name}"
                    )
                    imported.append(dst)

        if not imported and self.name.lower() not in ("wahoo", "garmin"):
            self.ui.print_message(
                f"   (Brak nowych plików {self.name} w folderze Downloads)"
            )

        return imported

    def get_clean_files(self) -> List[Path]:
        """
        Get list of clean/processed files ready for merging.
        Standard implementation for most loaders.
        """
        return self.fs.glob(self.source_dir, "*_clean.csv")

    def validate_dataframe(self, df: pd.DataFrame) -> ValidationResult:
        """
        Generic validation using LOADER_SPEC.
        Checks if all required source columns are present.
        """
        result = ValidationResult()
        existing_cols = set(df.columns)

        spec = self.LOADER_SPEC
        for col_spec in spec.required_columns:
            if col_spec.source_name not in existing_cols:
                result.add_error(
                    f"Brak wymaganej kolumny: '{col_spec.source_name}'",
                    column=col_spec.source_name,
                )

        return result

    def _copy_and_remove_from_downloads(self, src: Path, dst: Path) -> bool:
        """Copy file to destination and remove from source."""
        try:
            self.fs.copy(src, dst)
            self.fs.remove(src)
            return True
        except Exception as e:
            self.ui.print_error(f"Błąd przenoszenia {src.name}: {e}")
            return False
