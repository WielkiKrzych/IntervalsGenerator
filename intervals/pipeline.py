"""
Pipeline orchestration module.
Coordinates all loaders, validators, and merger (ISP implementation).

Uses LoaderRegistry for dynamic loader resolution - adding a new loader
requires no changes to this file.
"""

import logging
from pathlib import Path
from typing import List, Optional, Dict
import sys

from .config import Config
from .interfaces import UserInterface, FileSystemOperations
from .filesystem import RealFileSystem
from .ui import ConsoleUI
from .loaders import LoaderRegistry
from .loaders.base import BaseLoader
from .validators import IntegrityValidator
from .merger import DataMerger
from .exceptions import IntervalsValidationError


logger = logging.getLogger(__name__)


class Pipeline:
    """
    Main pipeline that orchestrates all data processing steps.

    Uses LoaderRegistry for dynamic loader resolution.
    Adding a new data source loader requires:
    1. Create new loader file with @LoaderRegistry.register decorator
    2. Import the loader in intervals/loaders/__init__.py

    No changes to this file are needed.

    ISP: Provides granular methods for each step, allowing
    users to run only the parts they need.
    """

    def __init__(
        self,
        config: Config,
        fs: Optional[FileSystemOperations] = None,
        ui: Optional[UserInterface] = None,
    ):
        self.config = config
        self.fs = fs or RealFileSystem()
        self.ui = ui or ConsoleUI()

        # Dynamically load all registered loaders
        self._loaders: Dict[str, BaseLoader] = {}
        self._init_loaders()

        # Initialize validator and merger
        self.validator = IntegrityValidator(self.ui)
        self.merger = DataMerger(config, self.fs, self.ui)

    def _init_loaders(self) -> None:
        """
        Initialize all registered loaders from LoaderRegistry.

        Loaders are instantiated in priority order.
        Each loader is accessible via self._loaders[name].
        """
        all_loaders = LoaderRegistry.get_all_loaders(self.config, self.fs, self.ui)
        for loader in all_loaders:
            name = loader.name.lower()
            self._loaders[name] = loader

    def get_loader(self, name: str) -> Optional[BaseLoader]:
        """
        Get a specific loader by name.

        Args:
            name: Loader name (case-insensitive)

        Returns:
            Loader instance or None if not found
        """
        return self._loaders.get(name.lower())

    @property
    def loaders(self) -> List[BaseLoader]:
        """Get all loaders in priority order."""
        return list(self._loaders.values())

    # Convenience properties for commonly used loaders
    @property
    def trainred(self) -> Optional[BaseLoader]:
        """TrainRed loader (for backward compatibility)."""
        return self.get_loader("trainred")

    @property
    def tymewear(self) -> Optional[BaseLoader]:
        """Tymewear loader (for backward compatibility)."""
        return self.get_loader("tymewear")

    @property
    def wahoo(self) -> Optional[BaseLoader]:
        """Wahoo loader (for backward compatibility)."""
        return self.get_loader("wahoo")

    @property
    def garmin(self) -> Optional[BaseLoader]:
        """Garmin loader (for backward compatibility)."""
        return self.get_loader("garmin")

    def run_cleanup(self) -> int:
        """
        Archive existing files from all source directories.

        Dynamically iterates over all registered loaders.

        Returns:
            Total number of files archived
        """
        self.ui.print_message(
            "🧹 CZYSZCZENIE FOLDERÓW - przenoszę istniejące pliki do *_old"
        )

        total_moved = 0

        # Archive files for all registered loaders
        for loader in self.loaders:
            moved = loader.archive_existing_files()
            total_moved += moved

        # Archive old training files
        total_moved += self._archive_old_training_files()

        self.ui.print_message(
            f"🧹 CZYSZCZENIE ZAKOŃCZONE: Łącznie przeniesiono {total_moved} plików.\n"
        )

        return total_moved

    def _archive_old_training_files(self) -> int:
        """Archive old Trening-*.csv files to 5_Treningi_Old."""
        self.ui.print_message(
            f"🧹 Archiwizacja starych plików wynikowych do 5_Treningi_Old"
        )

        self.fs.mkdir(self.config.treningi_old_dir)
        old_trainings = self.fs.glob(self.config.base_dir, "Trening-*.csv")
        moved = 0

        for src in old_trainings:
            try:
                dst = self.config.treningi_old_dir / src.name
                self.fs.move(src, dst)
                self.ui.print_message(f"   📦 Przeniesiono: {src.name}")
                moved += 1
            except Exception as e:
                self.ui.print_error(f"Błąd przenoszenia {src.name}: {e}")

        if not old_trainings:
            self.ui.print_message("   (Brak plików Trening-*.csv w głównym katalogu)")

        return moved

    def run_import(self) -> None:
        """
        Import files from downloads directory.

        Dynamically iterates over all registered loaders.
        """
        downloads_dir = self.config.downloads_dir

        # Import from downloads for each loader
        for loader in self.loaders:
            loader.import_from_downloads(downloads_dir)

    def run_processing(self) -> None:
        """
        Process all imported files.

        Dynamically iterates over all registered loaders.
        """
        for loader in self.loaders:
            # Check if there are files to process
            source_files = self.fs.glob(loader.source_dir, "*.csv")
            # Exclude already processed files
            source_files = [
                f
                for f in source_files
                if "_clean" not in f.name and "_avg" not in f.name
            ]

            if source_files or loader.name.lower() in ("wahoo",):
                loader.process_files()

    def run_validation(self) -> bool:
        """
        Validate all processed files for data integrity.

        Dynamically collects clean files from all registered loaders.

        Returns:
            True if all valid, False if issues found
        """
        files_to_validate = []

        # Collect clean files from all loaders
        for loader in self.loaders:
            for path in loader.get_clean_files():
                files_to_validate.append((path, loader.name))

        try:
            is_valid = self.validator.validate_files(
                files_to_validate, self.fs.read_csv
            )
        except IntervalsValidationError as e:
            logger.error(f"Wyjątek walidacji: {e}")
            self.ui.print_error(f"Błąd walidacji: {e}")
            is_valid = False
        except Exception as e:
            logger.critical(f"Nieoczekiwany błąd podczas walidacji: {e}")
            self.ui.print_error(f"Nieoczekiwany błąd walidacji: {e}")
            is_valid = False

        if not is_valid:
            if not self.ui.ask_yes_no(
                "Czy chcesz kontynuować łączenie plików mimo błędów?"
            ):
                self.ui.print_message("🛑 Przerwano na żądanie użytkownika.")
                raise RuntimeError(
                    "Przerwano na żądanie użytkownika z powodu błędów walidacji."
                )
            else:
                self.ui.print_warning("Kontynuuję mimo luk w danych...")

        return is_valid

    def run_merge(self) -> Optional[Path]:
        """
        Merge all processed files into final training file.

        Uses Wahoo as base, then merges clean files from all other loaders.

        Returns:
            Path to the created file, or None if failed
        """
        # Get base DataFrame from Wahoo
        wahoo = self.get_loader("wahoo")
        if not wahoo or not hasattr(wahoo, "get_base_dataframe"):
            self.ui.print_error(
                "Nie znaleziono loadera Wahoo lub brak metody get_base_dataframe"
            )
            return None

        base_df = wahoo.get_base_dataframe()
        if base_df.empty:
            self.ui.print_error("Nie można kontynuować bez pliku Wahoo.csv")
            return None

        # Collect all clean files from non-Wahoo loaders
        clean_files = []
        for loader in self.loaders:
            if loader.name.lower() != "wahoo":
                clean_files.extend(loader.get_clean_files())

        # Merge
        df_merged = self.merger.merge_files(base_df, clean_files)

        # Save
        output_path = self.merger.save_output(df_merged)

        return output_path

    def run_full(self) -> Optional[Path]:
        """
        Run the complete pipeline.

        Returns:
            Path to the created training file, or None if failed
        """
        # Show registered loaders
        loader_names = [l.name for l in self.loaders]
        self.ui.print_header(
            f"INTERVALS GENERATOR - Loaders: {', '.join(loader_names)}"
        )

        # 1. Cleanup
        self.run_cleanup()

        # 2. Import from downloads
        self.run_import()

        # 3. Process files
        self.run_processing()

        # 4. Validate
        self.run_validation()

        # 5. Merge and save
        return self.run_merge()
