"""
Configuration module for Intervals Generator.
Centralizes all path configurations (DIP - Dependency Inversion Principle).
"""

import os
import platform
from pathlib import Path
from dataclasses import dataclass
from datetime import date


@dataclass
class Config:
    """
    Central configuration for all paths and settings.
    Replaces hardcoded global constants.
    """

    base_dir: Path
    downloads_dir: Path

    # Magic number constants
    # File reading limits
    HEADER_SCAN_MAX_LINES: int = 60  # Max lines to scan for header

    # Parallelization
    DEFAULT_MAX_WORKERS: int = 4  # Default thread pool size

    # Data validation
    DEFAULT_GAP_THRESHOLD: int = 10  # Max consecutive NaN before error
    DEFAULT_SIMILARITY_THRESHOLD: float = 0.7  # Fuzzy matching threshold (0-1)

    # Derived paths (computed from base_dir)
    @property
    def trainred_dir(self) -> Path:
        return self.base_dir / "1_TrainRed_files"

    @property
    def trainred_old_dir(self) -> Path:
        return self.trainred_dir / "TrainRed_files_old"

    @property
    def tymewear_dir(self) -> Path:
        return self.base_dir / "2_Tymewear_files"

    @property
    def tymewear_old_dir(self) -> Path:
        return self.tymewear_dir / "Tymewear_files_old"

    @property
    def wahoo_dir(self) -> Path:
        return self.base_dir / "3_Wahoo_files"

    @property
    def wahoo_old_dir(self) -> Path:
        return self.wahoo_dir / "Wahoo_files_old"

    @property
    def garmin_dir(self) -> Path:
        return self.base_dir / "4_Garmin_files"

    @property
    def garmin_old_dir(self) -> Path:
        return self.garmin_dir / "Garmin_files_old"

    @property
    def treningi_old_dir(self) -> Path:
        return self.base_dir / "5_Treningi_Old"

    @property
    def today(self) -> date:
        return date.today()

    @property
    def output_filename(self) -> str:
        return f"Trening-{self.today.strftime('%d.%m.%Y')}-import.csv"

    @classmethod
    def from_env(cls) -> "Config":
        """
        Create config from environment variables or auto-detect based on OS.

        Environment variables:
        - INTERVALS_BASE_DIR: Base directory for the generator
        - INTERVALS_DOWNLOADS_DIR: Downloads directory
        """
        # Check for environment variables first
        base_dir_env = os.environ.get("INTERVALS_BASE_DIR")
        downloads_dir_env = os.environ.get("INTERVALS_DOWNLOADS_DIR")

        if base_dir_env and downloads_dir_env:
            return cls(
                base_dir=Path(base_dir_env), downloads_dir=Path(downloads_dir_env)
            )

        # Auto-detect based on OS
        system = platform.system()

        if system == "Darwin":  # macOS
            user = os.environ.get("USER", "user")
            base_dir = Path(f"/Users/{user}/Desktop/Intervals_Generator")
            downloads_dir = Path(f"/Users/{user}/Downloads")
        elif system == "Windows":
            user_profile = os.environ.get("USERPROFILE", "C:\\Users\\User")
            base_dir = Path(user_profile) / "Desktop" / "Intervals_Generator"
            downloads_dir = Path(user_profile) / "Downloads"
        else:  # Linux
            home = os.environ.get("HOME", "/home/user")
            base_dir = Path(home) / "Desktop" / "Intervals_Generator"
            downloads_dir = Path(home) / "Downloads"

        return cls(base_dir=base_dir, downloads_dir=downloads_dir)

    @classmethod
    def for_testing(cls, temp_dir: Path) -> "Config":
        """Create config for testing with a temporary directory."""
        return cls(
            base_dir=temp_dir / "Intervals_Generator",
            downloads_dir=temp_dir / "Downloads",
        )

    def ensure_directories(self) -> None:
        """Create all necessary directories if they don't exist."""
        directories = [
            self.base_dir,
            self.trainred_dir,
            self.trainred_old_dir,
            self.tymewear_dir,
            self.tymewear_old_dir,
            self.wahoo_dir,
            self.wahoo_old_dir,
            self.garmin_dir,
            self.garmin_old_dir,
            self.treningi_old_dir,
        ]
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
