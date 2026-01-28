"""
Real filesystem operations implementation.
Supports dry-run mode for simulation without modifications.
"""

import shutil
from pathlib import Path
from typing import List, Optional
import pandas as pd

from .interfaces import FileSystemOperations
from .logging_config import get_logger


class RealFileSystem(FileSystemOperations):
    """
    Real filesystem implementation using os/shutil/pandas.
    This is the production implementation.
    Supports dry_run mode for simulation.
    """
    
    def __init__(self, dry_run: bool = False):
        """
        Initialize filesystem operations.
        
        Args:
            dry_run: If True, simulate operations without making changes
        """
        self.dry_run = dry_run
        self._logger = None
        self._operations_log: List[str] = []
    
    @property
    def logger(self):
        if self._logger is None:
            self._logger = get_logger()
        return self._logger
    
    def _log_operation(self, operation: str) -> None:
        """Log a dry-run operation."""
        if self.dry_run:
            self._operations_log.append(operation)
            self.logger.info(f"[DRY-RUN] {operation}")
    
    def get_operations_log(self) -> List[str]:
        """Get all logged dry-run operations."""
        return self._operations_log.copy()
    
    def exists(self, path: Path) -> bool:
        return path.exists()
    
    def glob(self, directory: Path, pattern: str) -> List[Path]:
        if not directory.exists():
            return []
        return sorted(directory.glob(pattern))
    
    def copy(self, src: Path, dst: Path) -> None:
        if self.dry_run:
            self._log_operation(f"COPY: {src} -> {dst}")
            return
        shutil.copy2(str(src), str(dst))
    
    def move(self, src: Path, dst: Path) -> None:
        if self.dry_run:
            self._log_operation(f"MOVE: {src} -> {dst}")
            return
        shutil.move(str(src), str(dst))
    
    def remove(self, path: Path) -> None:
        if self.dry_run:
            self._log_operation(f"DELETE: {path}")
            return
        path.unlink()
    
    def read_csv(self, path: Path, **kwargs) -> pd.DataFrame:
        return pd.read_csv(path, **kwargs)
    
    def write_csv(self, df: pd.DataFrame, path: Path, **kwargs) -> None:
        if self.dry_run:
            self._log_operation(f"WRITE CSV: {path} ({len(df)} rows, {len(df.columns)} cols)")
            return
        df.to_csv(path, **kwargs)
    
    def mkdir(self, path: Path, parents: bool = True, exist_ok: bool = True) -> None:
        if self.dry_run:
            if not path.exists():
                self._log_operation(f"MKDIR: {path}")
            return
        path.mkdir(parents=parents, exist_ok=exist_ok)
    
    def list_files(self, directory: Path) -> List[Path]:
        if not directory.exists():
            return []
        return [f for f in directory.iterdir() if f.is_file()]


class DryRunFileSystem(RealFileSystem):
    """
    Convenience subclass that always runs in dry-run mode.
    """
    
    def __init__(self):
        super().__init__(dry_run=True)
