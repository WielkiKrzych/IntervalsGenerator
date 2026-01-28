"""
Backup management for Intervals Generator.
Creates folder-based backups before destructive operations.
"""

import shutil
from pathlib import Path
from datetime import datetime, timedelta
from typing import List

from .logging_config import get_logger


class BackupManager:
    """
    Manages automated backups of the working directory.
    Creates timestamped folder copies (no ZIP compression).
    """
    
    def __init__(self, base_dir: Path, backup_dir: Path = None):
        """
        Initialize backup manager.
        
        Args:
            base_dir: Directory to backup
            backup_dir: Where to store backups. Defaults to base_dir/backups
        """
        self.base_dir = base_dir
        self.backup_dir = backup_dir or (base_dir / "backups")
        self.logger = get_logger()
    
    def create_backup(self, include_patterns: List[str] = None) -> Path:
        """
        Create a folder backup of the working directory.
        
        Args:
            include_patterns: Glob patterns to include. Defaults to all CSVs.
            
        Returns:
            Path to created backup folder
        """
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"backup_{timestamp}"
        backup_path = self.backup_dir / backup_name
        backup_path.mkdir(parents=True, exist_ok=True)
        
        # TARGETED SEARCH: Instead of base_dir.glob("**/*.csv"), 
        # we search only specific folders to avoid scanning the 'backups/' directory.
        # This is critical when there are hundreds of old backups.
        
        self.logger.info(f"📦 Tworzenie backupu: {backup_name}")
        
        # 1. Get CSV files from base directory (non-recursive)
        files_to_copy = list(self.base_dir.glob("*.csv"))
        
        # 2. Get CSV files from source directories (recursive within each)
        for sub_dir in self.base_dir.iterdir():
            if sub_dir.is_dir() and sub_dir.name.endswith("_files") and sub_dir.name != "backups":
                files_to_copy.extend(sub_dir.rglob("*.csv"))
        
        file_count = 0
        total_files = len(files_to_copy)
        
        for i, file_path in enumerate(files_to_copy):
            # Double check to avoid backups path (insurance)
            if "backups" in str(file_path):
                continue
                
            try:
                # Preserve relative path structure
                rel_path = file_path.relative_to(self.base_dir)
                dest_path = backup_path / rel_path
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(file_path, dest_path)
                file_count += 1
                
                # Progress feedback every 50 files or at start/end
                if file_count % 50 == 0 or file_count == 1:
                    self.logger.info(f"   ⏳ Kopiowanie: {file_count}/{total_files} plików...")
            except Exception as e:
                self.logger.warning(f"   ⚠️ Nie udało się skopiować {file_path.name}: {e}")
        
        self.logger.info(f"   ✅ Skopiowano {file_count} plików do backupu")
        self.logger.info(f"   💾 Backup utworzony: {backup_path}")
        
        return backup_path
    
    def restore_backup(self, backup_path: Path) -> bool:
        """
        Restore from a backup folder.
        
        Args:
            backup_path: Path to the backup folder
            
        Returns:
            True if successful
        """
        if not backup_path.exists() or not backup_path.is_dir():
            self.logger.error(f"Backup nie istnieje: {backup_path}")
            return False
        
        self.logger.info(f"🔄 Przywracanie z backupu: {backup_path.name}")
        
        try:
            file_count = 0
            for src_file in backup_path.rglob("*"):
                if src_file.is_file():
                    rel_path = src_file.relative_to(backup_path)
                    dest_path = self.base_dir / rel_path
                    dest_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(src_file, dest_path)
                    file_count += 1
            
            self.logger.info(f"   ✅ Przywrócono {file_count} plików")
            return True
        except Exception as e:
            self.logger.error(f"   ❌ Błąd przywracania: {e}")
            return False
    
    def cleanup_old_backups(self, max_age_days: int = 30) -> int:
        """
        Remove backups older than specified days.
        
        Args:
            max_age_days: Maximum age for backups
            
        Returns:
            Number of backups removed
        """
        if not self.backup_dir.exists():
            return 0
        
        cutoff = datetime.now() - timedelta(days=max_age_days)
        removed = 0
        
        for backup_folder in self.backup_dir.glob("backup_*"):
            if not backup_folder.is_dir():
                continue
            try:
                # Parse timestamp from folder name
                timestamp_str = backup_folder.name.replace("backup_", "")
                folder_date = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                
                if folder_date < cutoff:
                    shutil.rmtree(backup_folder)
                    removed += 1
                    self.logger.info(f"   🗑️ Usunięto stary backup: {backup_folder.name}")
            except (ValueError, OSError):
                continue
        
        if removed > 0:
            self.logger.info(f"🧹 Usunięto {removed} starych backupów")
        
        return removed
    
    def list_backups(self) -> List[Path]:
        """List all available backups, newest first."""
        if not self.backup_dir.exists():
            return []
        return sorted(
            [d for d in self.backup_dir.glob("backup_*") if d.is_dir()],
            reverse=True
        )
