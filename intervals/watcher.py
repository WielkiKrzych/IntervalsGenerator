"""
File system watcher for automatic import from Downloads.
Uses watchdog library to monitor for new training files.
"""

import time
from pathlib import Path
from typing import Callable, Optional
import threading

from .logging_config import get_logger


class DownloadsWatcher:
    """
    Watches Downloads directory for new training files.
    Triggers callback when compatible files are detected.
    """
    
    def __init__(
        self,
        downloads_dir: Path,
        patterns: list = None,
        debounce_seconds: float = 2.0
    ):
        """
        Initialize the watcher.
        
        Args:
            downloads_dir: Directory to watch
            patterns: File patterns to watch for (default: *.csv)
            debounce_seconds: Wait time before triggering callback
        """
        self.downloads_dir = Path(downloads_dir)
        self.patterns = patterns or ["*.csv"]
        self.debounce_seconds = debounce_seconds
        self.logger = get_logger()
        self._observer = None
        self._stop_event = threading.Event()
        self._callback: Optional[Callable] = None
    
    def _check_for_new_files(self) -> list:
        """Check for matching files in downloads directory."""
        found_files = []
        for pattern in self.patterns:
            found_files.extend(self.downloads_dir.glob(pattern))
        return found_files
    
    def watch(self, callback: Callable[[list], None]) -> None:
        """
        Start watching for new files.
        Blocks until stop() is called.
        
        Args:
            callback: Function to call with list of new files
        """
        self._callback = callback
        self._stop_event.clear()
        
        self.logger.info(f"👁️ Rozpoczynam monitoring: {self.downloads_dir}")
        self.logger.info(f"   Wzorce: {self.patterns}")
        self.logger.info("   Naciśnij Ctrl+C aby zatrzymać...")
        
        known_files = set(f.name for f in self._check_for_new_files())
        
        try:
            while not self._stop_event.is_set():
                current_files = self._check_for_new_files()
                current_names = set(f.name for f in current_files)
                
                new_names = current_names - known_files
                
                if new_names:
                    self.logger.info(f"🆕 Wykryto {len(new_names)} nowych plików!")
                    for name in new_names:
                        self.logger.info(f"   • {name}")
                    
                    # Debounce - wait for file to finish copying
                    time.sleep(self.debounce_seconds)
                    
                    # Get full paths of new files
                    new_files = [f for f in current_files if f.name in new_names]
                    
                    if callback:
                        callback(new_files)
                    
                    known_files = current_names
                
                # Poll interval
                time.sleep(1.0)
                
        except KeyboardInterrupt:
            self.logger.info("\n⏹️ Zatrzymano monitoring.")
    
    def watch_async(self, callback: Callable[[list], None]) -> threading.Thread:
        """
        Start watching in a background thread.
        
        Args:
            callback: Function to call with list of new files
            
        Returns:
            Thread running the watcher
        """
        thread = threading.Thread(
            target=self.watch,
            args=(callback,),
            daemon=True
        )
        thread.start()
        return thread
    
    def stop(self) -> None:
        """Stop the watcher."""
        self._stop_event.set()
        self.logger.info("⏹️ Zatrzymuję monitoring...")


class AutoImporter:
    """
    Convenience class that combines watcher with Pipeline import.
    """
    
    def __init__(self, pipeline, downloads_dir: Path):
        """
        Args:
            pipeline: Pipeline instance to use for import
            downloads_dir: Directory to watch
        """
        self.pipeline = pipeline
        self.watcher = DownloadsWatcher(downloads_dir)
        self.logger = get_logger()
    
    def _on_new_files(self, files: list) -> None:
        """Callback when new files are detected."""
        self.logger.info("🚀 Uruchamiam automatyczny import...")
        
        try:
            # Run full pipeline
            result = self.pipeline.run_full()
            if result:
                self.logger.info(f"✅ Auto-import zakończony: {result}")
            else:
                self.logger.warning("⚠️ Auto-import zakończony z błędami")
        except Exception as e:
            self.logger.error(f"❌ Błąd auto-importu: {e}")
    
    def start(self) -> None:
        """Start watching and auto-importing."""
        self.watcher.watch(self._on_new_files)
    
    def stop(self) -> None:
        """Stop the auto-importer."""
        self.watcher.stop()
