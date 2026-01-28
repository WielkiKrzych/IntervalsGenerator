"""
User interface implementations.
Implements DIP by providing concrete implementations of UserInterface.
"""

from typing import Optional
import logging

from .interfaces import UserInterface
from .logging_config import get_logger


class ConsoleUI(UserInterface):
    """
    Console-based user interface using print() and input().
    This is the production implementation.
    Now also logs all messages to file.
    """
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize console UI.
        
        Args:
            logger: Optional logger instance. Uses default if not provided.
        """
        self._logger = logger
    
    @property
    def logger(self) -> logging.Logger:
        if self._logger is None:
            self._logger = get_logger()
        return self._logger
    
    def print_message(self, message: str) -> None:
        print(message)
        self.logger.info(message.replace("🚀", "").replace("📁", "").replace("📅", "").strip())
    
    def print_success(self, message: str) -> None:
        print(f"✅ {message}")
        self.logger.info(f"SUCCESS: {message}")
    
    def print_warning(self, message: str) -> None:
        print(f"⚠️  {message}")
        self.logger.warning(message)
    
    def print_error(self, message: str) -> None:
        print(f"❌ {message}")
        self.logger.error(message)
    
    def ask_yes_no(self, question: str) -> bool:
        self.logger.info(f"PROMPT: {question}")
        while True:
            response = input(f"{question} (Y/N): ").strip().upper()
            if response == 'Y':
                self.logger.info("USER: Y")
                return True
            elif response == 'N':
                self.logger.info("USER: N")
                return False
            print("Proszę odpowiedzieć Y lub N.")
    
    def print_header(self, title: str) -> None:
        print(f"\n{'=' * 60}")
        print(f"🚀 {title}")
        print('=' * 60)
        self.logger.info(f"=== {title} ===")
    
    def print_separator(self) -> None:
        print("-" * 60)
    
    def print_progress(self, current: int, total: int, prefix: str = "") -> None:
        """
        Display a simple progress indicator.
        
        Args:
            current: Current item number (1-indexed)
            total: Total number of items
            prefix: Optional prefix text
        """
        pct = (current / total) * 100 if total > 0 else 0
        bar_len = 30
        filled = int(bar_len * current / total) if total > 0 else 0
        bar = "█" * filled + "░" * (bar_len - filled)
        
        print(f"\r{prefix}[{bar}] {current}/{total} ({pct:.0f}%)", end="", flush=True)
        
        if current >= total:
            print()  # New line at end


class SilentUI(UserInterface):
    """
    Silent user interface for testing and automation.
    Stores all messages and returns configurable responses.
    """
    
    def __init__(self, default_yes_no: bool = True):
        self.messages: list[tuple[str, str]] = []
        self.default_yes_no = default_yes_no
    
    def print_message(self, message: str) -> None:
        self.messages.append(("MESSAGE", message))
    
    def print_success(self, message: str) -> None:
        self.messages.append(("SUCCESS", message))
    
    def print_warning(self, message: str) -> None:
        self.messages.append(("WARNING", message))
    
    def print_error(self, message: str) -> None:
        self.messages.append(("ERROR", message))
    
    def ask_yes_no(self, question: str) -> bool:
        self.messages.append(("ASK", question))
        return self.default_yes_no
    
    def print_header(self, title: str) -> None:
        self.messages.append(("HEADER", title))
    
    def print_separator(self) -> None:
        self.messages.append(("SEPARATOR", ""))
    
    def print_progress(self, current: int, total: int, prefix: str = "") -> None:
        self.messages.append(("PROGRESS", f"{prefix}{current}/{total}"))
    
    def get_all_messages(self) -> list[tuple[str, str]]:
        """Get all recorded messages for testing assertions."""
        return self.messages.copy()
    
    def clear(self) -> None:
        """Clear all recorded messages."""
        self.messages.clear()


class StreamlitUI(UserInterface):
    """
    Streamlit-compatible user interface.
    Provides real-time feedback by writing to a Streamlit container and logging to file.
    """
    
    def __init__(self, log_placeholder, logger: Optional[logging.Logger] = None):
        """
        Initialize Streamlit UI.
        
        Args:
            log_placeholder: Streamlit 'empty' or 'container' for real-time messages.
            logger: Optional logger instance.
        """
        self.log_placeholder = log_placeholder
        self._logger = logger
        self.log_buffer = []
    
    @property
    def logger(self) -> logging.Logger:
        if self._logger is None:
            self._logger = get_logger()
        return self._logger
    
    def _update_ui(self, message: str, type: str = "text"):
        """Append message to buffer and update Streamlit placeholder."""
        clean_msg = message.replace("🚀", "").replace("📁", "").replace("📅", "").strip()
        
        icon = ""
        if type == "SUCCESS": icon = "✅ "
        elif type == "WARNING": icon = "⚠️ "
        elif type == "ERROR": icon = "❌ "
        elif type == "PROGRESS": icon = "⏳ "
        
        self.log_buffer.append(f"{icon}{message}")
        if len(self.log_buffer) > 20:  # Keep only last 20 messages for performance
            self.log_buffer = self.log_buffer[-20:]
            
        logs_text = "\n".join(self.log_buffer)
        self.log_placeholder.code(logs_text)
        
        # Also log to file
        if type == "ERROR":
            self.logger.error(clean_msg)
        elif type == "WARNING":
            self.logger.warning(clean_msg)
        else:
            self.logger.info(clean_msg)

    def print_message(self, message: str) -> None:
        self._update_ui(message)
    
    def print_success(self, message: str) -> None:
        self._update_ui(message, "SUCCESS")
    
    def print_warning(self, message: str) -> None:
        self._update_ui(message, "WARNING")
    
    def print_error(self, message: str) -> None:
        self._update_ui(message, "ERROR")
    
    def ask_yes_no(self, question: str) -> bool:
        # In Streamlit mode, we auto-confirm to avoid hanging if no user interaction is possible
        # Alternatively, we could use st.button, but pipeline is usually non-interactive here
        self.logger.info(f"AUTO-CONFIRM: {question}")
        return True
    
    def print_header(self, title: str) -> None:
        header_text = f"\n=== {title} ==="
        self._update_ui(header_text)
    
    def print_separator(self) -> None:
        self._update_ui("-" * 40)
    
    def print_progress(self, current: int, total: int, prefix: str = "") -> None:
        self._update_ui(f"{prefix}{current}/{total}", "PROGRESS")
