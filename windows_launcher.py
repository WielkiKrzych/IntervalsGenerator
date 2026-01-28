#!/usr/bin/env python3
"""
Windows Launcher for Intervals Generator
Provides GUI dialog and drag-drop support for Windows users.
"""

import sys
import os
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox
from datetime import datetime
import subprocess

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

try:
    from quick_merge import main as quick_merge_main
except ImportError:
    print("Error: Could not import quick_merge module")
    sys.exit(1)


def show_file_dialog():
    """Show file selection dialog for CSV files."""
    root = tk.Tk()
    root.withdraw()  # Hide main window

    root.call('wm', 'attributes', '.', '-topmost', True)

    files = filedialog.askopenfilenames(
        title="Wybierz pliki CSV do połączenia",
        filetypes=[
            ("Pliki CSV", "*.csv"),
            ("Wszystkie pliki", "*.*")
        ],
        initialdir=str(Path.home() / "Downloads")
    )

    root.destroy()
    return list(files)


def show_success_message(output_path: Path):
    """Show success message with output file location."""
    root = tk.Tk()
    root.withdraw()
    root.call('wm', 'attributes', '.', '-topmost', True)

    message = f"""
✅ Pliki zostały połączone!

📁 Plik wyjściowy:
{output_path}

Możesz teraz otworzyć ten plik w Intervals.icu lub innej aplikacji treningowej.
    """.strip()

    messagebox.showinfo("Sukces", message)
    root.destroy()


def show_error_message(error: str):
    """Show error message."""
    root = tk.Tk()
    root.withdraw()
    root.call('wm', 'attributes', '.', '-topmost', True)

    messagebox.showerror("Błąd", f"❌ {error}")
    root.destroy()


def launch_quick_merge(files):
    """Launch quick_merge.py with selected files."""
    try:
        # Override sys.argv to pass files to quick_merge
        original_argv = sys.argv
        sys.argv = [sys.argv[0]] + [str(f) for f in files]

        # Call quick_merge_main with a custom sys.argv
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument("files", nargs="*", type=Path)
        args = parser.parse_args()

        # Import and use quick_merge functions directly
        from quick_merge import (
            find_csv_files,
            detect_file_type,
            process_wahoo,
            process_trainred,
            process_tymewear,
            process_garmin,
            merge_dataframes,
        )
        from pathlib import Path
        from datetime import datetime

        csv_files = files if files else find_csv_files(Path.cwd())

        if not csv_files:
            show_error_message("Nie znaleziono plików CSV!")
            return

        # File type detection
        files_by_type = {
            "wahoo": [],
            "garmin": [],
            "trainred": [],
            "tymewear": [],
            "unknown": [],
        }

        for f in csv_files:
            ftype = detect_file_type(f)
            if ftype:
                files_by_type[ftype].append(f)

        if not files_by_type["wahoo"]:
            show_error_message("Nie znaleziono pliku bazowego Wahoo!")
            return

        # Process files
        base_df = process_wahoo(files_by_type["wahoo"][0])
        if base_df.empty:
            show_error_message("Błąd przetwarzania pliku Wahoo!")
            return

        other_dfs = []
        for f in files_by_type["trainred"]:
            df = process_trainred(f)
            if not df.empty:
                other_dfs.append(df)
        for f in files_by_type["tymewear"]:
            df = process_tymewear(f)
            if not df.empty:
                other_dfs.append(df)
        for f in files_by_type["garmin"]:
            df = process_garmin(f)
            if not df.empty:
                other_dfs.append(df)

        # Merge
        df_merged = merge_dataframes(base_df, other_dfs)

        if df_merged.empty:
            show_error_message("Błąd łączenia plików!")
            return

        # Save output
        today = datetime.now().strftime("%d.%m.%Y")
        output_filename = f"Trening-{today}-import.csv"

        if files:
            output_dir = files[0].parent
        elif files_by_type["wahoo"]:
            output_dir = files_by_type["wahoo"][0].parent
        else:
            output_dir = Path.cwd()

        output_path = output_dir / output_filename
        df_merged.to_csv(output_path, index=False)

        # Restore original sys.argv
        sys.argv = original_argv

        # Show success
        show_success_message(output_path)

    except Exception as e:
        show_error_message(str(e))
        raise


def main():
    """Main entry point for Windows launcher."""
    # Check if files were drag-dropped
    dropped_files = sys.argv[1:]

    if dropped_files:
        # Filter for CSV files only
        csv_files = [Path(f) for f in dropped_files if Path(f).suffix.lower() == ".csv"]

        if not csv_files:
            show_error_message("Przeciągnij pliki CSV na tę ikonę!")
            return

        launch_quick_merge(csv_files)
    else:
        # No files dropped - show dialog
        files = show_file_dialog()

        if not files:
            # User cancelled
            return

        launch_quick_merge([Path(f) for f in files])


if __name__ == "__main__":
    main()
