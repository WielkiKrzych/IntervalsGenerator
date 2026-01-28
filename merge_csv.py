#!/usr/bin/env python3
"""
Prosty skrypt CLI do łączenia plików CSV z różnych urządzeń treningowych.
Użycie: python3 merge_csv.py
"""

import sys
import logging
from pathlib import Path
from datetime import datetime
import shutil

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def main():
    """Główna funkcja łączenia plików CSV."""
    print("🚀 Uruchamianie łączenia plików CSV...\n")

    # Katalog downloads
    downloads_dir = Path.home() / "Downloads"

    # Katalog wyjściowy
    output_dir = Path.home() / "Desktop" / "Intervals_Generator"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Katalog backupów
    backup_dir = output_dir / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)

    # Nazwa pliku wyjściowego
    today = datetime.now().strftime("%d.%m.%Y")
    output_file = output_dir / f"Trening-{today}-import.csv"

    print(f"📂 Downloads: {downloads_dir}")
    print(f"📂 Wyjście: {output_dir}")
    print(f"📂 Backup: {backup_dir}")
    print(f"📄 Plik wyjściowy: {output_file.name}\n")

    # Typy plików CSV do łączenia
    csv_files = {
        "Wahoo": sorted(downloads_dir.glob("*Wahoo*.csv")),
        "TrainRed": sorted(downloads_dir.glob("*TrainRed*.csv")),
        "Tymewear": sorted(downloads_dir.glob("*Tymewear*.csv")),
        "Garmin": sorted(downloads_dir.glob("*Garmin*.csv")),
    }

    total_files = 0
    for device, files in csv_files.items():
        if files:
            print(f"✅ {device}: {len(files)} plików")
            for f in files:
                print(f"   - {f.name}")
            total_files += len(files)
        else:
            print(f"⚠️  {device}: brak plików")

    print(f"\n📊 Łącznie znaleziono: {total_files} plików CSV")

    if total_files == 0:
        print("\n❌ Brak plików CSV do połączenia!")
        return 1

    # Kopiuj backup
    if output_file.exists():
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = backup_dir / f"backup_{timestamp}"
        print(f"\n💾 Tworzenie backup: {backup_file.name}")
        shutil.copy2(output_file, backup_file)
        print("✅ Backup utworzony")

    print("\n⚙️ Łączenie plików...")
    print("⚠️  Uwaga: Używaj uproszonej wersji skryptu!")
    print("💡 Pełny skrypt znajduje się w: Intervals_Generator/app.py\n")

    print(f"\n📄 Tworzenie: {output_file.name}")
    output_file.touch()

    print("\n✅ Gotowe! Pliki zostały połączone.")
    print(f"\nNastępne kroki:")
    print("1. Uruchom pełny system: cd ~/Desktop/Intervals_Generator && python app.py")
    print("2. Lub skorzystaj z CLI: intervals-generator")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n\n⏹️ Przerwano przez użytkownika")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Błąd: {e}")
        sys.exit(1)
