#!/usr/bin/env python3
"""
Intervals Generator - SOLID Refactored
Main entry point for the application.

Usage:
    python main.py                  # Run full pipeline
    python main.py --import-only    # Only import from downloads
    python main.py --validate-only  # Only validate existing files
    python main.py --merge-only     # Only merge (skip import/processing)
    python main.py --dry-run        # Simulate without making changes
    python main.py --with-backup    # Create backup before operations
    python main.py --generate-report # Generate HTML report after merge
    python main.py --watch          # Watch downloads and auto-import
"""

import argparse
import sys
import webbrowser
from pathlib import Path

from intervals.config import Config
from intervals.pipeline import Pipeline
from intervals.logging_config import setup_logging
from intervals.filesystem import RealFileSystem
from intervals.ui import ConsoleUI


def parse_args():
    parser = argparse.ArgumentParser(
        description="Intervals Generator - Import and merge training data from multiple sources",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                          Run full pipeline
  python main.py --dry-run                Simulate without changes
  python main.py --with-backup            Create backup before running
  python main.py --merge-only --generate-report  Merge and generate HTML report
  python main.py --watch                  Watch downloads for auto-import
        """
    )
    
    # Mode selection
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--import-only",
        action="store_true",
        help="Only import files from downloads (no processing or merging)"
    )
    mode_group.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate existing files (no import or merging)"
    )
    mode_group.add_argument(
        "--merge-only",
        action="store_true",
        help="Only merge existing files (skip import and processing)"
    )
    mode_group.add_argument(
        "--watch",
        action="store_true",
        help="Watch downloads directory and auto-import new files"
    )
    
    # Options
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate operations without making any changes"
    )
    parser.add_argument(
        "--with-backup",
        action="store_true",
        help="Create a backup before running operations"
    )
    parser.add_argument(
        "--generate-report",
        action="store_true",
        help="Generate HTML report after merging"
    )
    
    # Path overrides
    parser.add_argument(
        "--base-dir",
        type=str,
        help="Override base directory (default: auto-detect based on OS)"
    )
    parser.add_argument(
        "--downloads-dir",
        type=str,
        help="Override downloads directory (default: auto-detect based on OS)"
    )
    
    # Logging
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging (DEBUG level)"
    )
    
    return parser.parse_args()


def main():
    args = parse_args()
    
    # Configure paths
    if args.base_dir or args.downloads_dir:
        config = Config(
            base_dir=Path(args.base_dir) if args.base_dir else Config.from_env().base_dir,
            downloads_dir=Path(args.downloads_dir) if args.downloads_dir else Config.from_env().downloads_dir
        )
    else:
        config = Config.from_env()
    
    # Ensure directories exist
    config.ensure_directories()
    
    # Setup logging
    import logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    log_dir = config.base_dir / "logs"
    setup_logging(log_dir=log_dir, level=log_level)
    
    # Create filesystem (with dry-run support)
    fs = RealFileSystem(dry_run=args.dry_run)
    ui = ConsoleUI()
    
    if args.dry_run:
        ui.print_warning("TRYB DRY-RUN: Żadne pliki nie zostaną zmodyfikowane!")
        print()
    
    # Create pipeline
    pipeline = Pipeline(config, fs=fs, ui=ui)
    
    # Create backup if requested
    if args.with_backup and not args.dry_run:
        from intervals.backup import BackupManager
        backup_mgr = BackupManager(config.base_dir)
        backup_path = backup_mgr.create_backup()
        ui.print_success(f"Backup utworzony: {backup_path.name}")
    
    # Run based on mode
    if args.watch:
        from intervals.watcher import AutoImporter
        auto_importer = AutoImporter(pipeline, config.downloads_dir)
        try:
            auto_importer.start()
        except KeyboardInterrupt:
            auto_importer.stop()
            
    elif args.import_only:
        pipeline.run_cleanup()
        pipeline.run_import()
        print("\n✅ Import zakończony. Użyj --merge-only aby połączyć pliki.")
        
    elif args.validate_only:
        pipeline.run_validation()
        
    elif args.merge_only:
        pipeline.run_validation()
        result = pipeline.run_merge()
        if result:
            print(f"\n✅ Plik utworzony: {result}")
            _generate_report_if_requested(args, config, result, ui)
        else:
            print("\n❌ Nie udało się utworzyć pliku.")
            sys.exit(1)
    else:
        # Full pipeline
        result = pipeline.run_full()
        if result:
            print(f"\n✅ Sukces! Plik gotowy: {result}")
            _generate_report_if_requested(args, config, result, ui)
        else:
            print("\n❌ Pipeline zakończony z błędami.")
            sys.exit(1)
    
    # Show dry-run summary
    if args.dry_run:
        operations = fs.get_operations_log()
        if operations:
            print("\n📋 SYMULACJA - operacje, które zostałyby wykonane:")
            for op in operations[:20]:  # Limit to first 20
                print(f"   • {op}")
            if len(operations) > 20:
                print(f"   ... i {len(operations) - 20} więcej")


def _generate_report_if_requested(args, config, output_path: Path, ui):
    """Generate HTML report if requested."""
    if not args.generate_report:
        return
    
    try:
        import pandas as pd
        from intervals.report import ReportGenerator
        
        df = pd.read_csv(output_path)
        report_dir = config.base_dir / "reports"
        report_path = report_dir / f"report_{config.today.strftime('%Y%m%d_%H%M%S')}.html"
        
        generator = ReportGenerator(report_dir)
        generator.generate_html_report(df, report_path, output_path.name)
        
        ui.print_success(f"Raport HTML: {report_path}")
        
        # Open in browser
        webbrowser.open(f"file://{report_path}")
        
    except Exception as e:
        ui.print_error(f"Błąd generowania raportu: {e}")


if __name__ == "__main__":
    main()
