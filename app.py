#!/usr/bin/env python3
"""
Intervals Generator - Streamlit GUI
Visual interface for managing training data import and merge.
"""

import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime
import sys
import io
import contextlib

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from intervals.config import Config
from intervals.pipeline import Pipeline
from intervals.filesystem import RealFileSystem
from intervals.ui import SilentUI, StreamlitUI
from intervals.backup import BackupManager
from intervals.report import ReportGenerator
from intervals.logging_config import setup_logging


# Initialize logging for the web app
def init_app():
    config = Config.from_env()
    log_dir = config.base_dir / "logs"
    setup_logging(log_dir=log_dir)

init_app()


# Page config
st.set_page_config(
    page_title="Intervals Generator",
    page_icon="🏋️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        background: linear-gradient(90deg, #e94560, #ff6b6b);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 1rem;
    }
    .status-card {
        background: linear-gradient(135deg, #16213e, #0f3460);
        border-radius: 12px;
        padding: 1.5rem;
        margin: 0.5rem 0;
        border: 1px solid rgba(233, 69, 96, 0.3);
    }
    .stat-number {
        font-size: 2rem;
        font-weight: 700;
        color: #e94560;
    }
    .success-msg { color: #4ade80; }
    .warning-msg { color: #fbbf24; }
    .error-msg { color: #f87171; }
</style>
""", unsafe_allow_html=True)


def get_config() -> Config:
    """Get or create config from session state."""
    if 'config' not in st.session_state:
        st.session_state.config = Config.from_env()
        st.session_state.config.ensure_directories()
    return st.session_state.config


def count_files(directory: Path, pattern: str = "*.csv") -> int:
    """Count matching files in directory."""
    if not directory.exists():
        return 0
    return len(list(directory.glob(pattern)))


def get_latest_training_file(base_dir: Path) -> Path | None:
    """Get the most recent Trening-*.csv file."""
    files = sorted(base_dir.glob("Trening-*.csv"), reverse=True)
    return files[0] if files else None


def render_sidebar():
    """Render sidebar with status info."""
    config = get_config()
    
    st.sidebar.markdown("## 📊 Status")
    
    # File counts
    trainred_count = count_files(config.trainred_dir)
    tymewear_count = count_files(config.tymewear_dir)
    wahoo_count = count_files(config.wahoo_dir)
    garmin_count = count_files(config.garmin_dir)
    
    st.sidebar.metric("TrainRed", trainred_count)
    st.sidebar.metric("Tymewear", tymewear_count)
    st.sidebar.metric("Wahoo", wahoo_count)
    st.sidebar.metric("Garmin", garmin_count)
    
    st.sidebar.divider()
    
    # Latest output file
    latest = get_latest_training_file(config.base_dir)
    if latest:
        st.sidebar.success(f"📁 {latest.name}")
    else:
        st.sidebar.warning("Brak pliku wynikowego")
    
    st.sidebar.divider()
    st.sidebar.caption(f"📂 {config.base_dir}")


def render_main_panel():
    """Render main control panel."""
    config = get_config()
    
    st.markdown('<h1 class="main-header">🏋️ Intervals Generator</h1>', unsafe_allow_html=True)
    st.caption("Import i scalanie danych treningowych z wielu źródeł")
    
    # Control buttons
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("🚀 Uruchom Pipeline", type="primary", use_container_width=True):
            run_pipeline(config, mode="full")
    
    with col2:
        if st.button("📥 Tylko Import", use_container_width=True):
            run_pipeline(config, mode="import")
    
    with col3:
        if st.button("🔗 Tylko Merge", use_container_width=True):
            run_pipeline(config, mode="merge")
    
    with col4:
        if st.button("✅ Walidacja", use_container_width=True):
            run_pipeline(config, mode="validate")
    
    st.divider()
    
    # Options
    col1, col2 = st.columns(2)
    
    with col1:
        st.session_state.with_backup = st.checkbox(
            "💾 Utwórz backup przed operacją",
            value=st.session_state.get('with_backup', False)
        )
    
    with col2:
        st.session_state.generate_report = st.checkbox(
            "📋 Generuj raport HTML",
            value=st.session_state.get('generate_report', True)
        )


def run_pipeline(config: Config, mode: str):
    """Run pipeline with given mode and show progress."""
    
    with st.spinner(f"Przetwarzanie... ({mode})"):
        # Capture output
        output_buffer = io.StringIO()
        
        try:
            # Create live log area
            st.info("🚀 Rozpoczynam operację...")
            log_placeholder = st.empty()
            
            # Create Streamlit UI for real-time feedback
            ui = StreamlitUI(log_placeholder)
            fs = RealFileSystem(dry_run=False)
            pipeline = Pipeline(config, fs=fs, ui=ui)
            
            # Backup if requested
            if st.session_state.get('with_backup', False):
                with st.status("Tworzenie backupu...", expanded=False):
                    backup_mgr = BackupManager(config.base_dir)
                    backup_path = backup_mgr.create_backup()
                    st.success(f"💾 Backup utworzony: {backup_path.name}")
            
            # Run based on mode
            result = None
            if mode == "full":
                result = pipeline.run_full()
            elif mode == "import":
                pipeline.run_cleanup()
                pipeline.run_import()
                st.success("✅ Import zakończony.")
            elif mode == "merge":
                pipeline.run_validation()
                result = pipeline.run_merge()
            elif mode == "validate":
                is_valid = pipeline.run_validation()
                if is_valid:
                    st.success("✅ Walidacja OK - brak luk w danych")
                else:
                    st.warning("⚠️ Znaleziono luki w danych")
            
            # Success message logic (moved inside try to ensure it only shows on actual success)
            if result:
                st.success(f"✅ Sukces! Utworzono: {result.name}")
                
                # Generate report if requested
                if st.session_state.get('generate_report', False):
                    generate_and_show_report(config, result)
                
        except Exception as e:
            st.error(f"❌ Błąd: {e}")
            st.exception(e)


def generate_and_show_report(config: Config, output_path: Path):
    """Generate HTML report and display preview."""
    try:
        df = pd.read_csv(output_path)
        report_dir = config.base_dir / "reports"
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_path = report_dir / f"report_{timestamp}.html"
        
        generator = ReportGenerator(report_dir)
        generator.generate_html_report(df, report_path, output_path.name)
        
        st.success(f"📋 Raport zapisany: {report_path.name}")
        
        # Show data preview
        st.subheader("📊 Podgląd danych")
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Wiersze", len(df))
        col2.metric("Kolumny", len(df.columns))
        col3.metric("Czas (min)", len(df) // 60)
        
        st.dataframe(df.head(100), use_container_width=True)
        
        # Download button
        csv_data = df.to_csv(index=False)
        st.download_button(
            "📥 Pobierz CSV",
            csv_data,
            file_name=output_path.name,
            mime="text/csv"
        )
        
    except Exception as e:
        st.error(f"Błąd generowania raportu: {e}")


def render_backup_panel():
    """Render backup management panel."""
    config = get_config()
    
    st.subheader("💾 Zarządzanie backupami")
    
    backup_mgr = BackupManager(config.base_dir)
    backups = backup_mgr.list_backups()
    
    if backups:
        st.write(f"Znaleziono {len(backups)} backupów:")
        
        for backup in backups[:10]:  # Show last 10
            col1, col2 = st.columns([3, 1])
            size_kb = backup.stat().st_size / 1024
            col1.text(f"📦 {backup.name} ({size_kb:.1f} KB)")
            if col2.button("🔄 Przywróć", key=backup.name):
                with st.spinner("Przywracanie..."):
                    if backup_mgr.restore_backup(backup):
                        st.success("✅ Przywrócono!")
                    else:
                        st.error("❌ Błąd przywracania")
    else:
        st.info("Brak backupów")
    
    if st.button("🧹 Usuń stare backupy (>30 dni)"):
        removed = backup_mgr.cleanup_old_backups(30)
        st.success(f"Usunięto {removed} starych backupów")


def main():
    """Main Streamlit app."""
    render_sidebar()
    
    # Tabs
    tab1, tab2, tab3 = st.tabs(["🏠 Pipeline", "💾 Backupy", "📁 Pliki"])
    
    with tab1:
        render_main_panel()
    
    with tab2:
        render_backup_panel()
    
    with tab3:
        config = get_config()
        st.subheader("📁 Struktura katalogów")
        
        directories = [
            ("TrainRed", config.trainred_dir),
            ("Tymewear", config.tymewear_dir),
            ("Wahoo", config.wahoo_dir),
            ("Garmin", config.garmin_dir),
        ]
        
        for name, path in directories:
            with st.expander(f"📂 {name}"):
                if path.exists():
                    files = list(path.glob("*.csv"))
                    if files:
                        for f in files:
                            st.text(f"  📄 {f.name}")
                    else:
                        st.caption("Brak plików CSV")
                else:
                    st.caption("Katalog nie istnieje")


if __name__ == "__main__":
    main()
