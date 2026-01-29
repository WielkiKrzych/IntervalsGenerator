# Cross-Platform Support Guide

This document describes how to build and use Intervals Generator on both macOS and Windows.

## macOS (Mac Mini - Your home machine)

### Existing Setup
- **MergeCSV.app** - AppleScript droplet (already exists)
- Located in: `MergeCSV.app/`
- Accepts drag-and-drop of CSV files

### How to Use (macOS)
1. Drag and drop your CSV files (Wahoo, TrainRed, Garmin, Tymewear) onto `MergeCSV.app`
2. Wait for processing
3. Output file `Trening-DD.MM.YYYY-import.csv` appears in the same directory as source files

### After Git Pull
When you pull changes from GitHub on your Mac Mini:
```bash
cd ~/Desktop/IntervalsGenerator/IntervalsGenerator
git pull origin main
```

**The MergeCSV.app will continue to work normally.** No action needed.

### Regenerating .app (Optional)
If you ever need to recreate the .app:
1. Open **Script Editor** on macOS
2. Create a new droplet script that runs: `python3 quick_merge.py <dropped_files>`
3. Save as application bundle

The existing `MergeCSV.app` is a compiled AppleScript droplet that works perfectly.

---

## Windows (Laptop - Your current device)

### New Setup (Just Created)
- **MergeCSV.exe** - PyInstaller executable (newly built)
- Located in: `dist/MergeCSV.exe`
- Supports drag-and-drop AND GUI dialog for file selection

### How to Build (Windows)
```batch
# From IntervalsGenerator directory:
build_windows.bat
```

Or manually:
```batch
# Install PyInstaller (one-time):
pip install -r requirements.txt

# Build executable:
python -m PyInstaller --clean MergeCSV.spec
```

### How to Use (Windows)

**Method 1: Drag & Drop**
1. Select CSV files (Wahoo, TrainRed, Garmin, Tymewear)
2. Drag them onto `MergeCSV.exe`
3. Wait for processing
4. Success popup appears with output file location

**Method 2: Double-Click (GUI Dialog)**
1. Double-click `MergeCSV.exe`
2. File selection dialog appears (opens in Downloads by default)
3. Select one or more CSV files
4. Click "Open"
5. Wait for processing
6. Success popup appears with output file location

### Output Location
- Output file: `Trening-DD.MM.YYYY-import.csv`
- Location: Same directory as the source CSV files

---

## Cross-Platform Compatibility

### Files That Work on Both Platforms
| File | Platform | Purpose |
|-------|----------|---------|
| `quick_merge.py` | Both | Core CSV merging logic |
| `main.py` | Both | CLI entry point |
| `app.py` | Both | Streamlit web GUI |
| `requirements.txt` | Both | Python dependencies |
| `pyproject.toml` | Both | Package configuration |

### Platform-Specific Files
| File | Platform | Purpose |
|-------|----------|---------|
| `MergeCSV.app/` | macOS | Compiled AppleScript droplet |
| `MergeCSV.exe` | Windows | PyInstaller executable |
| `build_windows.bat` | Windows | Build script for .exe |
| `build_macos.sh` | macOS | Build script (for future PyInstaller builds) |
| `windows_launcher.py` | Windows | Launcher with GUI dialog |
| `MergeCSV.spec` | Windows | PyInstaller spec file |

### Files NOT in Git (Ignored by .gitignore)
These are excluded from version control because they're platform-specific build artifacts:
- `dist/` - Contains compiled executables (MergeCSV.exe)
- `build/` - PyInstaller temporary build files
- `*.spec` - PyInstaller spec files (MergeCSV.spec)

**Result:** When you pull on Mac, `dist/MergeCSV.exe` won't appear. When you pull on Windows, `MergeCSV.app` won't be affected.

---

## Syncing Between Devices

### On Windows
1. Work on Windows (building .exe, testing, etc.)
2. Commit changes:
   ```bash
   git add .
   git commit -m "feat: your changes"
   ```
3. Push to GitHub:
   ```bash
   git push
   ```

### On macOS
1. Pull latest changes:
   ```bash
   cd ~/Desktop/IntervalsGenerator/IntervalsGenerator
   git pull origin main
   ```
2. **MergeCSV.app continues working** - it's a compiled bundle not affected by Python code changes
3. If you want to update the .app to use new Python code, recreate it using Script Editor

---

## Testing Checklist

### On Windows (Current Device)
- [ ] Build `MergeCSV.exe` with `build_windows.bat`
- [ ] Test drag-drop with sample CSV files
- [ ] Test double-click to open GUI dialog
- [ ] Verify output file is created
- [ ] Verify console output shows processing steps

### On macOS (Home Device)
- [ ] Pull latest changes: `git pull origin main`
- [ ] Test drag-drop with sample CSV files on `MergeCSV.app`
- [ ] Verify output file is created
- [ ] Verify Python code changes work correctly

---

## Troubleshooting

### Windows: PyInstaller Fails
**Problem:** Build fails with import errors
**Solution:** Make sure all dependencies are installed:
```batch
pip install -r requirements.txt
```

### macOS: .app Doesn't Open
**Problem:** Double-clicking .app does nothing
**Solution:** Give it execution permission (unlikely needed, but try):
```bash
chmod +x MergeCSV.app/Contents/MacOS/droplet
```

### Cross-Platform: Python Version Mismatch
**Problem:** Code works on one device but not the other
**Solution:** Make sure Python 3.10+ is installed on both:
```bash
python3 --version  # Should be 3.10 or higher
```

---

## Summary

✅ **Windows executable created successfully:** `dist/MergeCSV.exe` (62MB)
✅ **All changes committed and pushed to GitHub**
✅ **Cross-platform compatible:** Mac .app continues working
✅ **No breaking changes:** Existing files unaffected

**Next Steps:**
1. Test `MergeCSV.exe` with real CSV files on Windows
2. Pull changes on Mac Mini and verify `MergeCSV.app` still works
3. (Optional) Create release on GitHub with both executables attached
