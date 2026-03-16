#!/bin/bash
# MergeCSV Droplet

PYTHON="/opt/homebrew/bin/python3"
SCRIPT="$HOME/Documents/IntervalsGenerator/quick_merge.py"

if [ $# -eq 0 ]; then
    osascript <<EOF
display dialog "To jest aplikacja typu droplet. Przeciągnij pliki CSV i/lub FIT na ikonę MergeCSV, aby je połączyć." buttons {"OK"} default button "OK" with icon note
EOF
    exit 0
fi

TMP_OUTPUT=$(mktemp)
$PYTHON "$SCRIPT" "$@" > "$TMP_OUTPUT" 2>&1
RESULT=$?
OUTPUT=$(cat "$TMP_OUTPUT")
rm "$TMP_OUTPUT"

if [ $RESULT -eq 0 ]; then
    osascript <<EOF
display dialog "✅ Pliki zostały połączone!" & return & return & "Sprawdź folder z plikami źródłowymi." buttons {"OK"} default button "OK" with icon note giving up after 5
EOF
else
    osascript <<EOF
display dialog "❌ Błąd:" & return & return & "$OUTPUT" buttons {"OK"} default button "OK" with icon stop
EOF
fi
