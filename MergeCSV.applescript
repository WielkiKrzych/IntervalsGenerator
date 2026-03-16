#!/usr/bin/osascript
--
-- MergeCSV Droplet - Prawidłowa obsługa przeciągania plików
--

on open theFiles
	set allPaths to {}
	
	-- Zbierz ścieżki wszystkich plików
	repeat with aFile in theFiles
		set end of allPaths to POSIX path of aFile
	end repeat
	
	-- Zbuduj komendę shell
	set pythonPath to "/opt/homebrew/bin/python3"
	set scriptPath to POSIX path of ((path to home folder as text) & "Documents:IntervalsGenerator:quick_merge.py")
	
	set shellCmd to quoted form of pythonPath & " " & quoted form of scriptPath
	
	-- Dodaj wszystkie pliki
	repeat with filePath in allPaths
		set shellCmd to shellCmd & " " & quoted form of filePath
	end repeat
	
	-- Wykonaj komendę
	try
		do shell script shellCmd
		display dialog "✅ Pliki zostały połączone pomyślnie!" & return & return & "Sprawdź folder z plikami źródłowymi." buttons {"OK"} default button "OK" with icon note giving up after 5
	on error errMsg
		display dialog "❌ Błąd podczas łączenia plików:" & return & return & errMsg buttons {"OK"} default button "OK" with icon stop
	end try
end open

on run
	display dialog "To jest aplikacja typu 'droplet'. Przeciągnij pliki CSV i/lub FIT na ikonę MergeCSV, aby je połączyć." buttons {"OK"} default button "OK" with icon note
end run
