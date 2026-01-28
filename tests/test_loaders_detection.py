import pytest
from pathlib import Path
from unittest.mock import Mock, patch, mock_open
from intervals.loaders.trainred import TrainRedLoader
from intervals.loaders.tymewear import TymewearLoader

class TestTrainRedDetection:
    @pytest.fixture
    def loader(self):
        config = Mock()
        config.trainred_dir = Path("/tmp/trainred")
        config.trainred_old_dir = Path("/tmp/trainred_old")
        return TrainRedLoader(config, Mock(), Mock())

    def test_detect_by_content_smo2_thb(self, loader):
        """Test detection by SmO2 and THb columns in header."""
        path = Path("session_20251231_104129.csv")
        content = "Timestamp (seconds passed),SmO2,THb,Other\n0.0,70,12,5\n"
        
        with patch("builtins.open", mock_open(read_data=content)):
            assert loader.detect_in_downloads(path) is True

    def test_detect_by_content_thb_unfiltered(self, loader):
        """Test detection with THb unfiltered variant."""
        path = Path("any_file_name.csv")
        content = "Time,SmO2,THb unfiltered,Data\n0.0,70,12,5\n"
        
        with patch("builtins.open", mock_open(read_data=content)):
            assert loader.detect_in_downloads(path) is True

    def test_reject_missing_columns(self, loader):
        """Test rejection when SmO2 or THb is missing."""
        path = Path("random.csv")
        content = "col1,col2,col3\n1,2,3\n"
        
        with patch("builtins.open", mock_open(read_data=content)):
            assert loader.detect_in_downloads(path) is False

    def test_reject_non_csv(self, loader):
        """Test rejection of non-CSV files."""
        assert loader.detect_in_downloads(Path("data.txt")) is False
        assert loader.detect_in_downloads(Path("data.xml")) is False

class TestTymewearDetection:
    @pytest.fixture
    def loader(self):
        config = Mock()
        config.tymewear_dir = Path("/tmp/tymewear")
        config.tymewear_old_dir = Path("/tmp/tymewear_old")
        return TymewearLoader(config, Mock(), Mock())

    def test_detect_case_insensitive(self, loader):
        """Test detection of headers with different casing."""
        path = Path("data.csv")
        
        # Standard case
        content_standard = "BR,VT,VE\n12,0.5,6.0\n"
        with patch("builtins.open", mock_open(read_data=content_standard)):
            assert loader.detect_in_downloads(path) is True

        # Lowercase (some exports might do this)
        content_lower = "br,vt,ve\n12,0.5,6.0\n"
        with patch("builtins.open", mock_open(read_data=content_lower)):
            assert loader.detect_in_downloads(path) is True

    def test_detect_with_metadata_header(self, loader):
        """Test detection when headers are not on the first line."""
        path = Path("data.csv")
        content = "Some device info\nMore info\nBR,VT,VE\n12,0.5,6.0\n"
        
        with patch("builtins.open", mock_open(read_data=content)):
            assert loader.detect_in_downloads(path) is True
