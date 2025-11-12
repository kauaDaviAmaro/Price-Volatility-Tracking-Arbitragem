"""
Tests for CSVStorageManager - CSV operations, file locking, data merging
"""
import pytest
import csv
import asyncio
from pathlib import Path
import tempfile
import shutil
from unittest.mock import patch

from src.pipelines.csv_storage import CSVStorageManager


@pytest.fixture
def temp_output_dir():
    """Create temporary output directory"""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


class TestCSVStorageManagerInitialization:
    """Tests for CSVStorageManager initialization"""
    
    def test_init_with_defaults(self, temp_output_dir):
        """Test initialization with defaults"""
        storage = CSVStorageManager(temp_output_dir)
        
        assert storage.output_dir == temp_output_dir
        assert storage.filename == "scraped_data.csv"
        assert storage.filepath == temp_output_dir / "scraped_data.csv"
    
    def test_init_with_custom_filename(self, temp_output_dir):
        """Test initialization with custom filename"""
        storage = CSVStorageManager(temp_output_dir, filename="custom.csv")
        
        assert storage.filename == "custom.csv"
        assert storage.filepath == temp_output_dir / "custom.csv"


class TestCSVStorageManagerFieldnameValidation:
    """Tests for fieldname validation"""
    
    def test_is_valid_fieldname(self, temp_output_dir):
        """Test fieldname validation"""
        storage = CSVStorageManager(temp_output_dir)
        
        assert storage.is_valid_fieldname("url") is True
        assert storage.is_valid_fieldname("price") is True
        assert storage.is_valid_fieldname("column_42") is False
        assert storage.is_valid_fieldname("column_123") is False
        assert storage.is_valid_fieldname("") is False
        assert storage.is_valid_fieldname(None) is False
    
    def test_filter_valid_fieldnames(self, temp_output_dir):
        """Test filtering valid fieldnames"""
        storage = CSVStorageManager(temp_output_dir)
        
        fieldnames = ["url", "price", "column_42", "title", "column_99"]
        filtered = storage.filter_valid_fieldnames(fieldnames)
        
        assert "url" in filtered
        assert "price" in filtered
        assert "title" in filtered
        assert "column_42" not in filtered
        assert "column_99" not in filtered


class TestCSVStorageManagerDataConversion:
    """Tests for data conversion"""
    
    def test_convert_result_to_row(self, temp_output_dir):
        """Test converting result to CSV row"""
        storage = CSVStorageManager(temp_output_dir)
        
        result = {
            "url": "https://example.com",
            "price": 100000,
            "images": ["img1.jpg", "img2.jpg"],
            "metadata": {"key": "value"},
            "column_42": "should_be_filtered"
        }
        
        row = storage.convert_result_to_row(result)
        
        assert row["url"] == "https://example.com"
        assert row["price"] == 100000
        assert isinstance(row["images"], str)  # List converted to string
        assert isinstance(row["metadata"], str)  # Dict converted to string
        assert "column_42" not in row  # Invalid fieldname filtered
    
    def test_get_all_fieldnames(self, temp_output_dir):
        """Test extracting all fieldnames from listings"""
        storage = CSVStorageManager(temp_output_dir)
        
        listings = [
            {"url": "https://example.com/1", "price": 100000},
            {"url": "https://example.com/2", "price": 200000, "title": "Test"},
            {"column_42": "should_be_filtered"}
        ]
        
        fieldnames = storage.get_all_fieldnames(listings)
        
        assert "url" in fieldnames
        assert "price" in fieldnames
        assert "title" in fieldnames
        assert "column_42" not in fieldnames


class TestCSVStorageManagerSaveOperations:
    """Tests for save operations"""
    
    @pytest.mark.asyncio
    async def test_save_single_listing(self, temp_output_dir):
        """Test saving single listing"""
        storage = CSVStorageManager(temp_output_dir)
        
        listing = {
            "url": "https://example.com/listing",
            "price": 100000,
            "title": "Test Listing"
        }
        
        await storage.save_single_listing(listing)
        
        assert storage.filepath.exists()
        
        with open(storage.filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 1
            assert rows[0]["url"] == "https://example.com/listing"
            assert rows[0]["price"] == "100000"
    
    @pytest.mark.asyncio
    async def test_save_single_listing_updates_existing(self, temp_output_dir):
        """Test updating existing listing"""
        storage = CSVStorageManager(temp_output_dir)
        
        # Save initial listing
        listing1 = {
            "url": "https://example.com/listing",
            "price": 100000,
            "title": "Initial Title"
        }
        await storage.save_single_listing(listing1)
        
        # Update with new data
        listing2 = {
            "url": "https://example.com/listing",
            "price": 150000,
            "title": "Updated Title",
            "area": 100
        }
        await storage.save_single_listing(listing2)
        
        # Verify update
        with open(storage.filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 1
            assert rows[0]["price"] == "150000"
            assert rows[0]["title"] == "Updated Title"
            assert rows[0]["area"] == "100"
    
    @pytest.mark.asyncio
    async def test_save_single_listing_skips_technical_errors(self, temp_output_dir):
        """Test that technical errors are not saved"""
        storage = CSVStorageManager(temp_output_dir)
        
        # Technical error (only error and url)
        error_listing = {
            "url": "https://example.com/listing",
            "error": "BrowserType.launch: Failed to launch browser"
        }
        
        await storage.save_single_listing(error_listing)
        
        # File should not exist or be empty
        if storage.filepath.exists():
            with open(storage.filepath, 'r', encoding='utf-8') as f:
                content = f.read()
                # Should not have the error listing
                assert "BrowserType.launch" not in content
    
    def test_save_page_listings(self, temp_output_dir):
        """Test saving page of listings"""
        storage = CSVStorageManager(temp_output_dir)
        
        listings = [
            {"url": "https://example.com/1", "price": 100000},
            {"url": "https://example.com/2", "price": 200000}
        ]
        
        storage.save_page_listings(1, listings)
        
        assert storage.filepath.exists()
        
        with open(storage.filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 2
    
    def test_save_listings_batch(self, temp_output_dir):
        """Test saving batch of listings"""
        storage = CSVStorageManager(temp_output_dir)
        
        listings = [
            {"url": "https://example.com/1", "price": 100000},
            {"url": "https://example.com/2", "price": 200000}
        ]
        
        storage.save_listings_batch(listings)
        
        assert storage.filepath.exists()
        
        with open(storage.filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 2
    
    def test_save_results(self, temp_output_dir):
        """Test saving results with search results flattening"""
        storage = CSVStorageManager(temp_output_dir)
        
        results = [
            {
                "type": "search_results",
                "listings": [
                    {"url": "https://example.com/1", "price": 100000},
                    {"url": "https://example.com/2", "price": 200000}
                ]
            },
            {"url": "https://example.com/3", "price": 300000}
        ]
        
        storage.save_results(results)
        
        assert storage.filepath.exists()
        
        with open(storage.filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 3  # 2 from search_results + 1 direct


class TestCSVStorageManagerFileLocking:
    """Tests for file locking"""
    
    @pytest.mark.asyncio
    async def test_concurrent_saves_use_locking(self, temp_output_dir):
        """Test that concurrent saves use file locking"""
        storage = CSVStorageManager(temp_output_dir)
        
        # Create multiple listings to save concurrently
        listings = [
            {"url": f"https://example.com/{i}", "price": i * 10000}
            for i in range(5)
        ]
        
        # Save concurrently
        tasks = [storage.save_single_listing(listing) for listing in listings]
        await asyncio.gather(*tasks)
        
        # Verify all were saved
        assert storage.filepath.exists()
        
        with open(storage.filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 5
    
    @pytest.mark.asyncio
    async def test_save_single_listing_empty_listing(self, temp_output_dir):
        """Test saving empty listing"""
        storage = CSVStorageManager(temp_output_dir)
        
        await storage.save_single_listing({})
        
        # File should not exist
        assert not storage.filepath.exists()
    
    @pytest.mark.asyncio
    async def test_save_single_listing_no_url(self, temp_output_dir):
        """Test saving listing without URL"""
        storage = CSVStorageManager(temp_output_dir)
        
        listing = {"price": 100000, "title": "Test"}
        await storage.save_single_listing(listing)
        
        # File should not exist
        assert not storage.filepath.exists()
    
    @pytest.mark.asyncio
    async def test_save_single_listing_no_fieldnames(self, temp_output_dir):
        """Test saving when no fieldnames available"""
        storage = CSVStorageManager(temp_output_dir)
        
        # Create a listing with only invalid fieldnames
        listing = {
            "url": "https://example.com/listing",
            "column_42": "invalid"
        }
        
        await storage.save_single_listing(listing)
        
        # Should still create file with URL
        assert storage.filepath.exists()
    
    def test_read_existing_data_with_no_headers(self, temp_output_dir):
        """Test reading CSV without headers"""
        storage = CSVStorageManager(temp_output_dir)
        
        # Create CSV file without headers (just data)
        with open(storage.filepath, 'w', newline='', encoding='utf-8') as f:
            f.write("https://example.com/listing,100000,Test\n")
        
        existing_data, fieldnames = storage._read_existing_data()
        
        # Should handle gracefully
        assert isinstance(existing_data, dict)
        assert isinstance(fieldnames, list)
    
    def test_read_existing_data_with_url_in_fieldnames(self, temp_output_dir):
        """Test reading CSV where fieldnames look like URLs"""
        storage = CSVStorageManager(temp_output_dir)
        
        # Create CSV with URLs as fieldnames (invalid)
        with open(storage.filepath, 'w', newline='', encoding='utf-8') as f:
            f.write("https://example.com/listing,100000\n")
        
        existing_data, fieldnames = storage._read_existing_data()
        
        # Should filter out URL-like fieldnames
        assert isinstance(existing_data, dict)
    
    def test_read_existing_data_with_rows_without_url(self, temp_output_dir):
        """Test reading CSV with rows that have URL in other fields"""
        storage = CSVStorageManager(temp_output_dir)
        
        # Create CSV with URL in a different field
        with open(storage.filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['other_field', 'price'])
            writer.writeheader()
            writer.writerow({
                'other_field': 'https://www.zapimoveis.com.br/imovel/123',
                'price': '100000'
            })
        
        existing_data, fieldnames = storage._read_existing_data()
        
        # Should find URL in other field
        assert len(existing_data) > 0
    
    def test_read_existing_data_with_rows_without_url_or_zap_url(self, temp_output_dir):
        """Test reading CSV with rows that have no URL at all"""
        storage = CSVStorageManager(temp_output_dir)
        
        # Create CSV without URL
        with open(storage.filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['price', 'title'])
            writer.writeheader()
            writer.writerow({'price': '100000', 'title': 'Test'})
        
        existing_data, fieldnames = storage._read_existing_data()
        
        # Should use index as key
        assert len(existing_data) > 0
    
    @pytest.mark.asyncio
    async def test_save_single_listing_long_error_message(self, temp_output_dir):
        """Test that long error messages are treated as technical errors"""
        storage = CSVStorageManager(temp_output_dir)
        
        # Create listing with very long error message
        long_error = "A" * 600  # Longer than 500
        error_listing = {
            "url": "https://example.com/listing",
            "error": long_error
        }
        
        await storage.save_single_listing(error_listing)
        
        # Should not save
        if storage.filepath.exists():
            with open(storage.filepath, 'r', encoding='utf-8') as f:
                content = f.read()
                assert long_error not in content
    
    def test_convert_result_to_row_with_dict_error(self, temp_output_dir):
        """Test converting result with dict that can't be JSON serialized"""
        storage = CSVStorageManager(temp_output_dir)
        
        # Create result with non-serializable dict (circular reference simulation)
        class NonSerializable:
            def __str__(self):
                return "non-serializable"
        
        result = {
            "url": "https://example.com",
            "metadata": {"key": NonSerializable()}
        }
        
        row = storage.convert_result_to_row(result)
        
        # Should handle gracefully
        assert "url" in row
        assert "metadata" in row
    
    def test_read_existing_data_with_exception(self, temp_output_dir):
        """Test reading CSV when exception occurs"""
        storage = CSVStorageManager(temp_output_dir)
        
        # Create a file that will cause an error when reading
        with open(storage.filepath, 'w', encoding='utf-8') as f:
            f.write("invalid csv content with special chars: \x00\n")
        
        # Should handle exception gracefully
        existing_data, fieldnames = storage._read_existing_data()
        
        assert isinstance(existing_data, dict)
        assert isinstance(fieldnames, list)
    
    def test_detect_has_headers_none(self, temp_output_dir):
        """Test detecting headers with None"""
        storage = CSVStorageManager(temp_output_dir)
        
        # Should handle None gracefully
        result = storage._detect_has_headers(None)
        assert result is False
    
    @pytest.mark.asyncio
    async def test_save_single_listing_lock_file_removal_error(self, temp_output_dir):
        """Test saving when lock file removal fails"""
        storage = CSVStorageManager(temp_output_dir)
        
        listing = {"url": "https://example.com/listing", "price": 100000}
        
        # Mock lock file to raise error on unlink
        with patch.object(Path, 'unlink', side_effect=PermissionError("Cannot remove")):
            await storage.save_single_listing(listing)
        
        # Should still save the listing
        assert storage.filepath.exists()
    
    @pytest.mark.asyncio
    async def test_save_single_listing_no_fieldnames_skips_write(self, temp_output_dir):
        """Test saving when no fieldnames available skips write"""
        storage = CSVStorageManager(temp_output_dir)
        
        # Create listing with only invalid fieldnames - but URL is valid
        listing = {
            "url": "https://example.com/listing",
            "column_42": "invalid"
        }
        
        # The listing has a valid URL fieldname, so it will create file
        # Let's test with a listing that has NO valid fieldnames at all
        # by mocking is_valid_fieldname to return False for everything
        original_is_valid = storage.is_valid_fieldname
        storage.is_valid_fieldname = lambda x: False
        
        await storage.save_single_listing(listing)
        
        # Should not create file when no valid fieldnames
        assert not storage.filepath.exists()
        
        # Restore
        storage.is_valid_fieldname = original_is_valid
    
    def test_save_listings_batch_empty(self, temp_output_dir):
        """Test save_listings_batch with empty list"""
        storage = CSVStorageManager(temp_output_dir)
        
        storage.save_listings_batch([])
        
        # Should not create file
        assert not storage.filepath.exists()
    
    def test_save_results_empty(self, temp_output_dir):
        """Test save_results with empty results"""
        storage = CSVStorageManager(temp_output_dir)
        
        storage.save_results([])
        
        # Should not create file
        assert not storage.filepath.exists()
    
    def test_save_results_empty_after_flattening(self, temp_output_dir):
        """Test save_results when flattening results in empty list"""
        storage = CSVStorageManager(temp_output_dir)
        
        # Results with empty search_results
        results = [
            {
                "type": "search_results",
                "listings": []
            }
        ]
        
        storage.save_results(results)
        
        # Should not create file
        assert not storage.filepath.exists()
    
    def test_merge_listing_data_with_none_existing_value(self, temp_output_dir):
        """Test merge when existing value is None and new is empty"""
        storage = CSVStorageManager(temp_output_dir)
        
        existing_row = {"url": "https://example.com", "price": None}
        new_listing = {"url": "https://example.com", "price": ""}
        
        merged = storage._merge_listing_data(existing_row, new_listing)
        
        # Should set to empty string (None) if both are empty
        assert merged["price"] == ""
    
    def test_save_page_listings_with_existing_file_read_error(self, temp_output_dir):
        """Test save_page_listings when reading existing file causes error"""
        storage = CSVStorageManager(temp_output_dir)
        
        # Create a valid CSV file first
        with open(storage.filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['url', 'price'])
            writer.writeheader()
            writer.writerow({'url': 'https://example.com/existing', 'price': '50000'})
        
        # Now create a file that will cause error when trying to read headers
        # We'll use a mock that raises on the DictReader part
        from unittest.mock import MagicMock
        
        # Create a mock that raises on fieldnames access
        mock_reader = MagicMock()
        mock_reader.fieldnames = None
        mock_reader.__iter__ = MagicMock(return_value=iter([]))
        
        # Mock csv.DictReader to raise exception
        with patch('csv.DictReader', side_effect=IOError("Cannot read file")):
            listings = [{"url": "https://example.com/1", "price": 100000}]
            # Should handle error gracefully
            storage.save_page_listings(1, listings)
        
        # File should still be updated
        assert storage.filepath.exists()

