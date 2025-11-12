"""
Tests for ImageDownloader - Image downloads, rate limiting, directory management
"""
import pytest
from unittest.mock import AsyncMock, Mock, patch
from pathlib import Path
import tempfile
import shutil

from src.pipelines.image_downloader import ImageDownloader
from src.config import Config


@pytest.fixture
def temp_output_dir():
    """Create temporary output directory"""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


class TestImageDownloaderInitialization:
    """Tests for ImageDownloader initialization"""
    
    def test_init(self, temp_output_dir):
        """Test initialization"""
        downloader = ImageDownloader(temp_output_dir)
        
        assert downloader.output_dir == temp_output_dir
        assert downloader.images_dir == temp_output_dir / "images"
        assert downloader.images_dir.exists()


class TestImageDownloaderHelpers:
    """Tests for helper methods"""
    
    def test_get_image_extension(self, temp_output_dir):
        """Test image extension extraction"""
        downloader = ImageDownloader(temp_output_dir)
        
        assert downloader.get_image_extension("image.jpg") == "jpg"
        assert downloader.get_image_extension("image.png?param=value") == "png"
        assert downloader.get_image_extension("image.webp") == "webp"
        assert downloader.get_image_extension("image.unknown") == "jpg"  # Default
        assert downloader.get_image_extension("noextension") == "jpg"  # Default
    
    def test_get_listing_id_from_url(self, temp_output_dir):
        """Test listing ID extraction from URL"""
        downloader = ImageDownloader(temp_output_dir)
        
        assert downloader.get_listing_id_from_url("https://example.com/imovel/id-123456/") == "listing_123456"
        assert downloader.get_listing_id_from_url("https://example.com/test/") != "unknown"
        assert downloader.get_listing_id_from_url("") == "unknown"


class TestImageDownloaderNormalization:
    """Tests for image list normalization"""
    
    def test_normalize_images_list_string(self, temp_output_dir):
        """Test normalizing images from string"""
        downloader = ImageDownloader(temp_output_dir)
        
        images_str = "img1.jpg, img2.jpg, img3.jpg"
        normalized = downloader._normalize_images_list(images_str)
        
        assert len(normalized) == 3
        assert normalized[0] == "img1.jpg"
        assert normalized[1] == "img2.jpg"
        assert normalized[2] == "img3.jpg"
    
    def test_normalize_images_list_list(self, temp_output_dir):
        """Test normalizing images from list"""
        downloader = ImageDownloader(temp_output_dir)
        
        images_list = ["img1.jpg", "img2.jpg"]
        normalized = downloader._normalize_images_list(images_list)
        
        assert len(normalized) == 2
        assert normalized == images_list
    
    def test_normalize_images_list_empty(self, temp_output_dir):
        """Test normalizing empty images"""
        downloader = ImageDownloader(temp_output_dir)
        
        assert downloader._normalize_images_list(None) == []
        assert downloader._normalize_images_list([]) == []
        assert downloader._normalize_images_list("") == []


class TestImageDownloaderDownload:
    """Tests for image downloading"""
    
    @pytest.mark.asyncio
    @patch('src.pipelines.image_downloader.Config')
    async def test_download_listing_images_disabled(self, mock_config, temp_output_dir):
        """Test image download when disabled"""
        mock_config.SAVE_IMAGES = False
        
        downloader = ImageDownloader(temp_output_dir)
        listing = {"url": "https://example.com/listing", "images": ["img1.jpg"]}
        
        await downloader.download_listing_images(listing)
        
        # Should not create any directories
        listing_id = downloader.get_listing_id_from_url(listing["url"])
        image_dir = downloader.images_dir / listing_id
        assert not image_dir.exists()
    
    @pytest.mark.asyncio
    @patch('src.pipelines.image_downloader.Config')
    @patch('src.pipelines.image_downloader.aiohttp.ClientSession')
    @patch('src.pipelines.image_downloader.aiofiles.open')
    async def test_download_listing_images(self, mock_aiofiles, mock_session_class, mock_config, temp_output_dir):
        """Test downloading listing images"""
        mock_config.SAVE_IMAGES = True
        mock_config.IMAGE_DOWNLOAD_DELAY = 0.0
        
        downloader = ImageDownloader(temp_output_dir)
        
        listing = {
            "url": "https://example.com/imovel/id-123456/",
            "images": ["https://example.com/img1.jpg", "https://example.com/img2.jpg"]
        }
        
        # Mock HTTP response
        async def mock_iter_chunked(chunk_size):
            yield b"image_data"
        
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.content.iter_chunked = mock_iter_chunked
        
        # Create async context manager for response
        class MockResponseContext:
            def __init__(self, response):
                self.response = response
            async def __aenter__(self):
                return self.response
            async def __aexit__(self, *args):
                return None
        
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_session.get = Mock(return_value=MockResponseContext(mock_response))
        mock_session_class.return_value = mock_session
        
        # Mock file writing
        mock_file = AsyncMock()
        mock_file.write = AsyncMock()
        mock_file_context = AsyncMock()
        mock_file_context.__aenter__ = AsyncMock(return_value=mock_file)
        mock_file_context.__aexit__ = AsyncMock(return_value=None)
        mock_aiofiles.return_value = mock_file_context
        
        await downloader.download_listing_images(listing)
        
        # Should have attempted to download
        assert mock_session.get.call_count == 2
        assert "images_local" in listing
        assert "images_local_count" in listing
        assert listing["images_local_count"] == 2
    
    @pytest.mark.asyncio
    @patch('src.pipelines.image_downloader.Config')
    async def test_download_listing_images_no_images(self, mock_config, temp_output_dir):
        """Test downloading when listing has no images"""
        mock_config.SAVE_IMAGES = True
        
        downloader = ImageDownloader(temp_output_dir)
        listing = {"url": "https://example.com/listing", "images": []}
        
        await downloader.download_listing_images(listing)
        
        # Should not have images_local
        assert "images_local" not in listing
    
    @pytest.mark.asyncio
    @patch('src.pipelines.image_downloader.Config')
    @patch('src.pipelines.image_downloader.aiohttp.ClientSession')
    async def test_download_listing_images_max_limit(self, mock_session_class, mock_config, temp_output_dir):
        """Test that download respects max_images limit"""
        mock_config.SAVE_IMAGES = True
        mock_config.IMAGE_DOWNLOAD_DELAY = 0.0
        
        downloader = ImageDownloader(temp_output_dir)
        
        listing = {
            "url": "https://example.com/listing",
            "images": [f"https://example.com/img{i}.jpg" for i in range(25)]  # 25 images
        }
        
        # Mock HTTP response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.content.iter_chunked = AsyncMock(return_value=iter([b"data"]))
        
        class MockResponseContext:
            def __init__(self, response):
                self.response = response
            async def __aenter__(self):
                return self.response
            async def __aexit__(self, *args):
                return None
        
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_session.get = Mock(return_value=MockResponseContext(mock_response))
        mock_session_class.return_value = mock_session
        
        # Mock file writing
        with patch('src.pipelines.image_downloader.aiofiles.open', new_callable=AsyncMock) as mock_aiofiles:
            mock_file = AsyncMock()
            mock_file.write = AsyncMock()
            mock_file_context = AsyncMock()
            mock_file_context.__aenter__ = AsyncMock(return_value=mock_file)
            mock_file_context.__aexit__ = AsyncMock(return_value=None)
            mock_aiofiles.return_value = mock_file_context
            
            await downloader.download_listing_images(listing, max_images=10)
            
            # Should only download 10 images
            assert mock_session.get.call_count == 10
            assert listing.get("images_local_count", 0) <= 10
    
    @pytest.mark.asyncio
    @patch('src.pipelines.image_downloader.Config')
    @patch('src.pipelines.image_downloader.aiohttp.ClientSession')
    async def test_download_listing_images_http_error(self, mock_session_class, mock_config, temp_output_dir):
        """Test downloading when HTTP error occurs"""
        mock_config.SAVE_IMAGES = True
        mock_config.IMAGE_DOWNLOAD_DELAY = 0.0
        
        downloader = ImageDownloader(temp_output_dir)
        
        listing = {
            "url": "https://example.com/listing",
            "images": ["https://example.com/img1.jpg"]
        }
        
        # Mock HTTP response with error
        mock_response = AsyncMock()
        mock_response.status = 404
        mock_response.content.iter_chunked = AsyncMock(return_value=iter([b"data"]))
        
        class MockResponseContext:
            def __init__(self, response):
                self.response = response
            async def __aenter__(self):
                return self.response
            async def __aexit__(self, *args):
                return None
        
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_session.get = Mock(return_value=MockResponseContext(mock_response))
        mock_session_class.return_value = mock_session
        
        await downloader.download_listing_images(listing)
        
        # Should not have images_local
        assert "images_local" not in listing
    
    @pytest.mark.asyncio
    @patch('src.pipelines.image_downloader.Config')
    @patch('src.pipelines.image_downloader.aiohttp.ClientSession')
    async def test_download_listing_images_download_exception(self, mock_session_class, mock_config, temp_output_dir):
        """Test downloading when download raises exception"""
        mock_config.SAVE_IMAGES = True
        mock_config.IMAGE_DOWNLOAD_DELAY = 0.0
        
        downloader = ImageDownloader(temp_output_dir)
        
        listing = {
            "url": "https://example.com/listing",
            "images": ["https://example.com/img1.jpg"]
        }
        
        # Mock session.get to raise exception
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_session.get = Mock(side_effect=Exception("Network error"))
        mock_session_class.return_value = mock_session
        
        await downloader.download_listing_images(listing)
        
        # Should handle gracefully
        assert "images_local" not in listing or listing.get("images_local_count", 0) == 0
    
    def test_get_listing_id_from_url_fallback(self, temp_output_dir):
        """Test listing ID extraction fallback"""
        downloader = ImageDownloader(temp_output_dir)
        
        # URL without id- pattern
        result = downloader.get_listing_id_from_url("https://example.com/test-listing-123/")
        assert result != "unknown"
        assert "test" in result.lower() or "listing" in result.lower()
    
    def test_get_image_extension_with_query_params(self, temp_output_dir):
        """Test image extension with query parameters"""
        downloader = ImageDownloader(temp_output_dir)
        
        assert downloader.get_image_extension("image.jpg?size=large&format=webp") == "jpg"
        assert downloader.get_image_extension("photo.png?v=123") == "png"
    
    def test_get_listing_id_from_url_empty_url(self, temp_output_dir):
        """Test listing ID extraction with empty URL"""
        downloader = ImageDownloader(temp_output_dir)
        
        result = downloader.get_listing_id_from_url("")
        assert result == "unknown"
    
    def test_get_listing_id_from_url_no_parts(self, temp_output_dir):
        """Test listing ID extraction when URL has no parts"""
        downloader = ImageDownloader(temp_output_dir)
        
        # URL that results in empty url_parts after processing
        result = downloader.get_listing_id_from_url("/")
        # Should return "unknown" if no parts
        assert result is not None
    
    def test_normalize_images_list_other_type(self, temp_output_dir):
        """Test normalizing images with other type"""
        downloader = ImageDownloader(temp_output_dir)
        
        # Test with non-string, non-list type
        result = downloader._normalize_images_list(123)
        assert result == []
        
        result = downloader._normalize_images_list({})
        assert result == []

