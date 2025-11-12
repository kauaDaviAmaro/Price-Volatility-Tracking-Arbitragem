"""
Tests for URLProcessor - URL processing, retry logic, error handling, compliance
"""
import pytest
from unittest.mock import AsyncMock, Mock, patch
from pathlib import Path

from src.pipelines.url_processor import URLProcessor
from src.core.compliance_manager import ComplianceManager
from src.core.human_behavior import HumanBehavior
from src.core.proxy_manager import ProxyManager
from src.core.browser_manager import BrowserManager
from src.config import Config


@pytest.fixture
def compliance_manager():
    """Create compliance manager"""
    return ComplianceManager(respect_robots=False)


@pytest.fixture
def human_behavior():
    """Create human behavior"""
    return HumanBehavior()


class TestURLProcessorInitialization:
    """Tests for URLProcessor initialization"""
    
    def test_init_with_defaults(self, compliance_manager, human_behavior):
        """Test initialization with defaults"""
        processor = URLProcessor(
            compliance_manager=compliance_manager,
            human_behavior=human_behavior
        )
        
        assert processor.compliance_manager == compliance_manager
        assert processor.human_behavior == human_behavior
        assert processor.proxy_manager is None
        assert processor.deep_search_only is False
    
    def test_init_with_all_params(self, compliance_manager, human_behavior):
        """Test initialization with all parameters"""
        proxy_manager = ProxyManager()
        
        processor = URLProcessor(
            compliance_manager=compliance_manager,
            human_behavior=human_behavior,
            proxy_manager=proxy_manager,
            deep_search_only=True
        )
        
        assert processor.proxy_manager == proxy_manager
        assert processor.deep_search_only is True


class TestURLProcessorHelpers:
    """Tests for helper methods"""
    
    def test_is_search_url(self, compliance_manager, human_behavior):
        """Test search URL detection"""
        processor = URLProcessor(compliance_manager, human_behavior)
        
        assert processor.is_search_url("https://example.com/venda/") is True
        assert processor.is_search_url("https://example.com/imovel/123") is False
    
    def test_is_blocked_error(self, compliance_manager, human_behavior):
        """Test blocked error detection"""
        processor = URLProcessor(compliance_manager, human_behavior)
        
        assert processor.is_blocked_error({"error": "403 Forbidden"}) is True
        assert processor.is_blocked_error({"error": "429 Too Many Requests"}) is True
        assert processor.is_blocked_error({"error": "500 Internal Server Error"}) is False
        assert processor.is_blocked_error({"error": "Network timeout"}) is False
    
    def test_calculate_retry_delay(self, compliance_manager, human_behavior):
        """Test retry delay calculation"""
        processor = URLProcessor(compliance_manager, human_behavior)
        
        delay1 = processor.calculate_retry_delay(0)
        delay2 = processor.calculate_retry_delay(1)
        delay3 = processor.calculate_retry_delay(2)
        
        # Should increase exponentially
        assert delay2 > delay1
        assert delay3 > delay2


class TestURLProcessorCompliance:
    """Tests for compliance checking"""
    
    @pytest.mark.asyncio
    async def test_check_compliance_public_data(self, compliance_manager, human_behavior):
        """Test compliance check for public data"""
        processor = URLProcessor(compliance_manager, human_behavior)
        
        with patch.object(compliance_manager, 'is_public_data', return_value=True):
            with patch.object(compliance_manager, 'can_fetch', return_value=True):
                with patch.object(compliance_manager, 'wait_for_rate_limit', return_value=None):
                    result = await processor.check_compliance("https://example.com/listings")
                    assert result is True
    
    @pytest.mark.asyncio
    async def test_check_compliance_private_data(self, compliance_manager, human_behavior):
        """Test compliance check for private data"""
        processor = URLProcessor(compliance_manager, human_behavior)
        
        with patch.object(compliance_manager, 'is_public_data', return_value=False):
            result = await processor.check_compliance("https://example.com/login")
            assert result is False
    
    @pytest.mark.asyncio
    async def test_check_compliance_robots_disallows(self, compliance_manager, human_behavior):
        """Test compliance check when robots.txt disallows"""
        processor = URLProcessor(compliance_manager, human_behavior)
        
        with patch.object(compliance_manager, 'is_public_data', return_value=True):
            with patch.object(compliance_manager, 'can_fetch', return_value=False):
                result = await processor.check_compliance("https://example.com/page")
                assert result is False


class TestURLProcessorScraping:
    """Tests for scraping attempts"""
    
    @pytest.mark.asyncio
    @patch('src.pipelines.url_processor.ZapImoveisService')
    async def test_attempt_scrape_listing_url(self, mock_service_class, compliance_manager, human_behavior):
        """Test scraping a listing URL"""
        processor = URLProcessor(compliance_manager, human_behavior)
        
        # Mock browser manager
        mock_browser_manager = AsyncMock()
        mock_browser_manager.context = None
        mock_browser_manager.initialize = AsyncMock()
        mock_browser_manager.create_page = AsyncMock(return_value=AsyncMock())
        mock_browser_manager.mark_proxy_success = AsyncMock()
        
        # Mock service
        mock_service = AsyncMock()
        mock_service.scrape_listing = AsyncMock(return_value={"url": "https://example.com/listing"})
        mock_service_class.return_value = mock_service
        
        result = await processor.attempt_scrape("https://example.com/listing", mock_browser_manager)
        
        assert result is not None
        assert "url" in result
        mock_service.scrape_listing.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('src.pipelines.url_processor.ZapImoveisService')
    @patch('src.config.Config')
    async def test_attempt_scrape_search_url(self, mock_config, mock_service_class, compliance_manager, human_behavior):
        """Test scraping a search URL"""
        mock_config.MAX_PAGES = 1
        
        processor = URLProcessor(compliance_manager, human_behavior)
        
        # Mock browser manager
        mock_browser_manager = AsyncMock()
        mock_browser_manager.context = None
        mock_browser_manager.initialize = AsyncMock()
        mock_browser_manager.create_page = AsyncMock(return_value=AsyncMock())
        mock_browser_manager.mark_proxy_success = AsyncMock()
        
        # Mock service
        mock_service = AsyncMock()
        mock_listings = [{"url": "test1"}, {"url": "test2"}]
        mock_service.scrape_search_results = AsyncMock(return_value=mock_listings)
        mock_service.deep_scrape_listings = AsyncMock(return_value=mock_listings)
        mock_service_class.return_value = mock_service
        
        result = await processor.attempt_scrape("https://example.com/venda/", mock_browser_manager)
        
        assert result is not None
        assert result["type"] == "search_results"
        assert "listings" in result
        mock_service.scrape_search_results.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('src.pipelines.url_processor.ZapImoveisService')
    async def test_attempt_scrape_deep_search_only(self, mock_service_class, compliance_manager, human_behavior):
        """Test scraping in deep_search_only mode"""
        processor = URLProcessor(
            compliance_manager,
            human_behavior,
            deep_search_only=True
        )
        
        # Mock browser manager
        mock_browser_manager = AsyncMock()
        mock_browser_manager.context = None
        mock_browser_manager.initialize = AsyncMock()
        mock_browser_manager.create_page = AsyncMock(return_value=AsyncMock())
        mock_browser_manager.mark_proxy_success = AsyncMock()
        
        # Mock service
        mock_service = AsyncMock()
        mock_service.scrape_listing = AsyncMock(return_value={"url": "https://example.com/listing"})
        mock_service_class.return_value = mock_service
        
        result = await processor.attempt_scrape("https://example.com/venda/", mock_browser_manager)
        
        # In deep_search_only mode, should treat as listing
        assert result is not None
        mock_service.scrape_listing.assert_called_once()


class TestURLProcessorErrorHandling:
    """Tests for error handling"""
    
    @pytest.mark.asyncio
    async def test_handle_blocked_error(self, compliance_manager, human_behavior):
        """Test handling blocked error"""
        processor = URLProcessor(compliance_manager, human_behavior)
        
        mock_browser_manager = AsyncMock()
        mock_browser_manager.mark_proxy_failure = AsyncMock()
        mock_browser_manager.rotate_fingerprint = Mock()
        
        await processor.handle_blocked_error(
            "https://example.com/page",
            {"error": "403 Forbidden"},
            mock_browser_manager
        )
        
        mock_browser_manager.mark_proxy_failure.assert_called_once()
        mock_browser_manager.rotate_fingerprint.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('src.pipelines.url_processor.URLProcessor.attempt_scrape')
    @patch('src.config.Config')
    async def test_process_scrape_attempt_success(self, mock_config, mock_attempt_scrape, compliance_manager, human_behavior):
        """Test successful scrape attempt"""
        mock_config.MAX_RETRIES = 2
        
        processor = URLProcessor(compliance_manager, human_behavior)
        
        mock_browser_manager = AsyncMock()
        mock_attempt_scrape.return_value = {"url": "https://example.com/listing"}
        
        result = await processor.process_scrape_attempt(
            "https://example.com/listing",
            mock_browser_manager,
            0,
            2
        )
        
        assert result is not None
        assert result["url"] == "https://example.com/listing"
    
    @pytest.mark.asyncio
    @patch('src.pipelines.url_processor.URLProcessor.attempt_scrape')
    @patch('src.pipelines.url_processor.URLProcessor.handle_blocked_error')
    @patch('src.config.Config')
    async def test_process_scrape_attempt_blocked(self, mock_config, mock_handle_blocked, mock_attempt_scrape, compliance_manager, human_behavior):
        """Test blocked scrape attempt"""
        mock_config.MAX_RETRIES = 2
        mock_config.RETRY_DELAY = 0.0
        mock_config.RETRY_BACKOFF = 2.0
        
        processor = URLProcessor(compliance_manager, human_behavior)
        
        mock_browser_manager = AsyncMock()
        mock_attempt_scrape.return_value = {"error": "403 Forbidden"}
        
        with patch('asyncio.sleep', return_value=None):
            result = await processor.process_scrape_attempt(
                "https://example.com/listing",
                mock_browser_manager,
                0,
                2
            )
        
        assert result is None  # Should retry
        mock_handle_blocked.assert_called_once()


class TestURLProcessorFullFlow:
    """Tests for full URL processing flow"""
    
    @pytest.mark.asyncio
    @patch('src.pipelines.url_processor.BrowserManager')
    @patch('src.pipelines.url_processor.URLProcessor.check_compliance')
    @patch('src.pipelines.url_processor.URLProcessor.process_scrape_attempt')
    @patch('src.config.Config')
    async def test_process_url_success(self, mock_config, mock_process_attempt, mock_check_compliance, mock_browser_manager_class, compliance_manager, human_behavior):
        """Test successful URL processing"""
        mock_config.MAX_RETRIES = 2
        mock_config.HEADLESS = True
        
        processor = URLProcessor(compliance_manager, human_behavior)
        
        mock_check_compliance.return_value = True
        mock_process_attempt.return_value = {"url": "https://example.com/listing"}
        
        mock_browser_manager = AsyncMock()
        mock_browser_manager.close = AsyncMock()
        mock_browser_manager_class.return_value = mock_browser_manager
        
        result = await processor.process_url("https://example.com/listing")
        
        assert result is not None
        assert result["url"] == "https://example.com/listing"
    
    @pytest.mark.asyncio
    @patch('src.pipelines.url_processor.URLProcessor.check_compliance')
    async def test_process_url_compliance_fails(self, mock_check_compliance, compliance_manager, human_behavior):
        """Test URL processing when compliance check fails"""
        processor = URLProcessor(compliance_manager, human_behavior)
        
        mock_check_compliance.return_value = False
        
        result = await processor.process_url("https://example.com/listing")
        
        assert result is None
    
    @pytest.mark.asyncio
    @patch('src.pipelines.url_processor.BrowserManager')
    @patch('src.pipelines.url_processor.URLProcessor.check_compliance')
    @patch('src.pipelines.url_processor.URLProcessor.process_scrape_attempt')
    @patch('src.config.Config')
    async def test_process_url_with_retries_exhausted(self, mock_config, mock_process_attempt, mock_check_compliance, mock_browser_manager_class, compliance_manager, human_behavior):
        """Test URL processing when all retries are exhausted"""
        mock_config.MAX_RETRIES = 2
        mock_config.HEADLESS = True
        
        processor = URLProcessor(compliance_manager, human_behavior)
        
        mock_check_compliance.return_value = True
        mock_process_attempt.return_value = None  # Always fails
        
        mock_browser_manager = AsyncMock()
        mock_browser_manager.close = AsyncMock()
        mock_browser_manager_class.return_value = mock_browser_manager
        
        result = await processor.process_url("https://example.com/listing")
        
        # Should return error after all retries
        assert result is not None
        assert "error" in result
    
    @pytest.mark.asyncio
    @patch('src.pipelines.url_processor.URLProcessor.attempt_scrape')
    @patch('src.config.Config')
    async def test_process_scrape_attempt_with_exception(self, mock_config, mock_attempt_scrape, compliance_manager, human_behavior):
        """Test process_scrape_attempt when exception occurs"""
        mock_config.MAX_RETRIES = 2
        mock_config.RETRY_DELAY = 0.0
        
        processor = URLProcessor(compliance_manager, human_behavior)
        
        mock_browser_manager = AsyncMock()
        mock_attempt_scrape.side_effect = Exception("Network error")
        
        with patch('asyncio.sleep', return_value=None):
            result = await processor.process_scrape_attempt(
                "https://example.com/listing",
                mock_browser_manager,
                0,
                2
            )
        
        # Should return None to trigger retry
        assert result is None
    
    @pytest.mark.asyncio
    @patch('src.pipelines.url_processor.ZapImoveisService')
    async def test_attempt_scrape_with_save_callback(self, mock_service_class, compliance_manager, human_behavior):
        """Test scraping with save callback"""
        save_callback = AsyncMock()
        
        processor = URLProcessor(
            compliance_manager,
            human_behavior,
            save_callback=save_callback
        )
        
        mock_browser_manager = AsyncMock()
        mock_browser_manager.context = None
        mock_browser_manager.initialize = AsyncMock()
        mock_browser_manager.create_page = AsyncMock(return_value=AsyncMock())
        mock_browser_manager.mark_proxy_success = AsyncMock()
        
        mock_service = AsyncMock()
        mock_service.scrape_listing = AsyncMock(return_value={"url": "https://example.com/listing"})
        mock_service_class.return_value = mock_service
        
        result = await processor.attempt_scrape("https://example.com/listing", mock_browser_manager)
        
        assert result is not None
        save_callback.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('src.pipelines.url_processor.ZapImoveisService')
    @patch('src.config.Config')
    async def test_attempt_scrape_search_with_page_callback(self, mock_config, mock_service_class, compliance_manager, human_behavior):
        """Test scraping search URL with page callback"""
        mock_config.MAX_PAGES = 1
        
        page_callback = AsyncMock()
        
        processor = URLProcessor(
            compliance_manager,
            human_behavior,
            page_callback=page_callback
        )
        
        mock_browser_manager = AsyncMock()
        mock_browser_manager.context = None
        mock_browser_manager.initialize = AsyncMock()
        mock_browser_manager.create_page = AsyncMock(return_value=AsyncMock())
        mock_browser_manager.mark_proxy_success = AsyncMock()
        
        mock_service = AsyncMock()
        mock_listings = [{"url": "test1"}]
        mock_service.scrape_search_results = AsyncMock(return_value=mock_listings)
        mock_service.deep_scrape_listings = AsyncMock(return_value=mock_listings)
        mock_service_class.return_value = mock_service
        
        result = await processor.attempt_scrape("https://example.com/venda/", mock_browser_manager)
        
        assert result is not None
        # Page callback should be called during scrape_search_results
        # (it's passed as parameter, so we verify the service was called)
        mock_service.scrape_search_results.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('src.pipelines.url_processor.ZapImoveisService')
    async def test_attempt_scrape_listing_with_save_callback(self, mock_service_class, compliance_manager, human_behavior):
        """Test scraping listing with save callback"""
        save_callback = AsyncMock()
        
        processor = URLProcessor(
            compliance_manager,
            human_behavior,
            save_callback=save_callback
        )
        
        mock_browser_manager = AsyncMock()
        mock_browser_manager.context = None
        mock_browser_manager.initialize = AsyncMock()
        mock_browser_manager.create_page = AsyncMock(return_value=AsyncMock())
        mock_browser_manager.mark_proxy_success = AsyncMock()
        
        mock_service = AsyncMock()
        mock_service.scrape_listing = AsyncMock(return_value={"url": "https://example.com/listing"})
        mock_service_class.return_value = mock_service
        
        result = await processor.attempt_scrape("https://example.com/listing", mock_browser_manager)
        
        assert result is not None
        save_callback.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('src.pipelines.url_processor.URLProcessor.attempt_scrape')
    @patch('src.config.Config')
    async def test_process_scrape_attempt_max_retries_exception(self, mock_config, mock_attempt_scrape, compliance_manager, human_behavior):
        """Test process_scrape_attempt when max retries reached with exception"""
        mock_config.MAX_RETRIES = 0
        mock_config.RETRY_DELAY = 0.0
        
        processor = URLProcessor(compliance_manager, human_behavior)
        
        mock_browser_manager = AsyncMock()
        mock_attempt_scrape.side_effect = Exception("Network error")
        
        with patch('asyncio.sleep', return_value=None):
            result = await processor.process_scrape_attempt(
                "https://example.com/listing",
                mock_browser_manager,
                0,
                0  # max_retries = 0 means only 1 attempt
            )
        
        # Should return error dict
        assert result is not None
        assert "error" in result
    
    @pytest.mark.asyncio
    @patch('src.pipelines.url_processor.ZapImoveisService')
    async def test_attempt_scrape_deep_search_only_with_callback(self, mock_service_class, compliance_manager, human_behavior):
        """Test deep_search_only mode with save callback"""
        save_callback = AsyncMock()
        
        processor = URLProcessor(
            compliance_manager,
            human_behavior,
            deep_search_only=True,
            save_callback=save_callback
        )
        
        mock_browser_manager = AsyncMock()
        mock_browser_manager.context = None
        mock_browser_manager.initialize = AsyncMock()
        mock_browser_manager.create_page = AsyncMock(return_value=AsyncMock())
        mock_browser_manager.mark_proxy_success = AsyncMock()
        
        mock_service = AsyncMock()
        mock_service.scrape_listing = AsyncMock(return_value={"url": "https://example.com/listing"})
        mock_service_class.return_value = mock_service
        
        result = await processor.attempt_scrape("https://example.com/venda/", mock_browser_manager)
        
        assert result is not None
        save_callback.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('src.pipelines.url_processor.ZapImoveisService')
    @patch('src.config.Config')
    async def test_attempt_scrape_search_with_deep_scrape_callback(self, mock_config, mock_service_class, compliance_manager, human_behavior):
        """Test search scraping with deep scrape save callback"""
        mock_config.MAX_PAGES = 1
        
        save_callback = AsyncMock()
        
        processor = URLProcessor(
            compliance_manager,
            human_behavior,
            save_callback=save_callback
        )
        
        mock_browser_manager = AsyncMock()
        mock_browser_manager.context = None
        mock_browser_manager.initialize = AsyncMock()
        mock_browser_manager.create_page = AsyncMock(return_value=AsyncMock())
        mock_browser_manager.mark_proxy_success = AsyncMock()
        
        mock_service = AsyncMock()
        mock_listings = [{"url": "test1"}]
        mock_service.scrape_search_results = AsyncMock(return_value=mock_listings)
        
        # Mock deep_scrape_listings to call the callback
        async def mock_deep_scrape(listings, save_callback=None):
            if save_callback:
                for listing in listings:
                    await save_callback(listing)
            return listings
        
        mock_service.deep_scrape_listings = AsyncMock(side_effect=mock_deep_scrape)
        mock_service_class.return_value = mock_service
        
        result = await processor.attempt_scrape("https://example.com/venda/", mock_browser_manager)
        
        assert result is not None
        # Save callback should be called for each listing
        assert save_callback.call_count == 1

