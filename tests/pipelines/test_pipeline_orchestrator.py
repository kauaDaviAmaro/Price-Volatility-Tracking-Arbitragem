"""
Tests for PipelineOrchestrator - Concurrency, modes, statistics
"""
import pytest
from unittest.mock import AsyncMock, Mock, patch
from pathlib import Path
import tempfile
import shutil

from src.pipelines.pipeline_orchestrator import PipelineOrchestrator
from src.core.compliance_manager import ComplianceManager
from src.core.human_behavior import HumanBehavior
from src.core.proxy_manager import ProxyManager
from src.config import Config


@pytest.fixture
def temp_output_dir():
    """Create temporary output directory"""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


@pytest.fixture
def compliance_manager():
    """Create compliance manager"""
    return ComplianceManager(respect_robots=False)


@pytest.fixture
def human_behavior():
    """Create human behavior"""
    return HumanBehavior()


class TestPipelineOrchestratorInitialization:
    """Tests for PipelineOrchestrator initialization"""
    
    def test_init(self, temp_output_dir, compliance_manager, human_behavior):
        """Test initialization"""
        orchestrator = PipelineOrchestrator(
            urls=["https://example.com"],
            output_dir=str(temp_output_dir),
            max_concurrent=3,
            proxy_manager=None,
            compliance_manager=compliance_manager,
            human_behavior=human_behavior,
            deep_search_only=False
        )
        
        assert orchestrator.urls == ["https://example.com"]
        assert orchestrator.max_concurrent == 3
        assert orchestrator.deep_search_only is False
        assert orchestrator.stats["total"] == 0


class TestPipelineOrchestratorStatistics:
    """Tests for statistics tracking"""
    
    def test_update_stats_from_result_success(self, temp_output_dir, compliance_manager, human_behavior):
        """Test updating stats from successful result"""
        orchestrator = PipelineOrchestrator(
            urls=[],
            output_dir=str(temp_output_dir),
            max_concurrent=1,
            proxy_manager=None,
            compliance_manager=compliance_manager,
            human_behavior=human_behavior
        )
        
        result = {"url": "https://example.com/listing"}
        orchestrator._update_stats_from_result(result, "https://example.com/listing")
        
        assert orchestrator.stats["success"] == 1
        assert orchestrator.stats["failed"] == 0
    
    def test_update_stats_from_result_search(self, temp_output_dir, compliance_manager, human_behavior):
        """Test updating stats from search result"""
        orchestrator = PipelineOrchestrator(
            urls=[],
            output_dir=str(temp_output_dir),
            max_concurrent=1,
            proxy_manager=None,
            compliance_manager=compliance_manager,
            human_behavior=human_behavior
        )
        
        result = {
            "type": "search_results",
            "listings": [
                {"url": "1"},
                {"url": "2"},
                {"url": "3"}
            ]
        }
        orchestrator._update_stats_from_result(result, "https://example.com/search")
        
        assert orchestrator.stats["success"] == 3
    
    def test_update_stats_from_result_error(self, temp_output_dir, compliance_manager, human_behavior):
        """Test updating stats from error result"""
        orchestrator = PipelineOrchestrator(
            urls=[],
            output_dir=str(temp_output_dir),
            max_concurrent=1,
            proxy_manager=None,
            compliance_manager=compliance_manager,
            human_behavior=human_behavior
        )
        
        result = {"url": "https://example.com/listing", "error": "Network error"}
        orchestrator._update_stats_from_result(result, "https://example.com/listing")
        
        assert orchestrator.stats["failed"] == 1
    
    def test_update_stats_from_result_blocked(self, temp_output_dir, compliance_manager, human_behavior):
        """Test updating stats from blocked result"""
        orchestrator = PipelineOrchestrator(
            urls=[],
            output_dir=str(temp_output_dir),
            max_concurrent=1,
            proxy_manager=None,
            compliance_manager=compliance_manager,
            human_behavior=human_behavior
        )
        
        result = {"url": "https://example.com/listing", "error": "403 Forbidden"}
        orchestrator._update_stats_from_result(result, "https://example.com/listing")
        
        assert orchestrator.stats["blocked"] == 1
    
    def test_get_stats(self, temp_output_dir, compliance_manager, human_behavior):
        """Test getting statistics"""
        orchestrator = PipelineOrchestrator(
            urls=[],
            output_dir=str(temp_output_dir),
            max_concurrent=1,
            proxy_manager=None,
            compliance_manager=compliance_manager,
            human_behavior=human_behavior
        )
        
        orchestrator.stats["success"] = 5
        stats = orchestrator.get_stats()
        
        assert stats["success"] == 5
        assert isinstance(stats, dict)


class TestPipelineOrchestratorConcurrency:
    """Tests for concurrency control"""
    
    @pytest.mark.asyncio
    @patch('src.pipelines.pipeline_orchestrator.URLProcessor.process_url')
    async def test_process_urls_with_concurrency(self, mock_process_url, temp_output_dir, compliance_manager, human_behavior):
        """Test URL processing with concurrency control"""
        mock_process_url.return_value = {"url": "test"}
        
        orchestrator = PipelineOrchestrator(
            urls=["https://example.com/1", "https://example.com/2", "https://example.com/3"],
            output_dir=str(temp_output_dir),
            max_concurrent=2,
            proxy_manager=None,
            compliance_manager=compliance_manager,
            human_behavior=human_behavior
        )
        
        results = await orchestrator._process_urls_with_concurrency()
        
        assert len(results) == 3
        assert mock_process_url.call_count == 3
    
    @pytest.mark.asyncio
    @patch('src.pipelines.pipeline_orchestrator.URLProcessor.process_url')
    async def test_process_urls_with_concurrency_handles_exceptions(self, mock_process_url, temp_output_dir, compliance_manager, human_behavior):
        """Test that concurrency handles exceptions"""
        mock_process_url.side_effect = [
            {"url": "test1"},
            Exception("Error"),
            {"url": "test3"}
        ]
        
        orchestrator = PipelineOrchestrator(
            urls=["https://example.com/1", "https://example.com/2", "https://example.com/3"],
            output_dir=str(temp_output_dir),
            max_concurrent=2,
            proxy_manager=None,
            compliance_manager=compliance_manager,
            human_behavior=human_behavior
        )
        
        results = await orchestrator._process_urls_with_concurrency()
        
        # Should handle exception and continue
        assert len(results) == 3  # 2 success + 1 error result
        assert orchestrator.stats["failed"] == 1


class TestPipelineOrchestratorDeepSearchOnly:
    """Tests for deep_search_only mode"""
    
    @pytest.mark.asyncio
    @patch('src.pipelines.pipeline_orchestrator.BrowserManager')
    @patch('src.pipelines.pipeline_orchestrator.URLProcessor.process_url')
    async def test_process_urls_with_single_browser(self, mock_process_url, mock_browser_manager_class, temp_output_dir, compliance_manager, human_behavior):
        """Test processing URLs with single browser in deep_search_only mode"""
        mock_process_url.return_value = {"url": "test"}
        
        # Mock browser manager
        mock_browser_manager = AsyncMock()
        mock_browser_manager.initialize = AsyncMock()
        mock_browser_manager.close = AsyncMock()
        mock_browser_manager_class.return_value = mock_browser_manager
        
        orchestrator = PipelineOrchestrator(
            urls=["https://example.com/1", "https://example.com/2"],
            output_dir=str(temp_output_dir),
            max_concurrent=1,
            proxy_manager=None,
            compliance_manager=compliance_manager,
            human_behavior=human_behavior,
            deep_search_only=True
        )
        
        results = await orchestrator._process_urls_with_single_browser()
        
        assert len(results) == 2
        mock_browser_manager.initialize.assert_called_once()
        mock_browser_manager.close.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('src.pipelines.pipeline_orchestrator.BrowserManager')
    async def test_process_urls_with_single_browser_handles_init_error(self, mock_browser_manager_class, temp_output_dir, compliance_manager, human_behavior):
        """Test that single browser mode handles initialization errors"""
        # Mock browser manager that fails to initialize
        mock_browser_manager = AsyncMock()
        mock_browser_manager.initialize = AsyncMock(side_effect=Exception("Browser init failed"))
        mock_browser_manager_class.return_value = mock_browser_manager
        
        orchestrator = PipelineOrchestrator(
            urls=["https://example.com/1"],
            output_dir=str(temp_output_dir),
            max_concurrent=1,
            proxy_manager=None,
            compliance_manager=compliance_manager,
            human_behavior=human_behavior,
            deep_search_only=True
        )
        
        with pytest.raises(Exception):
            await orchestrator._process_urls_with_single_browser()


class TestPipelineOrchestratorMainFlow:
    """Tests for main processing flow"""
    
    @pytest.mark.asyncio
    @patch('src.pipelines.pipeline_orchestrator.PipelineOrchestrator._process_urls_with_concurrency')
    async def test_process_urls_normal_mode(self, mock_process_concurrency, temp_output_dir, compliance_manager, human_behavior):
        """Test process_urls in normal mode"""
        mock_process_concurrency.return_value = [{"url": "test1"}, {"url": "test2"}]
        
        orchestrator = PipelineOrchestrator(
            urls=["https://example.com/1", "https://example.com/2"],
            output_dir=str(temp_output_dir),
            max_concurrent=2,
            proxy_manager=None,
            compliance_manager=compliance_manager,
            human_behavior=human_behavior,
            deep_search_only=False
        )
        
        results = await orchestrator.process_urls()
        
        assert len(results) == 2
        mock_process_concurrency.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('src.pipelines.pipeline_orchestrator.PipelineOrchestrator._process_urls_with_single_browser')
    async def test_process_urls_deep_search_only_mode(self, mock_process_single_browser, temp_output_dir, compliance_manager, human_behavior):
        """Test process_urls in deep_search_only mode"""
        mock_process_single_browser.return_value = [{"url": "test1"}]
        
        orchestrator = PipelineOrchestrator(
            urls=["https://example.com/1"],
            output_dir=str(temp_output_dir),
            max_concurrent=1,
            proxy_manager=None,
            compliance_manager=compliance_manager,
            human_behavior=human_behavior,
            deep_search_only=True
        )
        
        results = await orchestrator.process_urls()
        
        assert len(results) == 1
        mock_process_single_browser.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('src.pipelines.pipeline_orchestrator.URLProcessor.process_url')
    async def test_process_urls_with_concurrency_handles_none_results(self, mock_process_url, temp_output_dir, compliance_manager, human_behavior):
        """Test that concurrency handles None results"""
        mock_process_url.return_value = None
        
        orchestrator = PipelineOrchestrator(
            urls=["https://example.com/1", "https://example.com/2"],
            output_dir=str(temp_output_dir),
            max_concurrent=2,
            proxy_manager=None,
            compliance_manager=compliance_manager,
            human_behavior=human_behavior
        )
        
        results = await orchestrator._process_urls_with_concurrency()
        
        # Should handle None results
        assert len(results) == 0
        assert orchestrator.stats["skipped"] == 2
    
    @pytest.mark.asyncio
    @patch('src.pipelines.pipeline_orchestrator.Config')
    @patch('src.pipelines.pipeline_orchestrator.ImageDownloader.download_listing_images')
    @patch('src.pipelines.pipeline_orchestrator.CSVStorageManager.save_single_listing')
    async def test_save_callback_with_images(self, mock_save, mock_download, mock_config, temp_output_dir, compliance_manager, human_behavior):
        """Test save callback downloads images when enabled"""
        mock_config.SAVE_IMAGES = True
        
        orchestrator = PipelineOrchestrator(
            urls=[],
            output_dir=str(temp_output_dir),
            max_concurrent=1,
            proxy_manager=None,
            compliance_manager=compliance_manager,
            human_behavior=human_behavior
        )
        
        listing = {
            "url": "https://example.com/listing",
            "images": ["img1.jpg", "img2.jpg"]
        }
        
        # Call the callback directly
        await orchestrator.url_processor.save_callback(listing)
        
        mock_download.assert_called_once_with(listing)
        mock_save.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('src.pipelines.pipeline_orchestrator.Config')
    @patch('src.pipelines.pipeline_orchestrator.CSVStorageManager.save_single_listing')
    async def test_save_callback_without_images(self, mock_save, mock_config, temp_output_dir, compliance_manager, human_behavior):
        """Test save callback when no images"""
        mock_config.SAVE_IMAGES = True
        
        orchestrator = PipelineOrchestrator(
            urls=[],
            output_dir=str(temp_output_dir),
            max_concurrent=1,
            proxy_manager=None,
            compliance_manager=compliance_manager,
            human_behavior=human_behavior
        )
        
        listing = {
            "url": "https://example.com/listing",
            "images": []
        }
        
        # Call the callback directly
        await orchestrator.url_processor.save_callback(listing)
        
        mock_save.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('src.pipelines.pipeline_orchestrator.CSVStorageManager.save_page_listings')
    async def test_page_callback(self, mock_save_page, temp_output_dir, compliance_manager, human_behavior):
        """Test page callback saves page listings"""
        orchestrator = PipelineOrchestrator(
            urls=[],
            output_dir=str(temp_output_dir),
            max_concurrent=1,
            proxy_manager=None,
            compliance_manager=compliance_manager,
            human_behavior=human_behavior
        )
        
        listings = [{"url": "https://example.com/1", "price": 100000}]
        
        # Call the page callback directly
        await orchestrator.url_processor.page_callback(1, listings, "https://example.com")
        
        mock_save_page.assert_called_once_with(1, listings)
    
    @pytest.mark.asyncio
    @patch('src.pipelines.pipeline_orchestrator.URLProcessor.process_url')
    async def test_process_urls_with_single_browser_handles_none_result(self, mock_process_url, temp_output_dir, compliance_manager, human_behavior):
        """Test single browser mode handles None results"""
        mock_process_url.return_value = None
        
        with patch('src.pipelines.pipeline_orchestrator.BrowserManager') as mock_browser_class:
            mock_browser = AsyncMock()
            mock_browser.initialize = AsyncMock()
            mock_browser.close = AsyncMock()
            mock_browser_class.return_value = mock_browser
            
            orchestrator = PipelineOrchestrator(
                urls=["https://example.com/1"],
                output_dir=str(temp_output_dir),
                max_concurrent=1,
                proxy_manager=None,
                compliance_manager=compliance_manager,
                human_behavior=human_behavior,
                deep_search_only=True
            )
            
            results = await orchestrator._process_urls_with_single_browser()
            
            assert len(results) == 0
            assert orchestrator.stats["skipped"] == 1
    
    def test_update_stats_from_result_none(self, temp_output_dir, compliance_manager, human_behavior):
        """Test updating stats from None result"""
        orchestrator = PipelineOrchestrator(
            urls=[],
            output_dir=str(temp_output_dir),
            max_concurrent=1,
            proxy_manager=None,
            compliance_manager=compliance_manager,
            human_behavior=human_behavior
        )
        
        orchestrator._update_stats_from_result(None, "https://example.com/listing")
        
        assert orchestrator.stats["skipped"] == 1
    
    @pytest.mark.asyncio
    @patch('src.pipelines.pipeline_orchestrator.URLProcessor.process_url')
    async def test_process_urls_with_single_browser_exception_handling(self, mock_process_url, temp_output_dir, compliance_manager, human_behavior):
        """Test single browser mode handles exceptions during processing"""
        mock_process_url.side_effect = Exception("Processing error")
        
        with patch('src.pipelines.pipeline_orchestrator.BrowserManager') as mock_browser_class:
            mock_browser = AsyncMock()
            mock_browser.initialize = AsyncMock()
            mock_browser.close = AsyncMock()
            mock_browser_class.return_value = mock_browser
            
            orchestrator = PipelineOrchestrator(
                urls=["https://example.com/1"],
                output_dir=str(temp_output_dir),
                max_concurrent=1,
                proxy_manager=None,
                compliance_manager=compliance_manager,
                human_behavior=human_behavior,
                deep_search_only=True
            )
            
            results = await orchestrator._process_urls_with_single_browser()
            
            # Should handle exception and add error result
            assert len(results) == 1
            assert "error" in results[0]
            assert orchestrator.stats["failed"] == 1

