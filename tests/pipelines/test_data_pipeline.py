"""
Tests for DataPipeline - Public API tests
Tests focus on the public interface, not internal implementation details
"""
import pytest
import csv
from unittest.mock import AsyncMock, Mock, patch
from pathlib import Path
import tempfile
import shutil

from src.pipelines.data_pipeline import DataPipeline
from src.core.proxy_manager import ProxyManager
from src.core.compliance_manager import ComplianceManager
from src.core.human_behavior import HumanBehavior
from src.config import Config


@pytest.fixture
def temp_output_dir():
    """Create temporary output directory"""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


class TestDataPipelineInitialization:
    """Tests for DataPipeline initialization"""
    
    def test_init_with_defaults(self):
        """Test initialization with defaults"""
        pipeline = DataPipeline(urls=["https://example.com"])
        
        assert pipeline.urls == ["https://example.com"]
        assert pipeline.max_concurrent == Config.MAX_CONCURRENT
        assert pipeline.results == []
        assert isinstance(pipeline.compliance_manager, ComplianceManager)
        assert isinstance(pipeline.human_behavior, HumanBehavior)
    
    def test_init_with_custom_params(self, temp_output_dir):
        """Test initialization with custom parameters"""
        proxy_manager = ProxyManager()
        compliance_manager = ComplianceManager()
        human_behavior = HumanBehavior()
        
        pipeline = DataPipeline(
            urls=["https://example.com"],
            output_dir=str(temp_output_dir),
            max_concurrent=5,
            proxy_manager=proxy_manager,
            compliance_manager=compliance_manager,
            human_behavior=human_behavior
        )
        
        assert pipeline.output_dir == temp_output_dir
        assert pipeline.max_concurrent == 5
        assert pipeline.proxy_manager == proxy_manager
        assert pipeline.compliance_manager == compliance_manager
        assert pipeline.human_behavior == human_behavior


class TestDataPipelineCSVSaving:
    """Tests for CSV saving - Public API"""
    
    async def test_save_single_listing_to_csv(self, temp_output_dir):
        """Test saving single listing to CSV"""
        pipeline = DataPipeline(urls=[], output_dir=str(temp_output_dir))
        
        listing = {
            "url": "https://example.com/listing",
            "price": 100000,
            "title": "Test Listing"
        }
        
        await pipeline.save_single_listing_to_csv(listing, "https://example.com")
        
        csv_file = temp_output_dir / "scraped_data.csv"
        assert csv_file.exists()
        
        # Read and verify
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 1
            assert rows[0]["url"] == "https://example.com/listing"
            assert rows[0]["price"] == "100000"
    
    def test_save_page_to_csv(self, temp_output_dir):
        """Test saving page of listings to CSV"""
        pipeline = DataPipeline(urls=[], output_dir=str(temp_output_dir))
        
        listings = [
            {"url": "https://example.com/1", "price": 100000},
            {"url": "https://example.com/2", "price": 200000}
        ]
        
        pipeline.save_page_to_csv(1, listings)
        
        csv_file = temp_output_dir / "scraped_data.csv"
        assert csv_file.exists()
        
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 2
    
    def test_save_to_csv(self, temp_output_dir):
        """Test saving results to CSV"""
        pipeline = DataPipeline(urls=[], output_dir=str(temp_output_dir))
        
        # Set results directly
        pipeline.results = [
            {"url": "https://example.com/1", "price": 100000},
            {"url": "https://example.com/2", "price": 200000}
        ]
        
        pipeline.save_to_csv()
        
        csv_file = temp_output_dir / "scraped_data.csv"
        assert csv_file.exists()
        
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 2
    
    def test_save_deep_scraped_data_to_csv(self, temp_output_dir):
        """Test saving deep scraped listings to CSV"""
        pipeline = DataPipeline(urls=[], output_dir=str(temp_output_dir))
        
        listings = [
            {"url": "https://example.com/1", "price": 100000, "full_description": "Test"},
            {"url": "https://example.com/2", "price": 200000, "full_description": "Test 2"}
        ]
        
        pipeline.save_deep_scraped_data_to_csv(listings)
        
        csv_file = temp_output_dir / "scraped_data.csv"
        assert csv_file.exists()
        
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 2


@pytest.mark.asyncio
class TestDataPipelineProcessURLs:
    """Tests for process_urls - Public API"""
    
    @patch('src.pipelines.pipeline_orchestrator.PipelineOrchestrator.process_urls')
    async def test_process_urls_delegates_to_orchestrator(self, mock_process_urls):
        """Test that process_urls delegates to orchestrator"""
        mock_process_urls.return_value = [{"url": "test1"}, {"url": "test2"}]
        
        pipeline = DataPipeline(urls=["https://example.com/1", "https://example.com/2"])
        results = await pipeline.process_urls()
        
        assert len(results) == 2
        assert results[0]["url"] == "test1"
        assert results[1]["url"] == "test2"
        mock_process_urls.assert_called_once()
    
    @patch('src.pipelines.pipeline_orchestrator.PipelineOrchestrator.process_urls')
    async def test_process_urls_updates_stats(self, mock_process_urls):
        """Test that process_urls updates stats from orchestrator"""
        mock_process_urls.return_value = [{"url": "test"}]
        
        # Mock orchestrator stats
        mock_orchestrator = Mock()
        mock_orchestrator.get_stats.return_value = {
            "total": 1,
            "success": 1,
            "failed": 0,
            "blocked": 0,
            "skipped": 0
        }
        mock_orchestrator.process_urls = mock_process_urls
        
        pipeline = DataPipeline(urls=["https://example.com"])
        pipeline.orchestrator = mock_orchestrator
        
        await pipeline.process_urls()
        
        assert pipeline.stats["total"] == 1
        assert pipeline.stats["success"] == 1


@pytest.mark.asyncio
class TestDataPipelineRun:
    """Tests for run method - Public API"""
    
    @patch('src.pipelines.data_pipeline.DataPipeline.process_urls')
    @patch('src.pipelines.data_pipeline.DataPipeline.save_to_csv')
    async def test_run_completes_pipeline(self, mock_save_to_csv, mock_process_urls):
        """Test that run method completes full pipeline"""
        mock_process_urls.return_value = [{"url": "test"}]
        
        pipeline = DataPipeline(urls=["https://example.com"])
        await pipeline.run()
        
        mock_process_urls.assert_called_once()
        mock_save_to_csv.assert_called_once()
