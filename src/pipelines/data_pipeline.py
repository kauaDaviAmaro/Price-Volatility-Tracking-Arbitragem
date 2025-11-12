"""
Data Pipeline - Manages URL list and Concurrency
Orchestrates the scraping data flow
"""
import logging
from typing import List, Dict, Optional
from pathlib import Path

from src.core.compliance_manager import ComplianceManager
from src.core.human_behavior import HumanBehavior
from src.core.proxy_manager import ProxyManager
from src.pipelines.pipeline_orchestrator import PipelineOrchestrator
from src.pipelines.csv_storage import CSVStorageManager
from src.config import Config

logger = logging.getLogger(__name__)

# Default CSV filenames
DEFAULT_CSV_FILENAME = "scraped_data.csv"
DEEP_SEARCH_CSV_FILENAME = "deep_search_data.csv"


class DataPipeline:
    """Pipeline that manages the URL list and scraping concurrency with elite features"""
    
    def __init__(
        self,
        urls: List[str],
        output_dir: Optional[str] = None,
        max_concurrent: Optional[int] = None,
        proxy_manager: Optional[ProxyManager] = None,
        compliance_manager: Optional[ComplianceManager] = None,
        human_behavior: Optional[HumanBehavior] = None,
        deep_search_only: bool = False
    ):
        """
        Initializes the data pipeline
        
        Args:
            urls: List of URLs to scrape
            output_dir: Output directory for the data (uses Config if None)
            max_concurrent: Maximum number of concurrent requests (uses Config if None)
            proxy_manager: ProxyManager instance
            compliance_manager: ComplianceManager instance
            human_behavior: HumanBehavior instance
            deep_search_only: If True, treat all URLs as individual listings and skip search page scraping
        """
        self.urls = urls
        self.output_dir = Path(output_dir or Config.OUTPUT_DIR)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.max_concurrent = max_concurrent or Config.MAX_CONCURRENT
        self.results: List[Dict] = []
        self.deep_search_only = deep_search_only
        
        self.proxy_manager = proxy_manager
        self.compliance_manager = compliance_manager or ComplianceManager(
            cache_dir=Config.ROBOTS_CACHE_DIR,
            respect_robots=Config.RESPECT_ROBOTS_TXT
        )
        self.human_behavior = human_behavior or HumanBehavior(
            min_delay=Config.MIN_DELAY,
            max_delay=Config.MAX_DELAY,
            scroll_delay_min=Config.SCROLL_DELAY_MIN,
            scroll_delay_max=Config.SCROLL_DELAY_MAX,
            mouse_movement_enabled=Config.MOUSE_MOVEMENT_ENABLED,
            scroll_enabled=Config.SCROLL_ENABLED
        )
        
        # Initialize orchestrator
        self.orchestrator = PipelineOrchestrator(
            urls=self.urls,
            output_dir=str(self.output_dir),
            max_concurrent=self.max_concurrent,
            proxy_manager=self.proxy_manager,
            compliance_manager=self.compliance_manager,
            human_behavior=self.human_behavior,
            deep_search_only=self.deep_search_only
        )
        
        # Initialize CSV storage for backward compatibility
        self.csv_storage = CSVStorageManager(self.output_dir)
        
        # Statistics (delegated to orchestrator)
        self.stats = {
            "total": 0,
            "success": 0,
            "failed": 0,
            "blocked": 0,
            "skipped": 0
        }
    
    async def process_urls(self) -> List[Dict]:
        """
        Processes all URLs with concurrency control and elite features
        
        Returns:
            List[Dict]: List with all scraping results
        """
        # Delegate to orchestrator
        self.results = await self.orchestrator.process_urls()
        
        # Update stats from orchestrator
        orchestrator_stats = self.orchestrator.get_stats()
        self.stats.update(orchestrator_stats)
        
        return self.results
    
    def save_to_csv(self, filename: str = DEFAULT_CSV_FILENAME) -> None:
        """
        Saves results to a CSV file
        
        Args:
            filename: Output CSV filename
        """
        # Delegate to CSV storage
        self.csv_storage.filename = filename
        self.csv_storage.filepath = self.csv_storage.output_dir / filename
        self.csv_storage.save_results(self.results)
    
    async def save_single_listing_to_csv(
        self,
        listing: Dict,
        base_url: str,
        filename: str = DEFAULT_CSV_FILENAME
    ) -> None:
        """
        Saves a single listing to CSV, updating existing row or appending new one.
        Uses file locking to prevent data loss in concurrent scenarios.
        Preserves ALL existing data in the CSV.
        
        Args:
            listing: Listing dictionary to save
            base_url: Base URL (for logging purposes, optional)
            filename: Output CSV filename
        """
        # Delegate to CSV storage
        self.csv_storage.filename = filename
        self.csv_storage.filepath = self.csv_storage.output_dir / filename
        await self.csv_storage.save_single_listing(listing, base_url)
    
    def save_page_to_csv(
        self,
        page_num: int,
        page_listings: List[Dict],
        filename: str = DEFAULT_CSV_FILENAME
    ) -> None:
        """
        Saves a page of listings to CSV file incrementally (append mode)
        
        Args:
            page_num: Page number that was scraped
            page_listings: List of listings from this page
            filename: Output CSV filename
        """
        # Delegate to CSV storage
        self.csv_storage.filename = filename
        self.csv_storage.filepath = self.csv_storage.output_dir / filename
        self.csv_storage.save_page_listings(page_num, page_listings)
    
    def save_deep_scraped_data_to_csv(
        self,
        listings: List[Dict],
        filename: str = DEFAULT_CSV_FILENAME
    ) -> None:
        """
        Saves deep scraped listings to CSV, updating existing rows or appending new ones
        
        Args:
            listings: List of listings with deep scraped data
            filename: Output CSV filename
        """
        # Delegate to CSV storage
        self.csv_storage.filename = filename
        self.csv_storage.filepath = self.csv_storage.output_dir / filename
        self.csv_storage.save_listings_batch(listings)
    
    async def run(self) -> None:
        """
        Runs the complete pipeline: scraping and saving
        Note: Images are downloaded during deep scraping, not here
        """
        # Process URLs (includes deep scraping which downloads images)
        await self.process_urls()
        
        # Save to CSV
        self.save_to_csv()
        
        # Images are already downloaded during deep scraping in process_urls()
        # No need to download them again here
        
        logger.info("Pipeline run completed")
