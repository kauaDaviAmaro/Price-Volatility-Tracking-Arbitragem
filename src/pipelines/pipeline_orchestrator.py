"""
Pipeline Orchestrator - Coordinates the overall scraping flow
Unifies normal and deep_search_only modes, manages concurrency and statistics
"""
import asyncio
import logging
from typing import List, Dict, Optional, Callable, Awaitable

from src.core.browser_manager import BrowserManager
from src.core.compliance_manager import ComplianceManager
from src.core.human_behavior import HumanBehavior
from src.core.proxy_manager import ProxyManager
from src.pipelines.url_processor import URLProcessor
from src.pipelines.csv_storage import CSVStorageManager
from src.pipelines.image_downloader import ImageDownloader
from src.config import Config

logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Orchestrates the scraping pipeline flow"""
    
    def __init__(
        self,
        urls: List[str],
        output_dir: str,
        max_concurrent: int,
        proxy_manager: Optional[ProxyManager],
        compliance_manager: ComplianceManager,
        human_behavior: HumanBehavior,
        deep_search_only: bool = False
    ):
        """
        Initialize Pipeline Orchestrator
        
        Args:
            urls: List of URLs to scrape
            output_dir: Output directory for data
            max_concurrent: Maximum number of concurrent requests
            proxy_manager: Optional ProxyManager instance
            compliance_manager: ComplianceManager instance
            human_behavior: HumanBehavior instance
            deep_search_only: If True, treat all URLs as individual listings
        """
        self.urls = urls
        self.max_concurrent = max_concurrent
        self.deep_search_only = deep_search_only
        
        # Initialize managers
        self.csv_storage = CSVStorageManager(output_dir, Config.LISTINGS_CSV_FILENAME)
        self.image_downloader = ImageDownloader(output_dir)
        
        # Cache for listings data (to avoid reading CSV multiple times)
        self._listings_cache: Optional[Dict[str, Dict]] = None
        
        # Create save callback for deep search (merges and saves immediately to scraped_data.csv)
        async def save_deep_search_callback(listing: Dict) -> None:
            """Callback to merge and save deep search results immediately to scraped_data.csv"""
            listing_url = listing.get('url', 'unknown')
            logger.info(f"Deep search callback called for: {listing_url}")
            
            # Download images if enabled
            if Config.SAVE_IMAGES:
                images = listing.get("images")
                if images:
                    logger.info(f"Found {len(images) if isinstance(images, list) else 'unknown'} images for listing {listing.get('url', 'unknown')}")
                    await self.image_downloader.download_listing_images(listing)
                else:
                    logger.debug(f"No images found for listing {listing.get('url', 'unknown')}")
            
            # Load listings data if not already loaded
            if self._listings_cache is None:
                self._listings_cache, _ = self.csv_storage._read_existing_data()
                logger.info(f"Loaded {len(self._listings_cache)} listings from {Config.LISTINGS_CSV_FILENAME} for deep search updates")
            
            listing_url = listing.get('url')
            if not listing_url:
                logger.warning(f"Deep search result has no URL, skipping save")
                return
            
            # Check if cache is empty
            if not self._listings_cache:
                logger.warning(f"Listings cache is empty! Cannot update listing {listing_url}. Make sure scraped_data.csv has listings first.")
                return
            
            # Normalize URL for comparison (remove trailing slashes, etc.)
            listing_url_normalized = listing_url.strip().rstrip('/')
            
            # Try to find URL in cache (exact match first, then normalized)
            existing_listing = None
            cache_key = None
            
            if listing_url in self._listings_cache:
                cache_key = listing_url
                existing_listing = self._listings_cache[listing_url]
            elif listing_url_normalized in self._listings_cache:
                cache_key = listing_url_normalized
                existing_listing = self._listings_cache[listing_url_normalized]
            else:
                # Try to find by matching any URL in cache that contains the listing URL
                for cached_url, cached_data in self._listings_cache.items():
                    if cached_url and (listing_url in cached_url or cached_url in listing_url):
                        cache_key = cached_url
                        existing_listing = cached_data
                        logger.debug(f"Found URL match: {listing_url} matches {cached_url}")
                        break
            
            if existing_listing and cache_key:
                # Merge deep search data with existing listing data
                merged_listing = self.csv_storage._merge_listing_data(
                    existing_listing,
                    listing
                )
                # Update cache with merged data
                self._listings_cache[cache_key] = merged_listing
                # Save merged listing immediately to scraped_data.csv
                try:
                    await self.csv_storage.save_single_listing(merged_listing)
                    logger.info(f"✓ Updated listing {listing_url} with deep search data in {Config.LISTINGS_CSV_FILENAME}")
                except Exception as e:
                    logger.error(f"Error saving listing {listing_url}: {e}", exc_info=True)
                    raise  # Re-raise to see the error
            else:
                # Listing not found in scraped_data.csv
                logger.warning(f"Listing {listing_url} not found in {Config.LISTINGS_CSV_FILENAME} (cache has {len(self._listings_cache)} entries), skipping (deep search only updates existing listings)")
                # Log first few URLs in cache for debugging
                if self._listings_cache:
                    sample_urls = list(self._listings_cache.keys())[:3]
                    logger.debug(f"Sample URLs in cache: {sample_urls}")
                    # Also log the listing URL for comparison
                    logger.debug(f"Looking for URL: {listing_url}")
        
        # Create save callback for initial listings (only used in deep_search_only mode)
        async def save_listing_callback(listing: Dict) -> None:
            """Callback to merge and save listing data immediately to scraped_data.csv (for deep_search_only mode)"""
            # Download images if enabled
            if Config.SAVE_IMAGES:
                images = listing.get("images")
                if images:
                    logger.info(f"Found {len(images) if isinstance(images, list) else 'unknown'} images for listing {listing.get('url', 'unknown')}")
                    await self.image_downloader.download_listing_images(listing)
                else:
                    logger.debug(f"No images found for listing {listing.get('url', 'unknown')}")
            
            # Load listings data if not already loaded (for deep_search_only mode)
            if self._listings_cache is None:
                self._listings_cache, _ = self.csv_storage._read_existing_data()
                logger.info(f"Loaded {len(self._listings_cache)} listings from {Config.LISTINGS_CSV_FILENAME} for deep search updates")
            
            listing_url = listing.get('url')
            if not listing_url:
                logger.warning(f"Deep search result has no URL, skipping save")
                return
            
            # Normalize URL for comparison (remove trailing slashes, etc.)
            listing_url_normalized = listing_url.strip().rstrip('/')
            
            # Try to find URL in cache (exact match first, then normalized)
            existing_listing = None
            cache_key = None
            
            if listing_url in self._listings_cache:
                cache_key = listing_url
                existing_listing = self._listings_cache[listing_url]
            elif listing_url_normalized in self._listings_cache:
                cache_key = listing_url_normalized
                existing_listing = self._listings_cache[listing_url_normalized]
            else:
                # Try to find by matching any URL in cache that contains the listing URL
                for cached_url, cached_data in self._listings_cache.items():
                    if cached_url and (listing_url in cached_url or cached_url in listing_url):
                        cache_key = cached_url
                        existing_listing = cached_data
                        logger.debug(f"Found URL match: {listing_url} matches {cached_url}")
                        break
            
            if existing_listing and cache_key:
                # Merge deep search data with existing listing data
                merged_listing = self.csv_storage._merge_listing_data(
                    existing_listing,
                    listing
                )
                # Update cache with merged data
                self._listings_cache[cache_key] = merged_listing
                # Save merged listing immediately to scraped_data.csv
                try:
                    await self.csv_storage.save_single_listing(merged_listing)
                    logger.info(f"✓ Updated listing {listing_url} with deep search data in {Config.LISTINGS_CSV_FILENAME}")
                except Exception as e:
                    logger.error(f"Error saving listing {listing_url}: {e}", exc_info=True)
            else:
                # Listing not found in scraped_data.csv
                logger.warning(f"Listing {listing_url} not found in {Config.LISTINGS_CSV_FILENAME} (cache has {len(self._listings_cache)} entries), skipping (deep search only updates existing listings)")
                # Log first few URLs in cache for debugging
                if self._listings_cache:
                    sample_urls = list(self._listings_cache.keys())[:3]
                    logger.debug(f"Sample URLs in cache: {sample_urls}")
        
        # Create page callback for search results
        async def page_callback(page_num: int, page_listings: List[Dict], base_url: str) -> None:
            """Callback to save page data immediately after scraping"""
            # Save to scraped_data.csv
            self.csv_storage.save_page_listings(page_num, page_listings)
        
        # Initialize URL processor
        # In deep_search_only mode, use save_listing_callback
        # In normal mode, use deep_search_callback for deep scraping (updates scraped_data.csv)
        self.url_processor = URLProcessor(
            compliance_manager=compliance_manager,
            human_behavior=human_behavior,
            proxy_manager=proxy_manager,
            deep_search_only=deep_search_only,
            save_callback=save_listing_callback if deep_search_only else None,
            page_callback=page_callback,
            deep_search_callback=save_deep_search_callback if not deep_search_only else None  # Only use in normal mode
        )
        
        # Statistics
        self.stats = {
            "total": 0,
            "success": 0,
            "failed": 0,
            "blocked": 0,
            "skipped": 0
        }
    
    def _update_stats_from_result(self, result: Optional[Dict], url: str) -> None:
        """Update statistics based on result"""
        if result is None:
            self.stats["skipped"] += 1
            return
        
        if "error" in result:
            error_str = str(result.get("error", ""))
            if "403" in error_str or "429" in error_str:
                self.stats["blocked"] += 1
            else:
                self.stats["failed"] += 1
            return
        
        # Success
        if result.get("type") == "search_results" and "listings" in result:
            listings = result["listings"]
            self.stats["success"] += len(listings)
            logger.info(f"Successfully scraped {len(listings)} listings from search: {url}")
        else:
            self.stats["success"] += 1
            logger.info(f"Successfully scraped: {url}")
    
    async def process_urls(self) -> List[Dict]:
        """
        Process all URLs with appropriate concurrency strategy
        
        Returns:
            List[Dict]: List with all scraping results
        """
        logger.info(f"Starting pipeline: {len(self.urls)} URLs, max_concurrent={self.max_concurrent}, deep_search_only={self.deep_search_only}")
        self.stats["total"] = len(self.urls)
        
        # In deep-only mode, use a single browser instance for all URLs
        if self.deep_search_only:
            results = await self._process_urls_with_single_browser()
        else:
            # Normal mode: use semaphore for concurrency control
            results = await self._process_urls_with_concurrency()
        
        return results
    
    async def _process_urls_with_single_browser(self) -> List[Dict]:
        """
        Process all listing URLs in deep-only mode using a single browser instance.
        Sequential flow: process one URL, save immediately, move to next.
        
        Returns:
            List[Dict]: List with all scraping results
        """
        logger.info("=" * 80)
        logger.info(f"Starting deep search processing: {len(self.urls)} listing URLs")
        logger.info("=" * 80)
        
        # Create a single browser instance
        browser_manager = BrowserManager(
            headless=Config.HEADLESS,
            proxy_manager=self.url_processor.proxy_manager
        )
        
        results = []
        
        try:
            # Initialize browser once
            try:
                await browser_manager.initialize()
                logger.info("✓ Browser initialized - ready to process listings")
            except Exception as browser_error:
                error_msg = str(browser_error)
                logger.error(f"✗ Failed to initialize browser: {error_msg}", exc_info=True)
                logger.error("Browser initialization failed. This is a technical error and will not be saved to CSV.")
                logger.error("Please check:")
                logger.error("  1. Xvfb is running (if using Docker with non-headless mode)")
                logger.error("  2. DISPLAY environment variable is set correctly")
                logger.error("  3. Browser dependencies are installed")
                raise  # Re-raise to stop processing
            
            # Process all URLs sequentially
            for i, url in enumerate(self.urls, 1):
                logger.info("")
                logger.info(f"[{i}/{len(self.urls)}] Processing listing: {url}")
                
                try:
                    # Process URL with shared browser
                    result = await self.url_processor.process_url(
                        url,
                        browser_manager=browser_manager,
                        reuse_browser=True
                    )
                    
                    if result:
                        self._update_stats_from_result(result, url)
                        results.append(result)
                    else:
                        self.stats["skipped"] += 1
                        
                except Exception as e:
                    logger.error(f"  ✗ Error processing listing {url}: {e}", exc_info=True)
                    self.stats["failed"] += 1
                    results.append({"url": url, "error": str(e)})
        
        finally:
            # Close the single browser instance
            logger.info("")
            logger.info("Closing browser...")
            await browser_manager.close()
            logger.info("✓ Browser closed")
        
        # Filter out exceptions
        filtered_results = [r for r in results if r is not None and not isinstance(r, Exception)]
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("Deep search completed:")
        logger.info(f"  ✓ Success: {self.stats['success']}")
        logger.info(f"  ✗ Failed: {self.stats['failed']}")
        logger.info(f"  ⊘ Skipped: {self.stats['skipped']}")
        logger.info("=" * 80)
        
        return filtered_results
    
    async def _process_urls_with_concurrency(self) -> List[Dict]:
        """
        Process URLs with concurrency control using semaphore
        
        Returns:
            List[Dict]: List with all scraping results
        """
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def process_with_semaphore(url: str) -> Optional[Dict]:
            """Process URL with semaphore for concurrency control"""
            async with semaphore:
                return await self.url_processor.process_url(url)
        
        # Process URLs with concurrency control
        tasks = [process_with_semaphore(url) for url in self.urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions and None results, update stats
        filtered_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Exception processing URL {self.urls[i]}: {result}", exc_info=True)
                self.stats["failed"] += 1
                filtered_results.append({"url": self.urls[i], "error": str(result)})
            elif result is not None:
                self._update_stats_from_result(result, self.urls[i])
                filtered_results.append(result)
            else:
                self.stats["skipped"] += 1
        
        logger.info(f"Pipeline completed: {self.stats['success']} success, {self.stats['failed']} failed, {self.stats['blocked']} blocked, {self.stats['skipped']} skipped")
        
        return filtered_results
    
    def get_stats(self) -> Dict[str, int]:
        """Get current statistics"""
        return self.stats.copy()

