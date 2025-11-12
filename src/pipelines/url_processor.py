"""
URL Processor - Handles individual URL processing with retry logic
Manages scraping attempts, error handling, and compliance checking
"""
import asyncio
import logging
from typing import Dict, Optional, Callable, Awaitable

from src.core.browser_manager import BrowserManager
from src.core.compliance_manager import ComplianceManager
from src.core.human_behavior import HumanBehavior
from src.core.proxy_manager import ProxyManager
from src.services.zap_imoveis_service import ZapImoveisService
from src.config import Config

logger = logging.getLogger(__name__)


class URLProcessor:
    """Processes individual URLs with retry logic and error handling"""
    
    def __init__(
        self,
        compliance_manager: ComplianceManager,
        human_behavior: HumanBehavior,
        proxy_manager: Optional[ProxyManager] = None,
        deep_search_only: bool = False,
        save_callback: Optional[Callable[[Dict], Awaitable[None]]] = None,
        page_callback: Optional[Callable[[int, list, str], Awaitable[None]]] = None,
        deep_search_callback: Optional[Callable[[Dict], Awaitable[None]]] = None
    ):
        """
        Initialize URL Processor
        
        Args:
            compliance_manager: ComplianceManager instance
            human_behavior: HumanBehavior instance
            proxy_manager: Optional ProxyManager instance
            deep_search_only: If True, treat all URLs as individual listings
            save_callback: Optional callback to save listings (for deep_search_only mode)
            page_callback: Optional callback to save page data (for search results)
            deep_search_callback: Optional callback to save deep search results (for normal mode deep scraping)
        """
        self.compliance_manager = compliance_manager
        self.human_behavior = human_behavior
        self.proxy_manager = proxy_manager
        self.deep_search_only = deep_search_only
        self.save_callback = save_callback
        self.page_callback = page_callback
        self.deep_search_callback = deep_search_callback
    
    def is_search_url(self, url: str) -> bool:
        """Check if URL is a search results page"""
        search_indicators = ['/venda/']
        return any(indicator in url for indicator in search_indicators)
    
    async def check_compliance(self, url: str) -> bool:
        """
        Check if URL can be scraped (compliance checks)
        
        Args:
            url: URL to check
            
        Returns:
            True if compliant, False otherwise
        """
        if not self.compliance_manager.is_public_data(url):
            logger.warning(f"Skipping potentially private data: {url}")
            return False
        
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        if not await self.compliance_manager.can_fetch(url, user_agent):
            logger.warning(f"robots.txt disallows: {url}")
            return False
        
        await self.compliance_manager.wait_for_rate_limit(url)
        return True
    
    def is_blocked_error(self, result: Dict) -> bool:
        """Check if result indicates a blocking error"""
        error_str = str(result.get("error", ""))
        return "403" in error_str or "429" in error_str
    
    async def handle_blocked_error(
        self,
        url: str,
        result: Dict,
        browser_manager: BrowserManager
    ) -> None:
        """Handle blocked error by rotating proxy and fingerprint"""
        logger.warning(f"Blocked on {url}: {result.get('error')}")
        await browser_manager.mark_proxy_failure()
        browser_manager.rotate_fingerprint()
    
    def calculate_retry_delay(self, attempt: int) -> float:
        """Calculate retry delay with exponential backoff"""
        return Config.RETRY_DELAY * (Config.RETRY_BACKOFF ** attempt)
    
    async def attempt_scrape(
        self,
        url: str,
        browser_manager: BrowserManager
    ) -> Optional[Dict]:
        """
        Attempt to scrape a URL (single listing or search results)
        
        Args:
            url: URL to scrape
            browser_manager: BrowserManager instance (may be pre-initialized)
            
        Returns:
            Scraping result or None
        """
        # Only initialize if not already initialized (for single browser reuse)
        if browser_manager.context is None:
            await browser_manager.initialize()
        
        page = await browser_manager.create_page()
        
        try:
            service = ZapImoveisService(page, self.human_behavior)
            
            # If deep_search_only mode, treat all URLs as individual listings
            if self.deep_search_only:
                logger.info(f"Deep search only mode: Processing {url} as individual listing")
                result = await service.scrape_listing(url, deep_scrape=True)
                
                # Save listing immediately if callback provided
                if result and "error" not in result and self.save_callback:
                    await self.save_callback(result)
                
                await browser_manager.mark_proxy_success()
                return result
            
            # Normal mode: check if URL is a search URL
            if self.is_search_url(url):
                max_pages = Config.MAX_PAGES
                
                # Create page callback if provided
                page_cb = None
                if self.page_callback:
                    async def page_callback_wrapper(page_num: int, page_listings: list, base_url: str) -> None:
                        await self.page_callback(page_num, page_listings, base_url)
                    page_cb = page_callback_wrapper
                
                listings = await service.scrape_search_results(
                    url,
                    max_pages=max_pages,
                    page_callback=page_cb
                )
                
                if listings:
                    logger.info(f"Search completed. Starting deep scraping on {len(listings)} listings...")
                    
                    # Create save callback for deep scraping
                    # Use deep_search_callback if available (normal mode), otherwise use save_callback (deep_search_only mode)
                    async def save_deep_scraped_listing(listing: Dict) -> None:
                        # Prefer deep_search_callback for normal mode (saves to separate file)
                        if self.deep_search_callback:
                            await self.deep_search_callback(listing)
                        elif self.save_callback:
                            await self.save_callback(listing)
                    
                    listings = await service.deep_scrape_listings(
                        listings,
                        save_callback=save_deep_scraped_listing
                    )
                    logger.info(f"Deep scraping completed for all {len(listings)} listings")
                
                await browser_manager.mark_proxy_success()
                return {"url": url, "listings": listings, "type": "search_results"}
            else:
                # Single listing URL
                result = await service.scrape_listing(url, deep_scrape=True)
                
                # Save listing if callback provided
                if result and "error" not in result and self.save_callback:
                    await self.save_callback(result)
                
                await browser_manager.mark_proxy_success()
                return result
                
        finally:
            # Close the page after use
            try:
                await page.close()
            except Exception as e:
                logger.debug(f"Error closing page: {e}")
    
    async def process_scrape_attempt(
        self,
        url: str,
        browser_manager: BrowserManager,
        attempt: int,
        max_retries: int
    ) -> Optional[Dict]:
        """
        Process a single scrape attempt with error handling
        
        Args:
            url: URL to scrape
            browser_manager: BrowserManager instance
            attempt: Current attempt number (0-based)
            max_retries: Maximum number of retries
            
        Returns:
            Result dict if successful, None if should retry
        """
        try:
            result = await self.attempt_scrape(url, browser_manager)
            
            if result and "error" not in result:
                return result
            
            if result and self.is_blocked_error(result):
                await self.handle_blocked_error(url, result, browser_manager)
            
            if attempt < max_retries:
                delay = self.calculate_retry_delay(attempt)
                await asyncio.sleep(delay)
            return None
            
        except Exception as e:
            logger.error(f"Error processing {url} (attempt {attempt + 1}/{max_retries + 1}): {e}")
            if attempt >= max_retries:
                return {"url": url, "error": str(e)}
            delay = self.calculate_retry_delay(attempt)
            await asyncio.sleep(delay)
            return None
    
    async def process_url(
        self,
        url: str,
        browser_manager: Optional[BrowserManager] = None,
        reuse_browser: bool = False
    ) -> Optional[Dict]:
        """
        Process a single URL with retry logic and compliance checking
        
        Args:
            url: URL to be processed
            browser_manager: Optional pre-initialized BrowserManager (for reuse)
            reuse_browser: If True, reuse the provided browser_manager instead of creating new
            
        Returns:
            Optional[Dict]: Extracted data or None in case of error
        """
        # Check compliance
        if not await self.check_compliance(url):
            return None
        
        max_retries = Config.MAX_RETRIES
        should_close_browser = False
        
        # Create browser manager if not provided
        if browser_manager is None:
            browser_manager = BrowserManager(
                headless=Config.HEADLESS,
                proxy_manager=self.proxy_manager
            )
            should_close_browser = True
        
        try:
            for attempt in range(max_retries + 1):
                result = await self.process_scrape_attempt(
                    url, browser_manager, attempt, max_retries
                )
                
                if result is not None:
                    return result
            
            # All retries exhausted
            return {"url": url, "error": "Max retries exceeded"}
            
        finally:
            # Only close browser if we created it
            if should_close_browser and browser_manager:
                await browser_manager.close()

