"""
Zap Imóveis Service - Where Playwright runs
Focus: Zap Imóveis Data Extraction
"""
from playwright.async_api import Page
from typing import Dict, List, Optional, Callable, Awaitable
import asyncio
import logging
import os
import random
import time

from src.scraper_app.core.human_behavior import HumanBehavior
from src.scraper_app.config import Config
from src.scraper_app.services.zap_imoveis.extractors import DataExtractor
from src.scraper_app.services.zap_imoveis.pagination import PaginationHandler
from src.scraper_app.services.zap_imoveis.search_extractor import SearchExtractor
from src.scraper_app.services.zap_imoveis.selectors import SELECTOR_PROPERTY_CARD

logger = logging.getLogger(__name__)


class ZapImoveisService:
    """Service responsible for extracting data from Zap Imóveis"""
    
    def __init__(self, page: Page, human_behavior: Optional[HumanBehavior] = None):
        """
        Initializes the Zap Imóveis service
        
        Args:
            page: Playwright page to perform scraping
            human_behavior: HumanBehavior instance (creates new if None)
        """
        self.page = page
        self.human_behavior = human_behavior or HumanBehavior(
            min_delay=Config.MIN_DELAY,
            max_delay=Config.MAX_DELAY,
            scroll_delay_min=Config.SCROLL_DELAY_MIN,
            scroll_delay_max=Config.SCROLL_DELAY_MAX,
            mouse_movement_enabled=Config.MOUSE_MOVEMENT_ENABLED,
            scroll_enabled=Config.SCROLL_ENABLED
        )
        self.extractor = DataExtractor(page)
        self.pagination = PaginationHandler(page)
        self.search_extractor = SearchExtractor(page)
    
    async def scrape_listing(self, url: str, deep_scrape: bool = True) -> Dict:
        """
        Extracts data from a specific Zap Imóveis listing
        
        Args:
            url: URL of the Zap Imóveis listing
            deep_scrape: If True, performs deep scraping of all detailed information
            
        Returns:
            Dict: Dictionary with extracted data (price, title, images, etc.)
        """
        try:
            page_start_time = time.time()
            
            # Navigate with faster wait strategy - goto already waits, so skip redundant wait
            wait_strategy = Config.WAIT_UNTIL
            await self.page.goto(url, wait_until=wait_strategy, timeout=Config.NAVIGATION_TIMEOUT)
            
            # Skip redundant wait - goto already waited for the load state
            # Just a tiny delay for any dynamic content
            await asyncio.sleep(0.05)
            
            # Skip scroll for speed - not needed for extraction
            # if Config.HUMAN_BEHAVIOR_ENABLED and Config.SCROLL_ENABLED:
            #     await self.human_behavior.scroll_page(self.page, random.randint(200, 500), "down")
            
            # Extract basic data
            data = {
                "url": url,
                "title": await self.extractor.extract_title(),
                "price": await self.extractor.extract_price(),
                "location": await self.extractor.extract_location(),
                "property_type": await self.extractor.extract_property_type(),
                "area": await self.extractor.extract_area(),
                "bedrooms": await self.extractor.extract_bedrooms(),
                "bathrooms": await self.extractor.extract_bathrooms(),
                "parking_spaces": await self.extractor.extract_parking_spaces(),
                "images": await self.extractor.extract_images(),
                "description": await self.extractor.extract_description(),
                "amenities": await self.extractor.extract_amenities()
            }
            
            # Perform deep scraping if requested
            if deep_scrape:
                try:
                    deep_data = await self.extractor.extract_all_deep_data()
                    # Merge deep data into main data, prioritizing deep data for overlapping keys
                    for key, value in deep_data.items():
                        if value is not None:  # Only update if deep data has a value
                            data[key] = value
                    logger.debug(f"Deep scraping completed for {url}")
                except Exception as e:
                    logger.warning(f"Error during deep scraping for {url}: {e}. Continuing with basic data.")
            
            # Ensure page processing takes between MIN_PAGE_DELAY and MAX_PAGE_DELAY seconds
            elapsed_time = time.time() - page_start_time
            target_delay = random.uniform(Config.MIN_PAGE_DELAY, Config.MAX_PAGE_DELAY)
            if elapsed_time < target_delay:
                remaining_delay = target_delay - elapsed_time
                logger.debug(f"Listing processed in {elapsed_time:.2f}s, adding {remaining_delay:.2f}s delay to reach target")
                await asyncio.sleep(remaining_delay)
            
            logger.info(f"Successfully scraped listing: {data.get('title', 'Unknown')}")
            return data
            
        except Exception as e:
            logger.error(f"Error scraping listing {url}: {e}")
            return {
                "url": url,
                "error": str(e)
            }
    
    async def _initialize_search_page(self, search_url: str) -> None:
        """Navigate to search page - goto already waits, skip redundant wait"""
        wait_strategy = Config.WAIT_UNTIL
        await self.page.goto(search_url, wait_until=wait_strategy, timeout=Config.NAVIGATION_TIMEOUT)
        # Skip redundant wait - just tiny delay for dynamic content
        await asyncio.sleep(0.05)
    
    async def _scroll_to_load_listings(self, max_listings: Optional[int] = None) -> None:
        """Scroll search page to load more listings - optimized for speed"""
        if not Config.SCROLL_ENABLED:
            return
        
        scroll_count = 0
        max_scrolls = 5  # Reduced from 10 to 5 for speed
        last_listing_count = 0
        no_change_count = 0
        
        while scroll_count < max_scrolls:
            # Check current number of listings using data-cy attribute
            current_listings = await self.page.evaluate(f"""
                () => {{
                    const cards = document.querySelectorAll('{SELECTOR_PROPERTY_CARD}');
                    return cards.length;
                }}
            """)
            
            if max_listings and current_listings >= max_listings:
                break
            
            # If no new listings appeared, stop scrolling
            if current_listings == last_listing_count:
                no_change_count += 1
                if no_change_count >= 2:  # Stop after 2 scrolls with no change
                    break
            else:
                no_change_count = 0
            
            last_listing_count = current_listings
            
            # Fast instant scroll - no smooth scrolling
            await self.page.mouse.wheel(0, random.randint(500, 800))
            # Minimal delay - just enough for content to load
            await asyncio.sleep(0.05)
            
            scroll_count += 1
    
    async def _scrape_single_page(self, page_url: str, max_listings: Optional[int] = None) -> List[Dict]:
        """
        Scrapes a single page of search results (basic data only, no deep scraping)
        
        Args:
            page_url: URL of the page to scrape
            max_listings: Maximum number of listings to extract from this page
            
        Returns:
            List[Dict]: List of extracted listing data from this page (basic data only)
        """
        try:
            page_start_time = time.time()
            
            # Add initial delay to avoid rapid-fire requests (Cloudflare detection)
            await asyncio.sleep(random.uniform(2.0, 4.0))
            
            # Navigate to page with networkidle wait for better content loading in Docker
            # Use 'networkidle' instead of 'domcontentloaded' for Docker to ensure JS executes
            wait_strategy = "networkidle" if os.getenv("DOCKER_CONTAINER") == "true" else Config.WAIT_UNTIL
            
            # Use referer to make navigation look more natural
            await self.page.set_extra_http_headers({
                "Referer": "https://www.google.com/" if "page=" not in page_url else "https://www.zapimoveis.com.br/venda/"
            })
            
            await self.page.goto(page_url, wait_until=wait_strategy, timeout=Config.NAVIGATION_TIMEOUT)
            
            # Debug: Check page title and URL to see what loaded
            page_title = await self.page.title()
            current_url = self.page.url
            logger.debug(f"Page loaded - Title: {page_title[:100]}, URL: {current_url}")
            
            # Wait for property cards to appear (important for Docker/headless)
            try:
                await self.page.wait_for_selector('li[data-cy="rp-property-cd"]', timeout=10000)
                logger.debug("Property cards detected on page")
            except Exception as e:
                logger.warning(f"Property cards not found after wait: {e}")
                # Try to check if page loaded at all
                page_content = await self.page.content()
                content_length = len(page_content)
                logger.warning(f"Page content length: {content_length} characters")
                
                # Check for common blocking/error indicators
                blocking_indicators = [
                    "captcha", "blocked", "access denied", "forbidden", 
                    "cloudflare", "bot detection", "verify you are human"
                ]
                content_lower = page_content.lower()
                found_indicators = [ind for ind in blocking_indicators if ind in content_lower]
                if found_indicators:
                    logger.error(f"Possible blocking detected! Found indicators: {found_indicators}")
                
                # Check for alternative selectors that might indicate the page structure
                try:
                    # Check if there's any listing-related content
                    has_listing_text = "imovel" in content_lower or "venda" in content_lower or "aluguel" in content_lower
                    logger.debug(f"Page contains listing-related text: {has_listing_text}")
                    
                    # Try to find any cards or listings with alternative selectors
                    alt_selectors = [
                        'article',
                        '[class*="card"]',
                        '[class*="listing"]',
                        '[class*="property"]',
                        'li',
                        '[data-testid*="card"]'
                    ]
                    for alt_selector in alt_selectors:
                        count = await self.page.evaluate(f"""
                            () => {{
                                const elements = document.querySelectorAll('{alt_selector}');
                                return elements.length;
                            }}
                        """)
                        if count > 0:
                            logger.debug(f"Found {count} elements with selector '{alt_selector}'")
                except Exception as debug_error:
                    logger.debug(f"Error during debug checks: {debug_error}")
            
            # Additional wait for dynamic content
            await asyncio.sleep(1.0)  # Increased from 0.05 to 1.0 for Docker
            
            # Scroll to load more listings (infinite scroll)
            await self._scroll_to_load_listings(max_listings)
            
            # Debug: Check how many cards are visible
            card_count = await self.page.evaluate("""
                () => {
                    const cards = document.querySelectorAll('li[data-cy="rp-property-cd"]');
                    return cards.length;
                }
            """)
            logger.debug(f"Found {card_count} property cards via JavaScript evaluation")
            
            # Extract all listing URLs from the page
            listing_urls = await self.search_extractor.extract_listing_urls_from_search()
            
            if max_listings:
                listing_urls = listing_urls[:max_listings]
            
            logger.info(f"Found {len(listing_urls)} listings on page: {page_url}")
            
            # Extract basic data from search results (without visiting each page)
            listings = []
            for listing_url in listing_urls:
                try:
                    listing_data = await self.search_extractor.extract_listing_from_search_card(listing_url)
                    if listing_data:
                        listings.append(listing_data)
                except Exception as e:
                    logger.debug(f"Error extracting listing from card: {e}")
                    continue
            
            # Ensure page processing takes between MIN_PAGE_DELAY and MAX_PAGE_DELAY seconds
            elapsed_time = time.time() - page_start_time
            target_delay = random.uniform(Config.MIN_PAGE_DELAY, Config.MAX_PAGE_DELAY)
            if elapsed_time < target_delay:
                remaining_delay = target_delay - elapsed_time
                logger.debug(f"Page processed in {elapsed_time:.2f}s, adding {remaining_delay:.2f}s delay to reach target")
                await asyncio.sleep(remaining_delay)
            
            return listings
            
        except Exception as e:
            logger.error(f"Error scraping page {page_url}: {e}")
            return []
    
    async def deep_scrape_listings(
        self, 
        listings: List[Dict],
        save_callback: Optional[Callable[[Dict], Awaitable[None]]] = None
    ) -> List[Dict]:
        """
        Performs deep scraping on a list of listings (visits each listing URL)
        
        Args:
            listings: List of listing dictionaries with basic data (must have 'url' key)
            save_callback: Optional callback function(listing) called after each listing is deep scraped
            
        Returns:
            List[Dict]: List of listings with deep data merged in
        """
        if not listings:
            return listings
        
        logger.info(f"Starting deep scraping on {len(listings)} listings...")
        deep_scraped_listings = []
        
        for i, listing in enumerate(listings, 1):
            listing_url = listing.get("url")
            if not listing_url:
                logger.warning(f"Listing {i} has no URL, skipping deep scrape")
                deep_scraped_listings.append(listing)
                continue
            
            try:
                logger.info(f"Deep scraping listing {i}/{len(listings)}: {listing_url}")
                # Visit the listing page and perform deep scraping
                deep_data = await self.scrape_listing(listing_url, deep_scrape=True)
                # Merge deep data with basic data (deep data takes precedence)
                merged_listing = {**listing, **deep_data}
                deep_scraped_listings.append(merged_listing)
                
                # Save immediately if callback provided
                if save_callback:
                    try:
                        await save_callback(merged_listing)
                    except Exception as e:
                        logger.warning(f"Error saving listing {listing_url}: {e}")
                
                # Delay between deep scrapes (3-5 seconds) to avoid being too fast
                if i < len(listings):  # Don't delay after the last listing
                    delay = random.uniform(3.0, 5.0)
                    logger.debug(f"Waiting {delay:.2f} seconds before next deep scrape...")
                    await asyncio.sleep(delay)
                    
            except Exception as e:
                logger.warning(f"Error deep scraping listing {listing_url}: {e}. Using basic data.")
                deep_scraped_listings.append(listing)
                
                # Still save basic data if callback provided
                if save_callback:
                    try:
                        await save_callback(listing)
                    except Exception as e2:
                        logger.warning(f"Error saving listing {listing_url}: {e2}")
                
                # Still delay even on error to maintain rate
                if i < len(listings):
                    delay = random.uniform(3.0, 5.0)
                    await asyncio.sleep(delay)
        
        logger.info(f"Deep scraping completed for {len(deep_scraped_listings)} listings")
        return deep_scraped_listings
    
    async def _scrape_all_pages(
        self, 
        base_url: str, 
        total_pages: int, 
        max_listings: Optional[int],
        page_callback: Optional[Callable[[int, List[Dict], str], Awaitable[None]]] = None
    ) -> List[Dict]:
        """
        Scrape all pages and collect listings
        
        Args:
            base_url: Base search URL
            total_pages: Total number of pages to scrape
            max_listings: Maximum number of listings to collect
            page_callback: Optional callback function(page_num, page_listings) called after each page is scraped
        """
        all_listings = []
        listings_collected = 0
        
        for page_num in range(1, total_pages + 1):
            # Check if we've reached max_listings
            if max_listings and listings_collected >= max_listings:
                logger.info(f"Reached max_listings limit ({max_listings}), stopping pagination")
                break
            
            page_url = self.pagination.build_page_url(base_url, page_num)
            logger.info(f"Scraping page {page_num}/{total_pages}: {page_url}")
            
            remaining = (max_listings - listings_collected) if max_listings else None
            page_listings = await self._scrape_single_page(page_url, max_listings=remaining)
            
            all_listings.extend(page_listings)
            listings_collected += len(page_listings)
            
            # Call callback to save page data immediately
            if page_callback and page_listings:
                try:
                    await page_callback(page_num, page_listings, base_url)
                except Exception as e:
                    logger.error(f"Error in page callback for page {page_num}: {e}")
            
            # No additional delay between pages - each page already has controlled delay (2-5s)
        
        return all_listings
    
    async def scrape_search_results(
        self, 
        search_url: str, 
        max_listings: Optional[int] = None, 
        max_pages: Optional[int] = None,
        page_callback: Optional[Callable[[int, List[Dict], str], Awaitable[None]]] = None
    ) -> List[Dict]:
        """
        Scrapes multiple listings from search results pages, iterating through all pages
        (Only extracts basic data from search cards, no deep scraping)
        
        Args:
            search_url: URL of the Zap Imóveis search results page
            max_listings: Maximum number of listings to extract total (None for all)
            max_pages: Maximum number of pages to scrape (None for all available pages)
            page_callback: Optional callback function(page_num, page_listings, base_url) called after each page is scraped
            
        Returns:
            List[Dict]: List of extracted listing data from all pages (basic data only)
        """
        try:
            # Navigate to first page to detect total pages
            await self._initialize_search_page(search_url)
            
            # Detect total number of pages
            detected_pages = await self.pagination.get_total_pages()
            logger.info(f"Detected {detected_pages} total pages for search: {search_url}")
            
            # Limit pages if max_pages is specified
            if max_pages:
                total_pages = min(detected_pages, max_pages)
                if detected_pages > max_pages:
                    logger.info(f"Limiting pagination to {max_pages} pages (detected {detected_pages} pages)")
            else:
                total_pages = detected_pages
            
            # Check if URL already has a page parameter
            page_num = self.pagination.extract_page_from_url(search_url)
            if page_num:
                logger.info(f"Scraping specific page {page_num}")
                page_listings = await self._scrape_single_page(search_url, max_listings=max_listings)
                # Call callback for single page
                if page_callback and page_listings:
                    try:
                        base_url = self.pagination.build_base_url(search_url)
                        await page_callback(page_num, page_listings, base_url)
                    except Exception as e:
                        logger.error(f"Error in page callback for page {page_num}: {e}")
                return page_listings
            
            # Build base URL without page parameter
            base_url = self.pagination.build_base_url(search_url)
            
            # Scrape all pages (basic data only)
            all_listings = await self._scrape_all_pages(base_url, total_pages, max_listings, page_callback)
            
            logger.info(f"Total listings collected from {total_pages} pages: {len(all_listings)}")
            return all_listings
            
        except Exception as e:
            logger.error(f"Error scraping search results {search_url}: {e}")
            return []
    
    async def wait_for_page_load(self) -> None:
        """Waits for the page to fully load with human-like behavior"""
        if Config.HUMAN_BEHAVIOR_ENABLED:
            await self.human_behavior.wait_for_page_with_behavior(self.page)
        else:
            await self.page.wait_for_load_state("networkidle")
