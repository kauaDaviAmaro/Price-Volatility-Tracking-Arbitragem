"""
Data Pipeline - Manages URL list and Concurrency
Orchestrates the scraping data flow
"""
import asyncio
import csv
import json
import logging
import re
import time
from typing import List, Dict, Optional
from pathlib import Path
import aiohttp
import aiofiles

from src.scraper_app.core.browser_manager import BrowserManager
from src.scraper_app.core.compliance_manager import ComplianceManager
from src.scraper_app.core.human_behavior import HumanBehavior
from src.scraper_app.core.proxy_manager import ProxyManager
from src.scraper_app.services.zap_imoveis_service import ZapImoveisService
from src.scraper_app.config import Config

logger = logging.getLogger(__name__)


class DataPipeline:
    """Pipeline that manages the URL list and scraping concurrency with elite features"""
    
    def __init__(
        self,
        urls: List[str],
        output_dir: Optional[str] = None,
        max_concurrent: Optional[int] = None,
        proxy_manager: Optional[ProxyManager] = None,
        compliance_manager: Optional[ComplianceManager] = None,
        human_behavior: Optional[HumanBehavior] = None
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
        """
        self.urls = urls
        self.output_dir = Path(output_dir or Config.OUTPUT_DIR)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.max_concurrent = max_concurrent or Config.MAX_CONCURRENT
        self.results: List[Dict] = []
        
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
        logger.info(f"Starting pipeline: {len(self.urls)} URLs, max_concurrent={self.max_concurrent}")
        self.stats["total"] = len(self.urls)
        
        # Create semaphore for concurrency control
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        # Process URLs with concurrency control
        tasks = []
        for url in self.urls:
            task = self._process_url_with_semaphore(semaphore, url)
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions and None results
        self.results = [r for r in results if r is not None and not isinstance(r, Exception)]
        
        logger.info(f"Pipeline completed: {self.stats['success']} success, {self.stats['failed']} failed, {self.stats['blocked']} blocked, {self.stats['skipped']} skipped")
        
        return self.results
    
    async def _process_url_with_semaphore(self, semaphore: asyncio.Semaphore, url: str) -> Optional[Dict]:
        """Process URL with semaphore for concurrency control"""
        async with semaphore:
            return await self.process_single_url(url)
    
    async def _check_compliance(self, url: str) -> bool:
        """
        Check if URL can be scraped (compliance checks)
        
        Args:
            url: URL to check
            
        Returns:
            True if compliant, False otherwise
        """
        if not self.compliance_manager.is_public_data(url):
            logger.warning(f"Skipping potentially private data: {url}")
            self.stats["skipped"] += 1
            return False
        
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        if not await self.compliance_manager.can_fetch(url, user_agent):
            logger.warning(f"robots.txt disallows: {url}")
            self.stats["skipped"] += 1
            return False
        
        await self.compliance_manager.wait_for_rate_limit(url)
        return True
    
    def _is_search_url(self, url: str) -> bool:
        """Check if URL is a search results page"""
        search_indicators = ['/venda/']
        return any(indicator in url for indicator in search_indicators)
    
    async def _create_page_callback(self, url: str):
        """
        Create a callback function to save page data incrementally
        
        Args:
            url: Search URL being scraped
            
        Returns:
            Async callback function(page_num, page_listings, base_url)
        """
        async def page_callback(page_num: int, page_listings: List[Dict], base_url: str) -> None:
            """Callback to save page data immediately after scraping"""
            await self.save_page_to_csv(page_num, page_listings, base_url)
        
        return page_callback
    
    async def _attempt_scrape(self, url: str, browser_manager: BrowserManager) -> Optional[Dict]:
        """
        Attempt to scrape a URL (single listing or search results)
        
        Args:
            url: URL to scrape
            browser_manager: BrowserManager instance
            
        Returns:
            Scraping result or None (for search, returns list of listings)
        """
        await browser_manager.initialize()
        page = await browser_manager.create_page()
        service = ZapImoveisService(page, self.human_behavior)
        
        if self._is_search_url(url):
            max_pages = Config.MAX_PAGES
            page_callback = await self._create_page_callback(url)
            listings = await service.scrape_search_results(
                url, 
                max_pages=max_pages,
                page_callback=page_callback
            )
            
            if listings:
                logger.info(f"Search completed. Starting deep scraping on {len(listings)} listings...")
                
                async def save_deep_scraped_listing(listing: Dict) -> None:
                    if Config.SAVE_IMAGES:
                        images = listing.get("images")
                        if images:
                            logger.info(f"Found {len(images) if isinstance(images, list) else 'unknown'} images for listing {listing.get('url', 'unknown')}")
                            await self._download_listing_images(listing)
                        else:
                            logger.debug(f"No images found for listing {listing.get('url', 'unknown')}")
                    
                    await self.save_single_listing_to_csv(listing, url)
                
                listings = await service.deep_scrape_listings(listings, save_callback=save_deep_scraped_listing)
                logger.info(f"Deep scraping completed for all {len(listings)} listings")
            
            await browser_manager.mark_proxy_success()
            return {"url": url, "listings": listings, "type": "search_results"}
        else:
            result = await service.scrape_listing(url, deep_scrape=True)
            await browser_manager.mark_proxy_success()
            return result
    
    def _is_blocked_error(self, result: Dict) -> bool:
        """Check if result indicates a blocking error"""
        error_str = str(result.get("error", ""))
        return "403" in error_str or "429" in error_str
    
    async def _handle_blocked_error(
        self, 
        url: str, 
        result: Dict, 
        browser_manager: BrowserManager
    ) -> None:
        """Handle blocked error by rotating proxy and fingerprint"""
        self.stats["blocked"] += 1
        logger.warning(f"Blocked on {url}: {result.get('error')}")
        await browser_manager.mark_proxy_failure()
        await browser_manager.rotate_fingerprint()
    
    def _handle_successful_result(self, result: Dict, url: str) -> Dict:
        """Handle successful scraping result and update stats"""
        if result.get("type") == "search_results" and "listings" in result:
            listings = result["listings"]
            self.stats["success"] += len(listings)
            logger.info(f"Successfully scraped {len(listings)} listings from search: {url}")
        else:
            self.stats["success"] += 1
            logger.info(f"Successfully scraped: {url}")
        return result
    
    def _calculate_retry_delay(self, attempt: int) -> float:
        """Calculate retry delay with exponential backoff"""
        return Config.RETRY_DELAY * (Config.RETRY_BACKOFF ** attempt)
    
    async def _process_scrape_attempt(
        self, 
        url: str, 
        browser_manager: BrowserManager, 
        attempt: int, 
        max_retries: int
    ) -> Optional[Dict]:
        """Process a single scrape attempt with error handling"""
        try:
            result = await self._attempt_scrape(url, browser_manager)
            
            if result and "error" not in result:
                return self._handle_successful_result(result, url)
            
            if result and self._is_blocked_error(result):
                await self._handle_blocked_error(url, result, browser_manager)
            
            if attempt < max_retries:
                await asyncio.sleep(self._calculate_retry_delay(attempt))
            return None
            
        except Exception as e:
            logger.error(f"Error processing {url} (attempt {attempt + 1}/{max_retries + 1}): {e}")
            if attempt >= max_retries:
                self.stats["failed"] += 1
                return {"url": url, "error": str(e)}
            await asyncio.sleep(self._calculate_retry_delay(attempt))
            return None
    
    async def process_single_url(self, url: str) -> Optional[Dict]:
        """
        Processes a single URL with retry logic and compliance checking
        
        Args:
            url: URL to be processed
            
        Returns:
            Optional[Dict]: Extracted data or None in case of error
        """
        if not await self._check_compliance(url):
            return None
        
        max_retries = Config.MAX_RETRIES
        
        for attempt in range(max_retries + 1):
            browser_manager = BrowserManager(
                headless=Config.HEADLESS,
                proxy_manager=self.proxy_manager
            )
            
            try:
                result = await self._process_scrape_attempt(url, browser_manager, attempt, max_retries)
                if result is not None:
                    return result
            finally:
                await browser_manager.close()
        
        self.stats["failed"] += 1
        return None
    
    def _flatten_search_results(self) -> List[Dict]:
        """Flatten search results by expanding listings from search pages"""
        flattened_results = []
        for result in self.results:
            if result.get("type") == "search_results" and "listings" in result:
                flattened_results.extend(result.get("listings", []))
            else:
                flattened_results.append(result)
        return flattened_results
    
    def _get_csv_fieldnames(self, flattened_results: List[Dict]) -> List[str]:
        """Extract and sort all unique field names from results, filtering invalid ones"""
        fieldnames = set()
        for result in flattened_results:
            # Only add valid fieldnames
            valid_keys = [k for k in result.keys() if self._is_valid_fieldname(k)]
            fieldnames.update(valid_keys)
        return sorted(fieldnames)
    
    def _is_valid_fieldname(self, fieldname: str) -> bool:
        """Check if a fieldname is valid (not a generic column_* name)"""
        if not fieldname or not isinstance(fieldname, str):
            return False
        # Filter out generic column names like "column_42", "column_43", etc.
        if fieldname.startswith('column_') and fieldname[7:].isdigit():
            return False
        return True
    
    def _filter_valid_fieldnames(self, fieldnames: List[str]) -> List[str]:
        """Filter out invalid fieldnames"""
        return [f for f in fieldnames if self._is_valid_fieldname(f)]
    
    def _convert_result_to_row(self, result: Dict) -> Dict:
        """Convert a result dictionary to a CSV-compatible row"""
        row = {}
        for key, value in result.items():
            # Skip invalid fieldnames
            if not self._is_valid_fieldname(key):
                continue
            # Handle nested dictionaries by converting to string
            if isinstance(value, dict):
                # Convert dict to JSON-like string representation
                try:
                    row[key] = json.dumps(value, ensure_ascii=False)
                except (TypeError, ValueError):
                    row[key] = str(value)
            elif isinstance(value, list):
                row[key] = ', '.join(str(v) for v in value)
            else:
                row[key] = value
        return row
    
    def _ensure_csv_headers(self, filepath: Path, fieldnames: List[str]) -> None:
        """Ensure CSV file exists with headers if it doesn't exist"""
        if not filepath.exists():
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
    
    def _get_all_fieldnames(self, listings: List[Dict]) -> List[str]:
        """Extract all unique field names from listings, filtering invalid ones"""
        fieldnames = set()
        for listing in listings:
            # Only add valid fieldnames
            valid_keys = [k for k in listing.keys() if self._is_valid_fieldname(k)]
            fieldnames.update(valid_keys)
        return sorted(fieldnames)
    
    async def save_page_to_csv(
        self, 
        page_num: int, 
        page_listings: List[Dict], 
        base_url: str,
        filename: str = "scraped_data.csv"
    ) -> None:
        """
        Saves a page of listings to CSV file incrementally (append mode)
        
        Args:
            page_num: Page number that was scraped
            page_listings: List of listings from this page
            base_url: Base URL of the search
            filename: Output CSV filename
        """
        if not page_listings:
            logger.debug(f"No listings to save for page {page_num}")
            return
        
        filepath = self.output_dir / filename
        
        # Get all fieldnames from all results seen so far
        all_fieldnames = self._get_all_fieldnames(page_listings)
        
        # If file exists, read existing fieldnames and merge (filtering invalid ones)
        if filepath.exists():
            try:
                with open(filepath, 'r', newline='', encoding='utf-8') as f:
                    # Only read the header row
                    reader = csv.DictReader(f)
                    existing_fieldnames = reader.fieldnames or []
                    # Filter out invalid fieldnames from existing CSV
                    existing_fieldnames = self._filter_valid_fieldnames(existing_fieldnames)
                    all_fieldnames = sorted(set(all_fieldnames) | set(existing_fieldnames))
            except Exception as e:
                logger.warning(f"Error reading existing CSV headers: {e}. Using new fieldnames only.")
        
        # Ensure file exists with headers
        self._ensure_csv_headers(filepath, all_fieldnames)
        
        # Append page listings to CSV
        with open(filepath, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=all_fieldnames)
            
            for listing in page_listings:
                row = self._convert_result_to_row(listing)
                # Ensure all fieldnames are present in row
                for fieldname in all_fieldnames:
                    if fieldname not in row:
                        row[fieldname] = None
                writer.writerow(row)
        
        logger.info(f"Saved {len(page_listings)} listings from page {page_num} to {filepath}")
    
    async def save_deep_scraped_data_to_csv(
        self,
        listings: List[Dict],
        base_url: str,
        filename: str = "scraped_data.csv"
    ) -> None:
        """
        Saves deep scraped listings to CSV, updating existing rows or appending new ones
        
        Args:
            listings: List of listings with deep scraped data
            base_url: Base URL of the search
            filename: Output CSV filename
        """
        if not listings:
            logger.debug("No listings to save with deep data")
            return
        
        filepath = self.output_dir / filename
        
        # Get all fieldnames from deep scraped listings
        all_fieldnames = self._get_all_fieldnames(listings)
        
        # If file exists, read existing data and update
        existing_data = {}
        if filepath.exists():
            try:
                with open(filepath, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    existing_fieldnames = reader.fieldnames or []
                    # Filter out invalid fieldnames from existing CSV
                    existing_fieldnames = self._filter_valid_fieldnames(existing_fieldnames)
                    all_fieldnames = sorted(set(all_fieldnames) | set(existing_fieldnames))
                    
                    # Read existing rows by URL
                    for row in reader:
                        url = row.get('url')
                        if url:
                            existing_data[url] = row
            except Exception as e:
                logger.warning(f"Error reading existing CSV: {e}. Will overwrite.")
                existing_data = {}
        
        # Update existing data with deep scraped data or add new
        for listing in listings:
            url = listing.get('url')
            if url:
                # Merge existing data with new deep scraped data
                if url in existing_data:
                    existing_data[url].update(listing)
                else:
                    existing_data[url] = listing
        
        # Write all data back to CSV
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=all_fieldnames)
            writer.writeheader()
            
            for listing_data in existing_data.values():
                row = self._convert_result_to_row(listing_data)
                # Ensure all fieldnames are present in row
                for fieldname in all_fieldnames:
                    if fieldname not in row:
                        row[fieldname] = None
                writer.writerow(row)
        
        logger.info(f"Saved {len(existing_data)} listings with deep data to {filepath}")
    
    async def save_single_listing_to_csv(
        self,
        listing: Dict,
        base_url: str,
        filename: str = "scraped_data.csv"
    ) -> None:
        if not listing:
            return
        
        filepath = self.output_dir / filename
        
        # Only include valid fieldnames from listing
        listing_fieldnames = set([k for k in listing.keys() if self._is_valid_fieldname(k)])
        
        existing_data = {}
        existing_fieldnames = []
        if filepath.exists() and filepath.stat().st_size > 0:
            try:
                with open(filepath, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    existing_fieldnames = reader.fieldnames or []
                    # Filter out invalid fieldnames from existing CSV
                    existing_fieldnames = self._filter_valid_fieldnames(existing_fieldnames)
                    listing_fieldnames.update(existing_fieldnames)
                    
                    for row in reader:
                        url = row.get('url')
                        if url:
                            existing_data[url] = row
            except Exception as e:
                logger.warning(f"Error reading existing CSV: {e}. Will create new file.")
                existing_data = {}
                existing_fieldnames = []
        
        listing_url = listing.get('url')
        if listing_url:
            if listing_url in existing_data:
                existing_row = existing_data[listing_url].copy()
                for key, value in listing.items():
                    existing_value = existing_row.get(key)
                    
                    def is_empty(val):
                        if val is None:
                            return True
                        if isinstance(val, str):
                            return val.strip() == ''
                        if isinstance(val, list):
                            return len(val) == 0
                        return False
                    
                    if not is_empty(value):
                        existing_row[key] = value
                    elif not is_empty(existing_value):
                        continue
                
                existing_data[listing_url] = existing_row
            else:
                existing_data[listing_url] = listing
        else:
            existing_data['listing_' + str(len(existing_data))] = listing
        
        all_fieldnames = sorted(listing_fieldnames)
        
        if not all_fieldnames and listing:
            # Only include valid fieldnames
            all_fieldnames = sorted([k for k in listing.keys() if self._is_valid_fieldname(k)])
        
        if listing_url and listing_url not in existing_data:
            existing_data[listing_url] = listing
        
        if not all_fieldnames:
            logger.warning(f"No fieldnames available for CSV, skipping write")
            return
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=all_fieldnames)
            writer.writeheader()
            
            for listing_data in existing_data.values():
                row = self._convert_result_to_row(listing_data)
                for fieldname in all_fieldnames:
                    if fieldname not in row:
                        row[fieldname] = None
                writer.writerow(row)
        
        logger.debug(f"Saved listing {listing_url} to CSV with {len(all_fieldnames)} columns")
    
    def save_to_csv(self, filename: str = "scraped_data.csv") -> None:
        """
        Saves results to a CSV file
        
        Args:
            filename: Output CSV filename
        """
        if not self.results:
            logger.warning("No results to save")
            return
        
        filepath = self.output_dir / filename
        flattened_results = self._flatten_search_results()
        
        if not flattened_results:
            logger.warning("No results to save after flattening")
            return
        
        fieldnames = self._get_csv_fieldnames(flattened_results)
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for result in flattened_results:
                row = self._convert_result_to_row(result)
                writer.writerow(row)
        
        logger.info(f"Saved {len(flattened_results)} listings to {filepath}")
    
    def _get_image_extension(self, image_url: str) -> str:
        """Extract and validate image file extension from URL"""
        if '.' not in image_url:
            return 'jpg'
        
        ext = image_url.split('.')[-1].split('?')[0]
        valid_extensions = ['jpg', 'jpeg', 'png', 'gif', 'webp']
        return ext if ext in valid_extensions else 'jpg'
    
    async def _save_image(self, response: aiohttp.ClientResponse, filepath: Path) -> None:
        """Save image content to file"""
        async with aiofiles.open(filepath, 'wb') as f:
            async for chunk in response.content.iter_chunked(8192):
                await f.write(chunk)
    
    async def _download_single_image(
        self, 
        session: aiohttp.ClientSession, 
        image_url: str, 
        index: int, 
        output_path: Path
    ) -> None:
        """Download a single image"""
        try:
            async with session.get(image_url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status != 200:
                    return
                
                ext = self._get_image_extension(image_url)
                filename = f"image_{index+1}.{ext}"
                filepath = output_path / filename
                
                await self._save_image(response, filepath)
                logger.debug(f"Downloaded image: {filename}")
                
                if Config.HUMAN_BEHAVIOR_ENABLED:
                    await self.human_behavior.random_delay(0.5, 1.0)
        except Exception as e:
            logger.debug(f"Error downloading image {image_url}: {e}")
    
    def _get_listing_id_from_url(self, url: str) -> str:
        """
        Extract a safe listing ID from URL for folder naming
        
        Args:
            url: Listing URL
            
        Returns:
            Safe folder name for the listing
        """
        if not url:
            return "unknown"
        
        # Try to extract ID from URL
        id_match = re.search(r'id-(\d+)', url)
        if id_match:
            return f"listing_{id_match.group(1)}"
        
        # Fallback: use last part of URL
        url_parts = url.rstrip('/').split('/')
        if url_parts:
            last_part = url_parts[-1].split('?')[0]  # Remove query params
            # Sanitize for filesystem
            safe_name = re.sub(r'[^\w\-_\.]', '_', last_part)
            return safe_name[:50]  # Limit length
        
        return "unknown"
    
    async def _download_listing_images(self, listing: Dict) -> None:
        """
        Downloads images for a single listing during deep scraping
        
        Args:
            listing: Listing dictionary with images URLs
        """
        if not Config.SAVE_IMAGES:
            logger.debug("SAVE_IMAGES is disabled, skipping image download")
            return
        
        images = listing.get("images", [])
        logger.debug(f"Attempting to download images. Type: {type(images)}, Value: {images}")
        if not images:
            logger.debug("No images found in listing")
            return
        
        # Handle case where images might be a string (from CSV)
        if isinstance(images, str):
            # Split by comma and strip whitespace
            images = [img.strip() for img in images.split(',') if img.strip()]
        
        if not isinstance(images, list) or len(images) == 0:
            return
        
        listing_url = listing.get("url", "")
        listing_id = self._get_listing_id_from_url(listing_url)
        image_dir = self.output_dir / "images" / listing_id
        image_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Downloading images to: {image_dir}")
        
        # Download images
        downloaded_paths = []
        async with aiohttp.ClientSession() as session:
            for i, image_url in enumerate(images[:20]):  # Limit to 20 images
                try:
                    # Get file extension from URL
                    ext = self._get_image_extension(image_url)
                    filename = f"image_{i+1:03d}.{ext}"
                    filepath = image_dir / filename
                    
                    # Download image
                    async with session.get(image_url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                        if response.status == 200:
                            async with aiofiles.open(filepath, 'wb') as f:
                                async for chunk in response.content.iter_chunked(8192):
                                    await f.write(chunk)
                            
                            # Store relative path
                            relative_path = f"images/{listing_id}/{filename}"
                            downloaded_paths.append(relative_path)
                            logger.debug(f"Downloaded image {i+1}/{len(images[:20])}: {filename}")
                            
                            # Small delay between downloads
                            if Config.HUMAN_BEHAVIOR_ENABLED:
                                await asyncio.sleep(0.2)
                        else:
                            logger.debug(f"Failed to download image {image_url}: HTTP {response.status}")
                except Exception as e:
                    logger.debug(f"Error downloading image {image_url}: {e}")
                    continue
        
        # Update listing with local image paths
        if downloaded_paths:
            listing['images_local'] = downloaded_paths
            listing['images_local_count'] = len(downloaded_paths)
            logger.info(f"Downloaded {len(downloaded_paths)} images for listing {listing_id}")
    
    async def download_images(self, listing_data: Dict, output_path: Path) -> None:
        """
        Downloads images from a listing
        
        Args:
            listing_data: Listing data containing image URLs
            output_path: Path where to save the images
        """
        if not Config.SAVE_IMAGES:
            return
        
        images = listing_data.get("images", [])
        if not images:
            return
        
        output_path.mkdir(parents=True, exist_ok=True)
        
        async with aiohttp.ClientSession() as session:
            for i, image_url in enumerate(images[:10]):  # Limit to 10 images
                await self._download_single_image(session, image_url, i, output_path)
    
    async def run(self) -> None:
        """
        Runs the complete pipeline: scraping and saving
        Note: Images are downloaded during deep scraping, not here
        """
        # Process URLs (includes deep scraping which downloads images)
        results = await self.process_urls()
        
        # Save to CSV
        self.save_to_csv()
        
        # Images are already downloaded during deep scraping in process_urls()
        # No need to download them again here
        
        logger.info("Pipeline run completed")

