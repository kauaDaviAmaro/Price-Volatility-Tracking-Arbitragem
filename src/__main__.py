"""
Main entry point for the Elite Web Scraping application
"""
import argparse
import asyncio
import csv
import logging
import sys
from pathlib import Path
from typing import List, Dict

from src.config import Config
from src.core.proxy_manager import ProxyManager, ProxyType
from src.pipelines.data_pipeline import DataPipeline

# Configure logging - prevent duplicates
root_logger = logging.getLogger()

# Remove all existing handlers to prevent duplicates
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)
    try:
        handler.close()
    except Exception:
        pass

# Create a single handler
if Config.LOG_FILE:
    handler = logging.FileHandler(Config.LOG_FILE)
else:
    handler = logging.StreamHandler(sys.stdout)

# Configure formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Configure logging with force=True to replace any existing configuration
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[handler],
    force=True  # Python 3.8+: force reconfiguration
)

logger = logging.getLogger(__name__)

# Default search URL
DEFAULT_SEARCH_URL = "https://www.zapimoveis.com.br/venda/"


def is_listing_url(url: str) -> bool:
    """
    Validates if a URL is a listing individual page (not a search page)
    
    Args:
        url: URL to validate
        
    Returns:
        True if URL is a listing individual page, False otherwise
    """
    if not url or not isinstance(url, str):
        return False
    
    url = url.strip().lower()
    
    # Must be from zapimoveis.com.br
    if 'zapimoveis.com.br' not in url:
        return False
    
    # Must contain /imovel/ to be a listing page
    if '/imovel/' not in url:
        return False
    
    # Must NOT be a search page (venda/, aluguel/, etc.)
    search_indicators = ['/venda/', '/aluguel/', '/busca', '/pesquisa']
    for indicator in search_indicators:
        if indicator in url:
            return False
    
    return True


def discover_search_urls() -> List[str]:
    """
    Returns default Zap Imóveis search URLs
    The system automatically extracts ALL listings from all pages
    """
    logger.info("Using default Zap Imóveis search URLs...")
    
    base_search_urls = [
        DEFAULT_SEARCH_URL,
    ]
    
    return base_search_urls


def _count_filled_indicators(row: Dict[str, str], indicators: List[str]) -> int:
    """
    Counts how many deep search indicators are filled in a row.
    
    Args:
        row: CSV row dictionary
        indicators: List of indicator field names
        
    Returns:
        Number of filled indicators
    """
    filled_count = 0
    for indicator in indicators:
        value = row.get(indicator, '').strip()
        if value and value.lower() not in ['', 'none', 'null', 'false']:
            filled_count += 1
    return filled_count


def _needs_deep_search(row: Dict[str, str], indicators: List[str]) -> bool:
    """
    Checks if a row needs deep search based on filled indicators.
    
    Args:
        row: CSV row dictionary
        indicators: List of deep search indicator field names
        
    Returns:
        True if row needs deep search (less than 2 indicators filled)
    """
    filled_count = _count_filled_indicators(row, indicators)
    return filled_count < 2


def get_missing_deep_search_urls(csv_path: Path) -> List[str]:
    """
    Reads the CSV file and returns URLs of individual listings that are missing deep search data.
    Only returns URLs that are valid listing pages (not search pages).
    
    Args:
        csv_path: Path to the scraped_data.csv file
        
    Returns:
        List of valid listing URLs that need deep search
    """
    if not csv_path.exists():
        logger.warning(f"CSV file not found: {csv_path}")
        return []
    
    missing_urls = []
    invalid_urls = []
    
    # Fields that indicate deep search was completed
    deep_search_indicators = [
        'full_address',
        'full_description',
        'advertiser_name',
        'advertiser_code',
        'zap_code',
        'phone_partial',
        'has_whatsapp',
        'iptu',
        'condo_fee',
        'suites',
        'floor_level'
    ]
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            total_rows = 0
            
            for row in reader:
                total_rows += 1
                url = row.get('url', '').strip()
                
                if not url:
                    continue
                
                if not is_listing_url(url):
                    invalid_urls.append(url)
                    logger.debug(f"Skipping invalid URL (not a listing page): {url}")
                    continue
                
                filled_count = _count_filled_indicators(row, deep_search_indicators)
                if filled_count < 2:
                    missing_urls.append(url)
                    logger.debug(f"URL needs deep search: {url} (filled indicators: {filled_count})")
        
        logger.info(f"Read {total_rows} rows from CSV")
        logger.info(f"Found {len(missing_urls)} valid listing URLs that need deep search")
        if invalid_urls:
            logger.warning(f"Skipped {len(invalid_urls)} invalid URLs (search pages or invalid format)")
        
        return missing_urls
        
    except Exception as e:
        logger.error(f"Error reading CSV file {csv_path}: {e}", exc_info=True)
        return []


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Elite Web Scraping Application",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--deep-search-only",
        "--deep-only",
        action="store_true",
        dest="deep_search_only",
        help="Execute only deep search on listing URLs (skip search page scraping)"
    )
    return parser.parse_args()


async def main():
    """Main entry point"""
    args = parse_arguments()
    
    logger.info("Starting Web Scraping Application")
    logger.info(f"Configuration: {Config.to_dict()}")
    
    if args.deep_search_only:
        logger.info("=" * 80)
        logger.info("MODE: Deep Search Only")
        logger.info("=" * 80)
        
        # Read URLs from CSV that need deep search
        csv_path = Path(Config.OUTPUT_DIR) / "scraped_data.csv"
        urls = get_missing_deep_search_urls(csv_path)
        
        if not urls:
            logger.error("No valid listing URLs found in CSV that need deep search.")
            logger.error("Deep search only mode requires listing URLs from scraped_data.csv")
            logger.error("Please run normal mode first to populate the CSV with listing URLs.")
            sys.exit(1)
        
        logger.info(f"✓ Found {len(urls)} listing URLs that need deep search")
        logger.info("Starting deep search processing...")
    else:
        urls = discover_search_urls()
        if not urls:
            logger.warning("No search URLs discovered. Using default URLs.")
            urls = [
                DEFAULT_SEARCH_URL,
            ]
        logger.info(f"Automatically searching in {len(urls)} search URL(s)")
        logger.info("The system will automatically extract ALL listings from all pages")
    
    # Initialize proxy manager if enabled
    proxy_manager = None
    if Config.PROXY_ENABLED:
        proxy_manager = ProxyManager(
            rotation_strategy=Config.PROXY_ROTATION_STRATEGY,
            max_failures=Config.PROXY_MAX_FAILURES,
            cooldown_seconds=Config.PROXY_COOLDOWN_SECONDS
        )
        
        # Load proxies from config
        proxies = Config.get_all_proxies()
        if proxies:
            proxy_manager.load_proxies_from_config(proxies)
            logger.info(f"Loaded {len(proxies)} proxies")
        else:
            logger.warning("PROXY_ENABLED is True but no proxies configured")
            logger.info("Continuing without proxy rotation")
    else:
        logger.info("Running without proxies (direct connection)")
    
    # Create pipeline
    pipeline = DataPipeline(
        urls=urls,
        proxy_manager=proxy_manager,
        deep_search_only=args.deep_search_only
    )
    
    # Run pipeline
    try:
        await pipeline.run()
        logger.info("Pipeline completed successfully")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

