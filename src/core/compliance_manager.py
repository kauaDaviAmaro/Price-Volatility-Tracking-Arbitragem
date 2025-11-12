"""
Compliance Manager - Legal and Technical Compliance System
Manages robots.txt checking, ToS verification, and rate limiting
"""
import asyncio
import time
from typing import Dict, Optional, Set
from urllib.parse import urlparse, urljoin
from urllib.robotparser import RobotFileParser
import aiohttp
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class ComplianceManager:
    """Manages compliance with robots.txt and rate limiting"""
    
    def __init__(self, cache_dir: Optional[str] = None, respect_robots: bool = True):
        """
        Initialize ComplianceManager
        
        Args:
            cache_dir: Directory to cache robots.txt files
            respect_robots: Whether to respect robots.txt rules
        """
        self.respect_robots = respect_robots
        self.cache_dir = Path(cache_dir) if cache_dir else Path(".cache/robots")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        self.robots_cache: Dict[str, RobotFileParser] = {}
        self.robots_cache_time: Dict[str, float] = {}
        self.cache_ttl = 3600  # 1 hour cache TTL
        
        # Rate limiting per domain
        self.rate_limits: Dict[str, Dict] = {}  # domain -> {delay: float, last_request: float}
        self.request_times: Dict[str, list] = {}  # domain -> list of request timestamps
        
    async def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        """
        Check if URL can be fetched according to robots.txt
        
        Args:
            url: URL to check
            user_agent: User agent string (default: "*")
            
        Returns:
            True if allowed, False if disallowed
        """
        if not self.respect_robots:
            return True
        
        try:
            parsed = urlparse(url)
            domain = f"{parsed.scheme}://{parsed.netloc}"
            
            # Get or load robots.txt
            robots = await self._get_robots_parser(domain, user_agent)
            
            if robots is None:
                # If robots.txt doesn't exist or can't be loaded, allow by default
                logger.debug(f"No robots.txt found for {domain}, allowing access")
                return True
            
            # Check if URL is allowed
            allowed = robots.can_fetch(user_agent, url)
            
            if not allowed:
                logger.warning(f"robots.txt disallows {user_agent} from accessing {url}")
            
            return allowed
            
        except Exception as e:
            logger.error(f"Error checking robots.txt for {url}: {e}")
            # On error, allow by default (fail open)
            return True
    
    async def _get_robots_parser(self, domain: str, user_agent: str) -> Optional[RobotFileParser]:
        """
        Get RobotFileParser for a domain (with caching)
        
        Args:
            domain: Domain to get robots.txt for
            user_agent: User agent string
            
        Returns:
            RobotFileParser or None if not available
        """
        cache_key = f"{domain}_{user_agent}"
        current_time = time.time()
        
        # Check cache
        if cache_key in self.robots_cache:
            cache_time = self.robots_cache_time.get(cache_key, 0)
            if current_time - cache_time < self.cache_ttl:
                return self.robots_cache[cache_key]
        
        # Load robots.txt
        robots_url = urljoin(domain, "/robots.txt")
        robots = RobotFileParser()
        robots.set_url(robots_url)
        
        try:
            # Try to read from cache file first
            cache_file = self.cache_dir / f"{domain.replace('://', '_').replace('/', '_')}_robots.txt"
            
            if cache_file.exists():
                cache_time = cache_file.stat().st_mtime
                if current_time - cache_time < self.cache_ttl:
                    robots.set_url(f"file://{cache_file.absolute()}")
                    robots.read()
                    self.robots_cache[cache_key] = robots
                    self.robots_cache_time[cache_key] = cache_time
                    logger.debug(f"Loaded robots.txt from cache for {domain}")
                    return robots
            
            # Fetch robots.txt
            async with aiohttp.ClientSession() as session:
                async with session.get(robots_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        content = await response.text()
                        
                        # Save to cache file
                        cache_file.write_text(content)
                        
                        # Parse
                        robots.read()
                        self.robots_cache[cache_key] = robots
                        self.robots_cache_time[cache_key] = current_time
                        logger.debug(f"Fetched and cached robots.txt for {domain}")
                        return robots
                    else:
                        logger.debug(f"robots.txt not found for {domain} (status: {response.status})")
                        return None
                        
        except asyncio.TimeoutError:
            logger.warning(f"Timeout fetching robots.txt for {domain}")
            return None
        except Exception as e:
            logger.error(f"Error fetching robots.txt for {domain}: {e}")
            return None
    
    async def get_crawl_delay(self, url: str, user_agent: str = "*") -> float:
        """
        Get crawl delay for URL from robots.txt
        
        Args:
            url: URL to check
            user_agent: User agent string
            
        Returns:
            Crawl delay in seconds (0 if not specified)
        """
        if not self.respect_robots:
            return 0.0
        
        try:
            parsed = urlparse(url)
            domain = f"{parsed.scheme}://{parsed.netloc}"
            
            robots = await self._get_robots_parser(domain, user_agent)
            if robots is None:
                return 0.0
            
            # Get crawl delay for user agent
            delay = robots.crawl_delay(user_agent)
            return delay if delay else 0.0
            
        except Exception as e:
            logger.error(f"Error getting crawl delay for {url}: {e}")
            return 0.0
    
    async def wait_for_rate_limit(self, url: str, min_delay: float = 1.0) -> None:
        """
        Wait if necessary to respect rate limits
        
        Args:
            url: URL being accessed
            min_delay: Minimum delay between requests (seconds)
        """
        parsed = urlparse(url)
        domain = parsed.netloc
        
        # Get crawl delay from robots.txt
        crawl_delay = await self.get_crawl_delay(url)
        delay = max(min_delay, crawl_delay)
        
        # Check if we need to wait
        current_time = time.time()
        
        if domain in self.rate_limits:
            last_request = self.rate_limits[domain].get("last_request", 0)
            required_delay = self.rate_limits[domain].get("delay", delay)
            
            time_since_last = current_time - last_request
            if time_since_last < required_delay:
                wait_time = required_delay - time_since_last
                logger.debug(f"Rate limiting: waiting {wait_time:.2f}s for {domain}")
                await asyncio.sleep(wait_time)
        
        # Update rate limit tracking
        if domain not in self.rate_limits:
            self.rate_limits[domain] = {}
        
        self.rate_limits[domain]["last_request"] = time.time()
        self.rate_limits[domain]["delay"] = delay
        
        # Track request times for analysis
        if domain not in self.request_times:
            self.request_times[domain] = []
        self.request_times[domain].append(time.time())
        
        # Keep only last 100 requests
        if len(self.request_times[domain]) > 100:
            self.request_times[domain] = self.request_times[domain][-100:]
    
    def get_request_stats(self, domain: Optional[str] = None) -> Dict:
        """
        Get statistics about requests
        
        Args:
            domain: Optional domain to filter by
            
        Returns:
            Dictionary with request statistics
        """
        if domain:
            domains = [domain]
        else:
            domains = list(self.request_times.keys())
        
        stats = {}
        for dom in domains:
            if dom in self.request_times:
                times = self.request_times[dom]
                if times:
                    total = len(times)
                    if total > 1:
                        intervals = [times[i] - times[i-1] for i in range(1, len(times))]
                        avg_interval = sum(intervals) / len(intervals)
                        min_interval = min(intervals)
                        max_interval = max(intervals)
                    else:
                        avg_interval = min_interval = max_interval = 0
                    
                    stats[dom] = {
                        "total_requests": total,
                        "avg_interval": avg_interval,
                        "min_interval": min_interval,
                        "max_interval": max_interval,
                        "current_delay": self.rate_limits.get(dom, {}).get("delay", 0)
                    }
        
        return stats
    
    def check_tos_compliance(self, url: str, data_type: str) -> bool:
        """
        Check if data collection complies with Terms of Service
        
        This is a placeholder - actual implementation would need to:
        - Parse ToS documents
        - Check against known restrictions
        - Verify data is publicly available
        
        Args:
            url: URL being accessed
            data_type: Type of data being collected
            
        Returns:
            True if compliant (placeholder always returns True)
        
        Note: Full implementation would require:
        1. Fetching and parsing ToS documents from target domain
        2. Checking if data_type is explicitly allowed/disallowed
        3. Verifying data is publicly available (not behind authentication)
        4. Checking for any specific restrictions (rate limits, usage terms, etc.)
        5. Maintaining a database of known ToS restrictions per domain
        """
        logger.debug(f"ToS compliance check for {url} (data_type: {data_type}) - placeholder implementation")
        return True
    
    def is_public_data(self, url: str) -> bool:
        """
        Check if URL contains publicly available data
        
        Args:
            url: URL to check
            
        Returns:
            True if data appears to be public
        """
        # Simple heuristic: check if URL requires authentication
        # In production, you might want to actually try accessing the URL
        
        # URLs with login/auth in path are likely private
        private_indicators = ["/login", "/auth", "/account", "/profile", "/dashboard", "/admin"]
        url_lower = url.lower()
        
        for indicator in private_indicators:
            if indicator in url_lower:
                logger.warning(f"URL may contain private data: {url}")
                return False
        
        return True

