"""
Tests for ComplianceManager - robots.txt checking, rate limiting, cache management
"""
import pytest
import asyncio
import time
from unittest.mock import AsyncMock, Mock, patch, mock_open
from pathlib import Path

from src.core.compliance_manager import ComplianceManager


@pytest.mark.asyncio
class TestComplianceManagerInitialization:
    """Tests for ComplianceManager initialization"""
    
    def test_init_with_defaults(self):
        """Test initialization with defaults"""
        manager = ComplianceManager()
        
        assert manager.respect_robots is True
        assert manager.cache_dir.exists()
        assert manager.cache_ttl == 3600
        assert manager.robots_cache == {}
        assert manager.rate_limits == {}
    
    def test_init_with_custom_params(self):
        """Test initialization with custom parameters"""
        manager = ComplianceManager(
            cache_dir=".test_cache",
            respect_robots=False
        )
        
        assert manager.respect_robots is False
        assert manager.cache_dir == Path(".test_cache")
        assert manager.cache_dir.exists()


@pytest.mark.asyncio
class TestComplianceManagerRobotsTxt:
    """Tests for robots.txt checking"""
    
    async def test_can_fetch_when_respect_robots_disabled(self):
        """Test that can_fetch returns True when respect_robots is False"""
        manager = ComplianceManager(respect_robots=False)
        
        result = await manager.can_fetch("https://example.com/page", "MyBot")
        assert result is True
    
    @patch('src.core.compliance_manager.aiohttp.ClientSession')
    async def test_can_fetch_with_robots_txt_allowed(self, mock_session_class):
        """Test can_fetch when robots.txt allows access"""
        manager = ComplianceManager(respect_robots=True)
        
        # Mock robots.txt response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.text = AsyncMock(return_value="User-agent: *\nAllow: /")
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)
        
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_session.get = AsyncMock(return_value=mock_response)
        mock_session_class.return_value = mock_session
        
        result = await manager.can_fetch("https://example.com/page", "*")
        
        assert result is True
    
    async def test_can_fetch_with_robots_txt_disallowed(self):
        """Test can_fetch when robots.txt disallows access"""
        from urllib.robotparser import RobotFileParser
        import tempfile
        import os
        import urllib.request
        import urllib.parse
        
        manager = ComplianceManager(respect_robots=True)
        
        # Create a temporary robots.txt file that disallows access
        robots_content = "User-agent: *\nDisallow: /"
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write(robots_content)
            temp_path = f.name
        
        try:
            # Mock _get_robots_parser to return a parser reading from our temp file
            async def mock_get_robots_parser(domain, user_agent):
                robots = RobotFileParser()
                # Use proper file:// URL format for Windows
                file_url = urllib.parse.urljoin('file:', urllib.request.pathname2url(os.path.abspath(temp_path)))
                robots.set_url(file_url)
                robots.read()
                return robots
            
            with patch.object(manager, '_get_robots_parser', side_effect=mock_get_robots_parser):
                result = await manager.can_fetch("https://example.com/page", "*")
                assert result is False
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    @patch('src.core.compliance_manager.aiohttp.ClientSession')
    async def test_can_fetch_no_robots_txt(self, mock_session_class):
        """Test can_fetch when robots.txt doesn't exist"""
        manager = ComplianceManager(respect_robots=True)
        
        # Mock 404 response
        mock_response = AsyncMock()
        mock_response.status = 404
        mock_response.__aenter__ = AsyncMock(return_value=mock_response)
        mock_response.__aexit__ = AsyncMock(return_value=None)
        
        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)
        mock_session.get = AsyncMock(return_value=mock_response)
        mock_session_class.return_value = mock_session
        
        result = await manager.can_fetch("https://example.com/page", "*")
        
        # Should allow by default when robots.txt not found
        assert result is True
    
    async def test_can_fetch_error_handling(self):
        """Test can_fetch error handling"""
        manager = ComplianceManager(respect_robots=True)
        
        # Should return True on error (fail open)
        with patch.object(manager, '_get_robots_parser', side_effect=Exception("Error")):
            result = await manager.can_fetch("https://example.com/page", "*")
            assert result is True


@pytest.mark.asyncio
class TestComplianceManagerCrawlDelay:
    """Tests for crawl delay"""
    
    async def test_get_crawl_delay(self):
        """Test getting crawl delay from robots.txt"""
        from urllib.robotparser import RobotFileParser
        
        manager = ComplianceManager(respect_robots=True)
        
        # Mock _get_robots_parser to return a parser with crawl delay
        async def mock_get_robots_parser(domain, user_agent):
            robots = RobotFileParser()
            # Create a mock parser that returns crawl delay
            robots.set_url("https://example.com/robots.txt")
            # We can't easily set crawl_delay, so we'll mock the crawl_delay method
            original_crawl_delay = robots.crawl_delay
            def mock_crawl_delay(user_agent):
                return 5.0
            robots.crawl_delay = mock_crawl_delay
            return robots
        
        with patch.object(manager, '_get_robots_parser', side_effect=mock_get_robots_parser):
            delay = await manager.get_crawl_delay("https://example.com/page", "*")
            assert delay == 5.0
    
    async def test_get_crawl_delay_when_respect_robots_disabled(self):
        """Test crawl delay when respect_robots is disabled"""
        manager = ComplianceManager(respect_robots=False)
        
        delay = await manager.get_crawl_delay("https://example.com/page", "*")
        assert delay == 0.0
    
    async def test_get_crawl_delay_no_robots_txt(self):
        """Test crawl delay when robots.txt not found"""
        manager = ComplianceManager(respect_robots=True)
        
        with patch.object(manager, '_get_robots_parser', return_value=None):
            delay = await manager.get_crawl_delay("https://example.com/page", "*")
            assert delay == 0.0


@pytest.mark.asyncio
class TestComplianceManagerRateLimiting:
    """Tests for rate limiting"""
    
    async def test_wait_for_rate_limit(self):
        """Test rate limiting wait"""
        manager = ComplianceManager()
        
        start = time.time()
        await manager.wait_for_rate_limit("https://example.com/page", min_delay=0.1)
        elapsed = time.time() - start
        
        # Should have waited at least min_delay
        assert elapsed >= 0.1
    
    async def test_wait_for_rate_limit_respects_crawl_delay(self):
        """Test that rate limiting respects crawl delay"""
        manager = ComplianceManager()
        
        # Mock crawl delay to return async value (get_crawl_delay is called without user_agent in wait_for_rate_limit)
        async def mock_get_crawl_delay(url, user_agent="*"):
            return 0.2
        
        with patch.object(manager, 'get_crawl_delay', side_effect=mock_get_crawl_delay):
            # First call - no wait (no previous request)
            await manager.wait_for_rate_limit("https://example.com/page", min_delay=0.1)
            
            # Second call - should wait for crawl delay (0.2) since it's greater than min_delay (0.1)
            start = time.time()
            await manager.wait_for_rate_limit("https://example.com/page", min_delay=0.1)
            elapsed = time.time() - start
            
            # Should wait for crawl delay (0.2) not min_delay (0.1)
            assert elapsed >= 0.15  # Allow some margin for async overhead
    
    async def test_wait_for_rate_limit_tracks_requests(self):
        """Test that rate limiting tracks request times"""
        manager = ComplianceManager()
        
        await manager.wait_for_rate_limit("https://example.com/page", min_delay=0.01)
        
        domain = "example.com"
        assert domain in manager.request_times
        assert len(manager.request_times[domain]) > 0
    
    async def test_wait_for_rate_limit_limits_history(self):
        """Test that request history is limited"""
        manager = ComplianceManager()
        
        # Mock get_crawl_delay to return 0 to avoid any delays
        async def mock_get_crawl_delay(url, user_agent="*"):
            return 0.0
        
        with patch.object(manager, 'get_crawl_delay', side_effect=mock_get_crawl_delay):
            # Make many requests with minimal delay
            for _ in range(150):
                await manager.wait_for_rate_limit("https://example.com/page", min_delay=0.0)
        
        domain = "example.com"
        # Should only keep last 100
        assert len(manager.request_times[domain]) == 100


@pytest.mark.asyncio
class TestComplianceManagerRequestStats:
    """Tests for request statistics"""
    
    async def test_get_request_stats(self):
        """Test getting request statistics"""
        manager = ComplianceManager()
        
        # Make some requests
        for _ in range(5):
            await manager.wait_for_rate_limit("https://example.com/page", min_delay=0.01)
            await asyncio.sleep(0.01)
        
        stats = manager.get_request_stats("example.com")
        
        assert "example.com" in stats
        assert stats["example.com"]["total_requests"] == 5
        assert "avg_interval" in stats["example.com"]
    
    async def test_get_request_stats_all_domains(self):
        """Test getting stats for all domains"""
        manager = ComplianceManager()
        
        await manager.wait_for_rate_limit("https://example.com/page", min_delay=0.01)
        await manager.wait_for_rate_limit("https://test.com/page", min_delay=0.01)
        
        stats = manager.get_request_stats()
        
        assert "example.com" in stats
        assert "test.com" in stats


@pytest.mark.asyncio
class TestComplianceManagerToSCompliance:
    """Tests for Terms of Service compliance"""
    
    def test_check_tos_compliance(self):
        """Test ToS compliance check (placeholder)"""
        manager = ComplianceManager()
        
        # Placeholder always returns True
        result = manager.check_tos_compliance("https://example.com/page", "listing_data")
        assert result is True


@pytest.mark.asyncio
class TestComplianceManagerPublicData:
    """Tests for public data checking"""
    
    def test_is_public_data_allowed(self):
        """Test that public URLs are allowed"""
        manager = ComplianceManager()
        
        assert manager.is_public_data("https://example.com/listings") is True
        assert manager.is_public_data("https://example.com/search") is True
    
    def test_is_public_data_private_indicators(self):
        """Test that URLs with private indicators are rejected"""
        manager = ComplianceManager()
        
        assert manager.is_public_data("https://example.com/login") is False
        assert manager.is_public_data("https://example.com/auth") is False
        assert manager.is_public_data("https://example.com/account") is False
        assert manager.is_public_data("https://example.com/profile") is False
        assert manager.is_public_data("https://example.com/dashboard") is False
        assert manager.is_public_data("https://example.com/admin") is False
    
    def test_is_public_data_case_insensitive(self):
        """Test that private indicator check is case insensitive"""
        manager = ComplianceManager()
        
        assert manager.is_public_data("https://example.com/LOGIN") is False
        assert manager.is_public_data("https://example.com/Admin") is False

