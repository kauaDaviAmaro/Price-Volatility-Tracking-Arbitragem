"""
Tests for ProxyManager - rotation strategies, health tracking, failure handling
"""
import pytest
import asyncio
from unittest.mock import AsyncMock, patch

from src.core.proxy_manager import ProxyManager, Proxy, ProxyType


@pytest.mark.asyncio
class TestProxy:
    """Tests for Proxy dataclass"""
    
    def test_proxy_creation(self):
        """Test creating a proxy"""
        proxy = Proxy(
            host="127.0.0.1",
            port=8080,
            username="user",
            password="pass",
            proxy_type=ProxyType.RESIDENTIAL,
            protocol="http"
        )
        
        assert proxy.host == "127.0.0.1"
        assert proxy.port == 8080
        assert proxy.username == "user"
        assert proxy.password == "pass"
        assert proxy.proxy_type == ProxyType.RESIDENTIAL
        assert proxy.protocol == "http"
        assert proxy.success_count == 0
        assert proxy.failure_count == 0
        assert proxy.is_active is True
    
    def test_proxy_success_rate(self):
        """Test success rate calculation"""
        proxy = Proxy(host="127.0.0.1", port=8080)
        
        # No requests yet
        assert proxy.success_rate == 1.0
        
        # With successes
        proxy.success_count = 8
        proxy.failure_count = 2
        assert proxy.success_rate == 0.8
    
    def test_proxy_server_string(self):
        """Test proxy server string generation"""
        # HTTP proxy
        proxy = Proxy(host="127.0.0.1", port=8080, protocol="http")
        assert proxy.server == "http://127.0.0.1:8080"
        
        # SOCKS5 proxy
        proxy = Proxy(host="127.0.0.1", port=1080, protocol="socks5")
        assert proxy.server == "socks5://127.0.0.1:1080"
    
    def test_proxy_to_playwright_config(self):
        """Test conversion to Playwright config"""
        # With auth
        proxy = Proxy(
            host="127.0.0.1",
            port=8080,
            username="user",
            password="pass",
            protocol="http"
        )
        config = proxy.to_playwright_config()
        assert config["server"] == "http://127.0.0.1:8080"
        assert config["username"] == "user"
        assert config["password"] == "pass"
        
        # Without auth
        proxy = Proxy(host="127.0.0.1", port=8080)
        config = proxy.to_playwright_config()
        assert config["server"] == "http://127.0.0.1:8080"
        assert "username" not in config


@pytest.mark.asyncio
class TestProxyManagerInitialization:
    """Tests for ProxyManager initialization"""
    
    def test_init_with_defaults(self):
        """Test initialization with defaults"""
        manager = ProxyManager()
        
        assert manager.proxies == []
        assert manager.rotation_strategy == "round_robin"
        assert manager.max_failures == 3
        assert manager.cooldown_seconds == 300
        assert manager.current_index == 0
    
    def test_init_with_custom_params(self):
        """Test initialization with custom parameters"""
        proxies = [
            Proxy(host="127.0.0.1", port=8080),
            Proxy(host="127.0.0.1", port=8081)
        ]
        manager = ProxyManager(
            proxies=proxies,
            rotation_strategy="random",
            max_failures=5,
            cooldown_seconds=600
        )
        
        assert len(manager.proxies) == 2
        assert manager.rotation_strategy == "random"
        assert manager.max_failures == 5
        assert manager.cooldown_seconds == 600


@pytest.mark.asyncio
class TestProxyManagerAddProxy:
    """Tests for adding proxies"""
    
    def test_add_proxy(self):
        """Test adding a proxy"""
        manager = ProxyManager()
        proxy = manager.add_proxy(
            host="127.0.0.1",
            port=8080,
            username="user",
            password="pass",
            proxy_type=ProxyType.RESIDENTIAL,
            protocol="http"
        )
        
        assert len(manager.proxies) == 1
        assert proxy in manager.proxies
        assert proxy.host == "127.0.0.1"
        assert proxy.port == 8080
        assert proxy.proxy_type == ProxyType.RESIDENTIAL
    
    def test_load_proxies_from_config(self):
        """Test loading proxies from config"""
        manager = ProxyManager()
        configs = [
            {
                "host": "127.0.0.1",
                "port": 8080,
                "username": "user1",
                "password": "pass1",
                "type": "residential",
                "protocol": "http"
            },
            {
                "host": "127.0.0.1",
                "port": 8081,
                "type": "datacenter",
                "protocol": "socks5"
            }
        ]
        
        manager.load_proxies_from_config(configs)
        
        assert len(manager.proxies) == 2
        assert manager.proxies[0].username == "user1"
        assert manager.proxies[1].protocol == "socks5"


@pytest.mark.asyncio
class TestProxyManagerGetProxy:
    """Tests for getting proxies with rotation"""
    
    async def test_get_proxy_round_robin(self):
        """Test round-robin rotation strategy"""
        manager = ProxyManager(rotation_strategy="round_robin")
        manager.add_proxy("127.0.0.1", 8080)
        manager.add_proxy("127.0.0.1", 8081)
        manager.add_proxy("127.0.0.1", 8082)
        
        # Should cycle through proxies
        proxy1 = await manager.get_proxy()
        proxy2 = await manager.get_proxy()
        proxy3 = await manager.get_proxy()
        proxy4 = await manager.get_proxy()
        
        assert proxy1.port == 8080
        assert proxy2.port == 8081
        assert proxy3.port == 8082
        assert proxy4.port == 8080  # Should wrap around
    
    async def test_get_proxy_random(self):
        """Test random rotation strategy"""
        manager = ProxyManager(rotation_strategy="random")
        manager.add_proxy("127.0.0.1", 8080)
        manager.add_proxy("127.0.0.1", 8081)
        manager.add_proxy("127.0.0.1", 8082)
        
        # Get multiple proxies - should be random
        proxies = [await manager.get_proxy() for _ in range(10)]
        ports = [p.port for p in proxies]
        
        # Should have variety (not all same)
        assert len(set(ports)) > 1
    
    async def test_get_proxy_least_used(self):
        """Test least-used rotation strategy"""
        manager = ProxyManager(rotation_strategy="least_used")
        proxy1 = manager.add_proxy("127.0.0.1", 8080)
        proxy2 = manager.add_proxy("127.0.0.1", 8081)
        
        # Use proxy1 once
        proxy1.success_count = 1
        
        # Should get proxy2 (least used)
        selected = await manager.get_proxy()
        assert selected.port == 8081
    
    async def test_get_proxy_best_performance(self):
        """Test best-performance rotation strategy"""
        manager = ProxyManager(rotation_strategy="best_performance")
        proxy1 = manager.add_proxy("127.0.0.1", 8080)
        proxy2 = manager.add_proxy("127.0.0.1", 8081)
        
        # proxy1 has better success rate
        proxy1.success_count = 9
        proxy1.failure_count = 1
        proxy2.success_count = 5
        proxy2.failure_count = 5
        
        selected = await manager.get_proxy()
        assert selected.port == 8080
    
    async def test_get_proxy_with_preferred_type(self):
        """Test getting proxy with preferred type"""
        manager = ProxyManager()
        manager.add_proxy("127.0.0.1", 8080, proxy_type=ProxyType.RESIDENTIAL)
        manager.add_proxy("127.0.0.1", 8081, proxy_type=ProxyType.DATACENTER)
        
        # Request residential
        proxy = await manager.get_proxy(ProxyType.RESIDENTIAL)
        assert proxy.proxy_type == ProxyType.RESIDENTIAL
        assert proxy.port == 8080
    
    async def test_get_proxy_no_active_proxies(self):
        """Test getting proxy when none are active"""
        manager = ProxyManager()
        proxy = manager.add_proxy("127.0.0.1", 8080)
        proxy.is_active = False
        
        result = await manager.get_proxy()
        assert result is None
    
    async def test_get_proxy_updates_last_used(self):
        """Test that get_proxy updates last_used timestamp"""
        manager = ProxyManager()
        proxy = manager.add_proxy("127.0.0.1", 8080)
        
        assert proxy.last_used is None
        await manager.get_proxy()
        assert proxy.last_used is not None


@pytest.mark.asyncio
class TestProxyManagerHealthTracking:
    """Tests for proxy health tracking"""
    
    async def test_mark_success(self):
        """Test marking proxy as successful"""
        manager = ProxyManager()
        proxy = manager.add_proxy("127.0.0.1", 8080)
        
        initial_count = proxy.success_count
        await manager.mark_success(proxy)
        
        assert proxy.success_count == initial_count + 1
        assert proxy.is_active is True
    
    async def test_mark_failure(self):
        """Test marking proxy as failed"""
        manager = ProxyManager(max_failures=3)
        proxy = manager.add_proxy("127.0.0.1", 8080)
        
        initial_count = proxy.failure_count
        await manager.mark_failure(proxy)
        
        assert proxy.failure_count == initial_count + 1
        assert proxy.is_active is True  # Not yet at max failures
    
    async def test_mark_failure_deactivates_proxy(self):
        """Test that proxy is deactivated after max failures"""
        manager = ProxyManager(max_failures=2)
        proxy = manager.add_proxy("127.0.0.1", 8080)
        
        await manager.mark_failure(proxy)
        assert proxy.is_active is True  # 1 failure, max is 2, still active
        
        await manager.mark_failure(proxy)
        assert proxy.is_active is False  # 2 failures >= max_failures (2), deactivated
    
    async def test_proxy_reactivation_after_cooldown(self):
        """Test that proxy reactivates after cooldown period"""
        manager = ProxyManager(max_failures=1, cooldown_seconds=0.1)  # Short cooldown for test
        proxy = manager.add_proxy("127.0.0.1", 8080)
        
        # Use proxy first to set last_used
        await manager.get_proxy()
        assert proxy.last_used is not None
        
        # Fail enough to deactivate
        await manager.mark_failure(proxy)
        await manager.mark_failure(proxy)
        assert proxy.is_active is False
        
        # Wait for cooldown
        await asyncio.sleep(0.15)
        
        # Try to get proxy - should reactivate
        result = await manager.get_proxy()
        assert result is not None
        assert proxy.is_active is True
        assert proxy.failure_count == 0  # Should be reset


@pytest.mark.asyncio
class TestProxyManagerStats:
    """Tests for proxy statistics"""
    
    def test_get_stats(self):
        """Test getting proxy statistics"""
        manager = ProxyManager()
        manager.add_proxy("127.0.0.1", 8080, proxy_type=ProxyType.RESIDENTIAL)
        manager.add_proxy("127.0.0.1", 8081, proxy_type=ProxyType.DATACENTER)
        
        proxy2 = manager.proxies[1]
        proxy2.is_active = False
        
        stats = manager.get_stats()
        
        assert stats["total_proxies"] == 2
        assert stats["active_proxies"] == 1
        assert stats["inactive_proxies"] == 1
        assert "by_type" in stats
        assert "residential" in stats["by_type"]
        assert "datacenter" in stats["by_type"]

