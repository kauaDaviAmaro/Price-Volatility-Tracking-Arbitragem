"""
Tests for BrowserManager - browser initialization, fingerprint rotation, proxy integration
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, Mock
import os

from src.core.browser_manager import BrowserManager, is_docker_environment
from src.core.proxy_manager import ProxyManager, Proxy, ProxyType
from src.core.fingerprint_manager import FingerprintManager, BrowserFingerprint
from src.config import Config


@pytest.mark.asyncio
class TestDockerEnvironment:
    """Tests for Docker environment detection"""
    
    def test_is_docker_environment_with_dockerenv(self):
        """Test Docker detection via /.dockerenv"""
        with patch('os.path.exists', return_value=True):
            assert is_docker_environment() is True
    
    def test_is_docker_environment_with_env_var(self):
        """Test Docker detection via environment variable"""
        with patch.dict(os.environ, {'DOCKER_CONTAINER': 'true'}):
            assert is_docker_environment() is True
    
    def test_is_docker_environment_false(self):
        """Test Docker detection returns False when not in Docker"""
        with patch('os.path.exists', return_value=False):
            with patch.dict(os.environ, {}, clear=True):
                assert is_docker_environment() is False


@pytest.mark.asyncio
class TestBrowserManagerInitialization:
    """Tests for BrowserManager initialization"""
    
    @patch('src.core.browser_manager.Config')
    async def test_init_with_defaults(self, mock_config):
        """Test initialization with default parameters"""
        mock_config.HEADLESS = True
        mock_config.FINGERPRINT_REGION = "US"
        
        manager = BrowserManager()
        
        assert manager.headless is True
        assert manager.playwright is None
        assert manager.browser is None
        assert manager.context is None
        assert manager.proxy_manager is None
        assert isinstance(manager.fingerprint_manager, FingerprintManager)
        assert manager.current_fingerprint is None
        assert manager.current_proxy is None
    
    @patch('src.core.browser_manager.Config')
    async def test_init_with_headless_override(self, mock_config):
        """Test initialization with headless override"""
        mock_config.HEADLESS = True
        
        manager = BrowserManager(headless=False)
        assert manager.headless is False
    
    async def test_init_with_proxy_manager(self):
        """Test initialization with custom proxy manager"""
        proxy_manager = ProxyManager()
        manager = BrowserManager(proxy_manager=proxy_manager)
        
        assert manager.proxy_manager == proxy_manager
    
    async def test_init_with_fingerprint_manager(self):
        """Test initialization with custom fingerprint manager"""
        fingerprint_manager = FingerprintManager()
        manager = BrowserManager(fingerprint_manager=fingerprint_manager)
        
        assert manager.fingerprint_manager == fingerprint_manager
    
    async def test_init_with_fingerprint(self):
        """Test initialization with specific fingerprint"""
        fingerprint = BrowserFingerprint(
            user_agent="Mozilla/5.0",
            viewport_width=1920,
            viewport_height=1080,
            screen_width=1920,
            screen_height=1080,
            color_depth=24,
            timezone="America/New_York",
            locale="en-US",
            language="en",
            platform="Win32",
            hardware_concurrency=8
        )
        
        manager = BrowserManager(fingerprint=fingerprint)
        assert manager.current_fingerprint == fingerprint


@pytest.mark.asyncio
class TestBrowserManagerInitialization:
    """Tests for browser initialization"""
    
    @patch('src.core.browser_manager.async_playwright')
    @patch('src.core.browser_manager.Config')
    async def test_initialize_creates_playwright(self, mock_config, mock_playwright):
        """Test that initialize creates Playwright instance"""
        mock_config.HEADLESS = True
        mock_config.BROWSER_TYPE = "chromium"
        mock_config.FINGERPRINT_REGION = "US"
        mock_config.PROXY_ENABLED = False
        
        # Mock Playwright
        mock_pw = AsyncMock()
        mock_browser = AsyncMock()
        mock_context = AsyncMock()
        mock_pw.chromium.launch = AsyncMock(return_value=mock_browser)
        mock_browser.new_context = AsyncMock(return_value=mock_context)
        mock_playwright.return_value.start = AsyncMock(return_value=mock_pw)
        
        manager = BrowserManager()
        context = await manager.initialize()
        
        assert manager.playwright == mock_pw
        assert manager.browser == mock_browser
        assert manager.context == mock_context
        assert context == mock_context
        mock_pw.chromium.launch.assert_called_once()
    
    @patch('src.core.browser_manager.async_playwright')
    @patch('src.core.browser_manager.Config')
    async def test_initialize_generates_fingerprint(self, mock_config, mock_playwright):
        """Test that initialize generates fingerprint if not provided"""
        mock_config.HEADLESS = True
        mock_config.BROWSER_TYPE = "chromium"
        mock_config.FINGERPRINT_REGION = "BR"
        mock_config.PROXY_ENABLED = False
        
        mock_pw = AsyncMock()
        mock_browser = AsyncMock()
        mock_context = AsyncMock()
        mock_pw.chromium.launch = AsyncMock(return_value=mock_browser)
        mock_browser.new_context = AsyncMock(return_value=mock_context)
        mock_playwright.return_value.start = AsyncMock(return_value=mock_pw)
        
        manager = BrowserManager()
        await manager.initialize()
        
        assert manager.current_fingerprint is not None
        assert isinstance(manager.current_fingerprint, BrowserFingerprint)
    
    @patch('src.core.browser_manager.async_playwright')
    @patch('src.core.browser_manager.Config')
    async def test_initialize_with_proxy(self, mock_config, mock_playwright):
        """Test initialization with proxy enabled"""
        mock_config.HEADLESS = True
        mock_config.BROWSER_TYPE = "chromium"
        mock_config.FINGERPRINT_REGION = "US"
        mock_config.PROXY_ENABLED = True
        
        # Create proxy manager with proxy
        proxy_manager = ProxyManager()
        proxy = proxy_manager.add_proxy("127.0.0.1", 8080, proxy_type=ProxyType.DATACENTER)
        
        mock_pw = AsyncMock()
        mock_browser = AsyncMock()
        mock_context = AsyncMock()
        mock_pw.chromium.launch = AsyncMock(return_value=mock_browser)
        mock_browser.new_context = AsyncMock(return_value=mock_context)
        mock_playwright.return_value.start = AsyncMock(return_value=mock_pw)
        
        manager = BrowserManager(proxy_manager=proxy_manager)
        await manager.initialize()
        
        assert manager.current_proxy == proxy
        # Check that proxy config was passed to launch
        call_args = mock_pw.chromium.launch.call_args
        assert call_args[1]['proxy'] is not None


@pytest.mark.asyncio
class TestBrowserManagerPageCreation:
    """Tests for page creation"""
    
    @patch('src.core.browser_manager.async_playwright')
    @patch('src.core.browser_manager.Config')
    async def test_create_page_initializes_if_needed(self, mock_config, mock_playwright):
        """Test that create_page initializes browser if not initialized"""
        mock_config.HEADLESS = True
        mock_config.BROWSER_TYPE = "chromium"
        mock_config.FINGERPRINT_REGION = "US"
        mock_config.PROXY_ENABLED = False
        
        mock_pw = AsyncMock()
        mock_browser = AsyncMock()
        mock_context = AsyncMock()
        mock_page = AsyncMock()
        mock_pw.chromium.launch = AsyncMock(return_value=mock_browser)
        mock_browser.new_context = AsyncMock(return_value=mock_context)
        mock_context.new_page = AsyncMock(return_value=mock_page)
        mock_playwright.return_value.start = AsyncMock(return_value=mock_pw)
        
        manager = BrowserManager()
        page = await manager.create_page()
        
        assert page == mock_page
        mock_context.new_page.assert_called_once()
    
    @patch('src.core.browser_manager.async_playwright')
    @patch('src.core.browser_manager.Config')
    async def test_create_page_uses_existing_context(self, mock_config, mock_playwright):
        """Test that create_page uses existing context if available"""
        mock_config.HEADLESS = True
        mock_config.BROWSER_TYPE = "chromium"
        mock_config.FINGERPRINT_REGION = "US"
        mock_config.PROXY_ENABLED = False
        
        mock_pw = AsyncMock()
        mock_browser = AsyncMock()
        mock_context = AsyncMock()
        mock_page = AsyncMock()
        mock_pw.chromium.launch = AsyncMock(return_value=mock_browser)
        mock_browser.new_context = AsyncMock(return_value=mock_context)
        mock_context.new_page = AsyncMock(return_value=mock_page)
        mock_playwright.return_value.start = AsyncMock(return_value=mock_pw)
        
        manager = BrowserManager()
        await manager.initialize()
        page = await manager.create_page()
        
        assert page == mock_page
        # Should only call new_context once (during initialize)
        assert mock_browser.new_context.call_count == 1


@pytest.mark.asyncio
class TestBrowserManagerFingerprintRotation:
    """Tests for fingerprint rotation"""
    
    async def test_rotate_fingerprint(self):
        """Test fingerprint rotation"""
        manager = BrowserManager()
        manager.current_fingerprint = BrowserFingerprint(
            user_agent="Mozilla/5.0 Old",
            viewport_width=1920,
            viewport_height=1080,
            screen_width=1920,
            screen_height=1080,
            color_depth=24,
            timezone="America/New_York",
            locale="en-US",
            language="en",
            platform="Win32",
            hardware_concurrency=8
        )
        
        old_fingerprint = manager.current_fingerprint
        new_fingerprint = manager.rotate_fingerprint()
        
        assert new_fingerprint != old_fingerprint
        assert manager.current_fingerprint == new_fingerprint
        assert isinstance(new_fingerprint, BrowserFingerprint)


@pytest.mark.asyncio
class TestBrowserManagerProxyManagement:
    """Tests for proxy management"""
    
    @patch('src.core.browser_manager.async_playwright')
    @patch('src.core.browser_manager.Config')
    async def test_mark_proxy_success(self, mock_config, mock_playwright):
        """Test marking proxy as successful"""
        mock_config.HEADLESS = True
        mock_config.BROWSER_TYPE = "chromium"
        mock_config.FINGERPRINT_REGION = "US"
        mock_config.PROXY_ENABLED = True
        
        proxy_manager = ProxyManager()
        proxy = proxy_manager.add_proxy("127.0.0.1", 8080)
        
        mock_pw = AsyncMock()
        mock_browser = AsyncMock()
        mock_context = AsyncMock()
        mock_pw.chromium.launch = AsyncMock(return_value=mock_browser)
        mock_browser.new_context = AsyncMock(return_value=mock_context)
        mock_playwright.return_value.start = AsyncMock(return_value=mock_pw)
        
        manager = BrowserManager(proxy_manager=proxy_manager)
        await manager.initialize()
        
        initial_success = proxy.success_count
        await manager.mark_proxy_success()
        
        assert proxy.success_count == initial_success + 1
    
    @patch('src.core.browser_manager.async_playwright')
    @patch('src.core.browser_manager.Config')
    async def test_mark_proxy_failure(self, mock_config, mock_playwright):
        """Test marking proxy as failed"""
        mock_config.HEADLESS = True
        mock_config.BROWSER_TYPE = "chromium"
        mock_config.FINGERPRINT_REGION = "US"
        mock_config.PROXY_ENABLED = True
        
        proxy_manager = ProxyManager(max_failures=3)
        proxy = proxy_manager.add_proxy("127.0.0.1", 8080)
        
        mock_pw = AsyncMock()
        mock_browser = AsyncMock()
        mock_context = AsyncMock()
        mock_pw.chromium.launch = AsyncMock(return_value=mock_browser)
        mock_browser.new_context = AsyncMock(return_value=mock_context)
        mock_playwright.return_value.start = AsyncMock(return_value=mock_pw)
        
        manager = BrowserManager(proxy_manager=proxy_manager)
        await manager.initialize()
        
        initial_failure = proxy.failure_count
        await manager.mark_proxy_failure()
        
        assert proxy.failure_count == initial_failure + 1
        assert manager.current_proxy is None  # Should be cleared after failure


@pytest.mark.asyncio
class TestBrowserManagerCleanup:
    """Tests for browser cleanup"""
    
    @patch('src.core.browser_manager.async_playwright')
    @patch('src.core.browser_manager.Config')
    async def test_close_cleans_up_resources(self, mock_config, mock_playwright):
        """Test that close properly cleans up all resources"""
        mock_config.HEADLESS = True
        mock_config.BROWSER_TYPE = "chromium"
        mock_config.FINGERPRINT_REGION = "US"
        mock_config.PROXY_ENABLED = False
        
        mock_pw = AsyncMock()
        mock_browser = AsyncMock()
        mock_context = AsyncMock()
        mock_pw.chromium.launch = AsyncMock(return_value=mock_browser)
        mock_browser.new_context = AsyncMock(return_value=mock_context)
        mock_playwright.return_value.start = AsyncMock(return_value=mock_pw)
        
        manager = BrowserManager()
        await manager.initialize()
        await manager.close()
        
        mock_context.close.assert_called_once()
        mock_browser.close.assert_called_once()
        mock_pw.stop.assert_called_once()
        assert manager.context is None
        assert manager.browser is None
        assert manager.playwright is None
    
    @patch('src.core.browser_manager.async_playwright')
    @patch('src.core.browser_manager.Config')
    async def test_context_manager(self, mock_config, mock_playwright):
        """Test BrowserManager as context manager"""
        mock_config.HEADLESS = True
        mock_config.BROWSER_TYPE = "chromium"
        mock_config.FINGERPRINT_REGION = "US"
        mock_config.PROXY_ENABLED = False
        
        mock_pw = AsyncMock()
        mock_browser = AsyncMock()
        mock_context = AsyncMock()
        mock_pw.chromium.launch = AsyncMock(return_value=mock_browser)
        mock_browser.new_context = AsyncMock(return_value=mock_context)
        mock_playwright.return_value.start = AsyncMock(return_value=mock_pw)
        
        async with BrowserManager() as manager:
            assert manager.context is not None
        
        # Should be cleaned up after context exit
        mock_context.close.assert_called_once()
        mock_browser.close.assert_called_once()
        mock_pw.stop.assert_called_once()


@pytest.mark.asyncio
class TestBrowserManagerAntiBot:
    """Tests for anti-bot configuration"""
    
    @patch('src.core.browser_manager.async_playwright')
    @patch('src.core.browser_manager.Config')
    async def test_configure_anti_bot_injects_scripts(self, mock_config, mock_playwright):
        """Test that configure_anti_bot injects anti-detection scripts"""
        mock_config.HEADLESS = True
        mock_config.BROWSER_TYPE = "chromium"
        mock_config.FINGERPRINT_REGION = "US"
        mock_config.PROXY_ENABLED = False
        
        mock_pw = AsyncMock()
        mock_browser = AsyncMock()
        mock_context = AsyncMock()
        mock_pw.chromium.launch = AsyncMock(return_value=mock_browser)
        mock_browser.new_context = AsyncMock(return_value=mock_context)
        mock_playwright.return_value.start = AsyncMock(return_value=mock_pw)
        
        manager = BrowserManager()
        await manager.initialize()
        
        # Check that add_init_script was called (for anti-bot measures)
        assert mock_context.add_init_script.call_count >= 1

