"""
Tests for FingerprintManager - fingerprint generation, region-specific configs, Playwright integration
"""
import pytest
from unittest.mock import Mock, patch

from src.core.fingerprint_manager import FingerprintManager, BrowserFingerprint


@pytest.mark.asyncio
class TestBrowserFingerprint:
    """Tests for BrowserFingerprint dataclass"""
    
    def test_fingerprint_creation(self):
        """Test creating a fingerprint"""
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
        
        assert fingerprint.user_agent == "Mozilla/5.0"
        assert fingerprint.viewport_width == 1920
        assert fingerprint.viewport_height == 1080
    
    def test_to_playwright_viewport(self):
        """Test conversion to Playwright viewport"""
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
        
        viewport = fingerprint.to_playwright_viewport()
        assert viewport["width"] == 1920
        assert viewport["height"] == 1080
    
    def test_to_playwright_locale(self):
        """Test conversion to Playwright locale"""
        fingerprint = BrowserFingerprint(
            user_agent="Mozilla/5.0",
            viewport_width=1920,
            viewport_height=1080,
            screen_width=1920,
            screen_height=1080,
            color_depth=24,
            timezone="America/New_York",
            locale="pt-BR",
            language="pt",
            platform="Win32",
            hardware_concurrency=8
        )
        
        assert fingerprint.to_playwright_locale() == "pt-BR"


@pytest.mark.asyncio
class TestFingerprintManagerInitialization:
    """Tests for FingerprintManager initialization"""
    
    def test_init_with_defaults(self):
        """Test initialization with defaults"""
        manager = FingerprintManager()
        
        assert manager.ua is not None
        assert manager.generated_fingerprints == []
    
    def test_init_with_custom_ua(self):
        """Test initialization with custom UserAgent"""
        mock_ua = Mock()
        manager = FingerprintManager(ua=mock_ua)
        
        assert manager.ua == mock_ua


@pytest.mark.asyncio
class TestFingerprintGeneration:
    """Tests for fingerprint generation"""
    
    @patch('src.core.fingerprint_manager.UserAgent')
    def test_generate_fingerprint_us_region(self, mock_ua_class):
        """Test generating fingerprint for US region"""
        mock_ua = Mock()
        mock_ua.random = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        mock_ua_class.return_value = mock_ua
        
        manager = FingerprintManager()
        fingerprint = manager.generate_fingerprint(region="US")
        
        assert isinstance(fingerprint, BrowserFingerprint)
        assert fingerprint.user_agent is not None
        assert fingerprint.viewport_width > 0
        assert fingerprint.viewport_height > 0
        assert fingerprint.timezone in manager.TIMEZONES["US"]
        assert fingerprint.locale in manager.LOCALES["en"]
        assert fingerprint in manager.generated_fingerprints
    
    @patch('src.core.fingerprint_manager.UserAgent')
    def test_generate_fingerprint_br_region(self, mock_ua_class):
        """Test generating fingerprint for BR region"""
        mock_ua = Mock()
        mock_ua.random = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        mock_ua_class.return_value = mock_ua
        
        manager = FingerprintManager()
        fingerprint = manager.generate_fingerprint(region="BR")
        
        assert fingerprint.timezone in manager.TIMEZONES["BR"]
        assert fingerprint.locale in manager.LOCALES["pt"]
    
    @patch('src.core.fingerprint_manager.UserAgent')
    def test_generate_fingerprint_viewport_constraints(self, mock_ua_class):
        """Test that viewport is not larger than screen"""
        mock_ua = Mock()
        mock_ua.random = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        mock_ua_class.return_value = mock_ua
        
        manager = FingerprintManager()
        fingerprint = manager.generate_fingerprint()
        
        assert fingerprint.viewport_width <= fingerprint.screen_width
        assert fingerprint.viewport_height <= fingerprint.screen_height
    
    @patch('src.core.fingerprint_manager.UserAgent')
    def test_generate_fingerprint_platform_extraction(self, mock_ua_class):
        """Test platform extraction from user agent"""
        mock_ua = Mock()
        mock_ua.random = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        mock_ua_class.return_value = mock_ua
        
        manager = FingerprintManager()
        fingerprint = manager.generate_fingerprint()
        
        assert fingerprint.platform == "MacIntel"
    
    @patch('src.core.fingerprint_manager.UserAgent')
    def test_generate_fingerprint_webgl(self, mock_ua_class):
        """Test WebGL vendor and renderer generation"""
        mock_ua = Mock()
        mock_ua.random = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        mock_ua_class.return_value = mock_ua
        
        manager = FingerprintManager()
        fingerprint = manager.generate_fingerprint()
        
        assert fingerprint.webgl_vendor in manager.WEBGL_VENDORS
        assert fingerprint.webgl_renderer in manager.WEBGL_RENDERERS


@pytest.mark.asyncio
class TestFingerprintManagerPlatformExtraction:
    """Tests for platform extraction"""
    
    def test_extract_platform_windows(self):
        """Test Windows platform extraction"""
        manager = FingerprintManager()
        platform = manager._extract_platform("Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
        assert platform == "Win32"
    
    def test_extract_platform_mac(self):
        """Test Mac platform extraction"""
        manager = FingerprintManager()
        platform = manager._extract_platform("Mozilla/5.0 (Macintosh; Intel Mac OS X)")
        assert platform == "MacIntel"
    
    def test_extract_platform_linux(self):
        """Test Linux platform extraction"""
        manager = FingerprintManager()
        platform = manager._extract_platform("Mozilla/5.0 (X11; Linux x86_64)")
        assert platform == "Linux x86_64"
    
    def test_extract_platform_android(self):
        """Test Android platform extraction"""
        manager = FingerprintManager()
        platform = manager._extract_platform("Mozilla/5.0 (Linux; Android 10)")
        assert platform == "Linux armv7l"
    
    def test_extract_platform_default(self):
        """Test default platform when unknown"""
        manager = FingerprintManager()
        platform = manager._extract_platform("Unknown User Agent")
        assert platform == "Win32"


@pytest.mark.asyncio
class TestFingerprintManagerAntiDetectScript:
    """Tests for anti-detection script generation"""
    
    @patch('src.core.fingerprint_manager.UserAgent')
    def test_get_anti_detect_script(self, mock_ua_class):
        """Test anti-detection script generation"""
        mock_ua = Mock()
        mock_ua.random = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        mock_ua_class.return_value = mock_ua
        
        manager = FingerprintManager()
        fingerprint = manager.generate_fingerprint()
        script = manager.get_anti_detect_script(fingerprint)
        
        assert isinstance(script, str)
        assert fingerprint.user_agent in script
        assert str(fingerprint.hardware_concurrency) in script
        assert fingerprint.platform in script
    
    @patch('src.core.fingerprint_manager.UserAgent')
    def test_get_anti_detect_script_without_device_memory(self, mock_ua_class):
        """Test script generation without device_memory"""
        mock_ua = Mock()
        mock_ua.random = "Mozilla/5.0 (Firefox)"
        mock_ua_class.return_value = mock_ua
        
        manager = FingerprintManager()
        fingerprint = manager.generate_fingerprint()
        fingerprint.device_memory = None
        script = manager.get_anti_detect_script(fingerprint)
        
        assert "deviceMemory" not in script or "undefined" in script


@pytest.mark.asyncio
class TestFingerprintManagerHTTPHeaders:
    """Tests for HTTP headers generation"""
    
    @patch('src.core.fingerprint_manager.UserAgent')
    def test_get_http_headers(self, mock_ua_class):
        """Test HTTP headers generation"""
        mock_ua = Mock()
        mock_ua.random = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        mock_ua_class.return_value = mock_ua
        
        manager = FingerprintManager()
        fingerprint = manager.generate_fingerprint()
        headers = manager.get_http_headers(fingerprint)
        
        assert "User-Agent" in headers
        assert headers["User-Agent"] == fingerprint.user_agent
        assert "Accept-Language" in headers
        assert fingerprint.locale in headers["Accept-Language"]
        assert "Accept" in headers
    
    @patch('src.core.fingerprint_manager.UserAgent')
    def test_get_http_headers_chrome_specific(self, mock_ua_class):
        """Test Chrome-specific headers"""
        mock_ua = Mock()
        mock_ua.random = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        mock_ua_class.return_value = mock_ua
        
        manager = FingerprintManager()
        fingerprint = manager.generate_fingerprint()
        headers = manager.get_http_headers(fingerprint)
        
        if "Chrome" in fingerprint.user_agent:
            assert "sec-ch-ua" in headers
            assert "sec-ch-ua-mobile" in headers
            assert "sec-ch-ua-platform" in headers


@pytest.mark.asyncio
class TestFingerprintManagerLocaleSelection:
    """Tests for locale selection"""
    
    def test_get_locales_for_region_us(self):
        """Test locale selection for US region"""
        manager = FingerprintManager()
        locales = manager._get_locales_for_region("US")
        assert all(locale in manager.LOCALES["en"] for locale in locales)
    
    def test_get_locales_for_region_br(self):
        """Test locale selection for BR region"""
        manager = FingerprintManager()
        locales = manager._get_locales_for_region("BR")
        assert all(locale in manager.LOCALES["pt"] for locale in locales)
    
    def test_get_locales_for_region_unknown(self):
        """Test locale selection for unknown region"""
        manager = FingerprintManager()
        locales = manager._get_locales_for_region("UNKNOWN")
        assert all(locale in manager.LOCALES["en"] for locale in locales)

