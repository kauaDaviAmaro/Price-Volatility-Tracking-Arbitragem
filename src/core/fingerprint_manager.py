"""
Fingerprint Manager - Elite Browser Fingerprinting System
Generates unique, consistent browser fingerprints to avoid detection
"""
import random
import json
from typing import Dict, Optional, List
from dataclasses import dataclass, asdict
from fake_useragent import UserAgent
import logging

logger = logging.getLogger(__name__)


@dataclass
class BrowserFingerprint:
    """Represents a complete browser fingerprint"""
    user_agent: str
    viewport_width: int
    viewport_height: int
    screen_width: int
    screen_height: int
    color_depth: int
    timezone: str
    locale: str
    language: str
    platform: str
    hardware_concurrency: int
    device_memory: Optional[int] = None
    webgl_vendor: Optional[str] = None
    webgl_renderer: Optional[str] = None
    
    def to_playwright_viewport(self) -> Dict:
        """Convert to Playwright viewport configuration"""
        return {
            "width": self.viewport_width,
            "height": self.viewport_height
        }
    
    def to_playwright_locale(self) -> str:
        """Get locale string for Playwright"""
        return self.locale


class FingerprintManager:
    """Manages browser fingerprint generation and rotation"""
    
    # Common screen resolutions (realistic combinations)
    SCREEN_RESOLUTIONS = [
        (1920, 1080), (1366, 768), (1536, 864), (1440, 900),
        (1280, 720), (1600, 900), (2560, 1440), (3840, 2160),
        (1280, 1024), (1024, 768)
    ]
    
    # Common viewport sizes (browser window, not full screen)
    VIEWPORT_SIZES = [
        (1920, 1080), (1366, 768), (1536, 864), (1440, 900),
        (1280, 720), (1600, 900), (1280, 1024)
    ]
    
    # Timezones by region
    TIMEZONES = {
        "US": ["America/New_York", "America/Chicago", "America/Denver", "America/Los_Angeles"],
        "EU": ["Europe/London", "Europe/Paris", "Europe/Berlin", "Europe/Rome", "Europe/Madrid"],
        "BR": ["America/Sao_Paulo", "America/Fortaleza", "America/Manaus"],
        "ASIA": ["Asia/Tokyo", "Asia/Shanghai", "Asia/Hong_Kong", "Asia/Singapore"],
    }
    
    # Locales
    LOCALES = {
        "en": ["en-US", "en-GB", "en-CA", "en-AU"],
        "pt": ["pt-BR", "pt-PT"],
        "es": ["es-ES", "es-MX", "es-AR"],
        "fr": ["fr-FR", "fr-CA"],
        "de": ["de-DE"],
        "it": ["it-IT"],
        "ja": ["ja-JP"],
        "zh": ["zh-CN", "zh-TW"],
    }
    
    # WebGL vendors and renderers (realistic combinations)
    WEBGL_VENDORS = [
        "Google Inc. (Intel)",
        "Google Inc. (NVIDIA)",
        "Google Inc. (AMD)",
        "Intel Inc.",
        "NVIDIA Corporation",
        "Apple Inc."
    ]
    
    WEBGL_RENDERERS = [
        "ANGLE (Intel, Intel(R) UHD Graphics 620 Direct3D11 vs_5_0 ps_5_0, D3D11)",
        "ANGLE (NVIDIA, NVIDIA GeForce GTX 1060 6GB Direct3D11 vs_5_0 ps_5_0, D3D11)",
        "ANGLE (AMD, AMD Radeon RX 580 Series Direct3D11 vs_5_0 ps_5_0, D3D11)",
        "Intel(R) UHD Graphics 620",
        "NVIDIA GeForce GTX 1060 6GB/PCIe/SSE2",
        "Apple GPU"
    ]
    
    def __init__(self, ua: Optional[UserAgent] = None):
        """
        Initialize FingerprintManager
        
        Args:
            ua: Optional UserAgent instance (for caching)
        """
        self.ua = ua or UserAgent()
        self.generated_fingerprints: List[BrowserFingerprint] = []
    
    def generate_fingerprint(self, region: str = "US") -> BrowserFingerprint:
        """
        Generate a new unique browser fingerprint
        
        Args:
            region: Region for timezone/locale selection
            
        Returns:
            BrowserFingerprint object
        """
        # Get realistic user agent - prefer desktop for web scraping
        # Mobile user agents often get different content or are blocked
        max_attempts = 10
        user_agent = None
        for _ in range(max_attempts):
            ua = self.ua.random
            # Prefer Chrome/Edge/Firefox desktop user agents
            if any(browser in ua for browser in ['Chrome', 'Edg', 'Firefox']) and \
               not any(mobile in ua for mobile in ['Mobile', 'iPhone', 'iPad', 'Android']):
                user_agent = ua
                break
        
        # Fallback to any user agent if no desktop found
        if not user_agent:
            user_agent = self.ua.random
            logger.warning(f"Using random user agent (may be mobile): {user_agent[:50]}...")
        else:
            logger.debug(f"Generated desktop user agent: {user_agent[:50]}...")
        
        # Determine platform from user agent
        platform = self._extract_platform(user_agent)
        
        # Select screen resolution (realistic)
        screen_width, screen_height = random.choice(self.SCREEN_RESOLUTIONS)
        
        # Select viewport (usually smaller than screen)
        viewport_width, viewport_height = random.choice(self.VIEWPORT_SIZES)
        # Ensure viewport is not larger than screen
        if viewport_width > screen_width:
            viewport_width = screen_width
        if viewport_height > screen_height:
            viewport_height = screen_height
        
        # Select timezone based on region
        timezone = random.choice(self.TIMEZONES.get(region, self.TIMEZONES["US"]))
        
        # Select locale (match with region if possible)
        locale_options = self._get_locales_for_region(region)
        locale = random.choice(locale_options)
        language = locale.split("-")[0]
        
        # Hardware specs (realistic ranges)
        hardware_concurrency = random.choice([2, 4, 6, 8, 12, 16])
        device_memory = random.choice([2, 4, 8, 16, 32]) if "Chrome" in user_agent else None
        
        # WebGL (for fingerprinting evasion)
        webgl_vendor = random.choice(self.WEBGL_VENDORS)
        webgl_renderer = random.choice(self.WEBGL_RENDERERS)
        
        fingerprint = BrowserFingerprint(
            user_agent=user_agent,
            viewport_width=viewport_width,
            viewport_height=viewport_height,
            screen_width=screen_width,
            screen_height=screen_height,
            color_depth=24,  # Most common
            timezone=timezone,
            locale=locale,
            language=language,
            platform=platform,
            hardware_concurrency=hardware_concurrency,
            device_memory=device_memory,
            webgl_vendor=webgl_vendor,
            webgl_renderer=webgl_renderer
        )
        
        self.generated_fingerprints.append(fingerprint)
        logger.debug(f"Generated fingerprint: {user_agent[:50]}... ({viewport_width}x{viewport_height})")
        
        return fingerprint
    
    def _extract_platform(self, user_agent: str) -> str:
        """Extract platform from user agent string"""
        ua_lower = user_agent.lower()
        if "windows" in ua_lower:
            return "Win32"
        elif "mac" in ua_lower or "darwin" in ua_lower:
            return "MacIntel"
        elif "android" in ua_lower:
            return "Linux armv7l"  # Check Android before Linux (Android UAs contain "Linux")
        elif "iphone" in ua_lower or "ipad" in ua_lower:
            return "iPhone"
        elif "linux" in ua_lower:
            return "Linux x86_64"
        else:
            return "Win32"  # Default
    
    def _get_locales_for_region(self, region: str) -> List[str]:
        """Get locale options for a region"""
        region_locale_map = {
            "US": self.LOCALES["en"],
            "BR": self.LOCALES["pt"],
            "EU": self.LOCALES["en"] + self.LOCALES["fr"] + self.LOCALES["de"] + self.LOCALES["it"],
            "ASIA": self.LOCALES["ja"] + self.LOCALES["zh"]
        }
        return region_locale_map.get(region, self.LOCALES["en"])
    
    def get_anti_detect_script(self, fingerprint: BrowserFingerprint) -> str:
        """
        Generate JavaScript to inject for anti-detection
        
        Args:
            fingerprint: BrowserFingerprint to use
            
        Returns:
            JavaScript code as string
        """
        script = f"""
        // Override navigator properties
        Object.defineProperty(navigator, 'userAgent', {{
            get: () => '{fingerprint.user_agent}'
        }});
        
        Object.defineProperty(navigator, 'platform', {{
            get: () => '{fingerprint.platform}'
        }});
        
        Object.defineProperty(navigator, 'hardwareConcurrency', {{
            get: () => {fingerprint.hardware_concurrency}
        }});
        
        {f"Object.defineProperty(navigator, 'deviceMemory', {{ get: () => {fingerprint.device_memory} }});" if fingerprint.device_memory else ""}
        
        Object.defineProperty(navigator, 'language', {{
            get: () => '{fingerprint.language}'
        }});
        
        Object.defineProperty(navigator, 'languages', {{
            get: () => ['{fingerprint.locale}', '{fingerprint.language}']
        }});
        
        // Override screen properties
        Object.defineProperty(screen, 'width', {{
            get: () => {fingerprint.screen_width}
        }});
        
        Object.defineProperty(screen, 'height', {{
            get: () => {fingerprint.screen_height}
        }});
        
        Object.defineProperty(screen, 'availWidth', {{
            get: () => {fingerprint.screen_width}
        }});
        
        Object.defineProperty(screen, 'availHeight', {{
            get: () => {fingerprint.screen_height - 40}  // Account for taskbar
        }});
        
        Object.defineProperty(screen, 'colorDepth', {{
            get: () => {fingerprint.color_depth}
        }});
        
        // Override WebGL to prevent fingerprinting
        const getParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(parameter) {{
            if (parameter === 37445) {{
                return '{fingerprint.webgl_vendor}';
            }}
            if (parameter === 37446) {{
                return '{fingerprint.webgl_renderer}';
            }}
            return getParameter.call(this, parameter);
        }};
        
        const getParameter2 = WebGL2RenderingContext.prototype.getParameter;
        WebGL2RenderingContext.prototype.getParameter = function(parameter) {{
            if (parameter === 37445) {{
                return '{fingerprint.webgl_vendor}';
            }}
            if (parameter === 37446) {{
                return '{fingerprint.webgl_renderer}';
            }}
            return getParameter2.call(this, parameter);
        }};
        
        // Override Canvas fingerprinting
        const toBlob = HTMLCanvasElement.prototype.toBlob;
        const toDataURL = HTMLCanvasElement.prototype.toDataURL;
        const getImageData = CanvasRenderingContext2D.prototype.getImageData;
        
        // Add noise to canvas (subtle, to avoid detection)
        const noise = () => Math.random() * 0.0001;
        
        HTMLCanvasElement.prototype.toBlob = function(callback, type, quality) {{
            const context = this.getContext('2d');
            if (context) {{
                const imageData = context.getImageData(0, 0, this.width, this.height);
                for (let i = 0; i < imageData.data.length; i += 4) {{
                    imageData.data[i] += noise();
                }}
                context.putImageData(imageData, 0, 0);
            }}
            return toBlob.call(this, callback, type, quality);
        }};
        
        HTMLCanvasElement.prototype.toDataURL = function(type, quality) {{
            const context = this.getContext('2d');
            if (context) {{
                const imageData = context.getImageData(0, 0, this.width, this.height);
                for (let i = 0; i < imageData.data.length; i += 4) {{
                    imageData.data[i] += noise();
                }}
                context.putImageData(imageData, 0, 0);
            }}
            return toDataURL.call(this, type, quality);
        }};
        
        // Override WebRTC to prevent IP leakage
        const RTCPeerConnection = window.RTCPeerConnection || window.mozRTCPeerConnection || window.webkitRTCPeerConnection;
        if (RTCPeerConnection) {{
            const originalCreateOffer = RTCPeerConnection.prototype.createOffer;
            RTCPeerConnection.prototype.createOffer = function() {{
                return Promise.resolve({{}});
            }};
        }}
        
        // Override timezone
        Date.prototype.getTimezoneOffset = function() {{
            // Return offset for {fingerprint.timezone}
            // This is a simplified version - full implementation would calculate actual offset
            return 0;
        }};
        
        // Remove automation flags
        Object.defineProperty(navigator, 'webdriver', {{
            get: () => undefined
        }});
        
        // Chrome specific
        window.chrome = {{
            runtime: {{}}
        }};
        
        // Permissions API
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => (
            parameters.name === 'notifications' ?
                Promise.resolve({{ state: Notification.permission }}) :
                originalQuery(parameters)
        );
        """
        return script
    
    def get_http_headers(self, fingerprint: BrowserFingerprint) -> Dict[str, str]:
        """
        Generate HTTP headers that match the fingerprint (enhanced for Cloudflare evasion)
        
        Args:
            fingerprint: BrowserFingerprint to use
            
        Returns:
            Dictionary of HTTP headers
        """
        headers = {
            "User-Agent": fingerprint.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": f"{fingerprint.locale},{fingerprint.language};q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0"
        }
        
        # Add Chrome-specific headers if Chrome user agent
        if "Chrome" in fingerprint.user_agent:
            headers["sec-ch-ua"] = self._extract_ch_ua(fingerprint.user_agent)
            headers["sec-ch-ua-mobile"] = "?0"
            headers["sec-ch-ua-platform"] = f'"{fingerprint.platform}"'
        
        return headers
    
    def _extract_ch_ua(self, user_agent: str) -> str:
        """Extract sec-ch-ua header from user agent"""
        # Simplified version - in production, parse actual UA
        if "Chrome" in user_agent:
            version = user_agent.split("Chrome/")[1].split(".")[0] if "Chrome/" in user_agent else "120"
            return f'"Google Chrome";v="{version}", "Chromium";v="{version}", "Not A Brand";v="8"'
        return ""

