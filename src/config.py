"""
Configuration System - Centralized Configuration Management
Supports environment variables and configuration files
"""
import os
from typing import List, Dict, Optional
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """Centralized configuration management"""
    
    # Browser Configuration
    HEADLESS: bool = os.getenv("HEADLESS", "True").lower() == "true"
    BROWSER_TYPE: str = os.getenv("BROWSER_TYPE", "chromium")  # chromium, firefox, webkit
    
    # Proxy Configuration
    PROXY_ENABLED: bool = os.getenv("PROXY_ENABLED", "False").lower() == "true"
    PROXY_ROTATION_STRATEGY: str = os.getenv("PROXY_ROTATION_STRATEGY", "round_robin")
    PROXY_MAX_FAILURES: int = int(os.getenv("PROXY_MAX_FAILURES", "3"))
    PROXY_COOLDOWN_SECONDS: int = int(os.getenv("PROXY_COOLDOWN_SECONDS", "300"))
    
    # Proxy List (can be loaded from env or file)
    # Format: PROXY_1=host:port:username:password:type:protocol
    # Or use PROXY_CONFIG_FILE to load from JSON file
    PROXY_CONFIG_FILE: Optional[str] = os.getenv("PROXY_CONFIG_FILE")
    
    # Fingerprint Configuration
    FINGERPRINT_REGION: str = os.getenv("FINGERPRINT_REGION", "US")
    FINGERPRINT_ROTATION: bool = os.getenv("FINGERPRINT_ROTATION", "True").lower() == "true"
    
    # Human Behavior Configuration
    HUMAN_BEHAVIOR_ENABLED: bool = os.getenv("HUMAN_BEHAVIOR_ENABLED", "True").lower() == "true"
    MIN_DELAY: float = float(os.getenv("MIN_DELAY", "0.1"))
    MAX_DELAY: float = float(os.getenv("MAX_DELAY", "0.3"))
    SCROLL_DELAY_MIN: float = float(os.getenv("SCROLL_DELAY_MIN", "0.05"))
    SCROLL_DELAY_MAX: float = float(os.getenv("SCROLL_DELAY_MAX", "0.1"))
    MOUSE_MOVEMENT_ENABLED: bool = os.getenv("MOUSE_MOVEMENT_ENABLED", "False").lower() == "true"
    SCROLL_ENABLED: bool = os.getenv("SCROLL_ENABLED", "True").lower() == "true"
    
    # Compliance Configuration
    RESPECT_ROBOTS_TXT: bool = os.getenv("RESPECT_ROBOTS_TXT", "True").lower() == "true"
    ROBOTS_CACHE_DIR: str = os.getenv("ROBOTS_CACHE_DIR", ".cache/robots")
    ROBOTS_CACHE_TTL: int = int(os.getenv("ROBOTS_CACHE_TTL", "3600"))
    
    # Retry Configuration
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
    RETRY_DELAY: float = float(os.getenv("RETRY_DELAY", "2.0"))
    RETRY_BACKOFF: float = float(os.getenv("RETRY_BACKOFF", "2.0"))
    
    # Timeout Configuration
    PAGE_LOAD_TIMEOUT: int = int(os.getenv("PAGE_LOAD_TIMEOUT", "30000"))
    NAVIGATION_TIMEOUT: int = int(os.getenv("NAVIGATION_TIMEOUT", "30000"))
    WAIT_UNTIL: str = os.getenv("WAIT_UNTIL", "domcontentloaded")  # domcontentloaded, load, networkidle
    
    # Page Processing Speed Configuration
    MIN_PAGE_DELAY: float = float(os.getenv("MIN_PAGE_DELAY", "2.0"))  # Minimum delay per page (seconds)
    MAX_PAGE_DELAY: float = float(os.getenv("MAX_PAGE_DELAY", "5.0"))  # Maximum delay per page (seconds)
    
    # Concurrency Configuration
    MAX_CONCURRENT: int = int(os.getenv("MAX_CONCURRENT", "3"))
    
    # Pagination Configuration
    MAX_PAGES: Optional[int] = int(os.getenv("MAX_PAGES")) if os.getenv("MAX_PAGES") else None
    
    # Output Configuration
    OUTPUT_DIR: str = os.getenv("OUTPUT_DIR", "data")
    SAVE_IMAGES: bool = os.getenv("SAVE_IMAGES", "True").lower() == "true"
    IMAGE_DOWNLOAD_DELAY: float = float(os.getenv("IMAGE_DOWNLOAD_DELAY", "0.2"))  # Delay between image downloads (seconds)
    LISTINGS_CSV_FILENAME: str = os.getenv("LISTINGS_CSV_FILENAME", "scraped_data.csv")  # CSV file for initial listings
    DEEP_SEARCH_CSV_FILENAME: str = os.getenv("DEEP_SEARCH_CSV_FILENAME", "deep_search_data.csv")  # CSV file for deep search results
    
    # Logging Configuration
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE: Optional[str] = os.getenv("LOG_FILE")
    
    @classmethod
    def load_proxies_from_env(cls) -> List[Dict]:
        """
        Load proxy configurations from environment variables
        
        Format: PROXY_1=host:port:username:password:type:protocol
        Or: PROXY_1=host:port (for no auth)
        
        Returns:
            List of proxy configuration dictionaries
        """
        proxies = []
        i = 1
        
        while True:
            proxy_str = os.getenv(f"PROXY_{i}")
            if not proxy_str:
                break
            
            parts = proxy_str.split(":")
            if len(parts) >= 2:
                host = parts[0]
                port = int(parts[1])
                username = parts[2] if len(parts) > 2 and parts[2] else None
                password = parts[3] if len(parts) > 3 and parts[3] else None
                proxy_type = parts[4] if len(parts) > 4 and parts[4] else "datacenter"
                protocol = parts[5] if len(parts) > 5 and parts[5] else "http"
                
                proxies.append({
                    "host": host,
                    "port": port,
                    "username": username,
                    "password": password,
                    "type": proxy_type,
                    "protocol": protocol
                })
            
            i += 1
        
        return proxies
    
    @classmethod
    def load_proxies_from_file(cls, file_path: Optional[str] = None) -> List[Dict]:
        """
        Load proxy configurations from JSON file
        
        Args:
            file_path: Path to JSON file (uses PROXY_CONFIG_FILE if not provided)
            
        Returns:
            List of proxy configuration dictionaries
        """
        import json
        
        file_path = file_path or cls.PROXY_CONFIG_FILE
        if not file_path:
            return []
        
        config_file = Path(file_path)
        if not config_file.exists():
            return []
        
        try:
            with open(config_file, "r") as f:
                data = json.load(f)
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict) and "proxies" in data:
                    return data["proxies"]
                else:
                    return []
        except Exception as e:
            print(f"Error loading proxy config from {file_path}: {e}")
            return []
    
    @classmethod
    def get_all_proxies(cls) -> List[Dict]:
        """
        Get all proxy configurations from env and file
        
        Returns:
            List of proxy configuration dictionaries
        """
        proxies = []
        
        # Load from file first
        if cls.PROXY_CONFIG_FILE:
            proxies.extend(cls.load_proxies_from_file())
        
        # Load from env (overrides file if same index)
        proxies.extend(cls.load_proxies_from_env())
        
        return proxies
    
    @classmethod
    def to_dict(cls) -> Dict:
        """Convert configuration to dictionary"""
        return {
            "browser": {
                "headless": cls.HEADLESS,
                "browser_type": cls.BROWSER_TYPE
            },
            "proxy": {
                "enabled": cls.PROXY_ENABLED,
                "rotation_strategy": cls.PROXY_ROTATION_STRATEGY,
                "max_failures": cls.PROXY_MAX_FAILURES,
                "cooldown_seconds": cls.PROXY_COOLDOWN_SECONDS
            },
            "fingerprint": {
                "region": cls.FINGERPRINT_REGION,
                "rotation": cls.FINGERPRINT_ROTATION
            },
            "human_behavior": {
                "enabled": cls.HUMAN_BEHAVIOR_ENABLED,
                "min_delay": cls.MIN_DELAY,
                "max_delay": cls.MAX_DELAY,
                "scroll_delay_min": cls.SCROLL_DELAY_MIN,
                "scroll_delay_max": cls.SCROLL_DELAY_MAX,
                "mouse_movement_enabled": cls.MOUSE_MOVEMENT_ENABLED,
                "scroll_enabled": cls.SCROLL_ENABLED
            },
            "compliance": {
                "respect_robots_txt": cls.RESPECT_ROBOTS_TXT,
                "robots_cache_dir": cls.ROBOTS_CACHE_DIR,
                "robots_cache_ttl": cls.ROBOTS_CACHE_TTL
            },
            "retry": {
                "max_retries": cls.MAX_RETRIES,
                "retry_delay": cls.RETRY_DELAY,
                "retry_backoff": cls.RETRY_BACKOFF
            },
            "timeout": {
                "page_load_timeout": cls.PAGE_LOAD_TIMEOUT,
                "navigation_timeout": cls.NAVIGATION_TIMEOUT,
                "wait_until": cls.WAIT_UNTIL,
                "min_page_delay": cls.MIN_PAGE_DELAY,
                "max_page_delay": cls.MAX_PAGE_DELAY
            },
            "concurrency": {
                "max_concurrent": cls.MAX_CONCURRENT
            },
            "pagination": {
                "max_pages": cls.MAX_PAGES
            },
            "output": {
                "output_dir": cls.OUTPUT_DIR,
                "save_images": cls.SAVE_IMAGES
            },
            "logging": {
                "log_level": cls.LOG_LEVEL,
                "log_file": cls.LOG_FILE
            }
        }

