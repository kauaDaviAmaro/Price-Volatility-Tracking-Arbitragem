"""
Proxy Manager - Elite Proxy Rotation System
Manages proxy pools, rotation, and health tracking
"""
import random
import asyncio
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class ProxyType(Enum):
    """Types of proxies available"""
    RESIDENTIAL = "residential"
    MOBILE = "mobile"
    DATACENTER = "datacenter"
    ROTATING = "rotating"


@dataclass
class Proxy:
    """Represents a single proxy configuration"""
    host: str
    port: int
    username: Optional[str] = None
    password: Optional[str] = None
    proxy_type: ProxyType = ProxyType.DATACENTER
    protocol: str = "http"  # http, https, socks5
    success_count: int = 0
    failure_count: int = 0
    last_used: Optional[float] = None
    is_active: bool = True
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate of the proxy"""
        total = self.success_count + self.failure_count
        if total == 0:
            return 1.0
        return self.success_count / total
    
    @property
    def server(self) -> str:
        """Get proxy server string for Playwright"""
        if self.protocol == "socks5":
            return f"socks5://{self.host}:{self.port}"
        return f"{self.protocol}://{self.host}:{self.port}"
    
    def to_playwright_config(self) -> Dict:
        """Convert to Playwright proxy configuration"""
        config = {
            "server": self.server
        }
        if self.username and self.password:
            config["username"] = self.username
            config["password"] = self.password
        return config


class ProxyManager:
    """Manages proxy rotation and health tracking"""
    
    def __init__(
        self,
        proxies: Optional[List[Proxy]] = None,
        rotation_strategy: str = "round_robin",  # round_robin, random, least_used, best_performance
        max_failures: int = 3,
        cooldown_seconds: int = 300
    ):
        """
        Initialize ProxyManager
        
        Args:
            proxies: List of proxy configurations
            rotation_strategy: Strategy for proxy rotation
            max_failures: Maximum failures before marking proxy as inactive
            cooldown_seconds: Cooldown period for failed proxies
        """
        self.proxies: List[Proxy] = proxies or []
        self.rotation_strategy = rotation_strategy
        self.max_failures = max_failures
        self.cooldown_seconds = cooldown_seconds
        self.current_index = 0
        self._lock = asyncio.Lock()
        
    def add_proxy(
        self,
        host: str,
        port: int,
        username: Optional[str] = None,
        password: Optional[str] = None,
        proxy_type: ProxyType = ProxyType.DATACENTER,
        protocol: str = "http"
    ) -> Proxy:
        """
        Add a new proxy to the pool
        
        Returns:
            The created Proxy object
        """
        proxy = Proxy(
            host=host,
            port=port,
            username=username,
            password=password,
            proxy_type=proxy_type,
            protocol=protocol
        )
        self.proxies.append(proxy)
        logger.info(f"Added proxy: {host}:{port} ({proxy_type.value})")
        return proxy
    
    def load_proxies_from_config(self, proxy_configs: List[Dict]) -> None:
        """
        Load proxies from configuration list
        
        Args:
            proxy_configs: List of proxy configuration dicts
                Format: [{"host": "...", "port": ..., "username": "...", "password": "...", "type": "..."}, ...]
        """
        for config in proxy_configs:
            proxy_type = ProxyType(config.get("type", "datacenter"))
            self.add_proxy(
                host=config["host"],
                port=config["port"],
                username=config.get("username"),
                password=config.get("password"),
                proxy_type=proxy_type,
                protocol=config.get("protocol", "http")
            )
    
    async def get_proxy(self, preferred_type: Optional[ProxyType] = None) -> Optional[Proxy]:
        """
        Get next proxy based on rotation strategy
        
        Args:
            preferred_type: Preferred proxy type (if available)
            
        Returns:
            Proxy object or None if no active proxies available
        """
        async with self._lock:
            active_proxies = self._get_active_proxies(preferred_type)
            
            if not active_proxies:
                logger.warning("No active proxies available")
                return None
            
            proxy = self._select_proxy(active_proxies)
            proxy.last_used = asyncio.get_event_loop().time()
            return proxy
    
    def _get_active_proxies(self, preferred_type: Optional[ProxyType] = None) -> List[Proxy]:
        """Get list of active proxies, optionally filtered by type"""
        current_time = asyncio.get_event_loop().time()
        active = []
        
        for proxy in self.proxies:
            # Check if proxy is marked as active
            if not proxy.is_active:
                # Check if cooldown period has passed
                if proxy.last_used and (current_time - proxy.last_used) > self.cooldown_seconds:
                    proxy.is_active = True
                    proxy.failure_count = 0
                    logger.info(f"Proxy {proxy.host}:{proxy.port} reactivated after cooldown")
                else:
                    continue
            
            # Filter by type if specified
            if preferred_type and proxy.proxy_type != preferred_type:
                continue
                
            active.append(proxy)
        
        return active
    
    def _select_proxy(self, proxies: List[Proxy]) -> Proxy:
        """Select proxy based on rotation strategy"""
        if not proxies:
            raise ValueError("No proxies available")
        
        if self.rotation_strategy == "random":
            return random.choice(proxies)
        elif self.rotation_strategy == "round_robin":
            proxy = proxies[self.current_index % len(proxies)]
            self.current_index += 1
            return proxy
        elif self.rotation_strategy == "least_used":
            return min(proxies, key=lambda p: (p.success_count + p.failure_count))
        elif self.rotation_strategy == "best_performance":
            return max(proxies, key=lambda p: p.success_rate)
        else:
            return random.choice(proxies)
    
    async def mark_success(self, proxy: Proxy) -> None:
        """Mark proxy request as successful"""
        async with self._lock:
            proxy.success_count += 1
            proxy.is_active = True
    
    async def mark_failure(self, proxy: Proxy) -> None:
        """Mark proxy request as failed"""
        async with self._lock:
            proxy.failure_count += 1
            
            if proxy.failure_count >= self.max_failures:
                proxy.is_active = False
                logger.warning(f"Proxy {proxy.host}:{proxy.port} marked as inactive after {proxy.failure_count} failures")
    
    def get_stats(self) -> Dict:
        """Get statistics about proxy pool"""
        total = len(self.proxies)
        active = sum(1 for p in self.proxies if p.is_active)
        
        stats = {
            "total_proxies": total,
            "active_proxies": active,
            "inactive_proxies": total - active,
            "by_type": {}
        }
        
        for proxy_type in ProxyType:
            type_proxies = [p for p in self.proxies if p.proxy_type == proxy_type]
            stats["by_type"][proxy_type.value] = {
                "total": len(type_proxies),
                "active": sum(1 for p in type_proxies if p.is_active),
                "avg_success_rate": sum(p.success_rate for p in type_proxies) / len(type_proxies) if type_proxies else 0
            }
        
        return stats

