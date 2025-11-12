"""
Human Behavior Simulator - Elite Human-like Interaction System
Simulates realistic human behavior to avoid bot detection
"""
import random
import asyncio
import math
from typing import Optional, Tuple, List
from playwright.async_api import Page, Mouse
import logging

logger = logging.getLogger(__name__)


class HumanBehavior:
    """Simulates human-like behavior in browser interactions"""
    
    def __init__(
        self,
        min_delay: float = 1.0,
        max_delay: float = 4.0,
        scroll_delay_min: float = 0.5,
        scroll_delay_max: float = 2.0,
        mouse_movement_enabled: bool = True,
        scroll_enabled: bool = True
    ):
        """
        Initialize HumanBehavior
        
        Args:
            min_delay: Minimum delay between actions (seconds)
            max_delay: Maximum delay between actions (seconds)
            scroll_delay_min: Minimum delay between scroll steps (seconds)
            scroll_delay_max: Maximum delay between scroll steps (seconds)
            mouse_movement_enabled: Enable mouse movement simulation
            scroll_enabled: Enable scroll simulation
        """
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.scroll_delay_min = scroll_delay_min
        self.scroll_delay_max = scroll_delay_max
        self.mouse_movement_enabled = mouse_movement_enabled
        self.scroll_enabled = scroll_enabled
    
    async def random_delay(self, min_seconds: Optional[float] = None, max_seconds: Optional[float] = None) -> None:
        """
        Wait for a random amount of time (human-like delay)
        
        Args:
            min_seconds: Minimum delay (overrides instance default)
            max_seconds: Maximum delay (overrides instance default)
        """
        min_delay = min_seconds if min_seconds is not None else self.min_delay
        max_delay = max_seconds if max_seconds is not None else self.max_delay
        
        # Use normal distribution for more realistic delays (humans don't have uniform timing)
        delay = random.gauss(
            (min_delay + max_delay) / 2,
            (max_delay - min_delay) / 4
        )
        delay = max(min_delay, min(max_delay, delay))  # Clamp to range
        
        await asyncio.sleep(delay)
    
    async def move_mouse(self, page: Page, start_x: int, start_y: int, end_x: int, end_y: int) -> None:
        """
        Move mouse in a human-like curved path
        
        Args:
            page: Playwright page object
            start_x: Starting X coordinate
            start_y: Starting Y coordinate
            end_x: Ending X coordinate
            end_y: Ending Y coordinate
        """
        if not self.mouse_movement_enabled:
            return
        
        mouse = page.mouse
        
        # Calculate distance
        distance = math.sqrt((end_x - start_x) ** 2 + (end_y - start_y) ** 2)
        
        # Number of steps (more steps for longer distances, but not too many)
        num_steps = max(5, min(20, int(distance / 50)))
        
        # Generate curved path using Bezier curve control points
        # Add some randomness to the curve
        control_x = (start_x + end_x) / 2 + random.randint(-50, 50)
        control_y = (start_y + end_y) / 2 + random.randint(-50, 50)
        
        current_x, current_y = start_x, start_y
        
        for i in range(1, num_steps + 1):
            t = i / num_steps
            
            # Bezier curve interpolation
            x = (1 - t) ** 2 * start_x + 2 * (1 - t) * t * control_x + t ** 2 * end_x
            y = (1 - t) ** 2 * start_y + 2 * (1 - t) * t * control_y + t ** 2 * end_y
            
            # Add small random jitter (humans don't move perfectly)
            x += random.randint(-2, 2)
            y += random.randint(-2, 2)
            
            # Variable speed (slower at start and end, faster in middle)
            speed_factor = math.sin(t * math.pi)  # Creates smooth acceleration/deceleration
            step_delay = (0.01 + 0.02 * (1 - speed_factor)) * random.uniform(0.8, 1.2)
            
            await mouse.move(int(x), int(y))
            await asyncio.sleep(step_delay)
            
            current_x, current_y = int(x), int(y)
    
    async def random_mouse_movement(self, page: Page, viewport_width: int, viewport_height: int) -> None:
        """
        Perform random mouse movement within viewport
        
        Args:
            page: Playwright page object
            viewport_width: Viewport width
            viewport_height: Viewport height
        """
        if not self.mouse_movement_enabled:
            return
        
        # Random start and end positions
        start_x = random.randint(0, viewport_width)
        start_y = random.randint(0, viewport_height)
        end_x = random.randint(0, viewport_width)
        end_y = random.randint(0, viewport_height)
        
        await self.move_mouse(page, start_x, start_y, end_x, end_y)
    
    async def scroll_page(
        self,
        page: Page,
        scroll_amount: Optional[int] = None,
        direction: str = "down",
        smooth: bool = True
    ) -> None:
        """
        Scroll page in a human-like manner
        
        Args:
            page: Playwright page object
            scroll_amount: Amount to scroll (None for random)
            direction: "down", "up", or "random"
            smooth: Use smooth scrolling
        """
        if not self.scroll_enabled:
            return
        
        viewport_size = page.viewport_size
        if not viewport_size:
            return
        
        viewport_height = viewport_size["height"]
        
        # Determine scroll amount
        if scroll_amount is None:
            # Humans typically scroll 1-3 viewport heights at a time
            scroll_amount = random.randint(int(viewport_height * 0.3), int(viewport_height * 1.5))
        
        # Determine direction
        if direction == "random":
            direction = random.choice(["down", "up"])
        
        scroll_delta = scroll_amount if direction == "down" else -scroll_amount
        
        if smooth:
            # Smooth scroll: break into fewer steps for speed
            steps = random.randint(2, 3)  # Reduced from 3-8 to 2-3
            step_size = scroll_delta / steps
            
            for _ in range(steps):
                await page.mouse.wheel(0, step_size)
                # Minimal delay - just enough to allow rendering
                await asyncio.sleep(random.uniform(self.scroll_delay_min, self.scroll_delay_max))
        else:
            # Instant scroll
            await page.mouse.wheel(0, scroll_delta)
            # Minimal delay
            await asyncio.sleep(random.uniform(self.scroll_delay_min, self.scroll_delay_max))
    
    async def scroll_to_bottom(self, page: Page, pause_at_bottom: bool = True) -> None:
        """
        Scroll to bottom of page with human-like behavior
        
        Args:
            page: Playwright page object
            pause_at_bottom: Pause at bottom to simulate reading
        """
        if not self.scroll_enabled:
            return
        
        viewport_size = page.viewport_size
        if not viewport_size:
            return
        
        viewport_height = viewport_size["height"]
        
        # Get page height
        page_height = await page.evaluate("document.body.scrollHeight")
        current_scroll = 0
        
        while current_scroll < page_height:
            # Scroll a random amount (humans don't scroll consistently)
            scroll_amount = random.randint(int(viewport_height * 0.5), int(viewport_height * 1.2))
            current_scroll += scroll_amount
            
            await self.scroll_page(page, scroll_amount, "down", smooth=True)
            
            # Sometimes pause (like reading)
            if random.random() < 0.3:  # 30% chance
                await self.random_delay(0.5, 2.0)
        
        if pause_at_bottom:
            await self.random_delay(1.0, 3.0)
    
    async def simulate_reading(self, page: Page, element_selector: Optional[str] = None) -> None:
        """
        Simulate reading behavior (pauses and small movements)
        
        Args:
            page: Playwright page object
            element_selector: Optional selector to focus on
        """
        # Random pause (humans take time to read)
        reading_time = random.uniform(2.0, 5.0)
        
        if element_selector:
            try:
                element = await page.query_selector(element_selector)
                if element:
                    box = await element.bounding_box()
                    if box:
                        # Move mouse to element (like reading it)
                        center_x = int(box["x"] + box["width"] / 2)
                        center_y = int(box["y"] + box["height"] / 2)
                        
                        viewport = page.viewport_size
                        if viewport:
                            current_x = random.randint(0, viewport["width"])
                            current_y = random.randint(0, viewport["height"])
                            await self.move_mouse(page, current_x, current_y, center_x, center_y)
            except Exception as e:
                logger.debug(f"Error simulating reading on element: {e}")
        
        # Pause for reading
        await self.random_delay(reading_time * 0.5, reading_time)
        
        # Small random mouse movement (like following text)
        if self.mouse_movement_enabled and random.random() < 0.5:
            viewport = page.viewport_size
            if viewport:
                await self.random_mouse_movement(page, viewport["width"], viewport["height"])
    
    async def human_like_click(self, page: Page, selector: str, move_mouse: bool = True) -> None:
        """
        Perform human-like click (with mouse movement and delay)
        
        Args:
            page: Playwright page object
            selector: Element selector to click
            move_mouse: Whether to move mouse to element first
        """
        try:
            element = await page.query_selector(selector)
            if not element:
                logger.warning(f"Element not found: {selector}")
                return
            
            box = await element.bounding_box()
            if not box:
                logger.warning(f"Element has no bounding box: {selector}")
                return
            
            # Calculate click position (slightly random within element)
            click_x = int(box["x"] + box["width"] * random.uniform(0.2, 0.8))
            click_y = int(box["y"] + box["height"] * random.uniform(0.2, 0.8))
            
            if move_mouse and self.mouse_movement_enabled:
                # Get current mouse position (or random if unknown)
                viewport = page.viewport_size
                if viewport:
                    current_x = random.randint(0, viewport["width"])
                    current_y = random.randint(0, viewport["height"])
                    await self.move_mouse(page, current_x, current_y, click_x, click_y)
            
            # Small delay before click
            await self.random_delay(0.1, 0.3)
            
            # Click
            await page.mouse.click(click_x, click_y)
            
            # Small delay after click
            await self.random_delay(0.1, 0.2)
            
        except Exception as e:
            logger.error(f"Error in human_like_click: {e}")
            # Fallback to regular click
            await page.click(selector)
    
    async def wait_for_page_with_behavior(self, page: Page, timeout: int = 30000) -> None:
        """
        Wait for page load with human-like behavior
        
        Args:
            page: Playwright page object
            timeout: Maximum timeout in milliseconds
        """
        from src.config import Config
        # Use configurable wait strategy (default to domcontentloaded for speed)
        wait_strategy = getattr(Config, 'WAIT_UNTIL', 'domcontentloaded')
        
        # Use the configured wait strategy
        await page.wait_for_load_state(wait_strategy, timeout=timeout)
        
        # Minimal delay after page loads
        await self.random_delay(0.05, 0.1)