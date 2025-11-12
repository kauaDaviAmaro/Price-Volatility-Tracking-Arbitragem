"""
Tests for HumanBehavior - delay distributions, mouse movements, scrolling behavior
"""
import pytest
import asyncio
from unittest.mock import AsyncMock, Mock, patch

from src.core.human_behavior import HumanBehavior


@pytest.mark.asyncio
class TestHumanBehaviorInitialization:
    """Tests for HumanBehavior initialization"""
    
    def test_init_with_defaults(self):
        """Test initialization with default parameters"""
        behavior = HumanBehavior()
        
        assert behavior.min_delay == 1.0
        assert behavior.max_delay == 4.0
        assert behavior.scroll_delay_min == 0.5
        assert behavior.scroll_delay_max == 2.0
        assert behavior.mouse_movement_enabled is True
        assert behavior.scroll_enabled is True
    
    def test_init_with_custom_params(self):
        """Test initialization with custom parameters"""
        behavior = HumanBehavior(
            min_delay=0.5,
            max_delay=2.0,
            scroll_delay_min=0.1,
            scroll_delay_max=1.0,
            mouse_movement_enabled=False,
            scroll_enabled=False
        )
        
        assert behavior.min_delay == 0.5
        assert behavior.max_delay == 2.0
        assert behavior.mouse_movement_enabled is False
        assert behavior.scroll_enabled is False


@pytest.mark.asyncio
class TestHumanBehaviorDelays:
    """Tests for delay behavior"""
    
    async def test_random_delay_with_defaults(self):
        """Test random delay with instance defaults"""
        behavior = HumanBehavior(min_delay=0.1, max_delay=0.2)
        
        start = asyncio.get_event_loop().time()
        await behavior.random_delay()
        elapsed = asyncio.get_event_loop().time() - start
        
        assert 0.1 <= elapsed <= 0.3  # Allow some margin
    
    async def test_random_delay_with_overrides(self):
        """Test random delay with parameter overrides"""
        behavior = HumanBehavior(min_delay=1.0, max_delay=2.0)
        
        start = asyncio.get_event_loop().time()
        await behavior.random_delay(min_seconds=0.05, max_seconds=0.1)
        elapsed = asyncio.get_event_loop().time() - start
        
        assert 0.05 <= elapsed <= 0.15  # Allow some margin
    
    async def test_random_delay_clamps_to_range(self):
        """Test that delay is clamped to min/max range"""
        behavior = HumanBehavior(min_delay=0.1, max_delay=0.2)
        
        # Should always be within range (gauss can go outside, but should be clamped)
        start = asyncio.get_event_loop().time()
        await behavior.random_delay()
        elapsed = asyncio.get_event_loop().time() - start
        
        assert elapsed >= 0.1
        assert elapsed <= 0.3  # Allow some margin for async overhead


@pytest.mark.asyncio
class TestHumanBehaviorMouseMovement:
    """Tests for mouse movement simulation"""
    
    async def test_move_mouse_disabled(self):
        """Test that mouse movement is skipped when disabled"""
        behavior = HumanBehavior(mouse_movement_enabled=False)
        mock_page = AsyncMock()
        
        await behavior.move_mouse(mock_page, 0, 0, 100, 100)
        
        # Should not call mouse.move
        mock_page.mouse.move.assert_not_called()
    
    async def test_move_mouse_enabled(self):
        """Test mouse movement when enabled"""
        behavior = HumanBehavior(mouse_movement_enabled=True)
        mock_page = AsyncMock()
        mock_page.mouse = AsyncMock()
        
        await behavior.move_mouse(mock_page, 0, 0, 100, 100)
        
        # Should call mouse.move multiple times (for curved path)
        assert mock_page.mouse.move.call_count > 0
    
    async def test_random_mouse_movement(self):
        """Test random mouse movement"""
        behavior = HumanBehavior(mouse_movement_enabled=True)
        mock_page = AsyncMock()
        mock_page.mouse = AsyncMock()
        
        await behavior.random_mouse_movement(mock_page, 1920, 1080)
        
        # Should have moved mouse
        assert mock_page.mouse.move.call_count > 0


@pytest.mark.asyncio
class TestHumanBehaviorScrolling:
    """Tests for scrolling behavior"""
    
    async def test_scroll_page_disabled(self):
        """Test that scrolling is skipped when disabled"""
        behavior = HumanBehavior(scroll_enabled=False)
        mock_page = AsyncMock()
        mock_page.viewport_size = {"width": 1920, "height": 1080}
        mock_page.mouse = AsyncMock()
        
        await behavior.scroll_page(mock_page)
        
        # Should not call mouse.wheel
        mock_page.mouse.wheel.assert_not_called()
    
    async def test_scroll_page_down(self):
        """Test scrolling down"""
        behavior = HumanBehavior(scroll_enabled=True)
        mock_page = AsyncMock()
        mock_page.viewport_size = {"width": 1920, "height": 1080}
        mock_page.mouse = AsyncMock()
        
        await behavior.scroll_page(mock_page, scroll_amount=500, direction="down")
        
        # Should call mouse.wheel with positive delta
        mock_page.mouse.wheel.assert_called()
        call_args = mock_page.mouse.wheel.call_args
        assert call_args[0][1] > 0  # Positive Y delta for down
    
    async def test_scroll_page_up(self):
        """Test scrolling up"""
        behavior = HumanBehavior(scroll_enabled=True)
        mock_page = AsyncMock()
        mock_page.viewport_size = {"width": 1920, "height": 1080}
        mock_page.mouse = AsyncMock()
        
        await behavior.scroll_page(mock_page, scroll_amount=500, direction="up")
        
        # Should call mouse.wheel with negative delta
        mock_page.mouse.wheel.assert_called()
        call_args = mock_page.mouse.wheel.call_args
        assert call_args[0][1] < 0  # Negative Y delta for up
    
    async def test_scroll_page_random_amount(self):
        """Test scrolling with random amount"""
        behavior = HumanBehavior(scroll_enabled=True)
        mock_page = AsyncMock()
        mock_page.viewport_size = {"width": 1920, "height": 1080}
        mock_page.mouse = AsyncMock()
        
        await behavior.scroll_page(mock_page, scroll_amount=None)
        
        # Should have scrolled
        mock_page.mouse.wheel.assert_called()
    
    async def test_scroll_to_bottom(self):
        """Test scrolling to bottom of page"""
        behavior = HumanBehavior(scroll_enabled=True)
        mock_page = AsyncMock()
        mock_page.viewport_size = {"width": 1920, "height": 1080}
        mock_page.mouse = AsyncMock()
        mock_page.evaluate = AsyncMock(return_value=5000)  # Page height
        
        await behavior.scroll_to_bottom(mock_page, pause_at_bottom=False)
        
        # Should have scrolled multiple times
        assert mock_page.mouse.wheel.call_count > 0
    
    async def test_scroll_to_bottom_disabled(self):
        """Test scroll_to_bottom when scrolling is disabled"""
        behavior = HumanBehavior(scroll_enabled=False)
        mock_page = AsyncMock()
        mock_page.viewport_size = {"width": 1920, "height": 1080}
        mock_page.mouse = AsyncMock()
        
        await behavior.scroll_to_bottom(mock_page)
        
        # Should not scroll
        mock_page.mouse.wheel.assert_not_called()


@pytest.mark.asyncio
class TestHumanBehaviorReading:
    """Tests for reading simulation"""
    
    async def test_simulate_reading(self):
        """Test reading simulation"""
        behavior = HumanBehavior()
        mock_page = AsyncMock()
        mock_page.viewport_size = {"width": 1920, "height": 1080}
        mock_page.mouse = AsyncMock()
        
        start = asyncio.get_event_loop().time()
        await behavior.simulate_reading(mock_page)
        elapsed = asyncio.get_event_loop().time() - start
        
        # Should have paused (reading takes time)
        assert elapsed >= 1.0  # At least some delay
    
    async def test_simulate_reading_with_element(self):
        """Test reading simulation with element selector"""
        behavior = HumanBehavior(mouse_movement_enabled=True)
        mock_page = AsyncMock()
        mock_page.viewport_size = {"width": 1920, "height": 1080}
        mock_page.mouse = AsyncMock()
        
        mock_element = AsyncMock()
        mock_element.bounding_box = AsyncMock(return_value={
            "x": 100, "y": 100, "width": 200, "height": 50
        })
        mock_page.query_selector = AsyncMock(return_value=mock_element)
        
        await behavior.simulate_reading(mock_page, element_selector=".content")
        
        # Should have moved mouse to element
        mock_page.query_selector.assert_called_with(".content")


@pytest.mark.asyncio
class TestHumanBehaviorClicking:
    """Tests for human-like clicking"""
    
    async def test_human_like_click(self):
        """Test human-like click"""
        behavior = HumanBehavior(mouse_movement_enabled=True)
        mock_page = AsyncMock()
        mock_page.viewport_size = {"width": 1920, "height": 1080}
        mock_page.mouse = AsyncMock()
        
        mock_element = AsyncMock()
        mock_element.bounding_box = AsyncMock(return_value={
            "x": 100, "y": 100, "width": 200, "height": 50
        })
        mock_page.query_selector = AsyncMock(return_value=mock_element)
        
        await behavior.human_like_click(mock_page, ".button")
        
        # Should have clicked
        mock_page.mouse.click.assert_called_once()
    
    async def test_human_like_click_element_not_found(self):
        """Test click when element not found"""
        behavior = HumanBehavior()
        mock_page = AsyncMock()
        mock_page.query_selector = AsyncMock(return_value=None)
        mock_page.click = AsyncMock()
        
        await behavior.human_like_click(mock_page, ".nonexistent")
        
        # When element not found, it returns early (doesn't call click)
        # Only calls click in exception handler
        mock_page.query_selector.assert_called_with(".nonexistent")
        mock_page.click.assert_not_called()
    
    async def test_human_like_click_without_mouse_movement(self):
        """Test click without mouse movement"""
        behavior = HumanBehavior(mouse_movement_enabled=False)
        mock_page = AsyncMock()
        mock_page.mouse = AsyncMock()
        
        mock_element = AsyncMock()
        mock_element.bounding_box = AsyncMock(return_value={
            "x": 100, "y": 100, "width": 200, "height": 50
        })
        mock_page.query_selector = AsyncMock(return_value=mock_element)
        
        await behavior.human_like_click(mock_page, ".button", move_mouse=False)
        
        # Should still click but not move mouse first
        mock_page.mouse.click.assert_called_once()


@pytest.mark.asyncio
class TestHumanBehaviorPageLoad:
    """Tests for page load waiting"""
    
    @patch('src.config.Config')
    async def test_wait_for_page_with_behavior(self, mock_config):
        """Test waiting for page load with behavior"""
        mock_config.WAIT_UNTIL = "domcontentloaded"
        
        behavior = HumanBehavior()
        mock_page = AsyncMock()
        mock_page.wait_for_load_state = AsyncMock()
        
        await behavior.wait_for_page_with_behavior(mock_page, timeout=30000)
        
        # Should wait for load state
        mock_page.wait_for_load_state.assert_called_once_with("domcontentloaded", timeout=30000)

