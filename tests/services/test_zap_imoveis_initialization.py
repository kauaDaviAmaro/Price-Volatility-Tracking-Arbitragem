"""
Unit tests for ZapImoveisService using mocked Playwright
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path

from src.services.zap_imoveis_service import ZapImoveisService
from src.core.human_behavior import HumanBehavior


@pytest.mark.asyncio
class TestInitialization:
    """Tests for service initialization"""
    
    async def test_init_with_page(self, mock_page):
        """Test service initialization with page"""
        service = ZapImoveisService(mock_page)
        assert service.page == mock_page
        assert service.human_behavior is not None
        assert isinstance(service.human_behavior, HumanBehavior)
    
    async def test_init_with_custom_human_behavior(self, mock_page, mock_human_behavior):
        """Test service initialization with custom human_behavior"""
        service = ZapImoveisService(mock_page, mock_human_behavior)
        assert service.page == mock_page
        assert service.human_behavior == mock_human_behavior
    
    async def test_init_creates_default_human_behavior(self, mock_page):
        """Test that default human_behavior is created when None is provided"""
        service = ZapImoveisService(mock_page, None)
        assert service.human_behavior is not None
        assert isinstance(service.human_behavior, HumanBehavior)


