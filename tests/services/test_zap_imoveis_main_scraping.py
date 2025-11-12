"""
Unit tests for ZapImoveisService using mocked Playwright
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path

from src.services.zap_imoveis_service import ZapImoveisService
from src.core.human_behavior import HumanBehavior


@pytest.mark.asyncio
class TestMainScraping:
    """Tests for main scraping methods"""
    
    @patch('src.services.zap_imoveis_service.Config')
    @patch('src.services.zap_imoveis_service.random')
    async def test_scrape_listing_success(self, mock_random, mock_config, mock_page, mock_human_behavior):
        """Test successful listing scraping"""
        mock_config.HUMAN_BEHAVIOR_ENABLED = True
        mock_config.SCROLL_ENABLED = True
        mock_config.NAVIGATION_TIMEOUT = 30000
        mock_random.randint = MagicMock(return_value=300)
        
        service = ZapImoveisService(mock_page, mock_human_behavior)
        
        # Mock all extraction methods
        mock_page.goto = AsyncMock()
        mock_page.title = AsyncMock(return_value="Apartamento - Zap Imóveis")
        mock_page.query_selector = AsyncMock(return_value=None)
        mock_page.query_selector_all = AsyncMock(return_value=[])
        
        result = await service.scrape_listing("https://www.zapimoveis.com.br/imovel/test/")
        
        assert "url" in result
        assert result["url"] == "https://www.zapimoveis.com.br/imovel/test/"
        mock_page.goto.assert_called_once()
    
    @patch('src.services.zap_imoveis_service.Config')
    async def test_scrape_listing_error(self, mock_config, mock_page, mock_human_behavior):
        """Test listing scraping with error"""
        mock_config.HUMAN_BEHAVIOR_ENABLED = False
        mock_config.NAVIGATION_TIMEOUT = 30000
        
        service = ZapImoveisService(mock_page, mock_human_behavior)
        mock_page.goto = AsyncMock(side_effect=Exception("Navigation error"))
        
        result = await service.scrape_listing("https://www.zapimoveis.com.br/imovel/test/")
        
        assert "error" in result
        assert result["url"] == "https://www.zapimoveis.com.br/imovel/test/"
    
    @patch('src.services.zap_imoveis_service.Config')
    async def test_wait_for_page_load_with_human_behavior(self, mock_config, mock_page, mock_human_behavior):
        """Test wait_for_page_load with human behavior enabled"""
        mock_config.HUMAN_BEHAVIOR_ENABLED = True
        service = ZapImoveisService(mock_page, mock_human_behavior)
        
        await service.wait_for_page_load()
        mock_human_behavior.wait_for_page_with_behavior.assert_called_once_with(mock_page)
    
    @patch('src.services.zap_imoveis_service.Config')
    async def test_wait_for_page_load_without_human_behavior(self, mock_config, mock_page, mock_human_behavior):
        """Test wait_for_page_load without human behavior"""
        mock_config.HUMAN_BEHAVIOR_ENABLED = False
        service = ZapImoveisService(mock_page, mock_human_behavior)
        
        await service.wait_for_page_load()
        mock_page.wait_for_load_state.assert_called_once_with("networkidle")


