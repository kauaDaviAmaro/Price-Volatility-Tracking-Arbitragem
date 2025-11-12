"""
Unit tests for ZapImoveisService using mocked Playwright
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path

from src.services.zap_imoveis_service import ZapImoveisService
from src.core.human_behavior import HumanBehavior


@pytest.mark.asyncio
class TestPaginationMethods:
    """Tests for pagination-related methods"""
    
    async def test_build_page_url_no_params(self, mock_page):
        """Test building page URL from base URL without params"""
        service = ZapImoveisService(mock_page)
        base_url = "https://www.zapimoveis.com.br/busca/"
        page_url = service.pagination.build_page_url(base_url, 2)
        assert page_url == "https://www.zapimoveis.com.br/busca/?page=2"
    
    async def test_build_page_url_with_params(self, mock_page):
        """Test building page URL from base URL with existing params"""
        service = ZapImoveisService(mock_page)
        base_url = "https://www.zapimoveis.com.br/busca/?city=São Paulo"
        page_url = service.pagination.build_page_url(base_url, 2)
        assert "page=2" in page_url
        assert "city=São Paulo" in page_url
    
    async def test_build_page_url_replace_existing_page(self, mock_page):
        """Test building page URL replacing existing page param"""
        service = ZapImoveisService(mock_page)
        base_url = "https://www.zapimoveis.com.br/busca/?page=1&city=São Paulo"
        page_url = service.pagination.build_page_url(base_url, 3)
        assert "page=3" in page_url
        assert "page=1" not in page_url
    
    async def test_get_total_pages_from_pagination(self, mock_page, mock_element):
        """Test getting total pages from pagination element"""
        service = ZapImoveisService(mock_page)
        
        # Mock pagination element with page links
        page_link1 = AsyncMock()
        page_link1.text_content = AsyncMock(return_value="1")
        page_link2 = AsyncMock()
        page_link2.text_content = AsyncMock(return_value="2")
        page_link3 = AsyncMock()
        page_link3.text_content = AsyncMock(return_value="3")
        
        pagination = AsyncMock()
        pagination.query_selector_all = AsyncMock(return_value=[page_link1, page_link2, page_link3])
        
        mock_page.query_selector = AsyncMock(return_value=pagination)
        
        total_pages = await service.pagination.get_total_pages()
        assert total_pages == 3
    
    async def test_get_total_pages_from_href(self, mock_page, mock_element):
        """Test getting total pages from href attributes"""
        from src.services.zap_imoveis.selectors import PAGE_PARAM
        service = ZapImoveisService(mock_page)
        
        page_link = AsyncMock()
        page_link.text_content = AsyncMock(return_value="Next")
        page_link.get_attribute = AsyncMock(return_value=f"/busca/?{PAGE_PARAM}5")
        
        pagination = AsyncMock()
        pagination.query_selector_all = AsyncMock(return_value=[page_link])
        
        mock_page.query_selector = AsyncMock(return_value=pagination)
        
        total_pages = await service.pagination.get_total_pages()
        assert total_pages == 5
    
    async def test_get_total_pages_no_pagination(self, mock_page):
        """Test getting total pages when no pagination found"""
        service = ZapImoveisService(mock_page)
        mock_page.query_selector = AsyncMock(return_value=None)
        
        total_pages = await service.pagination.get_total_pages()
        assert total_pages == 1
    
    @patch('src.services.zap_imoveis_service.Config')
    async def test_scrape_single_page_success(self, mock_config, mock_page, mock_human_behavior):
        """Test scraping a single page"""
        mock_config.HUMAN_BEHAVIOR_ENABLED = False
        mock_config.SCROLL_ENABLED = False
        mock_config.NAVIGATION_TIMEOUT = 30000
        
        service = ZapImoveisService(mock_page, mock_human_behavior)
        
        # Mock URL extraction
        link = AsyncMock()
        link.get_attribute = AsyncMock(return_value="/imovel/venda-apartamento-id-123/?id=123")
        mock_page.query_selector_all = AsyncMock(return_value=[link])
        
        # Mock card extraction
        card = AsyncMock()
        title_elem = AsyncMock()
        title_elem.text_content = AsyncMock(return_value="Apartamento em Vila")
        price_elem = AsyncMock()
        price_elem.text_content = AsyncMock(return_value="R$ 440.000")
        mock_page.query_selector = AsyncMock(side_effect=[card, title_elem, price_elem, price_elem])
        
        results = await service._scrape_single_page("https://www.zapimoveis.com.br/busca/?page=1")
        assert isinstance(results, list)
    
    @patch('src.services.zap_imoveis_service.Config')
    async def test_scrape_single_page_error(self, mock_config, mock_page, mock_human_behavior):
        """Test scraping single page with error"""
        mock_config.HUMAN_BEHAVIOR_ENABLED = False
        mock_config.NAVIGATION_TIMEOUT = 30000
        
        service = ZapImoveisService(mock_page, mock_human_behavior)
        mock_page.goto = AsyncMock(side_effect=Exception("Navigation error"))
        
        results = await service._scrape_single_page("https://www.zapimoveis.com.br/busca/?page=1")
        assert results == []
    
    @patch('src.services.zap_imoveis_service.Config')
    async def test_scrape_search_results_with_max_pages(self, mock_config, mock_page, mock_human_behavior):
        """Test scraping search results with max_pages parameter"""
        mock_config.HUMAN_BEHAVIOR_ENABLED = False
        mock_config.SCROLL_ENABLED = False
        mock_config.NAVIGATION_TIMEOUT = 30000
        
        service = ZapImoveisService(mock_page, mock_human_behavior)
        
        # Mock pagination detection
        pagination = AsyncMock()
        page_link = AsyncMock()
        page_link.text_content = AsyncMock(return_value="5")
        pagination.query_selector_all = AsyncMock(return_value=[page_link])
        mock_page.query_selector = AsyncMock(return_value=pagination)
        
        # Mock URL extraction
        link = AsyncMock()
        link.get_attribute = AsyncMock(return_value="/imovel/venda-apartamento-id-123/?id=123")
        mock_page.query_selector_all = AsyncMock(return_value=[link])
        
        # Mock card extraction
        card = AsyncMock()
        title_elem = AsyncMock()
        title_elem.text_content = AsyncMock(return_value="Apartamento")
        price_elem = AsyncMock()
        price_elem.text_content = AsyncMock(return_value="R$ 440.000")
        
        # Setup side_effect for multiple calls
        def query_selector_side_effect(selector):
            if 'pagination' in selector or '[data-cy="pagination"]' in selector:
                return pagination
            return card
        
        mock_page.query_selector = AsyncMock(side_effect=query_selector_side_effect)
        
        results = await service.scrape_search_results(
            "https://www.zapimoveis.com.br/busca/",
            max_pages=2
        )
        assert isinstance(results, list)
    
    @patch('src.services.zap_imoveis_service.Config')
    async def test_scrape_search_results_with_specific_page(self, mock_config, mock_page, mock_human_behavior):
        """Test scraping search results with specific page in URL"""
        mock_config.HUMAN_BEHAVIOR_ENABLED = False
        mock_config.SCROLL_ENABLED = False
        mock_config.NAVIGATION_TIMEOUT = 30000
        
        service = ZapImoveisService(mock_page, mock_human_behavior)
        
        # Mock URL extraction
        link = AsyncMock()
        link.get_attribute = AsyncMock(return_value="/imovel/venda-apartamento-id-123/?id=123")
        mock_page.query_selector_all = AsyncMock(return_value=[link])
        
        # Mock card extraction
        card = AsyncMock()
        title_elem = AsyncMock()
        title_elem.text_content = AsyncMock(return_value="Apartamento")
        price_elem = AsyncMock()
        price_elem.text_content = AsyncMock(return_value="R$ 440.000")
        mock_page.query_selector = AsyncMock(side_effect=[card, title_elem, price_elem, price_elem])
        
        results = await service.scrape_search_results(
            "https://www.zapimoveis.com.br/busca/?page=2"
        )
        assert isinstance(results, list)

