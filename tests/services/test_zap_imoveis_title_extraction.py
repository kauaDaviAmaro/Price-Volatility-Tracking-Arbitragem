"""
Unit tests for ZapImoveisService using mocked Playwright
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path

from src.services.zap_imoveis_service import ZapImoveisService
from src.core.human_behavior import HumanBehavior


@pytest.mark.asyncio
class TestTitleExtraction:
    """Tests for title extraction methods"""
    
    async def test_extract_title_from_meta_success(self, mock_page, mock_element):
        """Test title extraction from meta tag"""
        service = ZapImoveisService(mock_page)
        mock_element.get_attribute = AsyncMock(return_value="Apartamento 3 quartos, 140m²")
        mock_page.query_selector = AsyncMock(return_value=mock_element)
        
        title = await service.extractor._extract_title_from_meta()
        assert title == "Apartamento 3 quartos, 140m²"
    
    async def test_extract_title_from_meta_not_found(self, mock_page):
        """Test title extraction when meta tag not found"""
        service = ZapImoveisService(mock_page)
        mock_page.query_selector = AsyncMock(return_value=None)
        
        title = await service.extractor._extract_title_from_meta()
        assert title is None
    
    async def test_extract_title_from_selectors_success(self, mock_page, mock_element):
        """Test title extraction from HTML selectors"""
        service = ZapImoveisService(mock_page)
        mock_element.text_content = AsyncMock(return_value="Apartamento 3 quartos")
        mock_page.query_selector = AsyncMock(return_value=mock_element)
        
        title = await service.extractor._extract_title_from_selectors()
        assert title == "Apartamento 3 quartos"
    
    async def test_extract_title_from_selectors_not_found(self, mock_page):
        """Test title extraction when selectors don't match"""
        service = ZapImoveisService(mock_page)
        mock_page.query_selector = AsyncMock(return_value=None)
        
        title = await service.extractor._extract_title_from_selectors()
        assert title is None
    
    async def test_extract_title_success_from_meta(self, mock_page, mock_element):
        """Test extract_title using meta tag"""
        service = ZapImoveisService(mock_page)
        mock_element.get_attribute = AsyncMock(return_value="Apartamento 3 quartos")
        mock_page.query_selector = AsyncMock(return_value=mock_element)
        
        title = await service.extractor.extract_title()
        assert title == "Apartamento 3 quartos"
    
    async def test_extract_title_fallback_to_selectors(self, mock_page, mock_element):
        """Test extract_title fallback to selectors"""
        service = ZapImoveisService(mock_page)
        # Meta returns None
        mock_page.query_selector = AsyncMock(side_effect=[None, mock_element])
        mock_element.text_content = AsyncMock(return_value="Apartamento 3 quartos")
        
        title = await service.extractor.extract_title()
        assert title == "Apartamento 3 quartos"
    
    async def test_extract_title_fallback_to_page_title(self, mock_page):
        """Test extract_title fallback to page title"""
        service = ZapImoveisService(mock_page)
        mock_page.query_selector = AsyncMock(return_value=None)
        mock_page.title = AsyncMock(return_value="Apartamento - Imóvel")
        
        title = await service.extractor.extract_title()
        assert title == "Apartamento - Imóvel"
    
    async def test_extract_title_not_found(self, mock_page):
        """Test extract_title when title is not found"""
        service = ZapImoveisService(mock_page)
        mock_page.query_selector = AsyncMock(return_value=None)
        mock_page.title = AsyncMock(return_value="Zap Imóveis")
        
        title = await service.extractor.extract_title()
        assert title is None


