"""
Unit tests for ZapImoveisService using mocked Playwright
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path

from src.services.zap_imoveis_service import ZapImoveisService
from src.core.human_behavior import HumanBehavior


@pytest.mark.asyncio
class TestPriceExtraction:
    """Tests for price extraction methods"""
    
    async def test_parse_price_text_valid_formats(self, mock_page):
        """Test parsing various price text formats"""
        service = ZapImoveisService(mock_page)
        
        # Test standard format
        assert service.extractor._parse_price_text("R$ 440.000") == 440000.0
        assert service.extractor._parse_price_text("R$ 1.500.000") == 1500000.0
        assert service.extractor._parse_price_text("R$ 20.998.000") == 20998000.0
        
        # Test with spaces
        assert service.extractor._parse_price_text("R$ 440.000") == 440000.0
        assert service.extractor._parse_price_text("R$440.000") == 440000.0
        
        # Test with comma as decimal
        assert service.extractor._parse_price_text("R$ 440.000,50") == 440000.5
    
    async def test_parse_price_text_invalid(self, mock_page):
        """Test parsing invalid price text"""
        service = ZapImoveisService(mock_page)
        
        assert service.extractor._parse_price_text("") is None
        assert service.extractor._parse_price_text("440.000") is None  # Missing R$
        assert service.extractor._parse_price_text("R$ abc") is None
        assert service.extractor._parse_price_text("R$ 5") is None  # Too low
        assert service.extractor._parse_price_text("R$ 2000000000") is None  # Too high
    
    async def test_extract_price_from_selectors_success(self, mock_page, mock_element):
        """Test price extraction from selectors"""
        service = ZapImoveisService(mock_page)
        mock_element.text_content = AsyncMock(return_value="R$ 440.000")
        mock_page.query_selector = AsyncMock(return_value=mock_element)
        
        price = await service.extractor._extract_price_from_selectors()
        assert price == 440000.0
    
    async def test_extract_price_from_selectors_not_found(self, mock_page):
        """Test price extraction when selectors don't match"""
        service = ZapImoveisService(mock_page)
        mock_page.query_selector = AsyncMock(return_value=None)
        
        price = await service.extractor._extract_price_from_selectors()
        assert price is None
    
    async def test_extract_price_from_content_success(self, mock_page):
        """Test price extraction from page content"""
        service = ZapImoveisService(mock_page)
        mock_page.content = AsyncMock(return_value='<html><body>Price: R$ 440.000</body></html>')
        
        price = await service.extractor._extract_price_from_content()
        assert price == 440000.0
    
    async def test_extract_price_from_content_not_found(self, mock_page):
        """Test price extraction when not in content"""
        service = ZapImoveisService(mock_page)
        mock_page.content = AsyncMock(return_value='<html><body>No price here</body></html>')
        
        price = await service.extractor._extract_price_from_content()
        assert price is None
    
    async def test_extract_price_success_from_selectors(self, mock_page, mock_element):
        """Test extract_price using selectors"""
        service = ZapImoveisService(mock_page)
        mock_element.text_content = AsyncMock(return_value="R$ 440.000")
        mock_page.query_selector = AsyncMock(return_value=mock_element)
        
        price = await service.extractor.extract_price()
        assert price == 440000.0
    
    async def test_extract_price_fallback_to_content(self, mock_page):
        """Test extract_price fallback to content"""
        service = ZapImoveisService(mock_page)
        mock_page.query_selector = AsyncMock(return_value=None)
        mock_page.content = AsyncMock(return_value='<html><body>R$ 440.000</body></html>')
        
        price = await service.extractor.extract_price()
        assert price == 440000.0
    
    async def test_extract_price_not_found(self, mock_page):
        """Test extract_price when price is not found"""
        service = ZapImoveisService(mock_page)
        mock_page.query_selector = AsyncMock(return_value=None)
        mock_page.content = AsyncMock(return_value='<html><body>No price</body></html>')
        
        price = await service.extractor.extract_price()
        assert price is None


