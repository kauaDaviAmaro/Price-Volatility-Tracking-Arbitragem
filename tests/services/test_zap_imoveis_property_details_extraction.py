"""
Unit tests for ZapImoveisService using mocked Playwright
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path

from src.services.zap_imoveis_service import ZapImoveisService
from src.core.human_behavior import HumanBehavior


@pytest.mark.asyncio
class TestPropertyDetailsExtraction:
    """Tests for property details extraction (area, bedrooms, bathrooms, parking)"""
    
    async def test_extract_area_success(self, mock_page, mock_element):
        """Test area extraction"""
        service = ZapImoveisService(mock_page)
        mock_element.text_content = AsyncMock(return_value="140 m²")
        mock_page.query_selector = AsyncMock(return_value=mock_element)
        
        area = await service.extractor.extract_area()
        assert area == 140.0
    
    async def test_extract_area_with_decimal(self, mock_page, mock_element):
        """Test area extraction with decimal"""
        service = ZapImoveisService(mock_page)
        mock_element.text_content = AsyncMock(return_value="140.5 m²")
        mock_page.query_selector = AsyncMock(return_value=mock_element)
        
        area = await service.extractor.extract_area()
        assert area == 140.5
    
    async def test_extract_area_not_found(self, mock_page):
        """Test area extraction when not found"""
        service = ZapImoveisService(mock_page)
        mock_page.query_selector = AsyncMock(return_value=None)
        
        area = await service.extractor.extract_area()
        assert area is None
    
    async def test_extract_bedrooms_success(self, mock_page, mock_element):
        """Test bedrooms extraction"""
        service = ZapImoveisService(mock_page)
        mock_element.text_content = AsyncMock(return_value="3")
        mock_page.query_selector = AsyncMock(return_value=mock_element)
        
        bedrooms = await service.extractor.extract_bedrooms()
        assert bedrooms == 3
    
    async def test_extract_bedrooms_not_found(self, mock_page):
        """Test bedrooms extraction when not found"""
        service = ZapImoveisService(mock_page)
        mock_page.query_selector = AsyncMock(return_value=None)
        
        bedrooms = await service.extractor.extract_bedrooms()
        assert bedrooms is None
    
    async def test_extract_bathrooms_success(self, mock_page, mock_element):
        """Test bathrooms extraction"""
        service = ZapImoveisService(mock_page)
        mock_element.text_content = AsyncMock(return_value="2")
        mock_page.query_selector = AsyncMock(return_value=mock_element)
        
        bathrooms = await service.extractor.extract_bathrooms()
        assert bathrooms == 2
    
    async def test_extract_bathrooms_with_range(self, mock_page, mock_element):
        """Test bathrooms extraction with range (takes first number)"""
        service = ZapImoveisService(mock_page)
        mock_element.text_content = AsyncMock(return_value="2-5")
        mock_page.query_selector = AsyncMock(return_value=mock_element)
        
        bathrooms = await service.extractor.extract_bathrooms()
        assert bathrooms == 2
    
    async def test_extract_bathrooms_not_found(self, mock_page):
        """Test bathrooms extraction when not found"""
        service = ZapImoveisService(mock_page)
        mock_page.query_selector = AsyncMock(return_value=None)
        
        bathrooms = await service.extractor.extract_bathrooms()
        assert bathrooms is None
    
    async def test_extract_parking_spaces_success(self, mock_page, mock_element):
        """Test parking spaces extraction"""
        service = ZapImoveisService(mock_page)
        mock_element.text_content = AsyncMock(return_value="1")
        mock_page.query_selector = AsyncMock(return_value=mock_element)
        
        parking = await service.extractor.extract_parking_spaces()
        assert parking == 1
    
    async def test_extract_parking_spaces_with_range(self, mock_page, mock_element):
        """Test parking spaces extraction with range"""
        service = ZapImoveisService(mock_page)
        mock_element.text_content = AsyncMock(return_value="4-5")
        mock_page.query_selector = AsyncMock(return_value=mock_element)
        
        parking = await service.extractor.extract_parking_spaces()
        assert parking == 4
    
    async def test_extract_parking_spaces_not_found(self, mock_page):
        """Test parking spaces extraction when not found"""
        service = ZapImoveisService(mock_page)
        mock_page.query_selector = AsyncMock(return_value=None)
        
        parking = await service.extractor.extract_parking_spaces()
        assert parking is None
    
    async def test_extract_property_type_success(self, mock_page, mock_element):
        """Test property type extraction"""
        service = ZapImoveisService(mock_page)
        mock_element.text_content = AsyncMock(return_value="Apartamento")
        mock_page.query_selector = AsyncMock(return_value=mock_element)
        
        prop_type = await service.extractor.extract_property_type()
        assert prop_type == "Apartamento"
    
    async def test_extract_property_type_not_found(self, mock_page):
        """Test property type extraction when not found"""
        service = ZapImoveisService(mock_page)
        mock_page.query_selector = AsyncMock(return_value=None)
        
        prop_type = await service.extractor.extract_property_type()
        assert prop_type is None


