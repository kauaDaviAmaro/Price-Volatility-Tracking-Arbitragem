"""
Unit tests for ZapImoveisService using mocked Playwright
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path

from src.services.zap_imoveis_service import ZapImoveisService
from src.core.human_behavior import HumanBehavior


@pytest.mark.asyncio
class TestDescriptionAndAmenitiesExtraction:
    """Tests for description and amenities extraction"""
    
    async def test_extract_description_from_meta_success(self, mock_page, mock_element):
        """Test description extraction from meta tag"""
        service = ZapImoveisService(mock_page)
        mock_element.get_attribute = AsyncMock(return_value="Apartamento espaçoso com 3 quartos")
        mock_page.query_selector = AsyncMock(return_value=mock_element)
        
        description = await service.extractor._extract_description_from_meta()
        assert description == "Apartamento espaçoso com 3 quartos"
    
    async def test_extract_description_from_selectors_success(self, mock_page, mock_element):
        """Test description extraction from selectors"""
        service = ZapImoveisService(mock_page)
        mock_element.text_content = AsyncMock(return_value="Apartamento espaçoso")
        mock_page.query_selector = AsyncMock(return_value=mock_element)
        
        description = await service.extractor._extract_description_from_selectors()
        assert description == "Apartamento espaçoso"
    
    async def test_extract_description_success(self, mock_page, mock_element):
        """Test extract_description using meta tag"""
        service = ZapImoveisService(mock_page)
        mock_element.get_attribute = AsyncMock(return_value="Apartamento espaçoso")
        mock_page.query_selector = AsyncMock(return_value=mock_element)
        
        description = await service.extractor.extract_description()
        assert description == "Apartamento espaçoso"
    
    async def test_extract_description_not_found(self, mock_page):
        """Test extract_description when not found"""
        service = ZapImoveisService(mock_page)
        mock_page.query_selector = AsyncMock(return_value=None)
        
        description = await service.extractor.extract_description()
        assert description is None
    
    async def test_extract_amenities_success(self, mock_page, mock_element):
        """Test amenities extraction"""
        service = ZapImoveisService(mock_page)
        
        amenity1 = AsyncMock()
        amenity1.text_content = AsyncMock(return_value="Piscina")
        amenity2 = AsyncMock()
        amenity2.text_content = AsyncMock(return_value="Academia")
        
        # The method tries multiple selectors, so we need to mock for each one
        call_count = [0]
        def query_selector_all_side_effect(selector):
            call_count[0] += 1
            if call_count[0] == 1:  # First selector
                return [amenity1, amenity2]
            return []  # Other selectors return empty
        
        mock_page.query_selector_all = AsyncMock(side_effect=query_selector_all_side_effect)
        
        amenities = await service.extractor.extract_amenities()
        assert len(amenities) == 2
        assert "Piscina" in amenities
        assert "Academia" in amenities
    
    async def test_extract_amenities_limit(self, mock_page, mock_element):
        """Test that amenities are limited to 30"""
        service = ZapImoveisService(mock_page)
        
        # Create 35 mock amenities
        amenities_list = []
        for i in range(35):
            amenity = AsyncMock()
            amenity.text_content = AsyncMock(return_value=f"Amenity {i}")
            amenities_list.append(amenity)
        
        mock_page.query_selector_all = AsyncMock(return_value=amenities_list)
        
        amenities = await service.extractor.extract_amenities()
        assert len(amenities) == 30
    
    async def test_extract_amenities_not_found(self, mock_page):
        """Test amenities extraction when not found"""
        service = ZapImoveisService(mock_page)
        mock_page.query_selector_all = AsyncMock(return_value=[])
        
        amenities = await service.extractor.extract_amenities()
        assert amenities == []


