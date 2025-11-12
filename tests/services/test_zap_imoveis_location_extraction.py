"""
Unit tests for ZapImoveisService using mocked Playwright
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path

from src.services.zap_imoveis_service import ZapImoveisService
from src.core.human_behavior import HumanBehavior


@pytest.mark.asyncio
class TestLocationExtraction:
    """Tests for location extraction"""
    
    async def test_extract_location_with_both_parts(self, mock_page, mock_element):
        """Test location extraction with both location and street"""
        service = ZapImoveisService(mock_page)
        
        # Mock location element
        location_elem = AsyncMock()
        location_elem.text_content = AsyncMock(return_value="Vila Nossa Senhora das Graças, Franca")
        
        # Mock street element
        street_elem = AsyncMock()
        street_elem.text_content = AsyncMock(return_value="Rua Osório Arantes")
        
        mock_page.query_selector = AsyncMock(side_effect=[location_elem, street_elem])
        
        location = await service.extractor.extract_location()
        assert location == "Vila Nossa Senhora das Graças, Franca, Rua Osório Arantes"
    
    async def test_extract_location_only_location(self, mock_page, mock_element):
        """Test location extraction with only location"""
        service = ZapImoveisService(mock_page)
        
        location_elem = AsyncMock()
        location_elem.text_content = AsyncMock(return_value="Vila Nossa Senhora das Graças, Franca")
        
        mock_page.query_selector = AsyncMock(side_effect=[location_elem, None])
        
        location = await service.extractor.extract_location()
        assert location == "Vila Nossa Senhora das Graças, Franca"
    
    async def test_extract_location_not_found(self, mock_page):
        """Test location extraction when not found"""
        service = ZapImoveisService(mock_page)
        mock_page.query_selector = AsyncMock(return_value=None)
        
        location = await service.extractor.extract_location()
        assert location is None


