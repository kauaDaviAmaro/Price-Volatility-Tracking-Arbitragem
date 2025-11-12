"""
Unit tests for ZapImoveisService using mocked Playwright
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path

from src.services.zap_imoveis_service import ZapImoveisService
from src.core.human_behavior import HumanBehavior


@pytest.mark.asyncio
class TestImageExtraction:
    """Tests for image extraction methods"""
    
    async def test_normalize_image_url_absolute(self, mock_page):
        """Test URL normalization for absolute URLs"""
        service = ZapImoveisService(mock_page)
        url = "https://example.com/image.jpg"
        assert service.extractor._normalize_image_url(url) == url
    
    async def test_normalize_image_url_protocol_relative(self, mock_page):
        """Test URL normalization for protocol-relative URLs"""
        service = ZapImoveisService(mock_page)
        url = "//example.com/image.jpg"
        assert service.extractor._normalize_image_url(url) == "https://example.com/image.jpg"
    
    async def test_normalize_image_url_relative(self, mock_page):
        """Test URL normalization for relative URLs"""
        service = ZapImoveisService(mock_page)
        service.page.url = "https://www.zapimoveis.com.br/imovel/test"
        url = "/image.jpg"
        normalized = service.extractor._normalize_image_url(url)
        assert normalized == "https://www.zapimoveis.com.br/image.jpg"
    
    async def test_clean_image_url_with_dimension(self, mock_page):
        """Test URL cleaning removes dimension parameters"""
        service = ZapImoveisService(mock_page)
        url = "https://example.com/image.jpg?action=fit-in&dimension=614x297"
        cleaned = service.extractor._clean_image_url(url)
        assert cleaned == "https://example.com/image.jpg"
    
    async def test_clean_image_url_without_dimension(self, mock_page):
        """Test URL cleaning when no dimension parameter"""
        service = ZapImoveisService(mock_page)
        url = "https://example.com/image.jpg?other=param"
        cleaned = service.extractor._clean_image_url(url)
        assert cleaned == url
    
    async def test_extract_images_from_selectors_success(self, mock_page, mock_element):
        """Test image extraction from selectors"""
        service = ZapImoveisService(mock_page)
        service.page.url = "https://www.zapimoveis.com.br/imovel/test"
        
        img1 = AsyncMock()
        img1.get_attribute = AsyncMock(return_value="https://resizedimgs.zapimoveis.com.br/img1.jpg?dimension=614x297")
        img2 = AsyncMock()
        img2.get_attribute = AsyncMock(return_value="https://resizedimgs.zapimoveis.com.br/img2.jpg?dimension=614x297")
        
        # The method tries multiple selectors, so we need to mock for each one
        # Return images for first selector, empty for others
        call_count = [0]
        def query_selector_all_side_effect(selector):
            call_count[0] += 1
            if call_count[0] == 1:  # First selector
                return [img1, img2]
            return []  # Other selectors return empty
        
        mock_page.query_selector_all = AsyncMock(side_effect=query_selector_all_side_effect)
        
        images = await service.extractor._extract_images_from_selectors()
        assert len(images) == 2
        assert "img1.jpg" in images[0]
        assert "img2.jpg" in images[1]
        assert "dimension" not in images[0]  # Should be cleaned
    
    async def test_extract_images_from_meta_success(self, mock_page, mock_element):
        """Test image extraction from meta tags"""
        service = ZapImoveisService(mock_page)
        mock_element.get_attribute = AsyncMock(return_value="https://example.com/og-image.jpg")
        mock_page.query_selector = AsyncMock(return_value=mock_element)
        
        images = await service.extractor._extract_images_from_meta()
        assert len(images) == 1
        assert images[0] == "https://example.com/og-image.jpg"
    
    async def test_extract_images_from_meta_not_found(self, mock_page):
        """Test image extraction when meta tag not found"""
        service = ZapImoveisService(mock_page)
        mock_page.query_selector = AsyncMock(return_value=None)
        
        images = await service.extractor._extract_images_from_meta()
        assert images == []
    
    async def test_extract_images_success(self, mock_page, mock_element):
        """Test extract_images combining selectors and meta"""
        service = ZapImoveisService(mock_page)
        service.page.url = "https://www.zapimoveis.com.br/imovel/test"
        
        # Mock selector images
        img1 = AsyncMock()
        img1.get_attribute = AsyncMock(return_value="https://resizedimgs.zapimoveis.com.br/img1.jpg")
        mock_page.query_selector_all = AsyncMock(return_value=[img1])
        
        # Mock meta image
        meta_elem = AsyncMock()
        meta_elem.get_attribute = AsyncMock(return_value="https://example.com/og-image.jpg")
        mock_page.query_selector = AsyncMock(side_effect=[None, meta_elem])  # First call for selectors, second for meta
        
        images = await service.extractor.extract_images()
        assert len(images) >= 1
    
    async def test_extract_images_limit(self, mock_page, mock_element):
        """Test that extract_images limits to 20 images"""
        service = ZapImoveisService(mock_page)
        service.page.url = "https://www.zapimoveis.com.br/imovel/test"
        
        # Create 25 mock images
        images_list = []
        for i in range(25):
            img = AsyncMock()
            img.get_attribute = AsyncMock(return_value=f"https://example.com/img{i}.jpg")
            images_list.append(img)
        
        mock_page.query_selector_all = AsyncMock(return_value=images_list)
        mock_page.query_selector = AsyncMock(return_value=None)
        
        images = await service.extractor.extract_images()
        assert len(images) == 20


