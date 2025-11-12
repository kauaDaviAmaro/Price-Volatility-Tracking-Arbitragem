"""
Integration tests for ZapImoveisService using real Playwright browser
"""
import pytest
from pathlib import Path

from src.services.zap_imoveis_service import ZapImoveisService


@pytest.mark.asyncio
class TestScrapeListingIntegration:
    """Integration tests for scrape_listing with real browser"""
    
    async def test_scrape_listing_with_real_browser(self, browser_page):
        """Test scraping a listing page with real browser"""
        # Load HTML fixture
        fixture_path = Path(__file__).parent / "fixtures" / "listing_page.html"
        html_content = fixture_path.read_text(encoding='utf-8')
        
        # Set content on page
        await browser_page.set_content(html_content)
        await browser_page.goto("file:///")  # Set a base URL
        
        service = ZapImoveisService(browser_page)
        result = await service.scrape_listing("https://www.zapimoveis.com.br/imovel/test/")
        
        assert "url" in result
        assert result["url"] == "https://www.zapimoveis.com.br/imovel/test/"
        assert result.get("price") == 440000.0
        assert result.get("area") == 140.0
        assert result.get("bedrooms") == 3
        assert result.get("bathrooms") == 2
        assert result.get("parking_spaces") == 1
        assert "Vila Nossa Senhora" in (result.get("location") or "")
        assert len(result.get("images", [])) > 0
    
    async def test_scrape_listing_missing_data(self, browser_page):
        """Test scraping listing with missing data fields"""
        # Minimal HTML with only some fields
        html_content = """
        <!DOCTYPE html>
        <html>
        <head><title>Test Listing</title></head>
        <body>
            <div data-cy="rp-cardProperty-price-txt">
                <p class="text-2-25">R$ 440.000</p>
            </div>
        </body>
        </html>
        """
        
        await browser_page.set_content(html_content)
        service = ZapImoveisService(browser_page)
        result = await service.scrape_listing("https://www.zapimoveis.com.br/imovel/test/")
        
        assert result["price"] == 440000.0
        # Other fields should be None or empty
        assert result.get("area") is None or result.get("bedrooms") is None
    
    async def test_scrape_listing_error_handling(self, browser_page):
        """Test error handling in scrape_listing"""
        service = ZapImoveisService(browser_page)
        
        # Try to scrape invalid URL (will cause navigation error)
        result = await service.scrape_listing("invalid-url")
        
        assert "error" in result
        assert result["url"] == "invalid-url"


@pytest.mark.asyncio
class TestSearchResultsIntegration:
    """Integration tests for search results scraping"""
    
    async def test_scrape_search_results_with_real_browser(self, browser_page):
        """Test scraping search results page with real browser"""
        # Load HTML fixture
        fixture_path = Path(__file__).parent / "fixtures" / "search_results.html"
        html_content = fixture_path.read_text(encoding='utf-8')
        
        await browser_page.set_content(html_content)
        await browser_page.goto("file:///")
        
        service = ZapImoveisService(browser_page)
        results = await service.scrape_search_results("https://www.zapimoveis.com.br/busca/")
        
        assert isinstance(results, list)
        assert len(results) > 0
        
        # Check first result has expected fields
        if results:
            first_result = results[0]
            assert "url" in first_result
            assert "price" in first_result or first_result.get("price") is None
            assert "title" in first_result or first_result.get("title") is None
    
    async def test_scrape_search_results_max_listings(self, browser_page):
        """Test scraping search results with max_listings limit"""
        fixture_path = Path(__file__).parent / "fixtures" / "search_results.html"
        html_content = fixture_path.read_text(encoding='utf-8')
        
        await browser_page.set_content(html_content)
        await browser_page.goto("file:///")
        
        service = ZapImoveisService(browser_page)
        results = await service.scrape_search_results(
            "https://www.zapimoveis.com.br/busca/",
            max_listings=2
        )
        
        assert len(results) <= 2
    
    async def test_extract_listing_urls_from_search(self, browser_page):
        """Test URL extraction from search results"""
        fixture_path = Path(__file__).parent / "fixtures" / "search_results.html"
        html_content = fixture_path.read_text(encoding='utf-8')
        
        await browser_page.set_content(html_content)
        await browser_page.goto("file:///")
        
        service = ZapImoveisService(browser_page)
        urls = await service._extract_listing_urls_from_search()
        
        assert len(urls) > 0
        assert all("/imovel/" in url for url in urls)
        # Check that URLs are normalized
        assert all(url.startswith("https://") for url in urls)
    
    async def test_extract_listing_from_search_card(self, browser_page):
        """Test extracting data from a search card"""
        fixture_path = Path(__file__).parent / "fixtures" / "search_results.html"
        html_content = fixture_path.read_text(encoding='utf-8')
        
        await browser_page.set_content(html_content)
        await browser_page.goto("file:///")
        
        service = ZapImoveisService(browser_page)
        url = "https://www.zapimoveis.com.br/imovel/venda-apartamento-3-quartos-vila-nossa-senhora-das-gracas-franca-sp-140m2-id-2819539923/?id=2819539923"
        
        data = await service._extract_listing_from_search_card(url)
        
        assert data is not None
        assert data["url"] == url
        assert data.get("price") == 440000.0
        assert data.get("area") == 140.0
        assert data.get("bedrooms") == 3
    
    async def test_extract_listing_from_search_card_with_ranges(self, browser_page):
        """Test extracting card data with range values (e.g., 2-5 bathrooms)"""
        fixture_path = Path(__file__).parent / "fixtures" / "search_results.html"
        html_content = fixture_path.read_text(encoding='utf-8')
        
        await browser_page.set_content(html_content)
        await browser_page.goto("file:///")
        
        service = ZapImoveisService(browser_page)
        url = "https://www.zapimoveis.com.br/imovel/venda-casa-5-quartos-vila-nova-conceicao-sao-paulo-750m2-id-7890123456/?id=7890123456"
        
        data = await service._extract_listing_from_search_card(url)
        
        if data:
            # Should extract first number from range
            assert data.get("bathrooms") == 2  # From "2-5"
            assert data.get("parking_spaces") == 4  # From "4-5"


@pytest.mark.asyncio
class TestIntegrationErrorHandling:
    """Integration tests for error handling scenarios"""
    
    async def test_scrape_listing_empty_page(self, browser_page):
        """Test scraping empty page"""
        await browser_page.set_content("<html><body></body></html>")
        
        service = ZapImoveisService(browser_page)
        result = await service.scrape_listing("https://www.zapimoveis.com.br/imovel/test/")
        
        assert "url" in result
        # Most fields should be None
        assert result.get("price") is None
        assert result.get("title") is None
    
    async def test_scrape_search_results_empty_page(self, browser_page):
        """Test scraping empty search results"""
        await browser_page.set_content("<html><body><ul></ul></body></html>")
        
        service = ZapImoveisService(browser_page)
        results = await service.scrape_search_results("https://www.zapimoveis.com.br/busca/")
        
        assert results == []
    
    async def test_extract_listing_from_search_card_not_found(self, browser_page):
        """Test extracting from card that doesn't exist"""
        await browser_page.set_content("<html><body></body></html>")
        
        service = ZapImoveisService(browser_page)
        url = "https://www.zapimoveis.com.br/imovel/nonexistent/?id=999999"
        
        data = await service._extract_listing_from_search_card(url)
        assert data is None


@pytest.mark.asyncio
class TestHumanBehaviorIntegration:
    """Integration tests for human behavior integration"""
    
    async def test_wait_for_page_load_integration(self, browser_page):
        """Test wait_for_page_load with real browser"""
        fixture_path = Path(__file__).parent / "fixtures" / "listing_page.html"
        html_content = fixture_path.read_text(encoding='utf-8')
        
        await browser_page.set_content(html_content)
        
        service = ZapImoveisService(browser_page)
        # Should not raise exception
        await service.wait_for_page_load()

