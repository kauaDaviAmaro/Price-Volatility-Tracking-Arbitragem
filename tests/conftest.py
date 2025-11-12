"""
Pytest configuration and shared fixtures for Zap Imóveis Service tests
"""
import pytest
from unittest.mock import AsyncMock, MagicMock
from pathlib import Path
from playwright.async_api import async_playwright

from src.core.human_behavior import HumanBehavior


@pytest.fixture
def mock_page():
    """Create a mocked Playwright Page object for unit tests"""
    page = AsyncMock()
    page.url = "https://www.zapimoveis.com.br/imovel/venda/apartamento/"
    page.query_selector = AsyncMock()
    page.query_selector_all = AsyncMock()
    page.content = AsyncMock()
    page.title = AsyncMock()
    page.goto = AsyncMock()
    page.wait_for_load_state = AsyncMock()
    page.evaluate = AsyncMock()
    return page


@pytest.fixture
def mock_element():
    """Create a mocked element for query_selector results"""
    element = AsyncMock()
    element.text_content = AsyncMock()
    element.get_attribute = AsyncMock()
    element.query_selector = AsyncMock()
    element.query_selector_all = AsyncMock()
    return element


@pytest.fixture
def mock_human_behavior():
    """Create a mocked HumanBehavior instance"""
    behavior = MagicMock(spec=HumanBehavior)
    behavior.wait_for_page_with_behavior = AsyncMock()
    behavior.random_delay = AsyncMock()
    behavior.scroll_page = AsyncMock()
    return behavior


@pytest.fixture
def sample_listing_html():
    """Sample HTML for a listing page"""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Apartamento para comprar - Zap Imóveis</title>
        <meta property="og:title" content="Apartamento 3 quartos, 140m² - Vila Nossa Senhora das Graças">
        <meta property="og:description" content="Apartamento com 3 quartos à venda">
        <meta property="og:image" content="https://resizedimgs.zapimoveis.com.br/image1.jpg">
    </head>
    <body>
        <h1>Apartamento 3 quartos, 140m²</h1>
        <div data-cy="rp-cardProperty-price-txt">
            <p class="text-2-25 text-neutral-120 font-semibold">R$ 440.000</p>
        </div>
        <div data-cy="rp-cardProperty-location-txt">
            Vila Nossa Senhora das Graças, Franca
        </div>
        <div data-cy="rp-cardProperty-street-txt">
            Rua Osório Arantes
        </div>
        <div data-cy="rp-cardProperty-propertyArea-txt">
            <h3>140 m²</h3>
        </div>
        <div data-cy="rp-cardProperty-bedroomQuantity-txt">
            <h3>3</h3>
        </div>
        <div data-cy="rp-cardProperty-bathroomQuantity-txt">
            <h3>2</h3>
        </div>
        <div data-cy="rp-cardProperty-parkingSpacesQuantity-txt">
            <h3>1</h3>
        </div>
        <div data-cy="rp-cardProperty-image-img">
            <img src="https://resizedimgs.zapimoveis.com.br/image1.jpg?dimension=614x297">
            <img src="https://resizedimgs.zapimoveis.com.br/image2.jpg?dimension=614x297">
        </div>
        <div data-testid="description">
            <p>Apartamento espaçoso com 3 quartos...</p>
        </div>
    </body>
    </html>
    """


@pytest.fixture
def sample_search_results_html():
    """Sample HTML for search results page"""
    return """
    <!DOCTYPE html>
    <html>
    <body>
        <ul>
            <li data-cy="rp-property-cd">
                <a href="https://www.zapimoveis.com.br/imovel/venda-apartamento-3-quartos-id-123456/?id=123456">
                    <div data-cy="rp-cardProperty-image-img">
                        <img src="https://resizedimgs.zapimoveis.com.br/img1.jpg?dimension=614x297">
                    </div>
                    <div data-cy="rp-cardProperty-location-txt">
                        <span>Apartamento para comprar com 140 m², 3 quartos em</span>
                        Vila Nossa Senhora das Graças, Franca
                    </div>
                    <div data-cy="rp-cardProperty-street-txt">Rua Osório Arantes</div>
                    <div data-cy="rp-cardProperty-price-txt">
                        <p class="text-2-25">R$ 440.000</p>
                    </div>
                    <div data-cy="rp-cardProperty-propertyArea-txt"><h3>140 m²</h3></div>
                    <div data-cy="rp-cardProperty-bedroomQuantity-txt"><h3>3</h3></div>
                    <div data-cy="rp-cardProperty-bathroomQuantity-txt"><h3>2</h3></div>
                    <div data-cy="rp-cardProperty-parkingSpacesQuantity-txt"><h3>1</h3></div>
                </a>
            </li>
            <li data-cy="rp-property-cd">
                <a href="https://www.zapimoveis.com.br/imovel/venda-casa-5-quartos-id-789012/?id=789012">
                    <div data-cy="rp-cardProperty-price-txt">
                        <p class="text-2-25">R$ 20.998.000</p>
                    </div>
                    <div data-cy="rp-cardProperty-propertyArea-txt"><h3>750 m²</h3></div>
                    <div data-cy="rp-cardProperty-bedroomQuantity-txt"><h3>5</h3></div>
                    <div data-cy="rp-cardProperty-bathroomQuantity-txt"><h3>2-5</h3></div>
                    <div data-cy="rp-cardProperty-parkingSpacesQuantity-txt"><h3>4-5</h3></div>
                </a>
            </li>
        </ul>
    </body>
    </html>
    """


@pytest.fixture
def sample_listing_data():
    """Sample extracted listing data structure"""
    return {
        "url": "https://www.zapimoveis.com.br/imovel/venda-apartamento-3-quartos-id-123456/",
        "title": "Apartamento 3 quartos, 140m²",
        "price": 440000.0,
        "location": "Vila Nossa Senhora das Graças, Franca, Rua Osório Arantes",
        "property_type": "Apartamento",
        "area": 140.0,
        "bedrooms": 3,
        "bathrooms": 2,
        "parking_spaces": 1,
        "images": [
            "https://resizedimgs.zapimoveis.com.br/image1.jpg",
            "https://resizedimgs.zapimoveis.com.br/image2.jpg"
        ],
        "description": "Apartamento espaçoso com 3 quartos...",
        "amenities": ["Piscina", "Academia", "Churrasqueira"]
    }


@pytest.fixture
async def browser_page():
    """Create a real Playwright browser page for integration tests"""
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        yield page
        await browser.close()


# Configure pytest-asyncio
pytest_plugins = ('pytest_asyncio',)

