"""
Pagination handling for Zap Imóveis search results
"""
from playwright.async_api import Page
from typing import Optional
import re
import logging

from src.services.zap_imoveis.selectors import SELECTOR_PROPERTY_CARD, PAGE_PARAM

logger = logging.getLogger(__name__)


class PaginationHandler:
    """Handles pagination logic for search results"""
    
    def __init__(self, page: Page):
        self.page = page
    
    def _build_page_url(self, base_url: str, page: int) -> str:
        """
        Builds a paginated URL from base URL
        
        Args:
            base_url: Base search URL
            page: Page number
            
        Returns:
            str: URL with page parameter
        """
        # Remove existing page parameter if present
        if '?' in base_url:
            base, params = base_url.split('?', 1)
            # Remove page parameter if exists
            params_list = [p for p in params.split('&') if not p.startswith(PAGE_PARAM)]
            if params_list:
                base_url = f"{base}?{'&'.join(params_list)}"
            else:
                base_url = base
        
        # Add page parameter
        separator = '?' if '?' not in base_url else '&'
        return f"{base_url}{separator}{PAGE_PARAM}{page}"
    
    async def _extract_page_from_link(self, link) -> Optional[int]:
        """Extract page number from a pagination link"""
        try:
            text = await link.text_content()
            href = await link.get_attribute('href')
            
            # Try to extract page number from text
            if text:
                page_match = re.search(r'(\d+)', text.strip())
                if page_match:
                    return int(page_match.group(1))
            
            # Try to extract page number from href
            if href and PAGE_PARAM in href:
                page_match = re.search(rf'{PAGE_PARAM}(\d+)', href)
                if page_match:
                    return int(page_match.group(1))
        except Exception:
            pass
        return None
    
    async def _find_pagination_element(self) -> Optional[object]:
        """Find pagination element on the page"""
        pagination_selectors = [
            '[data-cy="pagination"]',
            '.pagination',
            '[class*="pagination"]',
            'nav[aria-label*="pagination"]',
            'nav[aria-label*="Pagination"]',
        ]
        
        for selector in pagination_selectors:
            try:
                pagination = await self.page.query_selector(selector)
                if pagination:
                    return pagination
            except Exception:
                continue
        return None
    
    async def _get_pages_from_pagination(self, pagination) -> Optional[int]:
        """Extract max page number from pagination element"""
        try:
            page_links = await pagination.query_selector_all('a, button')
            max_page = 1
            
            for link in page_links:
                page_num = await self._extract_page_from_link(link)
                if page_num:
                    max_page = max(max_page, page_num)
            
            return max_page if max_page > 1 else None
        except Exception:
            return None
    
    async def _get_pages_from_next_button(self) -> Optional[int]:
        """Try to detect total pages by finding next button and parsing pagination text"""
        next_selectors = [
            'a[aria-label*="Próxima"]',
            'a[aria-label*="próxima"]',
            'a[aria-label*="Next"]',
            'a[aria-label*="next"]',
            'button[aria-label*="Próxima"]',
            'button[aria-label*="Next"]',
            'a:has-text("Próxima")',
            'a:has-text(">")',
        ]
        
        for selector in next_selectors:
            try:
                next_button = await self.page.query_selector(selector)
                if next_button:
                    pagination_text = await self.page.evaluate("""
                        () => {
                            const pagination = document.querySelector('[data-cy="pagination"], .pagination, [class*="pagination"]');
                            if (!pagination) return null;
                            return pagination.textContent;
                        }
                    """)
                    
                    if pagination_text:
                        page_numbers = re.findall(r'\b(\d+)\b', pagination_text)
                        if page_numbers:
                            return max(int(p) for p in page_numbers)
            except Exception:
                continue
        return None
    
    async def get_total_pages(self) -> Optional[int]:
        """
        Detects the total number of pages available in search results
        
        Returns:
            Optional[int]: Total number of pages or None if not found
        """
        try:
            # Try to find pagination elements
            pagination = await self._find_pagination_element()
            if pagination:
                max_page = await self._get_pages_from_pagination(pagination)
                if max_page:
                    return max_page
            
            # Fallback: check if there's a "next" button
            max_page = await self._get_pages_from_next_button()
            if max_page:
                return max_page
            
            # If no pagination found, assume only 1 page
            return 1
            
        except Exception as e:
            logger.debug(f"Error detecting total pages: {e}")
            return 1
    
    def extract_page_from_url(self, url: str) -> Optional[int]:
        """Extract page number from URL if present"""
        if PAGE_PARAM in url:
            page_match = re.search(rf'{PAGE_PARAM}(\d+)', url)
            if page_match:
                return int(page_match.group(1))
        return None
    
    def build_base_url(self, search_url: str) -> str:
        """Build base URL without page parameter"""
        base_url = search_url.split('?')[0] if '?' in search_url else search_url
        if '?' in search_url:
            params = search_url.split('?', 1)[1]
            params_list = [p for p in params.split('&') if not p.startswith(PAGE_PARAM)]
            if params_list:
                base_url = f"{base_url}?{'&'.join(params_list)}"
        return base_url
    
    def build_page_url(self, base_url: str, page: int) -> str:
        """Build URL for a specific page number"""
        return self._build_page_url(base_url, page)

