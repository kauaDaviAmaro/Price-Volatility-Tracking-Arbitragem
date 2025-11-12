"""
Data extraction methods for Zap Imóveis listings
"""
from playwright.async_api import Page
from typing import Optional, List, Dict, Any
import re
import logging

from src.services.zap_imoveis.selectors import (
    SELECTOR_LOCATION, SELECTOR_STREET, SELECTOR_PRICE, SELECTOR_AREA,
    SELECTOR_BEDROOMS, SELECTOR_BATHROOMS, SELECTOR_PARKING, SELECTOR_IMAGE,
    PATTERN_NUMBER
)

logger = logging.getLogger(__name__)


class DataExtractor:
    """Handles extraction of individual data fields from listing pages"""
    
    def __init__(self, page: Page):
        self.page = page
    
    def _parse_price_text(self, text: str, allow_small_values: bool = False) -> Optional[float]:
        """
        Parse price from text string
        
        Args:
            text: Text containing price (e.g., "R$ 320.000" or "R$ 306/mês")
            allow_small_values: If True, allows values < 10000 (for condo fees, IPTU, etc.)
        """
        if not text or 'R$' not in text:
            return None
        
        # Remove "R$" and clean the text
        price_text = text.replace('R$', '').strip()
        # Remove "/mês" or "/mês" for monthly fees
        price_text = re.sub(r'/\s*m[êe]s', '', price_text, flags=re.IGNORECASE)
        price_text = price_text.replace('.', '').replace(',', '.')
        
        price_match = re.search(r'[\d.]+', price_text)
        if price_match:
            try:
                price = float(price_match.group())
                if allow_small_values:
                    # For condo fees and IPTU, allow smaller values
                    if 0 <= price <= 10000000:  # Reasonable range for fees
                        return price
                else:
                    # For sale prices, use stricter range
                    if 10000 <= price <= 1000000000:
                        return price
            except ValueError:
                pass
        return None
    
    async def _extract_price_from_selectors(self) -> Optional[float]:
        """Try to extract price using CSS selectors"""
        price_selectors = [
            f'{SELECTOR_PRICE} p.text-2-25',
            f'{SELECTOR_PRICE} p',
            SELECTOR_PRICE,
            'p.text-2-25.text-neutral-120.font-semibold',
            'p:has-text("R$")',
        ]
        
        for selector in price_selectors:
            try:
                element = await self.page.query_selector(selector)
                if element:
                    text = await element.text_content()
                    price = self._parse_price_text(text or '')
                    if price:
                        return price
            except Exception:
                continue
        return None
    
    async def _extract_price_from_content(self) -> Optional[float]:
        """Fallback: extract price from page content"""
        try:
            page_text = await self.page.content()
            price_pattern = r'R\$\s*([\d.,]+)'
            matches = re.findall(price_pattern, page_text)
            
            for match in matches:
                try:
                    price_str = match.replace('.', '').replace(',', '.')
                    price = float(price_str)
                    if 10000 <= price <= 1000000000:
                        return price
                except ValueError:
                    continue
        except Exception:
            pass
        return None
    
    async def extract_price(self) -> Optional[float]:
        """
        Extracts the price from the current listing
        
        Returns:
            Optional[float]: Extracted price or None if not found
        """
        try:
            # Try selectors first
            price = await self._extract_price_from_selectors()
            if price:
                return price
            
            # Fallback to content search
            return await self._extract_price_from_content()
            
        except Exception as e:
            logger.debug(f"Error extracting price: {e}")
            return None
    
    async def _extract_title_from_meta(self) -> Optional[str]:
        """Extract title from meta tag"""
        try:
            element = await self.page.query_selector('meta[property="og:title"]')
            if element:
                title = await element.get_attribute('content')
                if title:
                    return title.strip()
        except Exception:
            pass
        return None
    
    async def _extract_title_from_selectors(self) -> Optional[str]:
        """Extract title from HTML selectors"""
        title_selectors = [
            'h1',
            '[data-testid="title"]',
            '[class*="title"]',
            '[class*="Title"]',
        ]
        
        for selector in title_selectors:
            try:
                element = await self.page.query_selector(selector)
                if element:
                    title = await element.text_content()
                    if title and title.strip():
                        return title.strip()
            except Exception:
                continue
        return None
    
    async def extract_title(self) -> Optional[str]:
        """
        Extracts the title from the current listing
        
        Returns:
            Optional[str]: Extracted title or None if not found
        """
        try:
            # Try meta tag first
            title = await self._extract_title_from_meta()
            if title:
                return title
            
            # Try HTML selectors
            title = await self._extract_title_from_selectors()
            if title:
                return title
            
            # Fallback: page title
            page_title = await self.page.title()
            if page_title and "Zap Imóveis" not in page_title:
                return page_title.strip()
            
            return None
            
        except Exception as e:
            logger.debug(f"Error extracting title: {e}")
            return None
    
    def _normalize_image_url(self, src: str) -> str:
        """Normalize image URL to absolute format"""
        if src.startswith('//'):
            return 'https:' + src
        elif src.startswith('/'):
            base_url = '/'.join(self.page.url.split('/')[:3])
            return base_url + src
        return src
    
    def _clean_image_url(self, src: str) -> str:
        """Remove dimension query parameters from image URL"""
        if '?' in src and 'dimension=' in src:
            return src.split('?')[0]
        return src
    
    async def _extract_images_from_selectors(self) -> List[str]:
        """Extract images using CSS selectors"""
        image_urls = []
        image_selectors = [
            f'{SELECTOR_IMAGE} img',
            '.olx-core-carousel img',
            'img[src*="resizedimgs.zapimoveis.com.br"]',
            'img[src*="zapimoveis"]',
            'img[alt*="Quartos"]',
        ]
        
        for selector in image_selectors:
            try:
                elements = await self.page.query_selector_all(selector)
                for element in elements:
                    src = await element.get_attribute('src')
                    if src and src not in image_urls:
                        src = self._normalize_image_url(src)
                        src = self._clean_image_url(src)
                        image_urls.append(src)
            except Exception:
                continue
        
        return image_urls
    
    async def _extract_images_from_meta(self) -> List[str]:
        """Extract images from meta tags"""
        try:
            og_image = await self.page.query_selector('meta[property="og:image"]')
            if og_image:
                og_src = await og_image.get_attribute('content')
                if og_src:
                    return [og_src]
        except Exception:
            pass
        return []
    
    async def extract_images(self) -> List[str]:
        """
        Extracts image URLs from the current listing
        
        Returns:
            List[str]: List of image URLs
        """
        try:
            image_urls = await self._extract_images_from_selectors()
            
            # Add meta tag images
            meta_images = await self._extract_images_from_meta()
            for img in meta_images:
                if img not in image_urls:
                    image_urls.append(img)
            
            return image_urls[:20]  # Limit to first 20 images
            
        except Exception as e:
            logger.debug(f"Error extracting images: {e}")
            return []
    
    async def extract_location(self) -> Optional[str]:
        """
        Extracts the location from the current listing
        
        Returns:
            Optional[str]: Extracted location or None if not found
        """
        try:
            location_parts = []
            
            # Get neighborhood/city from location
            location_elem = await self.page.query_selector(SELECTOR_LOCATION)
            if location_elem:
                location_text = await location_elem.text_content()
                if location_text:
                    # Extract the main location (after the description span)
                    location_text = location_text.strip()
                    location_parts.append(location_text)
            
            # Get street address
            street_elem = await self.page.query_selector(SELECTOR_STREET)
            if street_elem:
                street_text = await street_elem.text_content()
                if street_text:
                    location_parts.append(street_text.strip())
            
            if location_parts:
                return ', '.join(location_parts)
            
            # Fallback
            return None
            
        except Exception as e:
            logger.debug(f"Error extracting location: {e}")
            return None
    
    async def extract_property_type(self) -> Optional[str]:
        """
        Extracts the property type (Casa, Apartamento, etc.)
        
        Returns:
            Optional[str]: Property type or None if not found
        """
        try:
            property_type_selectors = [
                '[data-testid="property-type"]',
                '[class*="property-type"]',
                '[class*="tipo"]',
                'span:has-text("Casa")',
                'span:has-text("Apartamento")',
                'span:has-text("Terreno")'
            ]
            
            for selector in property_type_selectors:
                try:
                    element = await self.page.query_selector(selector)
                    if element:
                        text = await element.text_content()
                        if text:
                            return text.strip()
                except Exception:
                    continue
            
            return None
            
        except Exception as e:
            logger.debug(f"Error extracting property type: {e}")
            return None
    
    async def extract_area(self) -> Optional[float]:
        """
        Extracts the property area in m²
        
        Returns:
            Optional[float]: Area in square meters or None if not found
        """
        try:
            # Use data-cy attribute for area
            area_selectors = [
                SELECTOR_AREA,
                f'{SELECTOR_AREA} h3',
            ]
            
            for selector in area_selectors:
                try:
                    element = await self.page.query_selector(selector)
                    if element:
                        text = await element.text_content()
                        if text and 'm²' in text:
                            # Extract number from text (e.g., "140 m²" -> 140)
                            area_match = re.search(r'(\d+\.?\d*)\s*m²', text, re.IGNORECASE)
                            if area_match:
                                return float(area_match.group(1))
                except Exception:
                    continue
            
            return None
            
        except Exception as e:
            logger.debug(f"Error extracting area: {e}")
            return None
    
    async def extract_bedrooms(self) -> Optional[int]:
        """
        Extracts the number of bedrooms
        
        Returns:
            Optional[int]: Number of bedrooms or None if not found
        """
        try:
            # Use data-cy attribute for bedrooms
            bedroom_selectors = [
                SELECTOR_BEDROOMS,
                f'{SELECTOR_BEDROOMS} h3',
            ]
            
            for selector in bedroom_selectors:
                try:
                    element = await self.page.query_selector(selector)
                    if element:
                        text = await element.text_content()
                        if text:
                            # Extract number from text (just the number, e.g., "3" from "3")
                            bedroom_match = re.search(PATTERN_NUMBER, text)
                            if bedroom_match:
                                return int(bedroom_match.group(1))
                except Exception:
                    continue
            
            return None
            
        except Exception as e:
            logger.debug(f"Error extracting bedrooms: {e}")
            return None
    
    async def extract_bathrooms(self) -> Optional[int]:
        """
        Extracts the number of bathrooms
        
        Returns:
            Optional[int]: Number of bathrooms or None if not found
        """
        try:
            # Use data-cy attribute for bathrooms
            bathroom_selectors = [
                SELECTOR_BATHROOMS,
                f'{SELECTOR_BATHROOMS} h3',
            ]
            
            for selector in bathroom_selectors:
                try:
                    element = await self.page.query_selector(selector)
                    if element:
                        text = await element.text_content()
                        if text:
                            # Extract number from text (handle ranges like "2-5" by taking first number)
                            bathroom_match = re.search(PATTERN_NUMBER, text)
                            if bathroom_match:
                                return int(bathroom_match.group(1))
                except Exception:
                    continue
            
            return None
            
        except Exception as e:
            logger.debug(f"Error extracting bathrooms: {e}")
            return None
    
    async def extract_parking_spaces(self) -> Optional[int]:
        """
        Extracts the number of parking spaces
        
        Returns:
            Optional[int]: Number of parking spaces or None if not found
        """
        try:
            # Use data-cy attribute for parking spaces
            parking_selectors = [
                SELECTOR_PARKING,
                f'{SELECTOR_PARKING} h3',
            ]
            
            for selector in parking_selectors:
                try:
                    element = await self.page.query_selector(selector)
                    if element:
                        text = await element.text_content()
                        if text:
                            # Extract number from text (handle ranges like "4-5" by taking first number)
                            parking_match = re.search(PATTERN_NUMBER, text)
                            if parking_match:
                                return int(parking_match.group(1))
                except Exception:
                    continue
            
            return None
            
        except Exception as e:
            logger.debug(f"Error extracting parking spaces: {e}")
            return None
    
    async def _extract_description_from_meta(self) -> Optional[str]:
        """Extract description from meta tag"""
        try:
            element = await self.page.query_selector('meta[property="og:description"]')
            if element:
                description = await element.get_attribute('content')
                if description:
                    return description.strip()
        except Exception:
            pass
        return None
    
    async def _extract_description_from_selectors(self) -> Optional[str]:
        """Extract description from HTML selectors"""
        description_selectors = [
            '[data-testid="description"]',
            '[class*="description"]',
            '[class*="descricao"]',
        ]
        
        for selector in description_selectors:
            try:
                element = await self.page.query_selector(selector)
                if element:
                    description = await element.text_content()
                    if description and description.strip():
                        return description.strip()
            except Exception:
                continue
        return None
    
    async def extract_description(self) -> Optional[str]:
        """
        Extracts the property description
        
        Returns:
            Optional[str]: Description or None if not found
        """
        try:
            # Try meta tag first
            description = await self._extract_description_from_meta()
            if description:
                return description
            
            # Try HTML selectors
            return await self._extract_description_from_selectors()
            
        except Exception as e:
            logger.debug(f"Error extracting description: {e}")
            return None
    
    async def extract_amenities(self) -> List[str]:
        """
        Extracts the list of amenities/features
        
        Returns:
            List[str]: List of amenities
        """
        try:
            amenities = []
            
            amenity_selectors = [
                '[data-testid="amenity"]',
                '[class*="amenity"]',
                '[class*="feature"]',
                '[class*="caracteristica"]'
            ]
            
            for selector in amenity_selectors:
                try:
                    elements = await self.page.query_selector_all(selector)
                    for element in elements:
                        text = await element.text_content()
                        if text and text.strip():
                            amenities.append(text.strip())
                except Exception:
                    continue
            
            return amenities[:30]  # Limit to first 30 amenities
            
        except Exception as e:
            logger.debug(f"Error extracting amenities: {e}")
            return []
    
    async def extract_deep_price_details(self) -> Dict[str, Optional[float]]:
        """
        Extracts detailed price information (Venda, Condomínio, IPTU)
        
        Returns:
            Dict with 'sale_price', 'condo_fee', 'iptu' keys
        """
        try:
            price_details = {
                'sale_price': None,
                'condo_fee': None,
                'iptu': None
            }
            
            # Find price-info container
            price_container = await self.page.query_selector('.price-info__values')
            if not price_container:
                return price_details
            
            # Extract all value items
            value_items = await price_container.query_selector_all('.value-item')
            
            for item in value_items:
                try:
                    # Get title and value
                    title_elem = await item.query_selector('.value-item__title')
                    value_elem = await item.query_selector('.value-item__value')
                    
                    if not title_elem or not value_elem:
                        continue
                    
                    title = await title_elem.text_content()
                    value_text = await value_elem.text_content()
                    
                    if not title or not value_text:
                        continue
                    
                    title = title.strip().lower()
                    
                    # Use allow_small_values=True for condo fees and IPTU
                    if 'venda' in title:
                        value = self._parse_price_text(value_text, allow_small_values=False)
                        if value:
                            price_details['sale_price'] = value
                    elif 'condomínio' in title:
                        value = self._parse_price_text(value_text, allow_small_values=True)
                        if value:
                            price_details['condo_fee'] = value
                    elif 'iptu' in title:
                        value = self._parse_price_text(value_text, allow_small_values=True)
                        if value:
                            price_details['iptu'] = value
                        
                except Exception:
                    continue
            
            return price_details
            
        except Exception as e:
            logger.debug(f"Error extracting deep price details: {e}")
            return {'sale_price': None, 'condo_fee': None, 'iptu': None}
    
    async def extract_deep_characteristics(self) -> Dict[str, Any]:
        """
        Extracts detailed characteristics from amenities-container
        
        Returns:
            Dict with 'area', 'bedrooms', 'bathrooms', 'parking_spaces', 'floor_level', 
            'suites', 'amenities_list' and other structured data
        """
        try:
            characteristics = {
                'area': None,
                'bedrooms': None,
                'bathrooms': None,
                'parking_spaces': None,
                'floor_level': None,
                'suites': None,
                'has_gym': False,
                'has_gated_community': False,
                'has_party_hall': False,
                'has_gourmet_space': False,
                'has_playground': False,
                'has_spa': False,
                'has_pool': False,
                'has_balcony': False,
                'has_gourmet_balcony': False,
                'has_elevator': False,
                'has_barbecue': False,
                'has_garden': False,
                'has_deposit': False,
                'has_sports_court': False,
                'has_alarm_system': False,
                'has_intercom': False,
                'has_cable_tv': False,
                'has_kitchen': False,
                'has_dinner_room': False,
                'has_air_conditioning': False,
                'has_service_area': False,
                'has_large_window': False,
                'has_internet_access': False,
                'has_kitchen_cabinets': False,
                'has_builtin_wardrobe': False,
                'pets_allowed': False,
                'amenities_list': []
            }
            
            # Find amenities container
            amenities_container = await self.page.query_selector('[data-testid="amenities-container"]')
            if not amenities_container:
                # Fallback to amenities-list
                amenities_container = await self.page.query_selector('.amenities-list')
            
            if not amenities_container:
                return characteristics
            
            # Extract all amenities items
            amenity_items = await amenities_container.query_selector_all('.amenities-item')
            
            for item in amenity_items:
                try:
                    # Get the itemprop attribute to identify the characteristic type
                    itemprop = await item.get_attribute('itemprop')
                    
                    # Get the text content
                    text_elem = await item.query_selector('.amenities-item-text')
                    if not text_elem:
                        continue
                    
                    text = await text_elem.text_content()
                    if not text:
                        continue
                    
                    text = text.strip()
                    
                    # Extract based on itemprop attribute for structured data
                    if itemprop == 'floorSize':
                        # Area in m²
                        area_match = re.search(r'(\d+\.?\d*)\s*m²', text, re.IGNORECASE)
                        if area_match:
                            characteristics['area'] = float(area_match.group(1))
                    
                    elif itemprop == 'numberOfRooms':
                        # Bedrooms
                        bedroom_match = re.search(PATTERN_NUMBER, text)
                        if bedroom_match:
                            characteristics['bedrooms'] = int(bedroom_match.group(1))
                    
                    elif itemprop == 'numberOfBathroomsTotal':
                        # Bathrooms
                        bathroom_match = re.search(PATTERN_NUMBER, text)
                        if bathroom_match:
                            characteristics['bathrooms'] = int(bathroom_match.group(1))
                    
                    elif itemprop == 'numberOfParkingSpaces':
                        # Parking spaces
                        parking_match = re.search(PATTERN_NUMBER, text)
                        if parking_match:
                            characteristics['parking_spaces'] = int(parking_match.group(1))
                    
                    elif itemprop == 'floorLevel':
                        # Floor level (e.g., "7 andar")
                        floor_match = re.search(r'(\d+)\s*andar', text, re.IGNORECASE)
                        if floor_match:
                            characteristics['floor_level'] = int(floor_match.group(1))
                        else:
                            # Store as text if format is different
                            characteristics['floor_level'] = text
                    
                    elif itemprop == 'numberOfSuites':
                        # Suites
                        suite_match = re.search(PATTERN_NUMBER, text)
                        if suite_match:
                            characteristics['suites'] = int(suite_match.group(1))
                    
                    # Specific amenities with boolean flags
                    elif itemprop == 'GYM':
                        characteristics['has_gym'] = True
                        if text and text not in characteristics['amenities_list']:
                            characteristics['amenities_list'].append(text)
                    
                    elif itemprop == 'GATED_COMMUNITY':
                        characteristics['has_gated_community'] = True
                        if text and text not in characteristics['amenities_list']:
                            characteristics['amenities_list'].append(text)
                    
                    elif itemprop == 'PARTY_HALL':
                        characteristics['has_party_hall'] = True
                        if text and text not in characteristics['amenities_list']:
                            characteristics['amenities_list'].append(text)
                    
                    elif itemprop == 'GOURMET_SPACE':
                        characteristics['has_gourmet_space'] = True
                        if text and text not in characteristics['amenities_list']:
                            characteristics['amenities_list'].append(text)
                    
                    elif itemprop == 'PLAYGROUND':
                        characteristics['has_playground'] = True
                        if text and text not in characteristics['amenities_list']:
                            characteristics['amenities_list'].append(text)
                    
                    elif itemprop == 'SPA':
                        characteristics['has_spa'] = True
                        if text and text not in characteristics['amenities_list']:
                            characteristics['amenities_list'].append(text)
                    
                    elif itemprop == 'POOL':
                        characteristics['has_pool'] = True
                        if text and text not in characteristics['amenities_list']:
                            characteristics['amenities_list'].append(text)
                    
                    elif itemprop == 'BALCONY':
                        characteristics['has_balcony'] = True
                        if text and text not in characteristics['amenities_list']:
                            characteristics['amenities_list'].append(text)
                    
                    elif itemprop == 'GOURMET_BALCONY':
                        characteristics['has_gourmet_balcony'] = True
                        if text and text not in characteristics['amenities_list']:
                            characteristics['amenities_list'].append(text)
                    
                    elif itemprop == 'ELEVATOR':
                        characteristics['has_elevator'] = True
                        if text and text not in characteristics['amenities_list']:
                            characteristics['amenities_list'].append(text)
                    
                    elif itemprop == 'BARBECUE_GRILL':
                        characteristics['has_barbecue'] = True
                        if text and text not in characteristics['amenities_list']:
                            characteristics['amenities_list'].append(text)
                    
                    elif itemprop == 'GARDEN':
                        characteristics['has_garden'] = True
                        if text and text not in characteristics['amenities_list']:
                            characteristics['amenities_list'].append(text)
                    
                    elif itemprop == 'DEPOSIT':
                        characteristics['has_deposit'] = True
                        if text and text not in characteristics['amenities_list']:
                            characteristics['amenities_list'].append(text)
                    
                    elif itemprop == 'SPORTS_COURT':
                        characteristics['has_sports_court'] = True
                        if text and text not in characteristics['amenities_list']:
                            characteristics['amenities_list'].append(text)
                    
                    elif itemprop == 'ALARM_SYSTEM':
                        characteristics['has_alarm_system'] = True
                        if text and text not in characteristics['amenities_list']:
                            characteristics['amenities_list'].append(text)
                    
                    elif itemprop == 'INTERCOM':
                        characteristics['has_intercom'] = True
                        if text and text not in characteristics['amenities_list']:
                            characteristics['amenities_list'].append(text)
                    
                    elif itemprop == 'CABLE_TV':
                        characteristics['has_cable_tv'] = True
                        if text and text not in characteristics['amenities_list']:
                            characteristics['amenities_list'].append(text)
                    
                    elif itemprop == 'KITCHEN':
                        characteristics['has_kitchen'] = True
                        if text and text not in characteristics['amenities_list']:
                            characteristics['amenities_list'].append(text)
                    
                    elif itemprop == 'DINNER_ROOM':
                        characteristics['has_dinner_room'] = True
                        if text and text not in characteristics['amenities_list']:
                            characteristics['amenities_list'].append(text)
                    
                    elif itemprop == 'AIR_CONDITIONING':
                        characteristics['has_air_conditioning'] = True
                        if text and text not in characteristics['amenities_list']:
                            characteristics['amenities_list'].append(text)
                    
                    elif itemprop == 'SERVICE_AREA':
                        characteristics['has_service_area'] = True
                        if text and text not in characteristics['amenities_list']:
                            characteristics['amenities_list'].append(text)
                    
                    elif itemprop == 'LARGE_WINDOW':
                        characteristics['has_large_window'] = True
                        if text and text not in characteristics['amenities_list']:
                            characteristics['amenities_list'].append(text)
                    
                    elif itemprop == 'INTERNET_ACCESS':
                        characteristics['has_internet_access'] = True
                        if text and text not in characteristics['amenities_list']:
                            characteristics['amenities_list'].append(text)
                    
                    elif itemprop == 'KITCHEN_CABINETS':
                        characteristics['has_kitchen_cabinets'] = True
                        if text and text not in characteristics['amenities_list']:
                            characteristics['amenities_list'].append(text)
                    
                    elif itemprop == 'BUILTIN_WARDROBE':
                        characteristics['has_builtin_wardrobe'] = True
                        if text and text not in characteristics['amenities_list']:
                            characteristics['amenities_list'].append(text)
                    
                    elif itemprop == 'PETS_ALLOWED':
                        characteristics['pets_allowed'] = True
                        if text and text not in characteristics['amenities_list']:
                            characteristics['amenities_list'].append(text)
                    
                    # All other amenities - only if itemprop exists
                    elif itemprop:
                        if text and text not in characteristics['amenities_list']:
                            characteristics['amenities_list'].append(text)
                    
                    # Fallback: if no itemprop but text matches known patterns
                    if not itemprop:
                        # Check for area (m²)
                        if 'm²' in text and not characteristics['area']:
                            area_match = re.search(r'(\d+\.?\d*)\s*m²', text, re.IGNORECASE)
                            if area_match:
                                characteristics['area'] = float(area_match.group(1))
                        
                        # Check for bedrooms
                        elif 'quarto' in text.lower() and not characteristics['bedrooms']:
                            bedroom_match = re.search(PATTERN_NUMBER, text)
                            if bedroom_match:
                                characteristics['bedrooms'] = int(bedroom_match.group(1))
                        
                        # Check for bathrooms
                        elif 'banheiro' in text.lower() and not characteristics['bathrooms']:
                            bathroom_match = re.search(PATTERN_NUMBER, text)
                            if bathroom_match:
                                characteristics['bathrooms'] = int(bathroom_match.group(1))
                        
                        # Check for parking
                        elif 'vaga' in text.lower() and not characteristics['parking_spaces']:
                            parking_match = re.search(PATTERN_NUMBER, text)
                            if parking_match:
                                characteristics['parking_spaces'] = int(parking_match.group(1))
                        
                        # Check for floor level
                        elif 'andar' in text.lower() and not characteristics['floor_level']:
                            floor_match = re.search(r'(\d+)\s*andar', text, re.IGNORECASE)
                            if floor_match:
                                characteristics['floor_level'] = int(floor_match.group(1))
                            else:
                                characteristics['floor_level'] = text
                        
                        # Check for suites
                        elif 'suíte' in text.lower() or 'suite' in text.lower() and not characteristics['suites']:
                            suite_match = re.search(PATTERN_NUMBER, text)
                            if suite_match:
                                characteristics['suites'] = int(suite_match.group(1))
                        
                        # Other amenities
                        else:
                            if text and text not in characteristics['amenities_list']:
                                characteristics['amenities_list'].append(text)
                            
                except Exception as e:
                    logger.debug(f"Error extracting amenity item: {e}")
                    continue
            
            return characteristics
            
        except Exception as e:
            logger.debug(f"Error extracting deep characteristics: {e}")
            return {'area': None, 'bedrooms': None, 'bathrooms': None, 'parking_spaces': None, 
                   'floor_level': None, 'suites': None, 'has_gym': False, 'has_gated_community': False,
                   'has_party_hall': False, 'has_gourmet_space': False, 'has_playground': False,
                   'has_spa': False, 'has_pool': False, 'has_balcony': False, 'has_gourmet_balcony': False,
                   'has_elevator': False, 'has_barbecue': False, 'has_garden': False, 'has_deposit': False,
                   'has_sports_court': False, 'has_alarm_system': False, 'has_intercom': False,
                   'has_cable_tv': False, 'has_kitchen': False, 'has_dinner_room': False,
                   'has_air_conditioning': False, 'has_service_area': False, 'has_large_window': False,
                   'has_internet_access': False, 'has_kitchen_cabinets': False, 'has_builtin_wardrobe': False,
                   'pets_allowed': False, 'amenities_list': []}
    
    async def extract_deep_location(self) -> Optional[str]:
        """
        Extracts full location address from location section
        
        Returns:
            Full address string or None
        """
        try:
            # Find location address
            location_elem = await self.page.query_selector('[data-testid="location-address"]')
            if location_elem:
                address = await location_elem.text_content()
                if address:
                    return address.strip()
            
            # Fallback to location-address__text
            location_elem = await self.page.query_selector('.location-address__text')
            if location_elem:
                address = await location_elem.text_content()
                if address:
                    return address.strip()
            
            return None
            
        except Exception as e:
            logger.debug(f"Error extracting deep location: {e}")
            return None
    
    async def extract_deep_description(self) -> Optional[str]:
        """
        Extracts full description from description container
        
        Returns:
            Full description text or None
        """
        try:
            # Find description content
            desc_elem = await self.page.query_selector('[data-testid="description-content"]')
            if desc_elem:
                description = await desc_elem.text_content()
                if description:
                    return description.strip()
            
            # Fallback to description__content--text
            desc_elem = await self.page.query_selector('.description__content--text')
            if desc_elem:
                description = await desc_elem.text_content()
                if description:
                    return description.strip()
            
            return None
            
        except Exception as e:
            logger.debug(f"Error extracting deep description: {e}")
            return None
    
    async def extract_advertiser_info(self) -> Dict[str, Optional[str]]:
        """
        Extracts advertiser information
        
        Returns:
            Dict with 'advertiser_name', 'advertiser_creci', 'advertiser_is_premium', 
            'advertiser_properties_count', 'advertiser_rating', 'advertiser_rating_count', 
            'advertiser_since_date'
        """
        try:
            advertiser_info = {
                'advertiser_name': None,
                'advertiser_creci': None,
                'advertiser_is_premium': False,
                'advertiser_properties_count': None,
                'advertiser_rating': None,
                'advertiser_rating_count': None,
                'advertiser_since_date': None
            }
            
            # Find advertiser header
            advertiser_header = await self.page.query_selector('[data-testid="advertiser-info-header"]')
            if advertiser_header:
                # Extract name
                name_elem = await advertiser_header.query_selector('.advertiser-header__credentials_name')
                if name_elem:
                    name = await name_elem.text_content()
                    if name:
                        advertiser_info['advertiser_name'] = name.strip()
                
                # Check for premium badge
                premium_icon = await advertiser_header.query_selector('.advertiser-header__premium-icon')
                if premium_icon:
                    advertiser_info['advertiser_is_premium'] = True
                
                # Extract CRECI (usually in a paragraph after name)
                creci_text = await advertiser_header.evaluate(r"""
                    () => {
                        const text = document.querySelector('[data-testid="advertiser-info-header"]')?.textContent || '';
                        const creciMatch = text.match(/Creci[:\s]*([\w-]+)/i);
                        return creciMatch ? creciMatch[1] : null;
                    }
                """)
                if creci_text:
                    advertiser_info['advertiser_creci'] = creci_text.strip()
            
            # Extract rating information
            rating_container = await self.page.query_selector('[data-testid="rating-container"]')
            if rating_container:
                rating_text_elem = await rating_container.query_selector('.rating-container__text')
                if rating_text_elem:
                    rating_text = await rating_text_elem.text_content()
                    if rating_text:
                        # Extract rating (e.g., "5/5 (1 classificação)")
                        rating_match = re.search(r'(\d+(?:\.\d+)?)/\d+', rating_text)
                        if rating_match:
                            try:
                                advertiser_info['advertiser_rating'] = float(rating_match.group(1))
                            except ValueError:
                                pass
                        
                        # Extract rating count (e.g., "1 classificação")
                        count_match = re.search(r'\((\d+)\s*classificação', rating_text, re.IGNORECASE)
                        if count_match:
                            try:
                                advertiser_info['advertiser_rating_count'] = int(count_match.group(1))
                            except ValueError:
                                pass
            
            # Extract properties count from properties-container
            properties_elem = await self.page.query_selector('.properties-container')
            if properties_elem:
                props_text = await properties_elem.text_content()
                if props_text:
                    # Match pattern like "1.997 imóveis cadastrados" or "4.061 imóveis cadastrados"
                    props_match = re.search(r'([\d.]+)\s*imóveis', props_text, re.IGNORECASE)
                    if props_match:
                        count_str = props_match.group(1).replace('.', '')
                        try:
                            advertiser_info['advertiser_properties_count'] = int(count_str)
                        except ValueError:
                            pass
            
            # Extract since date
            since_elem = await self.page.query_selector('.extended-advertiser-info__icon-text')
            if since_elem:
                since_text = await since_elem.text_content()
                if since_text and 'desde' in since_text.lower():
                    advertiser_info['advertiser_since_date'] = since_text.strip()
            
            return advertiser_info
            
        except Exception as e:
            logger.debug(f"Error extracting advertiser info: {e}")
            return {'advertiser_name': None, 'advertiser_creci': None, 'advertiser_is_premium': False, 
                   'advertiser_properties_count': None, 'advertiser_rating': None, 'advertiser_rating_count': None,
                   'advertiser_since_date': None}
    
    async def extract_property_codes(self) -> Dict[str, Optional[str]]:
        """
        Extracts property codes (Código do anunciante, Código no Zap)
        
        Returns:
            Dict with 'advertiser_code', 'zap_code'
        """
        try:
            codes = {
                'advertiser_code': None,
                'zap_code': None
            }
            
            # Find property codes element
            codes_elem = await self.page.query_selector('[data-cy="ldp-propertyCodes-txt"]')
            if codes_elem:
                codes_text = await codes_elem.text_content()
                if codes_text:
                    # Extract advertiser code
                    adv_match = re.search(r'Código do anunciante[:\s]*([\w-]+)', codes_text, re.IGNORECASE)
                    if adv_match:
                        codes['advertiser_code'] = adv_match.group(1).strip()
                    
                    # Extract Zap code
                    zap_match = re.search(r'Código no\s*Zap[:\s]*([\d-]+)', codes_text, re.IGNORECASE)
                    if zap_match:
                        codes['zap_code'] = zap_match.group(1).strip()
            
            return codes
            
        except Exception as e:
            logger.debug(f"Error extracting property codes: {e}")
            return {'advertiser_code': None, 'zap_code': None}
    
    async def extract_listing_dates(self) -> Dict[str, Optional[str]]:
        """
        Extracts creation and update dates
        
        Returns:
            Dict with 'created_date', 'updated_info'
        """
        try:
            dates = {
                'created_date': None,
                'updated_info': None
            }
            
            # Find listing date element
            date_elem = await self.page.query_selector('[data-testid="listing-created-date"]')
            if date_elem:
                date_text = await date_elem.text_content()
                if date_text:
                    # Extract creation date
                    created_match = re.search(r'criado em\s*([^,]+)', date_text, re.IGNORECASE)
                    if created_match:
                        dates['created_date'] = created_match.group(1).strip()
                    
                    # Extract update info
                    updated_match = re.search(r'atualizado\s+(.+)', date_text, re.IGNORECASE)
                    if updated_match:
                        dates['updated_info'] = updated_match.group(1).strip()
            
            return dates
            
        except Exception as e:
            logger.debug(f"Error extracting listing dates: {e}")
            return {'created_date': None, 'updated_info': None}
    
    async def extract_listing_images(self) -> List[str]:
        """
        Extracts all image URLs from the carousel
        
        Returns:
            List of image URLs (preferably the high-resolution version)
        """
        try:
            images = []
            
            # Find carousel container
            carousel = await self.page.query_selector('[data-testid="carousel-photos"]')
            if not carousel:
                logger.debug("Carousel container not found")
                return images
            
            logger.debug("Carousel container found, extracting images...")
            
            # Get all carousel items
            carousel_items = await carousel.query_selector_all('.carousel-photos--item')
            logger.debug(f"Found {len(carousel_items)} carousel items")
            
            for item in carousel_items:
                try:
                    # Try to get the source element first (usually has better quality)
                    source_elem = await item.query_selector('source')
                    if source_elem:
                        srcset = await source_elem.get_attribute('srcset')
                        if srcset:
                            # Extract the high-res URL (870x707) from srcset
                            # Format: "url?action=fit-in&dimension=870x707"
                            url_match = re.search(r'(https://[^\s]+dimension=870x707)', srcset)
                            if url_match:
                                img_url = url_match.group(1)
                                if img_url not in images:
                                    images.append(img_url)
                                continue
                    
                    # Fallback to img element
                    img_elem = await item.query_selector('img[data-testid="carousel-item-image"]')
                    if img_elem:
                        srcset = await img_elem.get_attribute('srcset')
                        if srcset:
                            # Extract the highest quality URL (870x707)
                            url_match = re.search(r'(https://[^\s]+dimension=870x707)', srcset)
                            if url_match:
                                img_url = url_match.group(1)
                                if img_url not in images:
                                    images.append(img_url)
                            else:
                                # Fallback: get the first URL from srcset
                                url_match = re.search(r'(https://[^\s]+)', srcset)
                                if url_match:
                                    img_url = url_match.group(1)
                                    if img_url not in images:
                                        images.append(img_url)
                
                except Exception as e:
                    logger.debug(f"Error extracting image from carousel item: {e}")
                    continue
            
            logger.info(f"Extracted {len(images)} image URLs from carousel")
            return images
            
        except Exception as e:
            logger.error(f"Error extracting listing images: {e}", exc_info=True)
            return []
    
    async def extract_contact_info(self) -> Dict[str, Optional[str]]:
        """
        Extracts contact information (phone, WhatsApp availability)
        
        Returns:
            Dict with 'phone_partial', 'has_whatsapp'
        """
        try:
            contact = {
                'phone_partial': None,
                'has_whatsapp': False
            }
            
            # Find phone element
            phone_elem = await self.page.query_selector('[data-testid="info-phone"]')
            if phone_elem:
                phone_text = await phone_elem.text_content()
                if phone_text:
                    phone_match = re.search(r'\([\d]+\)\s*[\d-]+', phone_text)
                    if phone_match:
                        contact['phone_partial'] = phone_match.group(0).strip()
            
            # Check for WhatsApp button
            whatsapp_btn = await self.page.query_selector('[data-cy="ldp-whatsapp-btn"]')
            if whatsapp_btn:
                contact['has_whatsapp'] = True
            
            return contact
            
        except Exception as e:
            logger.debug(f"Error extracting contact info: {e}")
            return {'phone_partial': None, 'has_whatsapp': False}
    
    async def extract_all_deep_data(self) -> Dict:
        """
        Extracts all deep data from a listing page
        
        Returns:
            Dict with all deep extracted data
        """
        try:
            deep_data = {}
            
            # Price details
            price_details = await self.extract_deep_price_details()
            deep_data.update(price_details)
            
            # Characteristics
            characteristics = await self.extract_deep_characteristics()
            # Merge characteristics, but keep existing keys if they exist
            for key, value in characteristics.items():
                if key == 'amenities_list':
                    deep_data['amenities'] = value
                else:
                    deep_data[key] = value
            
            # Location
            deep_location = await self.extract_deep_location()
            if deep_location:
                deep_data['full_address'] = deep_location
            
            # Description
            deep_description = await self.extract_deep_description()
            if deep_description:
                deep_data['full_description'] = deep_description
            
            # Advertiser info
            advertiser_info = await self.extract_advertiser_info()
            deep_data.update(advertiser_info)
            
            # Property codes
            property_codes = await self.extract_property_codes()
            deep_data.update(property_codes)
            
            # Dates
            listing_dates = await self.extract_listing_dates()
            deep_data.update(listing_dates)
            
            # Contact info
            contact_info = await self.extract_contact_info()
            deep_data.update(contact_info)
            
            # Images
            images = await self.extract_listing_images()
            if images:
                deep_data['images'] = images
                deep_data['image_count'] = len(images)
                logger.info(f"Extracted {len(images)} images from listing")
            else:
                logger.debug("No images extracted from listing")
            
            return deep_data
            
        except Exception as e:
            logger.error(f"Error extracting all deep data: {e}")
            return {}

