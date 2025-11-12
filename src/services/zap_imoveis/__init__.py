"""
Zap Imóveis service modules
"""
from src.services.zap_imoveis.selectors import (
    SELECTOR_LOCATION, SELECTOR_STREET, SELECTOR_PRICE, SELECTOR_AREA,
    SELECTOR_BEDROOMS, SELECTOR_BATHROOMS, SELECTOR_PARKING, SELECTOR_IMAGE,
    SELECTOR_PROPERTY_CARD, PATTERN_NUMBER, URL_IMOVEL_PREFIX, PAGE_PARAM
)
from src.services.zap_imoveis.extractors import DataExtractor
from src.services.zap_imoveis.pagination import PaginationHandler
from src.services.zap_imoveis.search_extractor import SearchExtractor

__all__ = [
    'SELECTOR_LOCATION', 'SELECTOR_STREET', 'SELECTOR_PRICE', 'SELECTOR_AREA',
    'SELECTOR_BEDROOMS', 'SELECTOR_BATHROOMS', 'SELECTOR_PARKING', 'SELECTOR_IMAGE',
    'SELECTOR_PROPERTY_CARD', 'PATTERN_NUMBER', 'URL_IMOVEL_PREFIX', 'PAGE_PARAM',
    'DataExtractor', 'PaginationHandler', 'SearchExtractor'
]

