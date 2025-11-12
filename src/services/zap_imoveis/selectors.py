"""
CSS Selectors and Constants for Zap Imóveis scraping
"""
# Constants for data-cy selectors
SELECTOR_LOCATION = '[data-cy="rp-cardProperty-location-txt"]'
SELECTOR_STREET = '[data-cy="rp-cardProperty-street-txt"]'
SELECTOR_PRICE = '[data-cy="rp-cardProperty-price-txt"]'
SELECTOR_AREA = '[data-cy="rp-cardProperty-propertyArea-txt"]'
SELECTOR_BEDROOMS = '[data-cy="rp-cardProperty-bedroomQuantity-txt"]'
SELECTOR_BATHROOMS = '[data-cy="rp-cardProperty-bathroomQuantity-txt"]'
SELECTOR_PARKING = '[data-cy="rp-cardProperty-parkingSpacesQuantity-txt"]'
SELECTOR_IMAGE = '[data-cy="rp-cardProperty-image-img"]'
SELECTOR_PROPERTY_CARD = 'li[data-cy="rp-property-cd"]'

# Patterns
PATTERN_NUMBER = r'(\d+)'

# URL constants
URL_IMOVEL_PREFIX = '/imovel/'
PAGE_PARAM = 'page='

