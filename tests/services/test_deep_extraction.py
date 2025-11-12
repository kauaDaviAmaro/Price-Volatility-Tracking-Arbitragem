"""
Tests for deep extraction methods (characteristics, advertiser info, images)
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.zap_imoveis.extractors import DataExtractor


@pytest.mark.asyncio
class TestDeepCharacteristicsExtraction:
    """Tests for deep characteristics extraction"""
    
    async def test_extract_deep_characteristics_basic_fields(self, mock_page):
        """Test extraction of basic characteristic fields"""
        extractor = DataExtractor(mock_page)
        
        # Mock amenities container
        amenities_container = AsyncMock()
        amenity_item = AsyncMock()
        
        # Mock area item
        area_item = AsyncMock()
        area_item.get_attribute = AsyncMock(return_value="floorSize")
        area_text = AsyncMock()
        area_text.text_content = AsyncMock(return_value="82 m²")
        area_item.query_selector = AsyncMock(return_value=area_text)
        
        # Mock bedrooms item
        bedroom_item = AsyncMock()
        bedroom_item.get_attribute = AsyncMock(return_value="numberOfRooms")
        bedroom_text = AsyncMock()
        bedroom_text.text_content = AsyncMock(return_value="2 quartos")
        bedroom_item.query_selector = AsyncMock(return_value=bedroom_text)
        
        # Mock bathrooms item
        bathroom_item = AsyncMock()
        bathroom_item.get_attribute = AsyncMock(return_value="numberOfBathroomsTotal")
        bathroom_text = AsyncMock()
        bathroom_text.text_content = AsyncMock(return_value="2 banheiros")
        bathroom_item.query_selector = AsyncMock(return_value=bathroom_text)
        
        # Mock parking item
        parking_item = AsyncMock()
        parking_item.get_attribute = AsyncMock(return_value="numberOfParkingSpaces")
        parking_text = AsyncMock()
        parking_text.text_content = AsyncMock(return_value="2 vagas")
        parking_item.query_selector = AsyncMock(return_value=parking_text)
        
        # Mock floor level item
        floor_item = AsyncMock()
        floor_item.get_attribute = AsyncMock(return_value="floorLevel")
        floor_text = AsyncMock()
        floor_text.text_content = AsyncMock(return_value="9 andar")
        floor_item.query_selector = AsyncMock(return_value=floor_text)
        
        # Mock suites item
        suite_item = AsyncMock()
        suite_item.get_attribute = AsyncMock(return_value="numberOfSuites")
        suite_text = AsyncMock()
        suite_text.text_content = AsyncMock(return_value="1 suíte")
        suite_item.query_selector = AsyncMock(return_value=suite_text)
        
        amenities_container.query_selector_all = AsyncMock(
            return_value=[area_item, bedroom_item, bathroom_item, parking_item, floor_item, suite_item]
        )
        mock_page.query_selector = AsyncMock(return_value=amenities_container)
        
        characteristics = await extractor.extract_deep_characteristics()
        
        assert characteristics['area'] == 82.0
        assert characteristics['bedrooms'] == 2
        assert characteristics['bathrooms'] == 2
        assert characteristics['parking_spaces'] == 2
        assert characteristics['floor_level'] == 9
        assert characteristics['suites'] == 1
    
    async def test_extract_deep_characteristics_amenities(self, mock_page):
        """Test extraction of amenity boolean flags"""
        extractor = DataExtractor(mock_page)
        
        amenities_container = AsyncMock()
        
        # Mock various amenity items
        pool_item = AsyncMock()
        pool_item.get_attribute = AsyncMock(return_value="POOL")
        pool_text = AsyncMock()
        pool_text.text_content = AsyncMock(return_value="Piscina")
        pool_item.query_selector = AsyncMock(return_value=pool_text)
        
        gym_item = AsyncMock()
        gym_item.get_attribute = AsyncMock(return_value="GYM")
        gym_text = AsyncMock()
        gym_text.text_content = AsyncMock(return_value="Academia")
        gym_item.query_selector = AsyncMock(return_value=gym_text)
        
        elevator_item = AsyncMock()
        elevator_item.get_attribute = AsyncMock(return_value="ELEVATOR")
        elevator_text = AsyncMock()
        elevator_text.text_content = AsyncMock(return_value="Elevador")
        elevator_item.query_selector = AsyncMock(return_value=elevator_text)
        
        pets_item = AsyncMock()
        pets_item.get_attribute = AsyncMock(return_value="PETS_ALLOWED")
        pets_text = AsyncMock()
        pets_text.text_content = AsyncMock(return_value="Aceita animais")
        pets_item.query_selector = AsyncMock(return_value=pets_text)
        
        amenities_container.query_selector_all = AsyncMock(
            return_value=[pool_item, gym_item, elevator_item, pets_item]
        )
        mock_page.query_selector = AsyncMock(return_value=amenities_container)
        
        characteristics = await extractor.extract_deep_characteristics()
        
        assert characteristics['has_pool'] is True
        assert characteristics['has_gym'] is True
        assert characteristics['has_elevator'] is True
        assert characteristics['pets_allowed'] is True
        assert "Piscina" in characteristics['amenities_list']
        assert "Academia" in characteristics['amenities_list']
    
    async def test_extract_deep_characteristics_all_new_amenities(self, mock_page):
        """Test extraction of all newly added amenities"""
        extractor = DataExtractor(mock_page)
        
        amenities_container = AsyncMock()
        
        # Create mock items for all new amenities
        amenity_items = []
        amenity_map = {
            "GOURMET_BALCONY": ("Varanda gourmet", "has_gourmet_balcony"),
            "ALARM_SYSTEM": ("Sistema de alarme", "has_alarm_system"),
            "INTERCOM": ("Interfone", "has_intercom"),
            "CABLE_TV": ("TV a cabo", "has_cable_tv"),
            "KITCHEN": ("Cozinha", "has_kitchen"),
            "DINNER_ROOM": ("Sala de jantar", "has_dinner_room"),
            "AIR_CONDITIONING": ("Ar-condicionado", "has_air_conditioning"),
            "SERVICE_AREA": ("Área de serviço", "has_service_area"),
            "LARGE_WINDOW": ("Janela grande", "has_large_window"),
            "INTERNET_ACCESS": ("Conexão à internet", "has_internet_access"),
            "KITCHEN_CABINETS": ("Armário na cozinha", "has_kitchen_cabinets"),
            "BUILTIN_WARDROBE": ("Armário embutido", "has_builtin_wardrobe"),
        }
        
        for itemprop, (text, field_name) in amenity_map.items():
            item = AsyncMock()
            item.get_attribute = AsyncMock(return_value=itemprop)
            text_elem = AsyncMock()
            text_elem.text_content = AsyncMock(return_value=text)
            item.query_selector = AsyncMock(return_value=text_elem)
            amenity_items.append(item)
        
        amenities_container.query_selector_all = AsyncMock(return_value=amenity_items)
        mock_page.query_selector = AsyncMock(return_value=amenities_container)
        
        characteristics = await extractor.extract_deep_characteristics()
        
        # Check all boolean flags
        for _, (_, field_name) in amenity_map.items():
            assert characteristics[field_name] is True, f"{field_name} should be True"
        
        # Check amenities_list contains all texts
        for _, (text, _) in amenity_map.items():
            assert text in characteristics['amenities_list'], f"{text} should be in amenities_list"
    
    async def test_extract_deep_characteristics_not_found(self, mock_page):
        """Test extraction when amenities container not found"""
        extractor = DataExtractor(mock_page)
        mock_page.query_selector = AsyncMock(return_value=None)
        
        characteristics = await extractor.extract_deep_characteristics()
        
        assert characteristics['area'] is None
        assert characteristics['bedrooms'] is None
        assert characteristics['has_pool'] is False
        assert characteristics['amenities_list'] == []


@pytest.mark.asyncio
class TestAdvertiserInfoExtraction:
    """Tests for advertiser information extraction"""
    
    async def test_extract_advertiser_info_basic(self, mock_page):
        """Test basic advertiser info extraction"""
        extractor = DataExtractor(mock_page)
        
        # Mock advertiser header
        advertiser_header = AsyncMock()
        name_elem = AsyncMock()
        name_elem.text_content = AsyncMock(return_value="Invista Negocios Imobiliarios E Financiamentos Ltda.")
        # No premium icon found
        advertiser_header.query_selector = AsyncMock(side_effect=[name_elem, None])
        advertiser_header.evaluate = AsyncMock(return_value=None)
        
        mock_page.query_selector = AsyncMock(side_effect=[
            advertiser_header,  # advertiser-info-header
            None,  # rating-container
            None,  # properties-container
            None   # extended-advertiser-info__icon-text
        ])
        
        info = await extractor.extract_advertiser_info()
        
        assert info['advertiser_name'] == "Invista Negocios Imobiliarios E Financiamentos Ltda."
        assert info['advertiser_is_premium'] is False
    
    async def test_extract_advertiser_info_premium(self, mock_page):
        """Test advertiser info with premium badge"""
        extractor = DataExtractor(mock_page)
        
        advertiser_header = AsyncMock()
        name_elem = AsyncMock()
        name_elem.text_content = AsyncMock(return_value="Test Imobiliaria")
        premium_icon = AsyncMock()
        
        advertiser_header.query_selector = AsyncMock(side_effect=[name_elem, premium_icon])
        
        mock_page.query_selector = AsyncMock(side_effect=[
            advertiser_header,
            None,  # rating-container
            None,  # properties-container
            None   # extended-advertiser-info__icon-text
        ])
        
        info = await extractor.extract_advertiser_info()
        
        assert info['advertiser_is_premium'] is True
    
    async def test_extract_advertiser_info_rating(self, mock_page):
        """Test advertiser rating extraction"""
        extractor = DataExtractor(mock_page)
        
        # Mock rating container
        rating_container = AsyncMock()
        rating_text_elem = AsyncMock()
        rating_text_elem.text_content = AsyncMock(return_value="5/5 (1 classificação)")
        rating_container.query_selector = AsyncMock(return_value=rating_text_elem)
        
        mock_page.query_selector = AsyncMock(side_effect=[
            None,  # advertiser-info-header
            rating_container,  # rating-container
            None,  # properties-container
            None   # extended-advertiser-info__icon-text
        ])
        
        info = await extractor.extract_advertiser_info()
        
        assert info['advertiser_rating'] == 5.0
        assert info['advertiser_rating_count'] == 1
    
    async def test_extract_advertiser_info_properties_count(self, mock_page):
        """Test advertiser properties count extraction"""
        extractor = DataExtractor(mock_page)
        
        # Mock properties container
        properties_elem = AsyncMock()
        properties_elem.text_content = AsyncMock(return_value="1.997 imóveis cadastrados")
        
        mock_page.query_selector = AsyncMock(side_effect=[
            None,  # advertiser-info-header
            None,  # rating-container
            properties_elem,  # properties-container
            None   # extended-advertiser-info__icon-text
        ])
        
        info = await extractor.extract_advertiser_info()
        
        assert info['advertiser_properties_count'] == 1997
    
    async def test_extract_advertiser_info_all_fields(self, mock_page):
        """Test extraction of all advertiser info fields"""
        extractor = DataExtractor(mock_page)
        
        # Mock advertiser header
        advertiser_header = AsyncMock()
        name_elem = AsyncMock()
        name_elem.text_content = AsyncMock(return_value="Test Imobiliaria")
        premium_icon = AsyncMock()
        advertiser_header.query_selector = AsyncMock(side_effect=[name_elem, premium_icon])
        advertiser_header.evaluate = AsyncMock(return_value="CRECI-12345")
        
        # Mock rating container
        rating_container = AsyncMock()
        rating_text_elem = AsyncMock()
        rating_text_elem.text_content = AsyncMock(return_value="4.5/5 (10 classificação)")
        rating_container.query_selector = AsyncMock(return_value=rating_text_elem)
        
        # Mock properties container
        properties_elem = AsyncMock()
        properties_elem.text_content = AsyncMock(return_value="4.061 imóveis cadastrados")
        
        mock_page.query_selector = AsyncMock(side_effect=[
            advertiser_header,
            rating_container,
            properties_elem,
            None
        ])
        
        info = await extractor.extract_advertiser_info()
        
        assert info['advertiser_name'] == "Test Imobiliaria"
        assert info['advertiser_creci'] == "CRECI-12345"
        assert info['advertiser_is_premium'] is True
        assert info['advertiser_rating'] == 4.5
        assert info['advertiser_rating_count'] == 10
        assert info['advertiser_properties_count'] == 4061


@pytest.mark.asyncio
class TestListingImagesExtraction:
    """Tests for listing images extraction from carousel"""
    
    async def test_extract_listing_images_success(self, mock_page):
        """Test successful image extraction from carousel"""
        extractor = DataExtractor(mock_page)
        
        # Mock carousel
        carousel = AsyncMock()
        
        # Mock carousel items with source elements
        item1 = AsyncMock()
        source1 = AsyncMock()
        source1.get_attribute = AsyncMock(
            return_value="https://resizedimgs.zapimoveis.com.br/img/vr-listing/test1.webp?action=fit-in&dimension=870x707"
        )
        item1.query_selector = AsyncMock(side_effect=[source1, None])  # source found, no img needed
        
        item2 = AsyncMock()
        source2 = AsyncMock()
        source2.get_attribute = AsyncMock(
            return_value="https://resizedimgs.zapimoveis.com.br/img/vr-listing/test2.webp?action=fit-in&dimension=870x707"
        )
        item2.query_selector = AsyncMock(side_effect=[source2, None])
        
        carousel.query_selector_all = AsyncMock(return_value=[item1, item2])
        mock_page.query_selector = AsyncMock(return_value=carousel)
        
        images = await extractor.extract_listing_images()
        
        assert len(images) == 2
        assert "test1.webp" in images[0]
        assert "test2.webp" in images[1]
        assert "dimension=870x707" in images[0]
    
    async def test_extract_listing_images_fallback_to_img(self, mock_page):
        """Test image extraction fallback to img element"""
        extractor = DataExtractor(mock_page)
        
        carousel = AsyncMock()
        
        item = AsyncMock()
        # No source element
        img_elem = AsyncMock()
        img_elem.get_attribute = AsyncMock(
            return_value="https://resizedimgs.zapimoveis.com.br/img/vr-listing/test.webp?action=fit-in&dimension=870x707 1080w"
        )
        item.query_selector = AsyncMock(side_effect=[None, img_elem])  # no source, use img
        
        carousel.query_selector_all = AsyncMock(return_value=[item])
        mock_page.query_selector = AsyncMock(return_value=carousel)
        
        images = await extractor.extract_listing_images()
        
        assert len(images) == 1
        assert "test.webp" in images[0]
    
    async def test_extract_listing_images_no_carousel(self, mock_page):
        """Test image extraction when carousel not found"""
        extractor = DataExtractor(mock_page)
        mock_page.query_selector = AsyncMock(return_value=None)
        
        images = await extractor.extract_listing_images()
        
        assert images == []
    
    async def test_extract_listing_images_duplicate_removal(self, mock_page):
        """Test that duplicate images are removed"""
        extractor = DataExtractor(mock_page)
        
        carousel = AsyncMock()
        
        item1 = AsyncMock()
        source1 = AsyncMock()
        source1.get_attribute = AsyncMock(
            return_value="https://resizedimgs.zapimoveis.com.br/img/vr-listing/test.webp?action=fit-in&dimension=870x707"
        )
        item1.query_selector = AsyncMock(return_value=source1)
        
        item2 = AsyncMock()
        source2 = AsyncMock()
        source2.get_attribute = AsyncMock(
            return_value="https://resizedimgs.zapimoveis.com.br/img/vr-listing/test.webp?action=fit-in&dimension=870x707"
        )
        item2.query_selector = AsyncMock(return_value=source2)
        
        carousel.query_selector_all = AsyncMock(return_value=[item1, item2])
        mock_page.query_selector = AsyncMock(return_value=carousel)
        
        images = await extractor.extract_listing_images()
        
        assert len(images) == 1  # Duplicate removed


@pytest.mark.asyncio
class TestAllDeepDataExtraction:
    """Tests for complete deep data extraction"""
    
    async def test_extract_all_deep_data_integration(self, mock_page):
        """Test integration of all deep extraction methods"""
        extractor = DataExtractor(mock_page)
        
        # Mock all extraction methods to return data
        mock_page.query_selector = AsyncMock(return_value=None)
        mock_page.query_selector_all = AsyncMock(return_value=[])
        
        # Mock price details
        with patch.object(extractor, 'extract_deep_price_details', new_callable=AsyncMock) as mock_price:
            mock_price.return_value = {'sale_price': 320000.0, 'condo_fee': 306.0}
            
            # Mock characteristics
            with patch.object(extractor, 'extract_deep_characteristics', new_callable=AsyncMock) as mock_char:
                mock_char.return_value = {
                    'area': 82.0,
                    'bedrooms': 2,
                    'has_pool': True,
                    'amenities_list': ['Piscina']
                }
                
                # Mock location
                with patch.object(extractor, 'extract_deep_location', new_callable=AsyncMock) as mock_loc:
                    mock_loc.return_value = "Rua Test, 123, Bairro, Cidade"
                    
                    # Mock description
                    with patch.object(extractor, 'extract_deep_description', new_callable=AsyncMock) as mock_desc:
                        mock_desc.return_value = "Apartamento espaçoso"
                        
                        # Mock advertiser info
                        with patch.object(extractor, 'extract_advertiser_info', new_callable=AsyncMock) as mock_adv:
                            mock_adv.return_value = {
                                'advertiser_name': 'Test Imobiliaria',
                                'advertiser_rating': 5.0
                            }
                            
                            # Mock property codes
                            with patch.object(extractor, 'extract_property_codes', new_callable=AsyncMock) as mock_codes:
                                mock_codes.return_value = {'advertiser_code': 'ABC123'}
                                
                                # Mock dates
                                with patch.object(extractor, 'extract_listing_dates', new_callable=AsyncMock) as mock_dates:
                                    mock_dates.return_value = {'created_date': '01/01/2024'}
                                    
                                    # Mock contact
                                    with patch.object(extractor, 'extract_contact_info', new_callable=AsyncMock) as mock_contact:
                                        mock_contact.return_value = {'has_whatsapp': True}
                                        
                                        # Mock images
                                        with patch.object(extractor, 'extract_listing_images', new_callable=AsyncMock) as mock_images:
                                            mock_images.return_value = [
                                                'https://example.com/img1.jpg',
                                                'https://example.com/img2.jpg'
                                            ]
                                            
                                            deep_data = await extractor.extract_all_deep_data()
                                            
                                            assert deep_data['sale_price'] == 320000.0
                                            assert deep_data['area'] == 82.0
                                            assert deep_data['bedrooms'] == 2
                                            assert deep_data['has_pool'] is True
                                            assert deep_data['full_address'] == "Rua Test, 123, Bairro, Cidade"
                                            assert deep_data['full_description'] == "Apartamento espaçoso"
                                            assert deep_data['advertiser_name'] == 'Test Imobiliaria'
                                            assert deep_data['advertiser_rating'] == 5.0
                                            assert deep_data['advertiser_code'] == 'ABC123'
                                            assert deep_data['created_date'] == '01/01/2024'
                                            assert deep_data['has_whatsapp'] is True
                                            assert len(deep_data['images']) == 2
                                            assert deep_data['image_count'] == 2
                                            assert 'amenities' in deep_data  # amenities_list renamed to amenities
                                            assert 'Piscina' in deep_data['amenities']
    
    async def test_extract_all_deep_data_empty(self, mock_page):
        """Test extract_all_deep_data when no data found"""
        extractor = DataExtractor(mock_page)
        
        mock_page.query_selector = AsyncMock(return_value=None)
        mock_page.query_selector_all = AsyncMock(return_value=[])
        
        deep_data = await extractor.extract_all_deep_data()
        
        # Should return empty dict or dict with None values
        assert isinstance(deep_data, dict)
        # Images should not be present if empty
        assert 'images' not in deep_data or deep_data.get('images') == []

