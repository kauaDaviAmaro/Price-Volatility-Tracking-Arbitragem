"""
Image Downloader - Handles asynchronous image downloads
Manages image downloads, directory creation, and rate limiting
"""
import asyncio
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional

import aiofiles
import aiohttp

from src.config import Config

logger = logging.getLogger(__name__)


class ImageDownloader:
    """Manages asynchronous image downloads for listings"""
    
    def __init__(self, output_dir: Path):
        """
        Initialize Image Downloader
        
        Args:
            output_dir: Base output directory where images will be stored
        """
        self.output_dir = Path(output_dir)
        self.images_dir = self.output_dir / "images"
        self.images_dir.mkdir(parents=True, exist_ok=True)
    
    def get_image_extension(self, image_url: str) -> str:
        """Extract and validate image file extension from URL"""
        if '.' not in image_url:
            return 'jpg'
        
        ext = image_url.split('.')[-1].split('?')[0].lower()
        valid_extensions = ['jpg', 'jpeg', 'png', 'gif', 'webp']
        return ext if ext in valid_extensions else 'jpg'
    
    def get_listing_id_from_url(self, url: str) -> str:
        """
        Extract a safe listing ID from URL for folder naming
        
        Args:
            url: Listing URL
            
        Returns:
            Safe folder name for the listing
        """
        if not url:
            return "unknown"
        
        # Try to extract ID from URL
        id_match = re.search(r'id-(\d+)', url)
        if id_match:
            return f"listing_{id_match.group(1)}"
        
        # Fallback: use last part of URL
        url_parts = url.rstrip('/').split('/')
        if url_parts:
            last_part = url_parts[-1].split('?')[0]  # Remove query params
            # Sanitize for filesystem
            safe_name = re.sub(r'[^\w\-_\.]', '_', last_part)
            return safe_name[:50]  # Limit length
        
        return "unknown"
    
    def _normalize_images_list(self, images) -> List[str]:
        """Normalize images input to a list of URLs"""
        if not images:
            return []
        
        # Handle case where images might be a string (from CSV)
        if isinstance(images, str):
            # Split by comma and strip whitespace
            return [img.strip() for img in images.split(',') if img.strip()]
        
        if isinstance(images, list):
            return [str(img) for img in images if img]
        
        return []
    
    async def _download_single_image(
        self,
        session: aiohttp.ClientSession,
        image_url: str,
        index: int,
        output_path: Path,
        max_images: int = 20
    ) -> Optional[str]:
        """
        Download a single image
        
        Args:
            session: aiohttp session
            image_url: URL of the image to download
            index: Index of the image (for naming)
            output_path: Directory where to save the image
            max_images: Maximum number of images to download
            
        Returns:
            Relative path to downloaded image, or None if failed
        """
        if index >= max_images:
            return None
        
        try:
            async with session.get(image_url, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status != 200:
                    logger.debug(f"Failed to download image {image_url}: HTTP {response.status}")
                    return None
                
                ext = self.get_image_extension(image_url)
                filename = f"image_{index+1:03d}.{ext}"
                filepath = output_path / filename
                
                # Save image content
                async with aiofiles.open(filepath, 'wb') as f:
                    async for chunk in response.content.iter_chunked(8192):
                        await f.write(chunk)
                
                logger.debug(f"Downloaded image {index+1}/{max_images}: {filename}")
                
                # Calculate relative path
                relative_path = filepath.relative_to(self.output_dir)
                return str(relative_path).replace('\\', '/')  # Normalize path separators
                
        except Exception as e:
            logger.debug(f"Error downloading image {image_url}: {e}")
            return None
    
    async def download_listing_images(
        self,
        listing: Dict,
        max_images: int = 20
    ) -> None:
        """
        Downloads images for a single listing
        
        Args:
            listing: Listing dictionary with image URLs
            max_images: Maximum number of images to download per listing
        """
        if not Config.SAVE_IMAGES:
            logger.debug("SAVE_IMAGES is disabled, skipping image download")
            return
        
        images = listing.get("images", [])
        images_list = self._normalize_images_list(images)
        
        if not images_list:
            logger.debug("No images found in listing")
            return
        
        listing_url = listing.get("url", "")
        listing_id = self.get_listing_id_from_url(listing_url)
        image_dir = self.images_dir / listing_id
        image_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Downloading up to {min(len(images_list), max_images)} images to: {image_dir}")
        
        # Download images with rate limiting
        downloaded_paths = []
        async with aiohttp.ClientSession() as session:
            for i, image_url in enumerate(images_list[:max_images]):
                relative_path = await self._download_single_image(
                    session, image_url, i, image_dir, max_images
                )
                
                if relative_path:
                    downloaded_paths.append(relative_path)
                
                # Delay between downloads to avoid overwhelming the server
                if i < len(images_list[:max_images]) - 1:
                    await asyncio.sleep(Config.IMAGE_DOWNLOAD_DELAY)
        
        # Update listing with local image paths
        if downloaded_paths:
            listing['images_local'] = downloaded_paths
            listing['images_local_count'] = len(downloaded_paths)
            logger.info(f"Downloaded {len(downloaded_paths)} images for listing {listing_id}")
        else:
            logger.debug(f"No images were successfully downloaded for listing {listing_id}")

