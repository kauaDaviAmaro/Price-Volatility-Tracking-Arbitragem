"""
CSV Storage Manager - Handles all CSV operations
Manages reading, writing, merging, and validation of CSV data
"""
import asyncio
import csv
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class CSVStorageManager:
    """Manages CSV file operations with file locking and data merging"""
    
    def __init__(self, output_dir: Path, filename: str = "scraped_data.csv"):
        """
        Initialize CSV Storage Manager
        
        Args:
            output_dir: Directory where CSV file will be stored
            filename: Name of the CSV file
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.filename = filename
        self.filepath = self.output_dir / filename
    
    def is_valid_fieldname(self, fieldname: str) -> bool:
        """Check if a fieldname is valid (not a generic column_* name)"""
        if not fieldname or not isinstance(fieldname, str):
            return False
        # Filter out generic column names like "column_42", "column_43", etc.
        if fieldname.startswith('column_') and fieldname[7:].isdigit():
            return False
        return True
    
    def filter_valid_fieldnames(self, fieldnames: List[str]) -> List[str]:
        """Filter out invalid fieldnames"""
        return [f for f in fieldnames if self.is_valid_fieldname(f)]
    
    def convert_result_to_row(self, result: Dict) -> Dict:
        """
        Convert a result dictionary to a CSV-compatible row.
        Preserves all data types and handles None/empty values correctly.
        """
        row = {}
        for key, value in result.items():
            # Skip invalid fieldnames
            if not self.is_valid_fieldname(key):
                continue
            
            # Handle None values
            if value is None:
                row[key] = None
            # Handle nested dictionaries by converting to string
            elif isinstance(value, dict):
                try:
                    row[key] = json.dumps(value, ensure_ascii=False)
                except (TypeError, ValueError):
                    row[key] = str(value)
            # Handle lists - preserve as comma-separated string
            elif isinstance(value, list):
                if len(value) == 0:
                    row[key] = None  # Empty list becomes None
                else:
                    row[key] = ', '.join(str(v) for v in value)
            # Handle all other types (str, int, float, bool, etc.)
            else:
                row[key] = value
        
        return row
    
    def get_all_fieldnames(self, listings: List[Dict]) -> List[str]:
        """Extract all unique field names from listings, filtering invalid ones"""
        fieldnames: Set[str] = set()
        for listing in listings:
            valid_keys = [k for k in listing.keys() if self.is_valid_fieldname(k)]
            fieldnames.update(valid_keys)
        return sorted(fieldnames)
    
    def ensure_csv_headers(self, fieldnames: List[str]) -> None:
        """Ensure CSV file exists with headers if it doesn't exist"""
        if not self.filepath.exists():
            with open(self.filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
    
    def _detect_has_headers(self, first_line: str) -> bool:
        """Detect if first line of CSV contains headers"""
        if not first_line:
            return False
        header_indicators = ['url', 'price', 'title', 'location', 'area', 'bedrooms', 'bathrooms']
        first_line_lower = first_line.lower()
        return any(indicator in first_line_lower for indicator in header_indicators)
    
    def _read_existing_data(self) -> Tuple[Dict[str, Dict], List[str]]:
        """
        Read existing CSV data, preserving ALL data including empty fields.
        This is critical for deep search to not lose any existing data.
        Handles CSVs with or without headers.
        
        Returns:
            Tuple of (existing_data dict keyed by URL, existing_fieldnames list)
        """
        existing_data: Dict[str, Dict] = {}
        existing_fieldnames: List[str] = []
        
        if not self.filepath.exists() or self.filepath.stat().st_size == 0:
            return existing_data, existing_fieldnames
        
        try:
            with open(self.filepath, 'r', newline='', encoding='utf-8') as f:
                # Read first line to check if it's a header or data
                first_line = f.readline().strip()
                f.seek(0)  # Reset to beginning
                
                has_headers = self._detect_has_headers(first_line)
                
                if has_headers:
                    # CSV has headers - use DictReader normally
                    reader = csv.DictReader(f)
                    existing_fieldnames = reader.fieldnames or []
                    existing_fieldnames = self.filter_valid_fieldnames(existing_fieldnames)
                    
                    # CRITICAL: Preserve fieldnames even if rows are empty
                    # This ensures we don't lose column structure
                    logger.debug(f"Found CSV with headers: {len(existing_fieldnames)} columns")
                else:
                    # CSV has no headers - read as list and try to infer structure
                    logger.warning(f"CSV file has no headers, reading as data and will add headers when saving")
                    f.seek(0)  # Reset to beginning
                    list_reader = csv.reader(f)
                    rows = list(list_reader)
                    
                    if not rows:
                        return existing_data, existing_fieldnames
                    
                    # Find the column that contains URLs to identify the 'url' column
                    url_column_index = None
                    for row in rows[:10]:  # Check first 10 rows
                        for i, value in enumerate(row):
                            if isinstance(value, str) and 'zapimoveis.com.br/imovel' in value:
                                url_column_index = i
                                break
                        if url_column_index is not None:
                            break
                    
                    # Try to infer fieldnames from known structure
                    # We know common field positions from typical CSV structure
                    # But since we can't reliably infer, we'll use the data itself
                    # and let the save process create proper headers
                    
                    # For now, read all rows as data (not as dicts)
                    # We'll extract fieldnames from the data when we have listings
                    for row in rows:
                        # Check if row is completely empty
                        is_empty_row = True
                        for value in row:
                            if value and str(value).strip():
                                is_empty_row = False
                                break
                        
                        # Skip completely empty rows
                        if is_empty_row:
                            continue
                        
                        # Create a temporary dict with column indices as keys
                        row_dict = {f'column_{i}': value if value else None for i, value in enumerate(row)}
                        
                        # Try to find URL
                        url = None
                        if url_column_index is not None and url_column_index < len(row):
                            url = row[url_column_index].strip() if row[url_column_index] else None
                        
                        if not url:
                            # Search for URL in any column
                            for value in row:
                                if isinstance(value, str) and 'zapimoveis.com.br/imovel' in value:
                                    url = value.strip()
                                    break
                        
                        if url:
                            existing_data[url] = row_dict
                        else:
                            # No URL found, use index
                            key = f'listing_{len(existing_data) + 1}'
                            existing_data[key] = row_dict
                    
                    # Fieldnames will be inferred from data when saving
                    existing_fieldnames = []
                    # Don't use DictReader here since we already read the data
                    reader = iter([])  # Empty iterator since we already processed rows
                
                # Read ALL existing rows - preserve EVERYTHING including empty values
                row_count = 0
                for row in reader:
                    row_count += 1
                    
                    # Create a copy of the row to preserve all data
                    row_copy = {}
                    
                    # CRITICAL: Preserve ALL fieldnames from header, even if row values are empty
                    # This ensures we maintain the column structure
                    if has_headers and existing_fieldnames:
                        # For rows with headers, ensure ALL header fields are in row_copy
                        # This is critical - even if row is empty, we preserve the structure
                        for fieldname in existing_fieldnames:
                            if self.is_valid_fieldname(fieldname):
                                # Get value from row, default to empty string if not present
                                value = row.get(fieldname, '')
                                # Preserve the value as-is (empty strings become None for consistency)
                                row_copy[fieldname] = value.strip() if value and value.strip() else None
                    else:
                        # For rows without headers, process normally
                        for key, value in row.items():
                            if self.is_valid_fieldname(key):
                                row_copy[key] = value if value != '' else None
                    
                    # Check if row is completely empty (all values are None or empty)
                    is_empty_row = True
                    for value in row_copy.values():
                        if value is not None and str(value).strip():
                            is_empty_row = False
                            break
                    
                    # Skip completely empty rows - they don't contain any data
                    if is_empty_row:
                        logger.debug(f"Row {row_count} is completely empty, skipping")
                        continue
                    
                    # Find URL in the row
                    url = row_copy.get('url', '').strip() if row_copy.get('url') else ''
                    
                    if url:
                        # Use URL as key and store all row data
                        existing_data[url] = row_copy
                    else:
                        # Handle rows without URL - check if row has any URL-like values
                        url_found = False
                        for key, value in row_copy.items():
                            if isinstance(value, str) and 'zapimoveis.com.br/imovel' in value:
                                existing_data[value] = row_copy
                                url_found = True
                                break
                        
                        if not url_found:
                            # Row has data but no URL - this shouldn't happen in normal operation
                            # but we'll preserve it with an index key to avoid data loss
                            key = f'listing_{row_count}'
                            existing_data[key] = row_copy
                            logger.warning(f"Row {row_count} has data but no URL, preserving with index key: {key}")
            
            # If we had to infer fieldnames, try to extract real fieldnames from data
            if not has_headers and existing_data:
                # Extract fieldnames from all existing data
                all_keys = set()
                for listing_data in existing_data.values():
                    for key in listing_data.keys():
                        if self.is_valid_fieldname(key):
                            all_keys.add(key)
                if all_keys:
                    existing_fieldnames = sorted(all_keys)
                    logger.info(f"Inferred {len(existing_fieldnames)} fieldnames from existing data")
            
            # CRITICAL: Ensure we preserve fieldnames even if no data rows exist
            # This prevents losing column structure when CSV has header but empty rows
            if has_headers and existing_fieldnames and not existing_data:
                logger.info(f"CSV has header with {len(existing_fieldnames)} columns but no data rows - preserving header structure")
            
            logger.info(f"Read {len(existing_data)} existing listings from CSV with {len(existing_fieldnames)} fields")
        except Exception as e:
            logger.error(f"Error reading existing CSV: {e}", exc_info=True)
            existing_data = {}
            existing_fieldnames = []
        
        return existing_data, existing_fieldnames
    
    def _is_empty_value(self, val) -> bool:
        """Check if a value is considered empty"""
        if val is None:
            return True
        if isinstance(val, str):
            return val.strip() == '' or val.strip().lower() in ['none', 'null', 'false']
        if isinstance(val, list):
            return len(val) == 0
        return False
    
    def _normalize_value_for_comparison(self, val) -> str:
        """Normalize value for comparison (convert to string representation)"""
        if val is None:
            return ''
        if isinstance(val, list):
            return ', '.join(str(v) for v in val)
        return str(val)
    
    def _merge_listing_data(self, existing_row: Dict, new_listing: Dict) -> Dict:
        """
        Merge new listing data with existing row, preserving ALL existing data.
        Deep search data takes priority, but empty values never overwrite existing data.
        
        Args:
            existing_row: Existing row data from CSV
            new_listing: New listing data from deep search
            
        Returns:
            Merged dictionary with all data preserved
        """
        # Start with a deep copy of existing data to preserve everything
        merged = existing_row.copy()
        updated_fields = []
        new_fields = []
        
        for key, new_value in new_listing.items():
            # Skip invalid fieldnames
            if not self.is_valid_fieldname(key):
                continue
            
            existing_value = merged.get(key)
            
            # Convert values for comparison
            new_value_normalized = self._normalize_value_for_comparison(new_value)
            existing_value_normalized = self._normalize_value_for_comparison(existing_value) if existing_value is not None else ''
            
            # Strategy: Deep search data takes priority, but only if it's not empty
            if not self._is_empty_value(new_value):
                # New value is not empty - update (deep search data takes priority)
                merged[key] = new_value
                if existing_value is None:
                    new_fields.append(key)
                else:
                    updated_fields.append(key)
            elif existing_value is not None and not self._is_empty_value(existing_value):
                # Existing value is not empty, new value is empty - keep existing
                # Don't update, just keep the existing value
                pass
            elif existing_value is None:
                # Both are empty/None - set to None (new field with no data)
                merged[key] = None
        
        if updated_fields:
            logger.info(f"Updated {len(updated_fields)} existing fields: {', '.join(updated_fields[:10])}{'...' if len(updated_fields) > 10 else ''}")
        if new_fields:
            logger.info(f"Added {len(new_fields)} new fields: {', '.join(new_fields[:10])}{'...' if len(new_fields) > 10 else ''}")
        
        return merged
    
    def _is_technical_error(self, listing: Dict) -> bool:
        """Check if listing is a technical error that should not be saved"""
        listing_keys = set(listing.keys())
        
        # Check if it's only error and url fields
        if listing_keys == {'error', 'url'} or (len(listing_keys) == 2 and 'error' in listing_keys and 'url' in listing_keys):
            error_msg = listing.get('error', '')
            
            # Check if it's a browser initialization error
            if any(keyword in str(error_msg).lower() for keyword in ['browsertype.launch', 'xserver', 'display', 'xvfb', 'browser has been closed']):
                return True
            
            # Also skip if error message is very long (likely a technical error dump)
            if len(str(error_msg)) > 500:
                return True
        
        return False
    
    async def _acquire_lock(self, lock_file: Path, max_wait: float = 10.0) -> None:
        """Acquire file lock, waiting if necessary"""
        wait_time = 0.0
        while lock_file.exists() and wait_time < max_wait:
            await asyncio.sleep(0.1)
            wait_time += 0.1
    
    async def save_single_listing(
        self,
        listing: Dict,
        base_url: Optional[str] = None
    ) -> None:
        """
        Save a single listing to CSV, updating existing row or appending new one.
        Uses file locking to prevent data loss in concurrent scenarios.
        Preserves ALL existing data in the CSV.
        
        This method is critical for deep search - it must preserve all existing data
        while merging in new deep search data.
        
        Args:
            listing: Listing dictionary to save (from deep search)
            base_url: Base URL (for logging purposes, optional)
        """
        if not listing:
            logger.warning("Cannot save: listing is empty")
            return
        
        listing_url = listing.get('url')
        if not listing_url:
            logger.warning("Cannot save: listing has no URL")
            return
        
        # Don't save technical errors
        if self._is_technical_error(listing):
            error_msg = listing.get('error', '')
            logger.warning(f"Skipping save: Technical error detected: {error_msg[:100]}...")
            return
        
        # Use a lock file to prevent concurrent writes
        lock_file = self.filepath.with_suffix('.lock')
        
        # Wait for lock to be released
        await self._acquire_lock(lock_file)
        
        try:
            # Create lock file
            lock_file.touch(exist_ok=True)
            
            # Read ALL existing data from CSV
            existing_data, existing_fieldnames = self._read_existing_data()
            
            # Check if this listing already exists
            is_update = listing_url in existing_data
            
            if is_update:
                # Merge with existing data - preserve ALL existing fields
                logger.info(f"Updating existing listing: {listing_url}")
                existing_data[listing_url] = self._merge_listing_data(existing_data[listing_url], listing)
            else:
                # New listing - add it
                logger.info(f"Adding new listing: {listing_url}")
                existing_data[listing_url] = listing.copy()
            
            # Collect ALL fieldnames from ALL listings (existing + new)
            # CRITICAL: Preserve existing fieldnames first to maintain column structure
            # This prevents losing columns when updating with deep search data
            all_fieldnames: Set[str] = set()
            
            # First, add fieldnames from existing data (preserve existing structure)
            # This ensures we don't lose columns when updating
            for listing_data in existing_data.values():
                for key in listing_data.keys():
                    if self.is_valid_fieldname(key):
                        all_fieldnames.add(key)
            
            # Then, add fieldnames from new listing (these may add new columns)
            for key in listing.keys():
                if self.is_valid_fieldname(key):
                    all_fieldnames.add(key)
            
            # Sort fieldnames for consistent output
            all_fieldnames = sorted(all_fieldnames)
            
            # CRITICAL: Ensure we have valid fieldnames before writing
            if not all_fieldnames:
                logger.error("No valid fieldnames found! Cannot write CSV without headers.")
                return
            
            # Write ALL data back to CSV (preserving ALL existing listings and data)
            # ALWAYS write header - this is critical!
            written_count = 0
            with open(self.filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=all_fieldnames)
                # ALWAYS write header - never skip this!
                writer.writeheader()
                
                # Write all listings in sorted order (by URL) for consistency
                # Sort by URL if available, otherwise by key
                def sort_key(item):
                    url_key, listing_data = item
                    url = listing_data.get('url', '')
                    if url:
                        return (0, url)  # Entries with URL come first, sorted by URL
                    else:
                        return (1, url_key)  # Entries without URL come after, sorted by key
                
                for url_key, listing_data in sorted(existing_data.items(), key=sort_key):
                    # Convert listing to row format
                    row = self.convert_result_to_row(listing_data)
                    
                    # Ensure ALL fieldnames are present in row (fill missing with None)
                    for fieldname in all_fieldnames:
                        if fieldname not in row:
                            row[fieldname] = None
                    
                    writer.writerow(row)
                    written_count += 1
            
            action = "Updated" if is_update else "Added"
            logger.info(f"{action} listing in CSV: {listing_url} ({len(all_fieldnames)} columns, {written_count} total listings)")
            
        except Exception as e:
            logger.error(f"Error saving listing {listing_url}: {e}", exc_info=True)
            raise
        finally:
            # Remove lock file
            try:
                if lock_file.exists():
                    lock_file.unlink()
            except Exception as e:
                logger.debug(f"Error removing lock file: {e}")
    
    def save_page_listings(
        self,
        page_num: int,
        page_listings: List[Dict]
    ) -> None:
        """
        Save a page of listings to CSV file incrementally (append mode)
        
        Args:
            page_num: Page number that was scraped
            page_listings: List of listings from this page
        """
        if not page_listings:
            logger.debug(f"No listings to save for page {page_num}")
            return
        
        # Get all fieldnames from listings
        all_fieldnames = self.get_all_fieldnames(page_listings)
        
        # If file exists, read existing fieldnames and merge
        if self.filepath.exists():
            try:
                with open(self.filepath, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    existing_fieldnames = reader.fieldnames or []
                    existing_fieldnames = self.filter_valid_fieldnames(existing_fieldnames)
                    all_fieldnames = sorted(set(all_fieldnames) | set(existing_fieldnames))
            except Exception as e:
                logger.warning(f"Error reading existing CSV headers: {e}. Using new fieldnames only.")
        
        # Ensure file exists with headers
        self.ensure_csv_headers(all_fieldnames)
        
        # Append page listings to CSV
        with open(self.filepath, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=all_fieldnames)
            
            for listing in page_listings:
                row = self.convert_result_to_row(listing)
                # Ensure all fieldnames are present in row
                for fieldname in all_fieldnames:
                    if fieldname not in row:
                        row[fieldname] = None
                writer.writerow(row)
        
        logger.info(f"Saved {len(page_listings)} listings from page {page_num} to {self.filepath}")
    
    def save_listings_batch(
        self,
        listings: List[Dict]
    ) -> None:
        """
        Save a batch of listings to CSV, updating existing rows or appending new ones.
        Preserves ALL existing data and merges new deep search data correctly.
        
        Args:
            listings: List of listings to save (from deep search)
        """
        if not listings:
            logger.debug("No listings to save")
            return
        
        # Read ALL existing data first
        existing_data, existing_fieldnames = self._read_existing_data()
        
        # Track updates and additions
        updated_count = 0
        added_count = 0
        
        # Merge new listings with existing data
        for listing in listings:
            url = listing.get('url')
            if not url:
                logger.warning(f"Skipping listing without URL: {listing.get('title', 'Unknown')}")
                continue
            
            if url in existing_data:
                # Update existing listing - preserve all existing data
                existing_data[url] = self._merge_listing_data(existing_data[url], listing)
                updated_count += 1
            else:
                # Add new listing
                existing_data[url] = listing.copy()
                added_count += 1
        
        # Collect ALL fieldnames from ALL listings
        all_fieldnames: Set[str] = set(existing_fieldnames)
        for listing_data in existing_data.values():
            for key in listing_data.keys():
                if self.is_valid_fieldname(key):
                    all_fieldnames.add(key)
        
        # Sort fieldnames for consistent output
        all_fieldnames = sorted(all_fieldnames)
        
        # CRITICAL: Ensure we have valid fieldnames before writing
        if not all_fieldnames:
            logger.error("No valid fieldnames found! Cannot write CSV without headers.")
            return
        
        # Filter out generic column_* names - they should be replaced with real fieldnames
        all_fieldnames = [f for f in all_fieldnames if self.is_valid_fieldname(f)]
        
        if not all_fieldnames:
            logger.error("No valid fieldnames after filtering! Cannot write CSV.")
            return
        
        # Write ALL data back to CSV (preserving ALL existing listings and data)
        # ALWAYS write header - this is critical!
        with open(self.filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=all_fieldnames)
            # ALWAYS write header - never skip this!
            writer.writeheader()
            
            # Write all listings in sorted order (by URL) for consistency
            # Sort by URL if available, otherwise by key
            def sort_key(listing_data):
                url = listing_data.get('url', '')
                if url:
                    return (0, url)  # Entries with URL come first, sorted by URL
                else:
                    # For entries without URL, try to find a key in existing_data
                    for key, data in existing_data.items():
                        if data is listing_data:
                            return (1, key)  # Entries without URL come after, sorted by key
                    return (1, '')  # Fallback
            
            for listing_data in sorted(existing_data.values(), key=sort_key):
                row = self.convert_result_to_row(listing_data)
                # Ensure ALL fieldnames are present in row (fill missing with None)
                for fieldname in all_fieldnames:
                    if fieldname not in row:
                        row[fieldname] = None
                writer.writerow(row)
        
        logger.info(f"Saved batch: {updated_count} updated, {added_count} added, {len(existing_data)} total listings, {len(all_fieldnames)} columns")
    
    def save_results(self, results: List[Dict]) -> None:
        """
        Save results to CSV file, flattening search results if needed
        
        Args:
            results: List of results (may contain search_results with listings)
        """
        if not results:
            logger.warning("No results to save")
            return
        
        # Flatten search results
        flattened_results = []
        for result in results:
            if result.get("type") == "search_results" and "listings" in result:
                flattened_results.extend(result.get("listings", []))
            else:
                flattened_results.append(result)
        
        if not flattened_results:
            logger.warning("No results to save after flattening")
            return
        
        # Get all fieldnames
        fieldnames = self.get_all_fieldnames(flattened_results)
        
        # Write to CSV
        with open(self.filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for result in flattened_results:
                row = self.convert_result_to_row(result)
                writer.writerow(row)
        
        logger.info(f"Saved {len(flattened_results)} listings to {self.filepath}")

