#!/usr/bin/env python3
"""
Example usage of the school scraper.

This script demonstrates how to use the scraper.py module to fetch school data
from escuelasmex.com with different ct_codes.
"""

import asyncio
import logging
import random
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple
from dataclasses import dataclass, field

from scraper import scrape_school_by_code, scrape_schools_batch, SchoolScraper, print_school_data, SchoolData

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper_log.txt', mode='a'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class ScraperConfig:
    """Configuration for the scraper."""
    max_concurrent_requests: int = 3
    batch_delay_min: float = 1.0
    batch_delay_max: float = 2.0
    retry_max_attempts: int = 3
    retry_base_delay: float = 1.0
    retry_max_delay: float = 60.0
    request_jitter_min: float = 0.1
    request_jitter_max: float = 0.5
    output_file: str = "ct_codes_coords_googlelinks_federal_primaria.txt"
    progress_file: str = "scraper_progress_federal_primaria.txt"
    failed_codes_file: str = "failed_ct_codes_federal_primaria.txt"


@dataclass
class ScrapeResult:
    """Enhanced result with retry tracking."""
    school_data: Optional[SchoolData] = None
    ct_code: str = ""
    attempts: int = 0
    last_error: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)


class RateLimitedScraper:
    """Wrapper for SchoolScraper with rate limiting and retry logic."""
    
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.semaphore = asyncio.Semaphore(config.max_concurrent_requests)
        self.request_count = 0
        self.last_request_time = 0
        
    async def scrape_with_retry(self, scraper: SchoolScraper, ct_code: str) -> ScrapeResult:
        """Scrape with exponential backoff retry logic."""
        result = ScrapeResult(ct_code=ct_code)
        
        for attempt in range(1, self.config.retry_max_attempts + 1):
            result.attempts = attempt
            
            try:
                # Add random jitter before request
                jitter = random.uniform(
                    self.config.request_jitter_min,
                    self.config.request_jitter_max
                )
                await asyncio.sleep(jitter)
                
                # Acquire semaphore for rate limiting
                async with self.semaphore:
                    logger.info(f"Attempt {attempt}/{self.config.retry_max_attempts} for {ct_code}")
                    
                    # Make the request
                    school_data = await scraper.scrape_school_data(ct_code)
                    
                    if school_data.success:
                        result.school_data = school_data
                        logger.info(f"Successfully scraped {ct_code} on attempt {attempt}")
                        return result
                    else:
                        result.last_error = school_data.error_message
                        logger.warning(f"Failed to scrape {ct_code}: {school_data.error_message}")
                        
            except asyncio.TimeoutError as e:
                result.last_error = f"Timeout: {str(e)}"
                logger.error(f"Timeout for {ct_code} on attempt {attempt}")
            except Exception as e:
                result.last_error = str(e)
                logger.error(f"Error scraping {ct_code} on attempt {attempt}: {e}")
            
            # Calculate backoff delay if not the last attempt
            if attempt < self.config.retry_max_attempts:
                delay = min(
                    self.config.retry_base_delay * (2 ** (attempt - 1)),
                    self.config.retry_max_delay
                )
                # Add jitter to backoff
                delay += random.uniform(0, delay * 0.1)
                logger.info(f"Retrying {ct_code} in {delay:.2f} seconds...")
                await asyncio.sleep(delay)
        
        logger.error(f"Failed to scrape {ct_code} after {self.config.retry_max_attempts} attempts")
        return result


def load_progress(progress_file: str) -> set:
    """Load already processed CT codes from progress file."""
    processed = set()
    if Path(progress_file).exists():
        with open(progress_file, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    # Extract ct_code from saved line
                    ct_code = line.split('-')[0].strip()
                    processed.add(ct_code)
    return processed


def save_result_incrementally(result: ScrapeResult, config: ScraperConfig) -> bool:
    """Save successful result to file incrementally."""
    if result.school_data and result.school_data.success and result.school_data.href:
        # Extract coordinates from Google Maps URL
        coord_match = re.search(r'place/(-?\d+\.\d+),(-?\d+\.\d+)', result.school_data.href)
        if coord_match:
            lat, lng = coord_match.groups()
            coords = f"{lat},{lng}"
            line = f"{result.school_data.ct_code}-{coords}-{result.school_data.href}"
            
            # Append to output file
            with open(config.output_file, 'a', encoding='utf-8') as f:
                f.write(line + "\n")
            
            # Also save to progress file
            with open(config.progress_file, 'a', encoding='utf-8') as f:
                f.write(line + "\n")
            
            logger.info(f"Saved result for {result.ct_code}: {coords}")
            return True
    return False


def save_failed_code(ct_code: str, error: str, config: ScraperConfig):
    """Save failed CT code for later retry."""
    with open(config.failed_codes_file, 'a', encoding='utf-8') as f:
        f.write(f"{ct_code}|{error}|{datetime.now().isoformat()}\n")


def print_progress_bar(current: int, total: int, success: int, failed: int, width: int = 50):
    """Print a progress bar with statistics."""
    percentage = (current / total) * 100 if total > 0 else 0
    filled = int((current / total) * width) if total > 0 else 0
    bar = '█' * filled + '░' * (width - filled)
    
    print(f"\r[{bar}] {percentage:.1f}% | {current}/{total} | ✅ {success} | ❌ {failed}", end='', flush=True)


async def read_ct_codes_from_file():
    """Read ct_codes from file and create ct_code-coords-googlelink txt file with advanced scraping."""
    config = ScraperConfig()
    
    logger.info("=== Starting Advanced CT Code Processing ===")
    print("\n=== Advanced CT Code Processing ===")
    print(f"Configuration:")
    print(f"  - Max concurrent requests: {config.max_concurrent_requests}")
    print(f"  - Retry attempts: {config.retry_max_attempts}")
    print(f"  - Batch delay: {config.batch_delay_min}-{config.batch_delay_max}s")
    print(f"  - Output file: {config.output_file}")
    print(f"  - Progress tracking: {config.progress_file}")
    print(f"  - Failed codes log: {config.failed_codes_file}\n")
    
    try:
        # Read ct_codes from file
        with open("clave_ct_list_federal_primaria.txt", "r", encoding="utf-8") as f:
            all_ct_codes = [line.strip() for line in f if line.strip()]
        
        if not all_ct_codes:
            logger.warning("No ct_codes found in file.")
            print("No ct_codes found in file.")
            return
        
        # Load progress and filter out already processed codes
        processed_codes = load_progress(config.progress_file)
        ct_codes = [code for code in all_ct_codes if code not in processed_codes]
        
        if not ct_codes:
            print(f"All {len(all_ct_codes)} codes have already been processed.")
            logger.info(f"All codes already processed. Check {config.output_file}")
            return
        
        print(f"Found {len(all_ct_codes)} total codes, {len(processed_codes)} already processed")
        print(f"Processing {len(ct_codes)} remaining codes...\n")
        logger.info(f"Processing {len(ct_codes)} ct_codes (skipping {len(processed_codes)} already done)")
        
        # Initialize rate limiter and counters
        rate_limiter = RateLimitedScraper(config)
        successful_count = 0
        failed_count = 0
        total_processed = 0
        
        # Process in batches
        batch_size = config.max_concurrent_requests
        total_batches = (len(ct_codes) + batch_size - 1) // batch_size
        
        async with SchoolScraper(headless=True, timeout=45000) as scraper:
            for batch_num, i in enumerate(range(0, len(ct_codes), batch_size), 1):
                batch = ct_codes[i:i + batch_size]
                
                logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} codes)")
                print(f"\n📦 Batch {batch_num}/{total_batches} ({len(batch)} codes)")
                
                # Create tasks for concurrent processing
                tasks = []
                for ct_code in batch:
                    task = rate_limiter.scrape_with_retry(scraper, ct_code)
                    tasks.append(task)
                
                # Process batch concurrently
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Process results
                for result in batch_results:
                    total_processed += 1
                    
                    if isinstance(result, Exception):
                        failed_count += 1
                        logger.error(f"Exception processing code: {result}")
                        continue
                    
                    if isinstance(result, ScrapeResult):
                        if save_result_incrementally(result, config):
                            successful_count += 1
                            print(f"  ✅ {result.ct_code} - Saved successfully")
                        else:
                            failed_count += 1
                            save_failed_code(result.ct_code, result.last_error or "Unknown error", config)
                            print(f"  ❌ {result.ct_code} - Failed: {result.last_error}")
                    
                    # Update progress bar
                    print_progress_bar(
                        total_processed + len(processed_codes),
                        len(all_ct_codes),
                        successful_count + len(processed_codes),
                        failed_count
                    )
                
                # Add delay between batches
                if batch_num < total_batches:
                    delay = random.uniform(config.batch_delay_min, config.batch_delay_max)
                    logger.info(f"Waiting {delay:.2f}s before next batch...")
                    print(f"\n⏳ Waiting {delay:.2f}s before next batch...")
                    await asyncio.sleep(delay)
        
        # Final summary
        print("\n\n" + "=" * 60)
        print("📊 FINAL SUMMARY")
        print("=" * 60)
        print(f"✅ Successfully processed: {successful_count} new codes")
        print(f"📝 Previously processed: {len(processed_codes)} codes")
        print(f"✅ Total successful: {successful_count + len(processed_codes)}/{len(all_ct_codes)}")
        print(f"❌ Failed: {failed_count} codes")
        print(f"📁 Results saved to: {config.output_file}")
        print(f"📋 Failed codes logged to: {config.failed_codes_file}")
        print(f"📊 Progress tracked in: {config.progress_file}")
        print("=" * 60)
        
        logger.info(f"Processing complete. Success: {successful_count}, Failed: {failed_count}")
        
    except FileNotFoundError:
        error_msg = "File 'clave_ct_list_federal_primaria.txt' not found."
        print(f"❌ {error_msg}")
        logger.error(error_msg)
    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        print(f"❌ {error_msg}")
        logger.error(error_msg, exc_info=True)


async def retry_failed_codes():
    """Retry processing of previously failed CT codes."""
    config = ScraperConfig()
    
    if not Path(config.failed_codes_file).exists():
        print(f"No failed codes file found: {config.failed_codes_file}")
        return
    
    # Read failed codes
    failed_codes = []
    with open(config.failed_codes_file, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                parts = line.split('|')
                if parts:
                    failed_codes.append(parts[0].strip())
    
    if not failed_codes:
        print("No failed codes to retry.")
        return
    
    # Remove duplicates
    failed_codes = list(set(failed_codes))
    
    print(f"\n=== Retrying {len(failed_codes)} Failed Codes ===")
    logger.info(f"Retrying {len(failed_codes)} failed codes")
    
    # Clear the failed codes file (we'll re-add any that fail again)
    Path(config.failed_codes_file).rename(f"{config.failed_codes_file}.bak")
    
    # Process failed codes with the same logic
    rate_limiter = RateLimitedScraper(config)
    successful_count = 0
    still_failed_count = 0
    
    async with SchoolScraper(headless=True, timeout=45000) as scraper:
        for i in range(0, len(failed_codes), config.max_concurrent_requests):
            batch = failed_codes[i:i + config.max_concurrent_requests]
            
            tasks = []
            for ct_code in batch:
                task = rate_limiter.scrape_with_retry(scraper, ct_code)
                tasks.append(task)
            
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in batch_results:
                if isinstance(result, ScrapeResult):
                    if save_result_incrementally(result, config):
                        successful_count += 1
                        print(f"  ✅ {result.ct_code} - Now successful!")
                    else:
                        still_failed_count += 1
                        save_failed_code(result.ct_code, result.last_error or "Unknown error", config)
                        print(f"  ❌ {result.ct_code} - Still failing")
            
            # Add delay between batches
            if i + config.max_concurrent_requests < len(failed_codes):
                delay = random.uniform(config.batch_delay_min, config.batch_delay_max)
                await asyncio.sleep(delay)
    
    print(f"\n📊 Retry Summary:")
    print(f"✅ Successfully recovered: {successful_count}/{len(failed_codes)}")
    print(f"❌ Still failing: {still_failed_count}")


async def main():
    """Run all examples."""
    print("🚀 Advanced School Scraper")
    print("=" * 60)
    
    # Check if user wants to retry failed codes
    if len(sys.argv) > 1 and sys.argv[1] == "--retry-failed":
        await retry_failed_codes()
    else:
        await read_ct_codes_from_file()
    
    print("\n✨ All operations completed!")


if __name__ == "__main__":
    asyncio.run(main())