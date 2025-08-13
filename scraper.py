#!/usr/bin/env python3
"""
Web scraper for escuelasmex.com using Playwright.

This module provides functionality to scrape school data from escuelasmex.com
using the Playwright library with async/await patterns for optimal performance.
"""

import asyncio
import logging
from typing import Dict, Optional, Any, List
from dataclasses import dataclass
from playwright.async_api import async_playwright, Browser, Page, TimeoutError, Error

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class SchoolData:
    """Data class to represent scraped school information."""
    text: Optional[str] = None
    href: Optional[str] = None
    title: Optional[str] = None
    ct_code: Optional[str] = None
    success: bool = False
    error_message: Optional[str] = None


class SchoolScraper:
    """
    A robust web scraper for escuelasmex.com using Playwright.
    
    This class handles browser management, error handling, and data extraction
    with proper async/await patterns.
    """
    
    def __init__(
        self,
        headless: bool = True,
        timeout: int = 30000,
        user_agent: Optional[str] = None
    ):
        """
        Initialize the scraper with configuration options.
        
        Args:
            headless: Whether to run browser in headless mode
            timeout: Timeout for page operations in milliseconds
            user_agent: Custom user agent string
        """
        self.headless = headless
        self.timeout = timeout
        self.user_agent = user_agent or (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        self.browser: Optional[Browser] = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self._initialize_browser()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit with proper cleanup."""
        await self._cleanup()
    
    async def _initialize_browser(self) -> None:
        """Initialize the Playwright browser with proper configuration."""
        try:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(
                headless=self.headless,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled'
                ]
            )
            logger.info("Browser initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize browser: {e}")
            raise
    
    async def _cleanup(self) -> None:
        """Clean up browser resources."""
        try:
            if self.browser:
                await self.browser.close()
                logger.info("Browser closed successfully")
            if hasattr(self, 'playwright'):
                await self.playwright.stop()
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
    
    async def _create_page(self) -> Page:
        """Create a new page with proper configuration."""
        if not self.browser:
            raise RuntimeError("Browser not initialized. Use async context manager.")
        
        # Create browser context with user agent
        context = await self.browser.new_context(
            user_agent=self.user_agent,
            viewport={"width": 1280, "height": 720}
        )
        
        page = await context.new_page()
        
        # Set default timeout
        page.set_default_timeout(self.timeout)
        
        return page
    
    async def scrape_school_data(self, ct_code: str) -> SchoolData:
        """
        Scrape school data for a given ct_code.
        
        Args:
            ct_code: The school code to search for
            
        Returns:
            SchoolData object containing scraped information
        """
        url = f"https://escuelasmex.com/directorio/{ct_code}"
        xpath = "/html/body/div/div[5]/div[1]/div/a"
        
        logger.info(f"Scraping data for ct_code: {ct_code}")
        
        page = None
        try:
            page = await self._create_page()
            
            # Navigate to the URL with a more flexible wait strategy
            logger.info(f"Navigating to: {url}")
            response = await page.goto(url, wait_until="domcontentloaded")
            
            if not response:
                error_msg = "Failed to get response from page"
                logger.error(error_msg)
                return SchoolData(
                    ct_code=ct_code,
                    success=False,
                    error_message=error_msg
                )
                
            # Check if we got redirected or if the status is acceptable
            final_url = page.url
            if response.status >= 400:
                error_msg = f"HTTP error: {response.status} for URL: {final_url}"
                logger.error(error_msg)
                return SchoolData(
                    ct_code=ct_code,
                    success=False,
                    error_message=error_msg
                )
            
            logger.info(f"Page loaded successfully. Final URL: {final_url}")
            
            # Wait for the element to be present using XPath locator
            element = page.locator(f"xpath={xpath}")
            
            try:
                await element.wait_for(state="visible", timeout=10000)
            except TimeoutError:
                logger.warning(f"Target element not found for ct_code: {ct_code}")
                return SchoolData(
                    ct_code=ct_code,
                    success=False,
                    error_message="Target element not found"
                )
            
            # Check if element exists
            if await element.count() == 0:
                logger.warning(f"Element not found at xpath: {xpath}")
                return SchoolData(
                    ct_code=ct_code,
                    success=False,
                    error_message="Element not found at specified xpath"
                )
            
            # Extract data from the element
            text = await element.text_content()
            href = await element.get_attribute("href")
            title = await element.get_attribute("title")
            print(text, href, title)
            logger.info(f"Successfully scraped data for ct_code: {ct_code}")
            
            return SchoolData(
                text=text.strip() if text else None,
                href=href,
                title=title,
                ct_code=ct_code,
                success=True
            )
            
        except TimeoutError as e:
            error_msg = f"Timeout error for ct_code {ct_code}: {e}"
            logger.error(error_msg)
            return SchoolData(
                ct_code=ct_code,
                success=False,
                error_message=error_msg
            )
        except Error as e:
            error_msg = f"Playwright error for ct_code {ct_code}: {e}"
            logger.error(error_msg)
            return SchoolData(
                ct_code=ct_code,
                success=False,
                error_message=error_msg
            )
        except Exception as e:
            error_msg = f"Unexpected error for ct_code {ct_code}: {e}"
            logger.error(error_msg)
            return SchoolData(
                ct_code=ct_code,
                success=False,
                error_message=error_msg
            )
        finally:
            if page:
                # Close the page and its context
                context = page.context
                await page.close()
                await context.close()
    
    async def scrape_multiple_schools(self, ct_codes: List[str]) -> List[SchoolData]:
        """
        Scrape data for multiple school codes.
        
        Args:
            ct_codes: List of school codes to scrape
            
        Returns:
            List of SchoolData objects
        """
        logger.info(f"Starting batch scraping for {len(ct_codes)} schools")
        results = []
        
        for i, ct_code in enumerate(ct_codes, 1):
            logger.info(f"Processing school {i}/{len(ct_codes)}: {ct_code}")
            result = await self.scrape_school_data(ct_code)
            results.append(result)
            
            # Add a small delay between requests to be respectful
            if i < len(ct_codes):
                await asyncio.sleep(1)
        
        logger.info(f"Batch scraping completed. Success rate: {sum(1 for r in results if r.success)}/{len(results)}")
        return results


async def scrape_school_by_code(
    ct_code: str,
    headless: bool = True,
    timeout: int = 30000
) -> SchoolData:
    """
    Convenience function to scrape a single school by ct_code.
    
    Args:
        ct_code: The school code to search for
        headless: Whether to run browser in headless mode
        timeout: Timeout for operations in milliseconds
        
    Returns:
        SchoolData object containing scraped information
    """
    async with SchoolScraper(headless=headless, timeout=timeout) as scraper:
        return await scraper.scrape_school_data(ct_code)


async def scrape_schools_batch(
    ct_codes: List[str],
    headless: bool = True,
    timeout: int = 30000
) -> List[SchoolData]:
    """
    Convenience function to scrape multiple schools by ct_codes.
    
    Args:
        ct_codes: List of school codes to search for
        headless: Whether to run browser in headless mode
        timeout: Timeout for operations in milliseconds
        
    Returns:
        List of SchoolData objects
    """
    async with SchoolScraper(headless=headless, timeout=timeout) as scraper:
        return await scraper.scrape_multiple_schools(ct_codes)


def print_school_data(data: SchoolData) -> None:
    """Print school data in a formatted way."""
    print(f"{'='*50}")
    print(f"CT Code: {data.ct_code}")
    print(f"Success: {data.success}")
    if data.success:
        print(f"Text: {data.text}")
        print(f"Href: {data.href}")
        print(f"Title: {data.title}")
    else:
        print(f"Error: {data.error_message}")
    print(f"{'='*50}")


async def main():
    """
    Example usage of the school scraper.
    
    This function demonstrates how to use the scraper with different ct_codes.
    """
    # Example ct_codes - replace with actual codes
    example_codes = ["21DPR0653I", "21DPR0653I", "21DPR0653I"]
    
    print("School Data Scraper - Example Usage")
    print("="*50)
    
    # Example 1: Scrape a single school
    print("\n1. Scraping single school:")
    single_result = await scrape_school_by_code("21DPR0653I")
    print_school_data(single_result)
    
    # # Example 2: Scrape multiple schools
    # print("\n2. Scraping multiple schools:")
    # batch_results = await scrape_schools_batch(example_codes)
    # for result in batch_results:
    #     print_school_data(result)
    
    # # Example 3: Using the scraper class directly
    # print("\n3. Using scraper class directly:")
    # async with SchoolScraper(headless=True) as scraper:
    #     result = await scraper.scrape_school_data("21DPR0653I")
    #     print_school_data(result)


if __name__ == "__main__":
    # Run the example
    asyncio.run(main())