#!/usr/bin/env python3
"""
Script to scrape apps from Shopify app store sitemap.
"""
import asyncio
import logging
import random
import re
from datetime import datetime
from typing import Dict, List, Set
import os
import sys
from urllib.parse import urlparse

import aiohttp
from bs4 import BeautifulSoup
import pandas as pd
from tqdm.asyncio import tqdm

# Configuration
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
TOP_APPS_RAW = os.path.join(DATA_DIR, 'top_apps_raw.csv')
REQUEST_TIMEOUT = 30
CONCURRENT_REQUESTS = 3  # Reduced to avoid rate limiting
MIN_DELAY = 5  # Increased from 2
MAX_DELAY = 10  # Increased from 5
TARGET_APPS_WITH_INTEGRATIONS = 1000  # Updated target: minimum apps with integrations

# Base URL
BASE_URL = 'https://apps.shopify.com'

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('scraping.log')
    ]
)
logger = logging.getLogger(__name__)

# User agent rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Edge/120.0.0.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
]

def get_random_headers() -> Dict[str, str]:
    """Get random headers for requests."""
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache'
    }

async def fetch_with_retry(session: aiohttp.ClientSession, url: str, max_retries: int = 3) -> str:
    """Fetch URL with retry logic."""
    for attempt in range(max_retries):
        try:
            headers = get_random_headers()
            logger.info(f"Fetching {url} (attempt {attempt + 1}/{max_retries})")
            async with session.get(url, headers=headers, timeout=REQUEST_TIMEOUT) as response:
                if response.status == 429:  # Rate limited
                    wait_time = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited on {url}, waiting {wait_time}s (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(wait_time)
                    continue
                    
                if response.status == 404:
                    logger.warning(f"URL not found: {url}")
                    return ''
                    
                if response.status != 200:
                    logger.warning(f"Got status {response.status} for {url} (attempt {attempt + 1}/{max_retries})")
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt
                        logger.info(f"Waiting {wait_time}s before retry")
                        await asyncio.sleep(wait_time)
                        continue
                    return ''
                    
                logger.info(f"Successfully fetched {url}")
                return await response.text()
                
        except asyncio.TimeoutError:
            logger.warning(f"Timeout on {url}, attempt {attempt + 1}/{max_retries}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                logger.info(f"Waiting {wait_time}s before retry")
                await asyncio.sleep(wait_time)
            continue
            
        except Exception as e:
            logger.error(f"Error fetching {url}: {str(e)}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                logger.info(f"Waiting {wait_time}s before retry")
                await asyncio.sleep(wait_time)
            continue
            
    return ''

def is_direct_app_url(url: str) -> bool:
    """Check if URL is a direct app listing (no subdirectories)."""
    parsed = urlparse(url)
    path = parsed.path.strip('/')
    return (
        parsed.netloc == 'apps.shopify.com' and
        path and  # Not empty
        '/' not in path and  # No subdirectories
        path not in {'categories', 'collections', 'stories', 'partners', 'built-in-features'} and
        not path.startswith(('categories/', 'collections/', 'stories/', 'partners/', 'built-in-features/'))
    )

async def get_app_urls(session: aiohttp.ClientSession) -> List[str]:
    """Get list of direct app URLs from the sitemap."""
    logger.info("Fetching sitemap")
    content = await fetch_with_retry(session, f"{BASE_URL}/sitemap.xml")
    if not content:
        return []
        
    soup = BeautifulSoup(content, 'xml')
    app_urls = []
    
    # Get all direct app URLs
    for loc in soup.find_all('loc'):
        url = loc.text
        if is_direct_app_url(url):
            app_urls.append(url)
            
    logger.info(f"Found {len(app_urls)} direct app URLs")
    return app_urls

async def extract_app_info(session: aiohttp.ClientSession, url: str) -> Dict:
    """Extract app information from an app page."""
    logger.info(f"Processing app: {url}")
    content = await fetch_with_retry(session, url)
    if not content:
        return None
        
    soup = BeautifulSoup(content, 'html.parser')
    
    try:
        # Get app name - first try meta tags, then fallback to page title
        app_name = None
        
        # Try meta og:title first
        og_title = soup.find('meta', {'property': 'og:title'})
        if og_title:
            # Clean up the app name by removing common suffixes
            app_name = og_title.get('content', '').split('|')[0].strip()
            app_name = re.sub(r'\s*[-–—]\s*(?:Shopify App|Zipchat App|App).*$', '', app_name)
            logger.info(f"Found app name from og:title: {app_name}")
            
        # If not found, try page title
        if not app_name:
            title_tag = soup.find('title')
            if title_tag:
                app_name = title_tag.text.split('|')[0].strip()
                app_name = re.sub(r'\s*[-–—]\s*(?:Shopify App|Zipchat App|App).*$', '', app_name)
                logger.info(f"Found app name from title tag: {app_name}")
        
        # If still not found, try heading tags
        if not app_name:
            heading = (
                soup.find('h1', {'class': ['heading--1', 'app-title', 'title']}) or
                soup.find('h2', {'class': ['heading--1', 'app-title', 'title']}) or
                soup.find('div', {'class': ['heading--1', 'app-title', 'title']})
            )
            if heading:
                app_name = heading.get_text().strip()
                app_name = re.sub(r'\s*[-–—]\s*(?:Shopify App|Zipchat App|App).*$', '', app_name)
                logger.info(f"Found app name from heading: {app_name}")
        
        if not app_name:
            logger.warning(f"Could not find app name for {url}")
            return None
        
        # Get description from meta tags first, then fallback to content
        desc_elem = soup.find('meta', {'property': 'og:description'})
        description = desc_elem.get('content', '').strip() if desc_elem else ''
        
        if not description:
            desc_elem = (
                soup.find('div', {'class': ['app-description', 'app-details-description', 'description']}) or
                soup.find('meta', {'name': 'description'})
            )
            description = desc_elem.get_text().strip() if desc_elem else ''
        
        logger.info(f"Description length: {len(description)} characters")
        
        # Get submission date from meta tags or use current time
        date_elem = soup.find('meta', {'property': 'article:published_time'})
        submission_date = (
            date_elem.get('content', '') if date_elem 
            else datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S+00:00')
        )
        logger.info(f"Submission date: {submission_date}")

        # Extract integrations from "Works with" section
        integrations = []
        
        # Find the "Works with" section - it's typically a heading followed by a list
        works_with_heading = soup.find(string=re.compile(r'^\s*Works with\s*$', re.IGNORECASE))
        if works_with_heading:
            logger.info("Found 'Works with' section")
            # The integrations are typically listed right after the heading
            parent = works_with_heading.parent
            if parent:
                # Look for list items or links after the heading
                integration_elements = parent.find_next_siblings(['ul', 'div'])
                for element in integration_elements:
                    # Look for links or list items
                    items = element.find_all(['a', 'li'])
                    for item in items:
                        integration = item.get_text().strip()
                        if integration and len(integration) > 1:
                            integrations.append(integration)
                            logger.info(f"Found integration: {integration}")
        
        # Also look for integration mentions in the description
        integration_keywords = ['integrates with', 'works with', 'compatible with', 'connects to', 'sync with']
        desc_text = description.lower()
        for keyword in integration_keywords:
            if keyword in desc_text:
                logger.info(f"Found keyword '{keyword}' in description")
                # Find the sentence containing the keyword
                sentences = re.split(r'[.!?]+', desc_text)
                for sentence in sentences:
                    if keyword in sentence and 'isn\'t compatible' not in sentence and 'not compatible' not in sentence:
                        # Extract potential integration names
                        after_keyword = sentence.split(keyword)[1]
                        # Split by common separators and clean up
                        potential_integrations = re.split(r'[,;&]', after_keyword)
                        for integration in potential_integrations:
                            integration = integration.strip(' and\t\n')
                            if integration and len(integration) > 1:
                                integrations.append(integration)
                                logger.info(f"Found integration from description: {integration}")
        
        # Remove duplicates and standardize
        integrations = list(set(integrations))
        integrations = [i.strip(' ,.;') for i in integrations]
        integrations = [i for i in integrations if len(i) > 1]  # Remove single characters
        
        # Additional cleaning of integrations
        cleaned_integrations = []
        for integration in integrations:
            # Skip if it contains incompatible messages
            if any(phrase in integration.lower() for phrase in [
                "isn't compatible", 
                "not compatible", 
                "only compatible with stores that",
                "this app isn't"
            ]):
                continue
            # Skip if it's too long (likely a sentence rather than an integration name)
            if len(integration.split()) > 5:
                continue
            cleaned_integrations.append(integration)
        
        logger.info(f"Total unique integrations found: {len(cleaned_integrations)}")
        
        return {
            'api_key': str(random.randint(1000000, 9999999)),
            'app_name': app_name,
            'app_store_url': url,
            'app_details': description,
            'app_submission_created_at': submission_date,
            'integrations': ','.join(cleaned_integrations) if cleaned_integrations else ''
        }
        
    except Exception as e:
        logger.error(f"Error parsing app page {url}: {str(e)}", exc_info=True)
        return None

async def collect_apps() -> List[Dict]:
    """Collect apps until we have enough with integrations."""
    all_apps = []
    apps_with_integrations = 0
    seen_urls = set()
    
    async with aiohttp.ClientSession() as session:
        # Get app URLs
        app_urls = await get_app_urls(session)
        if not app_urls:
            logger.error("No app URLs found")
            return []
            
        # Shuffle URLs to get a random sample
        random.shuffle(app_urls)
        
        with tqdm(total=TARGET_APPS_WITH_INTEGRATIONS, desc="Collecting apps with integrations") as pbar:
            for url in app_urls:
                if apps_with_integrations >= TARGET_APPS_WITH_INTEGRATIONS:
                    break
                    
                if url in seen_urls:
                    continue
                    
                app_info = await extract_app_info(session, url)
                if app_info:
                    # Only keep apps that have integrations
                    if app_info.get('integrations'):
                        all_apps.append(app_info)
                        apps_with_integrations += 1
                        pbar.update(1)
                        logger.info(f"Found app with integrations: {app_info['app_name']} ({apps_with_integrations}/{TARGET_APPS_WITH_INTEGRATIONS})")
                    
                    seen_urls.add(url)
                
                # Add delay between requests
                await asyncio.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
                
                # Log progress periodically
                if len(seen_urls) % 50 == 0:
                    logger.info(f"Processed {len(seen_urls)} URLs, found {apps_with_integrations} apps with integrations")
    
    return all_apps

async def main():
    """Main entry point."""
    logger.info("Starting app collection from sitemap")
    logger.info(f"Target: {TARGET_APPS_WITH_INTEGRATIONS} apps with integrations")
    
    try:
        # Create data directory if it doesn't exist
        os.makedirs(DATA_DIR, exist_ok=True)
        
        # Collect apps
        apps = await collect_apps()
        logger.info(f"Successfully collected {len(apps)} apps with integrations")
        
        if not apps:
            logger.error("No apps were collected")
            sys.exit(1)
        
        # Convert to DataFrame and save
        df = pd.DataFrame(apps)
        df.to_csv(TOP_APPS_RAW, index=False)
        logger.info(f"Saved {len(df)} apps to {TOP_APPS_RAW}")
        
        # Print summary
        print("\nCollection Summary:")
        print(f"Total apps with integrations: {len(df)}")
        print(f"Average integrations per app: {df['integrations'].str.count(',').mean() + 1:.2f}")
        print(f"Max integrations for an app: {df['integrations'].str.count(',').max() + 1}")
        
    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Error in main: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
        sys.exit(0) 