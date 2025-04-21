#!/usr/bin/env python3
"""
Script to scrape integration information from Shopify app store pages.
Uses async requests with rate limiting and retry logic.
"""
import asyncio
import logging
import random
import re
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple, Any
import json
import os

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm

# Configuration
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
TOP_APPS_RAW = os.path.join(DATA_DIR, 'top_apps_raw.csv')
INTEGRATIONS_DATA = os.path.join(DATA_DIR, 'integrations.csv')
REQUEST_TIMEOUT = 30

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Scraping settings
MAX_RETRIES = 5
MIN_DELAY = 5
MAX_DELAY = 10
CONCURRENT_REQUESTS = 2

# User agent rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Edge/120.0.0.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
]

# Integration keywords
INTEGRATION_KEYWORDS = [
    'integrate', 'integration', 'connects with', 'compatible with',
    'works with', 'sync with', 'import from', 'export to',
    'integration with', 'integrated with', 'connect to',
    'plugin', 'addon', 'extension', 'connector', 'api', 'webhook',
    'compatible', 'partnership', 'partner', 'ecosystem'
]

INTEGRATION_SPLIT_KEYWORDS = [
    'integrates with', 'connects to', 'works with', 'compatible with', 
    'sync with', 'integration with', 'integrated with', 'connect to',
    'plugin for', 'extension for', 'addon for', 'api for', 'webhook for',
    'compatible with', 'partners with', 'ecosystem includes'
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
        'Pragma': 'no-cache',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'DNT': '1'
    }

def clean_url(url: str) -> str:
    """Clean and normalize a Shopify app store URL."""
    if not url:
        return ''
    
    # Remove query parameters and hash fragments
    url = re.sub(r'[?#].*$', '', url.strip())
    
    # Remove trailing slash
    url = url.rstrip('/')
    
    # Extract app name/slug from URL
    match = re.search(r'apps\.shopify\.com/([^/]+)', url)
    if not match:
        return ''
    
    app_slug = match.group(1)
    
    # Basic validation
    if not app_slug or len(app_slug) < 2:
        return ''
    
    # Construct clean URL
    return f'https://apps.shopify.com/{app_slug}'

def generate_app_urls(app_name: str, api_key: str, app_store_url: str) -> List[str]:
    """Generate possible app store URLs for an app."""
    urls = set()
    
    # Clean and add original URL if provided
    if app_store_url:
        clean_original_url = clean_url(app_store_url)
        if clean_original_url:
            urls.add(clean_original_url)
    
    # Clean app name
    app_name = app_name.strip()
    if not app_name:
        return list(urls)
    
    # Generate slug from app name
    def make_slug(text: str) -> str:
        # Remove special characters and extra spaces
        text = re.sub(r'[^\w\s-]', '', text)
        text = re.sub(r'[-\s]+', '-', text).strip('-')
        return text.lower()[:50]  # Limit length
    
    # Try variations of the app name
    name_variations = []
    
    # Full name
    name_variations.append(app_name)
    
    # Without special characters
    clean_name = re.sub(r'[^\w\s]', '', app_name)
    if clean_name != app_name:
        name_variations.append(clean_name)
    
    # First word only (if multiple words)
    words = app_name.split()
    if len(words) > 1:
        name_variations.append(words[0])
    
    # First two words (if more than two words)
    if len(words) > 2:
        name_variations.append(' '.join(words[:2]))
    
    # Generate URLs for each variation
    for variation in name_variations:
        slug = make_slug(variation)
        if slug and len(slug) >= 2:
            urls.add(f'https://apps.shopify.com/{slug}')
    
    # Try API key if it's numeric
    if api_key and api_key.isdigit():
        urls.add(f'https://apps.shopify.com/app/{api_key}')
    
    # Remove empty URLs
    urls = {url for url in urls if url}
    
    return list(urls)

async def is_valid_app_page(soup: BeautifulSoup) -> bool:
    """Check if the page is a valid app listing."""
    # Check for various app page indicators
    indicators = [
        # Main app container
        soup.find('div', {'class': ['app-details', 'app-listing', 'app-block', 'app-listing-hero', 'app-listing__hero']}),
        
        # App title
        soup.find(['h1', 'h2'], {'class': ['heading--1', 'app-title', 'title', 'app-listing__heading']}),
        
        # App description
        soup.find('div', {'class': ['app-description', 'app-details-description', 'description', 'app-listing__description']}),
        
        # Pricing section
        soup.find('div', {'class': ['app-pricing', 'pricing-section', 'pricing', 'app-listing__pricing']}),
        
        # Developer info
        soup.find('div', {'class': ['app-developer', 'developer-info', 'developer', 'app-listing__developer']}),
        
        # Reviews section
        soup.find('div', {'class': ['app-reviews', 'reviews-section', 'reviews', 'app-listing__reviews']})
    ]
    
    # Check meta tags
    meta_title = soup.find('meta', {'property': 'og:title'})
    meta_type = soup.find('meta', {'property': 'og:type'})
    meta_url = soup.find('meta', {'property': 'og:url'})
    
    # Check JSON-LD data
    json_ld = None
    for script in soup.find_all('script', {'type': 'application/ld+json'}):
        try:
            data = json.loads(script.string)
            if data.get('@type') == 'SoftwareApplication':
                json_ld = data
                break
        except:
            continue
    
    return any(indicators) or (meta_title and meta_type and 'apps.shopify.com' in str(meta_url)) or json_ld is not None

async def extract_integrations(session: aiohttp.ClientSession, url: str, retries: int = 3) -> Tuple[List[str], bool]:
    """Extract integrations from app store page."""
    integrations = set()
    page_found = False
    
    # Integration keywords to look for
    integration_keywords = [
        'integrates with', 'integration', 'integrated with', 'connects with',
        'connect to', 'connected to', 'works with', 'compatible with',
        'sync with', 'syncs with', 'synchronize with', 'synchronizes with',
        'import from', 'imports from', 'export to', 'exports to',
        'api integration', 'api connection', 'api connector',
        'plugin for', 'extension for', 'addon for', 'add-on for',
        'integration with', 'connector for', 'connects to'
    ]
    
    # Integration patterns to match
    integration_patterns = [
        re.compile(rf'\b{re.escape(kw)}\b\s+([^.!?\n]+)', re.IGNORECASE)
        for kw in integration_keywords
    ]
    
    for attempt in range(retries):
        try:
            async with session.get(url, timeout=30) as response:
                if response.status == 404:
                    logging.warning(f"URL not found: {url}")
                    return list(integrations), False
                
                if response.status != 200:
                    logging.warning(f"Got status {response.status} for {url}")
                    if attempt < retries - 1:
                        await asyncio.sleep(2 ** attempt)  # Exponential backoff
                        continue
                    return list(integrations), False
                
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                # Check if this is a valid app page
                if not soup.find('div', {'class': 'app-details'}) and not soup.find('main', {'role': 'main'}):
                    logging.warning(f"Not a valid app page: {url}")
                    return list(integrations), False
                
                page_found = True
                
                # Extract text from relevant sections
                text_sections = []
                
                # App description
                description = soup.find('div', {'class': ['app-details__description', 'description']})
                if description:
                    text_sections.append(description.get_text())
                
                # Features section
                features = soup.find('div', {'class': ['app-details__features', 'features']})
                if features:
                    text_sections.append(features.get_text())
                
                # Key benefits section
                benefits = soup.find('div', {'class': ['app-details__benefits', 'benefits']})
                if benefits:
                    text_sections.append(benefits.get_text())
                
                # Integration section (if exists)
                integrations_section = soup.find('div', {'class': ['app-details__integrations', 'integrations']})
                if integrations_section:
                    text_sections.append(integrations_section.get_text())
                
                # Process all text sections
                for text in text_sections:
                    # Clean text
                    text = re.sub(r'\s+', ' ', text).strip()
                    
                    # Look for integration mentions
                    for pattern in integration_patterns:
                        matches = pattern.finditer(text)
                        for match in matches:
                            # Extract and clean integration name
                            integration = match.group(1).strip()
                            integration = re.sub(r'[,.!?].*$', '', integration)  # Remove everything after punctuation
                            integration = re.sub(r'\s+', ' ', integration).strip()
                            
                            # Basic validation
                            if (len(integration) > 2 and  # More than 2 chars
                                len(integration) < 50 and  # Less than 50 chars
                                not integration.lower().startswith('your') and  # Skip generic mentions
                                not integration.lower().startswith('other')):
                                integrations.add(integration)
                
                break  # Success, exit retry loop
                
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logging.warning(f"Error fetching {url}: {str(e)}")
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
            else:
                return list(integrations), False
        
        except Exception as e:
            logging.error(f"Unexpected error processing {url}: {str(e)}")
            return list(integrations), False
    
    return list(integrations), page_found

async def try_urls(
    session: aiohttp.ClientSession,
    urls: List[str],
    retry_count: int = 0,
    total_delay: float = 0
) -> Tuple[Optional[str], Dict[str, Any]]:
    """Try multiple URLs until one works."""
    if not urls:
        return None, {
            'success': False,
            'error': 'No URLs provided',
            'integrations': []
        }
    
    # Exponential backoff with jitter
    if retry_count > 0:
        delay = min(300, (2 ** retry_count) + random.uniform(0, retry_count * 2))
        total_delay += delay
        if total_delay > 600:  # Max 10 minutes total delay
            return None, {
                'success': False,
                'error': 'Max retry time exceeded',
                'integrations': []
            }
        await asyncio.sleep(delay)
    
    for url in urls:
        try:
            # Random delay between requests
            delay = random.uniform(MIN_DELAY, MAX_DELAY)
            await asyncio.sleep(delay)
            
            headers = get_random_headers()
            async with session.get(url, headers=headers, timeout=REQUEST_TIMEOUT, allow_redirects=True) as response:
                if response.status == 429 and retry_count < MAX_RETRIES:
                    logger.warning(f"Rate limited on {url}, attempt {retry_count + 1}/{MAX_RETRIES}")
                    return await try_urls(session, urls, retry_count + 1, total_delay)
                
                if response.status == 404:
                    logger.warning(f"URL not found: {url}")
                    continue
                
                if response.status != 200:
                    logger.warning(f"Status {response.status} for {url}")
                    if retry_count < MAX_RETRIES:
                        return await try_urls(session, urls, retry_count + 1, total_delay)
                    continue
                
                html = await response.text()
                soup = BeautifulSoup(html, 'lxml')
                
                if await is_valid_app_page(soup):
                    integrations, page_found = await extract_integrations(session, url)
                    if integrations:
                        return url, {
                            'success': True,
                            'integrations': integrations,
                            'page_found': page_found,
                            'error': None
                        }
                    else:
                        logger.warning(f"No integrations found on valid page: {url}")
                else:
                    logger.warning(f"Not a valid app page: {url}")
            
        except asyncio.TimeoutError:
            logger.warning(f"Timeout on {url}")
            if retry_count < MAX_RETRIES:
                return await try_urls(session, urls, retry_count + 1, total_delay)
            continue
        except aiohttp.ClientError as e:
            logger.warning(f"Client error on {url}: {str(e)}")
            if retry_count < MAX_RETRIES:
                return await try_urls(session, urls, retry_count + 1, total_delay)
            continue
        except Exception as e:
            logger.error(f"Error scraping {url}: {str(e)}", exc_info=True)
            if retry_count < MAX_RETRIES:
                return await try_urls(session, urls, retry_count + 1, total_delay)
            continue
    
    return None, {
        'success': False,
        'error': 'No valid URL found',
        'integrations': []
    }

async def process_apps(df: pd.DataFrame) -> pd.DataFrame:
    """Process a DataFrame of apps asynchronously to extract integration information."""
    results = []
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    
    async def process_app(row):
        async with semaphore:
            urls = generate_app_urls(row['app_name'], row.get('api_key', ''), row['app_store_url'])
            working_url, result = await try_urls(session, urls)
            return row, working_url, result
    
    conn = aiohttp.TCPConnector(limit_per_host=CONCURRENT_REQUESTS, ssl=False)
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT * 2)
    
    async with aiohttp.ClientSession(connector=conn, timeout=timeout) as session:
        tasks = [process_app(row) for _, row in df.iterrows()]
        
        for task in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Scraping apps"):
            try:
                row, working_url, result = await task
                
                results.append({
                    'api_key': row.get('api_key', ''),
                    'app_name': row['app_name'],
                    'app_store_url': working_url or row['app_store_url'],
                    'integrations': ','.join(result['integrations']) if result['success'] else '',
                    'integration_count': len(result['integrations']) if result['success'] else 0,
                    'scrape_success': result['success'],
                    'scrape_error': result['error'],
                    'page_found': result['page_found'],
                    'processed_at': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S+00:00')
                })
                
            except Exception as e:
                logger.error(f"Error processing task: {str(e)}", exc_info=True)
                continue
    
    return pd.DataFrame(results)

def load_apps_from_csv(csv_path: str) -> pd.DataFrame:
    """
    Load apps data from CSV file with proper data types.
    
    Args:
        csv_path: Path to the CSV file
        
    Returns:
        DataFrame with app information
    """
    try:
        df = pd.read_csv(csv_path, dtype={
            'app_name': str,
            'app_store_url': str,
            'api_key': str,
            'description': str
        })
        logger.info(f"Successfully loaded {len(df)} apps from {csv_path}")
        return df
    except Exception as e:
        logger.error(f"Error loading apps from {csv_path}: {str(e)}")
        raise

async def main():
    """Main entry point for the script."""
    logger.info("Starting integration scraping")
    
    try:
        # Load apps from CSV
        apps_df = load_apps_from_csv(TOP_APPS_RAW)
        logger.info(f"Loaded {len(apps_df)} apps from CSV")
        
        # Process apps and extract integrations
        results_df = await process_apps(apps_df)
        logger.info(f"Successfully processed {len(results_df)} apps")
        
        # Save results
        results_df.to_csv(INTEGRATIONS_DATA, index=False)
        logger.info(f"Saved integration data to {INTEGRATIONS_DATA}")
        
    except Exception as e:
        logger.error(f"Error in main: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(main()) 