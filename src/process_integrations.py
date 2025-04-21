#!/usr/bin/env python3
"""
Script to process and standardize integration data from scraped app information.
"""
import os
import logging
import pandas as pd
from typing import List, Dict, Set

# Configuration
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
RAW_FILE = os.path.join(DATA_DIR, 'top_apps_raw.csv')
PROCESSED_FILE = os.path.join(DATA_DIR, 'processed_integrations.csv')

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('processing.log')
    ]
)
logger = logging.getLogger(__name__)

# Known Shopify products/features for standardization
SHOPIFY_PRODUCTS = {
    'flow': 'Shopify Flow',
    'pos': 'Shopify POS',
    'checkout': 'Shopify Checkout',
    'markets': 'Shopify Markets',
    'shipping': 'Shopify Shipping',
    'email': 'Shopify Email',
    'forms': 'Shopify Forms',
    'fulfillment': 'Shopify Fulfillment',
    'payments': 'Shopify Payments',
    'analytics': 'Shopify Analytics'
}

def standardize_integration_name(name: str) -> str:
    """
    Standardize an integration name using known patterns and rules.
    """
    # Convert to lowercase for comparison
    name = name.lower().strip()
    
    # Check for Shopify products first
    for key, standard_name in SHOPIFY_PRODUCTS.items():
        if key in name:
            return standard_name
            
    # Handle common variations
    name_map = {
        'shopify pos': 'Shopify POS',
        'point of sale': 'Shopify POS',
        'facebook shop': 'Facebook Shop',
        'fb shop': 'Facebook Shop',
        'instagram shop': 'Instagram Shop',
        'ig shop': 'Instagram Shop',
        'google merchant': 'Google Merchant Center',
        'google shopping': 'Google Merchant Center',
        'tiktok shop': 'TikTok Shop',
    }
    
    # Try direct mapping
    if name in name_map:
        return name_map[name]
    
    # Capitalize words for consistency
    words = name.split()
    if len(words) > 0:
        name = ' '.join(word.capitalize() for word in words)
    
    return name

def process_integrations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process and standardize integration data from the DataFrame.
    """
    logger.info("Starting integration processing")
    
    # Create new DataFrame for processed data
    processed_df = df.copy()
    
    # Initialize new columns
    processed_df['processed_integrations'] = ''
    processed_df['integration_count'] = 0
    
    # Process each row
    for idx, row in processed_df.iterrows():
        if pd.isna(row['integrations']) or not row['integrations'].strip():
            continue
            
        # Split integrations and process each one
        raw_integrations = row['integrations'].split(',')
        processed_integrations = set()
        
        for integration in raw_integrations:
            integration = integration.strip()
            if integration:
                standardized = standardize_integration_name(integration)
                if standardized:
                    processed_integrations.add(standardized)
        
        # Update processed data
        processed_df.at[idx, 'processed_integrations'] = ','.join(sorted(processed_integrations))
        processed_df.at[idx, 'integration_count'] = len(processed_integrations)
        
        if idx % 100 == 0:
            logger.info(f"Processed {idx} rows")
    
    # Generate statistics
    total_apps = len(processed_df)
    apps_with_integrations = len(processed_df[processed_df['integration_count'] > 0])
    unique_integrations = set()
    
    for integrations in processed_df['processed_integrations'].dropna():
        if integrations:
            unique_integrations.update(integrations.split(','))
    
    logger.info(f"Total apps processed: {total_apps}")
    logger.info(f"Apps with integrations: {apps_with_integrations}")
    logger.info(f"Unique integrations found: {len(unique_integrations)}")
    
    return processed_df

def main():
    """Main entry point."""
    try:
        # Create data directory if it doesn't exist
        os.makedirs(DATA_DIR, exist_ok=True)
        
        # Read raw data
        logger.info(f"Reading raw data from {RAW_FILE}")
        df = pd.read_csv(RAW_FILE)
        
        # Process integrations
        processed_df = process_integrations(df)
        
        # Save processed data
        logger.info(f"Saving processed data to {PROCESSED_FILE}")
        processed_df.to_csv(PROCESSED_FILE, index=False)
        
        # Output summary statistics
        print("\nProcessing Summary:")
        print(f"Total apps: {len(processed_df)}")
        print(f"Apps with integrations: {len(processed_df[processed_df['integration_count'] > 0])}")
        
        # Show sample of processed integrations
        print("\nSample of processed integrations:")
        sample = processed_df[processed_df['integration_count'] > 0].head()
        for _, row in sample.iterrows():
            print(f"\nApp: {row['app_name']}")
            print(f"Raw integrations: {row['integrations']}")
            print(f"Processed integrations: {row['processed_integrations']}")
        
    except Exception as e:
        logger.error(f"Error in main: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main() 