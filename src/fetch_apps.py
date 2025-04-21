#!/usr/bin/env python3
"""
Script to fetch top 100 published apps from Shopify's data warehouse.
"""
import os
import pandas as pd
from datetime import datetime

def fetch_top_apps():
    """
    Query BigQuery to get top 100 published apps ordered by submission date.
    Returns a DataFrame with app details.
    """
    query = """
    WITH top_apps AS (
        SELECT 
            api_key,
            app_name,
            CONCAT('https://apps.shopify.com/', api_key) as app_store_url,
            LEFT(app_details, 500) as app_details,
            app_submission_created_at
        FROM `shopify-dw.apps_and_developers.public_apps` 
        WHERE publication_state = 'published'
        ORDER BY app_submission_created_at DESC
        LIMIT 100
    )
    SELECT * FROM top_apps
    """
    
    # Check if results file exists
    results_file = os.path.join('data', 'top_apps_raw.csv')
    if os.path.exists(results_file):
        df = pd.read_csv(results_file)
        if len(df) == 100:  # Only use the file if it has all 100 apps
            return df
    
    # If we don't have complete data, instruct user to run query
    print("Please execute this query through MCP data portal:")
    print("\nQUERY:")
    print(query)
    print("\nThen save the results to: data/top_apps_raw.csv")
    print("\nMake sure the file contains all 100 apps!")
    return None

def save_results(df, filename=None):
    """
    Save the results to a CSV file in the data directory.
    """
    if df is None or len(df) == 0:
        print("No data to save")
        return
        
    if filename is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'top_apps_{timestamp}.csv'
    
    output_path = os.path.join('data', filename)
    
    # Save with proper encoding and escaping
    df.to_csv(output_path, index=False, encoding='utf-8', escapechar='\\', quoting=1)
    print(f"Results saved to {output_path}")

def main():
    """Main execution function."""
    print("Fetching top 100 published apps...")
    df = fetch_top_apps()
    
    if df is not None:
        print(f"Retrieved {len(df)} apps")
        if len(df) != 100:
            print(f"WARNING: Expected 100 apps but got {len(df)}. Please check the data.")
        save_results(df)
    else:
        print("No results retrieved")

if __name__ == "__main__":
    main() 