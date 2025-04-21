#!/usr/bin/env python3
"""
Script to save query results to our data file.
"""
import os
import sys
import pandas as pd

def save_results(result_id, output_file):
    """Save query results to a CSV file."""
    # Load the temporary results file
    df = pd.read_csv(f"/tmp/{result_id}")
    
    # Save with proper encoding and escaping
    df.to_csv(output_file, index=False, encoding='utf-8', escapechar='\\', quoting=1)
    print(f"Saved {len(df)} rows to {output_file}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python save_query_results.py <result_id> <output_file>")
        sys.exit(1)
        
    result_id = sys.argv[1]
    output_file = sys.argv[2]
    save_results(result_id, output_file) 