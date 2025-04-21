"""
Configuration settings for the app integration analysis project.
"""
import os
from pathlib import Path

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent.absolute()

# Data directories
DATA_DIR = PROJECT_ROOT / 'data'
RAW_DATA_DIR = DATA_DIR / 'raw'
PROCESSED_DATA_DIR = DATA_DIR / 'processed'

# Create directories if they don't exist
os.makedirs(RAW_DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

# File paths
TOP_APPS_RAW = DATA_DIR / 'export.csv'
INTEGRATIONS_DATA = PROCESSED_DATA_DIR / 'app_integrations.csv'

# Scraping settings
REQUEST_TIMEOUT = 30  # seconds
REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'
}

# Analysis settings
MAX_APPS = 100
BATCH_SIZE = 10  # Number of apps to process in parallel 