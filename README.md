# App Listing Integrations Analysis

This project analyzes integration information from Shopify app listings to create a standardized taxonomy of integrations.

## Project Structure

```
.
├── README.md           # This file
├── context.md         # Project context and planning
├── requirements.txt   # Python dependencies
├── data/             # Data directory for CSV files
└── src/              # Source code
    ├── fetch_apps.py           # Script to fetch top apps from BigQuery
    ├── scrape_integrations.py  # Script to scrape integration info
    └── analyze_integrations.py # Script to analyze using GPT-4
```

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

The analysis is done in three steps:

1. Fetch top apps from BigQuery:
```bash
python src/fetch_apps.py
```
This will provide instructions to run a BigQuery query through MCP and save results.

2. Scrape integration information:
```bash
python src/scrape_integrations.py
```
This will process the app list and scrape integration information from each app's page.

3. Analyze integrations:
```bash
python src/analyze_integrations.py
```
This will use GPT-4 via Shopify's proxy to analyze and categorize the integrations.

## Data Files

- `data/top_apps_raw.csv`: Raw app data from BigQuery
- `data/app_integrations_*.csv`: Scraped integration data
- `data/integration_analysis_*.json`: Analysis results

## Notes

- The scraper includes a delay between requests to avoid overwhelming the app store
- Integration data is stored as comma-separated strings in the CSV
- Analysis results include categorization and standardization recommendations 