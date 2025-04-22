#!/usr/bin/env python3
"""
Script to generate detailed category analysis and visualizations for Shopify app integrations.
"""
import os
import ast
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import defaultdict

# Configuration
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, 'processed')
VISUALIZATIONS_DIR = os.path.join(DATA_DIR, 'visualizations')
os.makedirs(VISUALIZATIONS_DIR, exist_ok=True)

# Integration categories
INTEGRATION_CATEGORIES = {
    'shopify_native': [
        'Shopify Checkout', 'Customer Accounts', 'Shopify Flow', 'Shopify POS',
        'Shopify Email', 'Shopify Analytics', 'Shopify Shipping', 'Shopify Fulfillment',
        'Shopify Markets', 'Shopify Payments'
    ],
    'marketplace': [
        'Amazon', 'eBay', 'Etsy', 'Walmart', 'Facebook Shop', 'Instagram Shop',
        'TikTok Shop', 'Google Shopping', 'Pinterest Shop'
    ],
    'marketing': [
        'Klaviyo', 'Mailchimp', 'HubSpot', 'Facebook', 'Instagram', 'TikTok',
        'Google Analytics', 'Facebook Pixel', 'Google Ads', 'Meta Ads',
        'Email Marketing', 'SMS Marketing'
    ],
    'shipping': [
        'ShipStation', 'UPS', 'FedEx', 'USPS', 'DHL', 'Canada Post',
        'Royal Mail', 'Australia Post'
    ],
    'payment': [
        'PayPal', 'Stripe', 'Square', 'QuickBooks', 'Xero', 'Klarna',
        'Afterpay', 'Affirm'
    ],
    'productivity': [
        'Slack', 'Microsoft Teams', 'Google Workspace', 'Zapier', 'Trello',
        'Asana', 'Monday.com'
    ]
}

def load_data():
    """Load and preprocess the data."""
    df = pd.read_csv(os.path.join(DATA_DIR, 'top_apps_raw.csv'))
    # Convert string representation of list to actual list
    df['integrations'] = df['integrations'].apply(lambda x: [] if pd.isna(x) else x.split(','))
    return df

def categorize_integrations(integrations):
    """Categorize a list of integrations."""
    categories = defaultdict(list)
    for integration in integrations:
        for category, members in INTEGRATION_CATEGORIES.items():
            if any(member.lower() in integration.lower() for member in members):
                categories[category].append(integration)
    return dict(categories)

def analyze_categories(df):
    """Analyze integration categories."""
    category_stats = defaultdict(lambda: {'count': 0, 'apps': 0, 'integrations': []})
    
    for _, row in df.iterrows():
        categories = categorize_integrations(row['integrations'])
        for category, integrations in categories.items():
            category_stats[category]['count'] += len(integrations)
            category_stats[category]['apps'] += 1
            category_stats[category]['integrations'].extend(integrations)
    
    # Calculate percentages and unique integrations
    total_apps = len(df)
    for stats in category_stats.values():
        stats['app_percentage'] = (stats['apps'] / total_apps) * 100
        stats['unique_integrations'] = len(set(stats['integrations']))
        
    return category_stats

def create_category_plot(category_stats):
    """Create a bar plot of category statistics."""
    plt.figure(figsize=(12, 6))
    categories = list(category_stats.keys())
    apps_percentage = [stats['app_percentage'] for stats in category_stats.values()]
    
    plt.bar(categories, apps_percentage)
    plt.xticks(rotation=45, ha='right')
    plt.title('Integration Categories by App Percentage')
    plt.ylabel('Percentage of Apps (%)')
    plt.tight_layout()
    
    plt.savefig(os.path.join(VISUALIZATIONS_DIR, 'category_distribution.png'))
    plt.close()

def create_heatmap(df):
    """Create a heatmap of integration co-occurrence."""
    # Get all unique integrations
    all_integrations = set()
    for integrations in df['integrations']:
        all_integrations.update(integrations)
    
    # Create co-occurrence matrix
    n = len(all_integrations)
    integration_list = list(all_integrations)
    cooccurrence = pd.DataFrame(0, columns=integration_list, index=integration_list)
    
    for integrations in df['integrations']:
        for i in integrations:
            for j in integrations:
                if i != j:
                    cooccurrence.loc[i, j] += 1
    
    # Plot heatmap
    plt.figure(figsize=(15, 15))
    sns.heatmap(cooccurrence, cmap='YlOrRd')
    plt.title('Integration Co-occurrence Matrix')
    plt.tight_layout()
    
    plt.savefig(os.path.join(VISUALIZATIONS_DIR, 'integration_cooccurrence.png'))
    plt.close()

def generate_report(category_stats):
    """Generate a markdown report of the analysis."""
    report = ["# Detailed Integration Category Analysis\n\n"]
    
    report.append("## Category Overview\n")
    for category, stats in category_stats.items():
        report.append(f"### {category.replace('_', ' ').title()}\n")
        report.append(f"- Apps with {category} integrations: {stats['apps']} ({stats['app_percentage']:.1f}%)")
        report.append(f"- Total integrations in category: {stats['count']}")
        report.append(f"- Unique integrations: {stats['unique_integrations']}")
        report.append(f"- Most common integrations in category:")
        
        # Get top 5 integrations in category
        integration_counts = pd.Series(stats['integrations']).value_counts()
        for integration, count in integration_counts.head().items():
            report.append(f"  - {integration}: {count} apps")
        report.append("\n")
    
    report.append("\n## Visualization Notes\n")
    report.append("1. `category_distribution.png`: Shows the percentage of apps that have integrations in each category")
    report.append("2. `integration_cooccurrence.png`: Heatmap showing how often integrations appear together in the same app")
    
    with open(os.path.join(VISUALIZATIONS_DIR, 'category_analysis.md'), 'w') as f:
        f.write('\n'.join(report))

def main():
    """Main execution function."""
    # Load data
    df = load_data()
    
    # Analyze categories
    category_stats = analyze_categories(df)
    
    # Create visualizations
    create_category_plot(category_stats)
    create_heatmap(df)
    
    # Generate report
    generate_report(category_stats)

if __name__ == "__main__":
    main() 