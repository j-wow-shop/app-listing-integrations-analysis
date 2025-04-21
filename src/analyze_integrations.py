#!/usr/bin/env python3
"""
Script to analyze integration data from Shopify apps.
Performs various analyses including:
- Integration frequency analysis
- Category/cluster analysis
- Network analysis of app-integration relationships
- Popular integration patterns
"""
import logging
from pathlib import Path
from typing import Dict, List, Set, Tuple
import os
import argparse

import networkx as nx
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import DBSCAN
import matplotlib.pyplot as plt
import json
import numpy as np
from sklearn.cluster import KMeans
import seaborn as sns
from collections import Counter

from config import PROCESSED_DATA_DIR, INTEGRATIONS_DATA
from utils import setup_logging

# Set up logging
logger = logging.getLogger(__name__)

# Create processed_data directory if it doesn't exist
os.makedirs('processed_data', exist_ok=True)

def clean_integration_name(name: str) -> str:
    """
    Clean and standardize integration names.
    """
    if not name:
        return ""
        
    # Convert to lowercase for standardization
    name = name.lower().strip()
    
    # Common name mappings
    name_mappings = {
        'facebook': 'Facebook',
        'fb': 'Facebook',
        'face book': 'Facebook',
        'instagram': 'Instagram',
        'ig': 'Instagram',
        'google analytics': 'Google Analytics',
        'ga': 'Google Analytics',
        'shopify pos': 'Shopify POS',
        'pos': 'Shopify POS',
        'shopify checkout': 'Shopify Checkout',
        'checkout': 'Shopify Checkout',
        'whatsapp': 'WhatsApp',
        'wa': 'WhatsApp',
        'tiktok': 'TikTok',
        'klaviyo': 'Klaviyo',
        'mailchimp': 'Mailchimp',
        'shopify flow': 'Shopify Flow',
        'flow': 'Shopify Flow',
        'customer accounts': 'Customer Accounts',
        'judge.me': 'Judge.me',
        'judgeme': 'Judge.me',
        'zapier': 'Zapier',
        'slack': 'Slack',
        'zendesk': 'Zendesk',
        'hubspot': 'HubSpot',
        'quickbooks': 'QuickBooks',
        'xero': 'Xero',
        'paypal': 'PayPal',
        'stripe': 'Stripe'
    }
    
    # Generic terms to exclude
    generic_terms = {
        'all theme support', 'products', 'cart', 'api', 'mobile', 'desktop',
        'translation apps', 'review widgets', 'currency convertors',
        'landing page builder', 'and many more', '2048 variants',
        'rest api', 'webhooks', 'your store', 'custom integrations'
    }
    
    if name in generic_terms:
        return ""
        
    # Apply name mappings
    for pattern, replacement in name_mappings.items():
        if name == pattern:
            return replacement
            
    # If no mapping found, capitalize properly
    return name.title()

def load_integration_data(csv_path: str) -> pd.DataFrame:
    """
    Load and process integration data from CSV file.
    """
    logger.info(f"Loading data from {csv_path}")
    df = pd.read_csv(csv_path)
    
    def extract_integrations(details: str) -> List[str]:
        if not isinstance(details, str):
            return []
            
        # Common integration keywords and patterns
        integration_keywords = [
            'integrates with', 'works with', 'compatible with', 'connects to',
            'sync with', 'syncs with', 'integration with', 'integrated with',
            'connect your', 'connects your', 'integration for', 'plugin for'
        ]
        
        # Common platforms and services
        platforms = [
            'Shopify', 'Facebook', 'Instagram', 'Google', 'TikTok', 'Twitter',
            'Pinterest', 'WhatsApp', 'Snapchat', 'YouTube', 'Amazon', 'eBay',
            'Etsy', 'Walmart', 'Klaviyo', 'Mailchimp', 'Zapier', 'Slack',
            'Zendesk', 'Salesforce', 'HubSpot', 'QuickBooks', 'Xero', 'PayPal',
            'Stripe', 'Square', 'Klarna', 'Affirm', 'ShipStation', 'ShipBob',
            'FedEx', 'UPS', 'USPS', 'DHL'
        ]
        
        integrations = set()
        details_lower = details.lower()
        
        # Look for integration keywords followed by platform names
        for keyword in integration_keywords:
            if keyword.lower() in details_lower:
                # Find the position of the keyword
                pos = details_lower.find(keyword.lower())
                # Look for platforms in the text after the keyword
                remaining_text = details[pos:pos+100]  # Look at next 100 chars
                for platform in platforms:
                    if platform.lower() in remaining_text.lower():
                        integrations.add(platform)
                        
        # Also look for direct mentions of platforms
        for platform in platforms:
            if platform.lower() in details_lower:
                # Verify it's a likely integration mention
                context = details_lower[max(0, details_lower.find(platform.lower())-50):
                                     min(len(details_lower), details_lower.find(platform.lower())+50)]
                if any(keyword in context for keyword in integration_keywords):
                    integrations.add(platform)
        
        return list(integrations)
    
    # Extract integrations from app details
    df['extracted_integrations'] = df['app_details'].apply(extract_integrations)
    
    # Parse existing integrations - handle string representation of lists
    def parse_integrations(integrations_str):
        if pd.isna(integrations_str):
            return []
        try:
            # Remove outer quotes and parse as list
            cleaned = integrations_str.strip('[]').replace('"', '').replace("'", '')
            # Split on commas and clean each integration
            return [item.strip() for item in cleaned.split(',') if item.strip()]
        except:
            return []
            
    df['existing_integrations'] = df['integrations'].apply(parse_integrations)
    
    # Combine and clean integrations
    df['all_integrations'] = df.apply(
        lambda row: list(set(row['existing_integrations'] + row['extracted_integrations'])), 
        axis=1
    )
    
    # Clean integration names
    df['cleaned_integrations'] = df['all_integrations'].apply(
        lambda x: [clean_integration_name(i) for i in x if clean_integration_name(i)]
    )
    
    # Remove duplicates and empty strings
    df['cleaned_integrations'] = df['cleaned_integrations'].apply(
        lambda x: list(set(i for i in x if i))
    )
    
    # Add integration count
    df['integration_count'] = df['cleaned_integrations'].apply(len)
    
    # Create final processed DataFrame
    processed_df = df[[
        'api_key', 'app_name', 'app_store_url', 'app_details',
        'app_submission_created_at', 'cleaned_integrations', 'integration_count'
    ]].copy()
    
    # Rename column for clarity
    processed_df = processed_df.rename(columns={'cleaned_integrations': 'integrations'})
    
    # Log statistics
    logger.info(f"Processed {len(processed_df)} apps")
    logger.info(f"Found {processed_df['integration_count'].sum()} total integrations")
    logger.info(f"Average integrations per app: {processed_df['integration_count'].mean():.2f}")
    
    return processed_df

def analyze_integration_frequency(df: pd.DataFrame) -> pd.DataFrame:
    """
    Analyze frequency of integrations across apps.
    """
    # Explode the integrations list to get one row per integration
    exploded = df.explode('integrations')
    
    # Count frequency of each integration
    freq_df = pd.DataFrame(
        exploded['integrations'].value_counts()
    ).reset_index()
    
    # Rename columns
    freq_df.columns = ['integration', 'frequency']
    
    # Calculate percentage
    total_apps = len(df)
    freq_df['percentage'] = (freq_df['frequency'] / total_apps * 100).round(2)
    
    # Sort by frequency descending
    freq_df = freq_df.sort_values('frequency', ascending=False)
    
    # Log top integrations
    logger.info("\nTop 10 most common integrations:")
    for _, row in freq_df.head(10).iterrows():
        logger.info(f"{row['integration']}: {row['frequency']} apps ({row['percentage']}%)")
        
    return freq_df

def plot_integration_frequencies(freq_df: pd.DataFrame, output_dir: str):
    """
    Create visualizations of integration frequencies.
    """
    # Only plot top 20 integrations
    plot_df = freq_df.head(20)
    
    # Create bar plot
    plt.figure(figsize=(12, 6))
    sns.barplot(data=plot_df, x='frequency', y='integration')
    plt.title('Top 20 Most Common Integrations')
    plt.xlabel('Number of Apps')
    plt.ylabel('Integration')
    
    # Save plot
    plot_path = os.path.join(output_dir, 'integration_frequencies.png')
    plt.savefig(plot_path, bbox_inches='tight', dpi=300)
    plt.close()
    
    # Save data
    csv_path = os.path.join(output_dir, 'integration_frequencies.csv')
    freq_df.to_csv(csv_path, index=False)
    
    logger.info(f"Saved frequency plot to {plot_path}")
    logger.info(f"Saved frequency data to {csv_path}")

def build_integration_network(df: pd.DataFrame) -> nx.Graph:
    """
    Build a network graph of integrations where edges represent apps that share integrations.
    
    Args:
        df (pd.DataFrame): DataFrame containing app data with integrations column
        
    Returns:
        nx.Graph: NetworkX graph of integration relationships
    """
    G = nx.Graph()
    
    # Add nodes for each app
    for idx, row in df.iterrows():
        integrations = row['integrations']
        if not isinstance(integrations, list) or not integrations:
            continue
            
        # Add edges between all pairs of integrations in this app
        for i, int1 in enumerate(integrations):
            for int2 in integrations[i+1:]:
                if G.has_edge(int1, int2):
                    G[int1][int2]['weight'] += 1
                else:
                    G.add_edge(int1, int2, weight=1)
    
    return G

def cluster_apps_by_integrations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cluster apps based on their integration patterns using K-means clustering.
    """
    # Create a binary matrix of apps and their integrations
    integration_matrix = pd.get_dummies(df['integrations'].explode()).groupby(level=0).sum()
    
    # Determine optimal number of clusters using elbow method
    max_clusters = min(10, len(integration_matrix))
    inertias = []
    
    for k in range(1, max_clusters + 1):
        kmeans = KMeans(n_clusters=k, random_state=42)
        kmeans.fit(integration_matrix)
        inertias.append(kmeans.inertia_)
    
    # Find optimal number of clusters using elbow method
    optimal_k = 2  # Default to 2 clusters
    if len(inertias) > 2:
        for i in range(1, len(inertias)-1):
            # Calculate the angle between consecutive points
            prev_diff = inertias[i-1] - inertias[i]
            next_diff = inertias[i] - inertias[i+1]
            if next_diff != 0 and prev_diff / next_diff < 0.3:
                optimal_k = i + 1
                break
    
    # Perform final clustering with optimal k
    kmeans = KMeans(n_clusters=optimal_k, random_state=42)
    df['cluster'] = kmeans.fit_predict(integration_matrix)
    
    # Add cluster characteristics
    cluster_profiles = []
    for i in range(optimal_k):
        cluster_mask = df['cluster'] == i
        cluster_apps = df[cluster_mask]
        
        # Get most common integrations in cluster
        cluster_integrations = []
        for app_integrations in cluster_apps['integrations']:
            if isinstance(app_integrations, list):
                cluster_integrations.extend(app_integrations)
        
        top_integrations = Counter(cluster_integrations).most_common(3)
        profile = f"Cluster {i}: " + ", ".join([f"{integration}" for integration, _ in top_integrations])
        cluster_profiles.append(profile)
    
    df['cluster_profile'] = df['cluster'].map(dict(enumerate(cluster_profiles)))
    
    return df

def generate_visualizations(df: pd.DataFrame, freq_df: pd.DataFrame, G: nx.Graph) -> None:
    """
    Generate various visualizations of the integration data.
    
    Args:
        df: DataFrame with integration data
        freq_df: DataFrame with integration frequencies
        G: NetworkX graph of integrations
    """
    # Create visualizations directory if it doesn't exist
    os.makedirs('visualizations', exist_ok=True)
    
    # 1. Top integrations bar chart
    top_integrations = px.bar(
        freq_df.head(20),
        x='integration',
        y='frequency',
        title='Top 20 Most Common Integrations'
    )
    top_integrations.update_layout(
        xaxis_title="Integration",
        yaxis_title="Number of Apps",
        xaxis_tickangle=45
    )
    top_integrations.write_html("visualizations/top_integrations.html")
    
    # 2. Integration network graph
    plt.figure(figsize=(15, 15))
    pos = nx.spring_layout(G, k=1, iterations=50)
    
    # Draw edges with varying thickness based on weight
    edge_weights = [G[u][v]['weight'] for u, v in G.edges()]
    nx.draw_networkx_edges(G, pos, alpha=0.2, width=[w/2 for w in edge_weights])
    
    # Draw nodes with size based on frequency
    node_sizes = []
    for node in G.nodes():
        freq = freq_df[freq_df['integration'] == node]['frequency'].values
        size = freq[0] * 100 if len(freq) > 0 else 100
        node_sizes.append(size)
    
    nx.draw_networkx_nodes(G, pos, node_size=node_sizes, alpha=0.6)
    
    # Add labels
    nx.draw_networkx_labels(G, pos, font_size=8)
    
    plt.title("Integration Network Graph")
    plt.axis('off')
    plt.savefig("visualizations/integration_network.png", dpi=300, bbox_inches='tight')
    plt.close()
    
    # 3. Integration count distribution
    integration_counts = px.histogram(
        df,
        x='integration_count',
        title='Distribution of Integration Counts per App',
        nbins=20
    )
    integration_counts.update_layout(
        xaxis_title="Number of Integrations",
        yaxis_title="Number of Apps"
    )
    integration_counts.write_html("visualizations/integration_counts.html")
    
    # 4. Cluster visualization
    if 'cluster' in df.columns:
        cluster_sizes = df['cluster'].value_counts().sort_index()
        cluster_plot = px.bar(
            x=cluster_sizes.index,
            y=cluster_sizes.values,
            title='App Clusters by Size',
            labels={'x': 'Cluster', 'y': 'Number of Apps'}
        )
        cluster_plot.write_html("visualizations/cluster_sizes.html")
    
    # 5. Integration categories
    categories = {
        'Marketing': ['email', 'marketing', 'ads', 'social'],
        'Payment': ['payment', 'pay', 'checkout'],
        'Shipping': ['shipping', 'delivery', 'fulfillment'],
        'Analytics': ['analytics', 'tracking', 'reporting'],
        'Customer Support': ['support', 'chat', 'help'],
        'Marketplace': ['marketplace', 'amazon', 'ebay'],
        'Other': []
    }
    
    # Categorize integrations
    integration_categories = {}
    for integration in freq_df['integration']:
        integration_lower = str(integration).lower()
        categorized = False
        for category, keywords in categories.items():
            if any(keyword in integration_lower for keyword in keywords):
                integration_categories[integration] = category
                categorized = True
                break
        if not categorized:
            integration_categories[integration] = 'Other'
    
    # Create category distribution plot
    category_counts = pd.Series(integration_categories).value_counts()
    category_plot = px.pie(
        values=category_counts.values,
        names=category_counts.index,
        title='Integration Categories Distribution'
    )
    category_plot.write_html("visualizations/integration_categories.html")

def load_app_data(csv_path: str = 'data/export.csv') -> pd.DataFrame:
    """
    Load app data from CSV and extract integrations.
    
    Args:
        csv_path: Path to CSV file with app data
        
    Returns:
        DataFrame with app data and extracted integrations
    """
    try:
        # Load the CSV file
        df = pd.read_csv(csv_path)
        required_cols = ['app_name', 'app_store_url', 'app_details']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        # Extract integrations from app details
        def extract_integrations(details: str) -> List[str]:
            # Common integration keywords
            integration_keywords = [
                'integrates with', 'works with', 'compatible with',
                'connects to', 'syncs with', 'integration with'
            ]
            
            details = details.lower()
            integrations = []
            
            # Look for integration mentions
            for keyword in integration_keywords:
                if keyword in details:
                    # Get the text after the keyword
                    text_after = details.split(keyword)[1].split('.')[0]
                    # Split by common separators
                    found = [i.strip() for i in text_after.split(',')]
                    found = [i.split('and') for i in found]
                    # Flatten the list
                    found = [item.strip() for sublist in found for item in sublist]
                    # Remove empty strings and duplicates
                    found = [i for i in found if i]
                    integrations.extend(found)
            
            return list(set(integrations))
        
        # Apply extraction to each app
        df['integrations'] = df['app_details'].apply(extract_integrations)
        df['integration_count'] = df['integrations'].apply(len)
        df['integrations'] = df['integrations'].apply(lambda x: ','.join(x) if x else '')
        df['scrape_success'] = True  # All rows are considered successful since we have the data
        
        logger.info(f"Loaded {len(df)} apps")
        return df
    except Exception as e:
        logger.error(f"Error loading app data: {str(e)}")
        raise

def create_integration_graph(df: pd.DataFrame) -> nx.Graph:
    """Create a network graph of app integrations."""
    G = nx.Graph()
    
    # Add nodes for each app
    for _, row in df.iterrows():
        app_name = row['app_name']
        G.add_node(app_name, type='app')
        
        # Parse integrations from the app details field
        integrations = str(row['app_details']).lower()
        integration_keywords = ['works with', 'integrates with', 'compatible with']
        
        for keyword in integration_keywords:
            if keyword in integrations:
                # Extract integration names after the keyword
                integration_text = integrations.split(keyword)[1].split('.')[0]
                integration_names = [name.strip() for name in integration_text.split(',')]
                
                # Add edges for each integration
                for integration in integration_names:
                    if integration and len(integration) > 2:  # Filter out empty or very short names
                        G.add_node(integration, type='integration')
                        G.add_edge(app_name, integration)
    
    return G

def analyze_integration_patterns(G: nx.Graph) -> Dict:
    """Analyze patterns in the integration network."""
    analysis = {
        'total_apps': len([n for n, attr in G.nodes(data=True) if attr['type'] == 'app']),
        'total_integrations': len([n for n, attr in G.nodes(data=True) if attr['type'] == 'integration']),
        'total_connections': G.number_of_edges(),
        'avg_integrations_per_app': G.number_of_edges() / len([n for n, attr in G.nodes(data=True) if attr['type'] == 'app']),
        'most_common_integrations': [],
        'isolated_apps': len(list(nx.isolates(G)))
    }
    
    # Find most common integrations
    integration_nodes = [n for n, attr in G.nodes(data=True) if attr['type'] == 'integration']
    integration_degrees = [(n, G.degree(n)) for n in integration_nodes]
    sorted_integrations = sorted(integration_degrees, key=lambda x: x[1], reverse=True)
    analysis['most_common_integrations'] = sorted_integrations[:10]
    
    return analysis

def visualize_integration_network(G: nx.Graph, output_path: str):
    """Create and save a visualization of the integration network."""
    plt.figure(figsize=(15, 10))
    
    # Set up layout
    pos = nx.spring_layout(G, k=1, iterations=50)
    
    # Draw nodes
    app_nodes = [n for n, attr in G.nodes(data=True) if attr['type'] == 'app']
    integration_nodes = [n for n, attr in G.nodes(data=True) if attr['type'] == 'integration']
    
    nx.draw_networkx_nodes(G, pos, nodelist=app_nodes, node_color='lightblue', 
                          node_size=100, alpha=0.6, label='Apps')
    nx.draw_networkx_nodes(G, pos, nodelist=integration_nodes, node_color='lightgreen',
                          node_size=200, alpha=0.6, label='Integrations')
    
    # Draw edges
    nx.draw_networkx_edges(G, pos, alpha=0.2)
    
    # Add labels for most connected nodes
    node_degrees = dict(G.degree())
    labels = {node: node for node, degree in node_degrees.items() 
             if degree > np.percentile(list(node_degrees.values()), 90)}
    nx.draw_networkx_labels(G, pos, labels, font_size=8)
    
    plt.title('App Integration Network')
    plt.legend()
    plt.axis('off')
    
    # Save the plot
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Analyze app integrations')
    parser.add_argument('--input', type=str, default='data/top_apps_raw.csv',
                      help='Path to input CSV file')
    parser.add_argument('--output-dir', type=str, default='processed_data',
                      help='Directory for output files')
    args = parser.parse_args()
    
    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Load and process data
    df = load_integration_data(args.input)
    
    # Save processed data
    processed_path = os.path.join(args.output_dir, 'processed_apps.csv')
    df.to_csv(processed_path, index=False)
    logger.info(f"Saved processed data to {processed_path}")
    
    # Analyze integration frequencies
    freq_df = analyze_integration_frequency(df)
    
    # Create visualizations
    plot_integration_frequencies(freq_df, args.output_dir)

if __name__ == '__main__':
    setup_logging()
    main() 