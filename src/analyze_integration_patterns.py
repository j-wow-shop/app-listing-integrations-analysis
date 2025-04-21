#!/usr/bin/env python3
"""
Script to analyze integration patterns and relationships in Shopify apps.
"""
import os
import logging
from typing import Dict, List, Set, Tuple
from collections import defaultdict
import pandas as pd
import numpy as np
from itertools import combinations
import networkx as nx
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import DBSCAN
from sklearn.metrics.pairwise import cosine_similarity
import difflib

# Configuration
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
PROCESSED_FILE = os.path.join(DATA_DIR, 'processed_integrations.csv')
ANALYSIS_OUTPUT = os.path.join(DATA_DIR, 'integration_patterns.md')
VISUALIZATIONS_DIR = os.path.join(DATA_DIR, 'visualizations')

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_integration_pairs(integrations: str) -> List[Tuple[str, str]]:
    """Get all pairs of integrations that appear together."""
    if pd.isna(integrations) or not integrations:
        return []
    integration_list = integrations.split(',')
    return list(combinations(sorted(integration_list), 2))

def analyze_integration_frequency(df: pd.DataFrame) -> pd.Series:
    """Analyze how frequently each integration appears."""
    all_integrations = []
    for integrations in df['processed_integrations'].dropna():
        if integrations:
            all_integrations.extend(integrations.split(','))
    return pd.Series(all_integrations).value_counts()

def analyze_integration_pairs(df: pd.DataFrame) -> pd.DataFrame:
    """Analyze which integrations commonly appear together."""
    pair_counts = defaultdict(int)
    for integrations in df['processed_integrations'].dropna():
        if integrations:
            pairs = get_integration_pairs(integrations)
            for pair in pairs:
                pair_counts[pair] += 1
    
    # Convert to DataFrame
    pairs_df = pd.DataFrame([
        {'integration1': p[0], 'integration2': p[1], 'count': c}
        for p, c in pair_counts.items()
    ])
    return pairs_df.sort_values('count', ascending=False)

def analyze_integration_categories(df: pd.DataFrame) -> Dict[str, List[str]]:
    """Group integrations into categories based on patterns."""
    categories = {
        'shopify_native': [],
        'marketplace': [],
        'marketing': [],
        'shipping': [],
        'payment': [],
        'analytics': [],
        'social': [],
        'other': []
    }
    
    # Get all unique integrations
    all_integrations = set()
    for integrations in df['processed_integrations'].dropna():
        if integrations:
            all_integrations.update(integrations.split(','))
    
    # Categorize each integration
    for integration in all_integrations:
        if integration.startswith('Shopify'):
            categories['shopify_native'].append(integration)
        elif any(x in integration.lower() for x in ['amazon', 'ebay', 'etsy', 'walmart']):
            categories['marketplace'].append(integration)
        elif any(x in integration.lower() for x in ['email', 'marketing', 'klaviyo', 'mailchimp', 'campaign']):
            categories['marketing'].append(integration)
        elif any(x in integration.lower() for x in ['shipping', 'delivery', 'fulfillment', 'ups', 'fedex']):
            categories['shipping'].append(integration)
        elif any(x in integration.lower() for x in ['pay', 'payment', 'stripe', 'paypal']):
            categories['payment'].append(integration)
        elif any(x in integration.lower() for x in ['analytics', 'tracking', 'pixel']):
            categories['analytics'].append(integration)
        elif any(x in integration.lower() for x in ['facebook', 'instagram', 'tiktok', 'twitter', 'social']):
            categories['social'].append(integration)
        else:
            categories['other'].append(integration)
    
    return categories

def analyze_app_complexity(df: pd.DataFrame) -> Dict[str, int]:
    """Analyze the complexity of apps based on their integration count."""
    complexity = {
        'simple': 0,      # 1-2 integrations
        'moderate': 0,    # 3-5 integrations
        'complex': 0,     # 6-8 integrations
        'very_complex': 0 # 9+ integrations
    }
    
    for count in df['integration_count'].dropna():
        if count <= 2:
            complexity['simple'] += 1
        elif count <= 5:
            complexity['moderate'] += 1
        elif count <= 8:
            complexity['complex'] += 1
        else:
            complexity['very_complex'] += 1
    
    return complexity

def create_integration_network(pairs_df: pd.DataFrame, min_connections: int = 2) -> nx.Graph:
    """Create a network graph of integration relationships."""
    G = nx.Graph()
    
    # Add edges for pairs that appear together at least min_connections times
    frequent_pairs = pairs_df[pairs_df['count'] >= min_connections]
    for _, row in frequent_pairs.iterrows():
        G.add_edge(
            row['integration1'], 
            row['integration2'], 
            weight=row['count']
        )
    
    return G

def analyze_rare_integrations(df: pd.DataFrame, threshold: int = 2) -> pd.DataFrame:
    """
    Analyze integrations that appear infrequently.
    Returns DataFrame with rare integrations and their context.
    """
    # Get frequency counts
    freq = analyze_integration_frequency(df)
    rare = freq[freq <= threshold]
    
    # Collect context for rare integrations
    rare_context = []
    for integration in rare.index:
        # Find apps that use this integration
        apps_with_integration = []
        for _, row in df.iterrows():
            if pd.notna(row['processed_integrations']) and integration in row['processed_integrations'].split(','):
                apps_with_integration.append({
                    'app_name': row['app_name'],
                    'app_details': row['app_details'],
                    'other_integrations': [i for i in row['processed_integrations'].split(',') if i != integration]
                })
        
        # Analyze context
        context = {
            'integration': integration,
            'frequency': rare[integration],
            'apps': [app['app_name'] for app in apps_with_integration],
            'common_co_integrations': pd.Series([i for app in apps_with_integration for i in app['other_integrations']]).value_counts().head(3).to_dict(),
            'typical_app_types': ', '.join(set([app['app_details'].split('.')[0].strip() for app in apps_with_integration]))
        }
        rare_context.append(context)
    
    return pd.DataFrame(rare_context)

def guess_integration_purpose(integration: str, app_details: str, co_integrations: List[str]) -> str:
    """
    Attempt to guess the purpose of an integration based on its name and context.
    """
    integration_lower = integration.lower()
    
    # Check for common patterns
    if any(term in integration_lower for term in ['api', 'sdk', 'connector']):
        return "API/SDK Integration"
    elif any(term in integration_lower for term in ['sync', 'import', 'export']):
        return "Data Synchronization"
    elif any(term in integration_lower for term in ['chat', 'support', 'help']):
        return "Customer Support"
    elif any(term in integration_lower for term in ['track', 'monitor', 'analytics']):
        return "Analytics/Tracking"
    elif any(term in integration_lower for term in ['ship', 'delivery', 'carrier']):
        return "Shipping/Logistics"
    elif any(term in integration_lower for term in ['pay', 'payment', 'checkout']):
        return "Payment Processing"
    elif any(term in integration_lower for term in ['market', 'ads', 'campaign']):
        return "Marketing"
    elif any(term in integration_lower for term in ['social', 'facebook', 'instagram', 'twitter']):
        return "Social Media"
    elif any(term in integration_lower for term in ['erp', 'accounting', 'inventory']):
        return "Business Operations"
    
    # Check co-integrations for context
    co_integration_text = ' '.join(co_integrations).lower()
    if 'shopify' in co_integration_text:
        return "Shopify Extension"
    
    return "Other/Custom Integration"

def advanced_integration_clustering(integrations: List[str]) -> Dict[str, List[str]]:
    """
    Use TF-IDF and DBSCAN to cluster similar integrations.
    """
    # Convert integrations to feature vectors
    vectorizer = TfidfVectorizer(analyzer='char', ngram_range=(2, 3))
    tfidf_matrix = vectorizer.fit_transform([i.lower() for i in integrations])
    
    # Calculate similarity matrix
    similarity_matrix = cosine_similarity(tfidf_matrix)
    
    # Cluster using DBSCAN
    clustering = DBSCAN(eps=0.3, min_samples=2, metric='precomputed')
    clustering.fit(1 - similarity_matrix)  # Convert similarity to distance
    
    # Group integrations by cluster
    clusters = defaultdict(list)
    for integration, label in zip(integrations, clustering.labels_):
        if label >= 0:  # Ignore noise points (-1)
            clusters[f"cluster_{label}"].append(integration)
    
    # Name clusters based on common patterns
    named_clusters = {}
    for cluster_id, members in clusters.items():
        # Find common substrings
        common_substr = difflib.get_close_matches(members[0], members, n=len(members), cutoff=0.3)
        if common_substr:
            common_pattern = common_substr[0]
            for m in common_substr[1:]:
                common_pattern = ''.join(c[0] for c in zip(common_pattern, m) if c[0] == c[1])
            cluster_name = common_pattern.strip().replace(' ', '_').lower()
            if cluster_name:
                named_clusters[cluster_name + "_services"] = members
            else:
                named_clusters[cluster_id] = members
    
    return named_clusters

def analyze_integration_relationships(df: pd.DataFrame) -> Dict:
    """
    Analyze relationships between integrations including mutual exclusivity and complementarity.
    """
    relationships = {
        'complementary': [],  # Integrations that often appear together
        'exclusive': [],      # Integrations that rarely appear together
        'primary': [],        # Integrations that are often the only integration
        'dependent': []       # Integrations that rarely appear alone
    }
    
    # Get all unique integrations
    all_integrations = set()
    for integrations in df['processed_integrations'].dropna():
        if integrations:
            all_integrations.update(integrations.split(','))
    
    # Analyze co-occurrence patterns
    total_apps = len(df)
    for int1, int2 in combinations(all_integrations, 2):
        # Count co-occurrences
        together = 0
        int1_total = 0
        int2_total = 0
        
        for integrations in df['processed_integrations'].dropna():
            if not integrations:
                continue
            integration_list = integrations.split(',')
            if int1 in integration_list:
                int1_total += 1
                if int2 in integration_list:
                    together += 1
            elif int2 in integration_list:
                int2_total += 1
        
        # Calculate relationship metrics
        expected_together = (int1_total / total_apps) * (int2_total / total_apps) * total_apps
        if together > 0:
            lift = together / expected_together
            if lift > 2:  # Strong positive correlation
                relationships['complementary'].append((int1, int2, lift))
            elif lift < 0.5:  # Strong negative correlation
                relationships['exclusive'].append((int1, int2, lift))
    
    # Analyze standalone patterns
    for integration in all_integrations:
        standalone_count = 0
        total_count = 0
        
        for integrations in df['processed_integrations'].dropna():
            if not integrations:
                continue
            integration_list = integrations.split(',')
            if integration in integration_list:
                total_count += 1
                if len(integration_list) == 1:
                    standalone_count += 1
        
        if total_count > 0:
            standalone_ratio = standalone_count / total_count
            if standalone_ratio > 0.5:
                relationships['primary'].append((integration, standalone_ratio))
            elif standalone_ratio < 0.1:
                relationships['dependent'].append((integration, standalone_ratio))
    
    return relationships

def create_visualizations(df: pd.DataFrame, clusters: Dict[str, List[str]], relationships: Dict):
    """
    Create visualizations of integration patterns.
    """
    os.makedirs(VISUALIZATIONS_DIR, exist_ok=True)
    
    # 1. Integration Network Graph
    plt.figure(figsize=(15, 10))
    G = nx.Graph()
    
    # Add nodes and edges from complementary relationships
    for int1, int2, lift in relationships['complementary']:
        G.add_edge(int1, int2, weight=lift)
    
    # Draw the network
    pos = nx.spring_layout(G)
    nx.draw(G, pos, with_labels=True, node_color='lightblue', 
            node_size=1000, font_size=8, font_weight='bold')
    plt.title("Integration Relationships Network")
    plt.savefig(os.path.join(VISUALIZATIONS_DIR, 'integration_network.png'))
    plt.close()
    
    # 2. Cluster Sizes
    plt.figure(figsize=(12, 6))
    cluster_sizes = {k: len(v) for k, v in clusters.items()}
    plt.bar(cluster_sizes.keys(), cluster_sizes.values())
    plt.xticks(rotation=45, ha='right')
    plt.title("Integration Cluster Sizes")
    plt.tight_layout()
    plt.savefig(os.path.join(VISUALIZATIONS_DIR, 'cluster_sizes.png'))
    plt.close()
    
    # 3. Integration Frequency Heatmap
    plt.figure(figsize=(15, 10))
    freq_matrix = pd.DataFrame(0, 
                             index=list(clusters.keys()), 
                             columns=['Standalone', 'Primary', 'Complementary', 'Dependent'])
    
    for cluster, members in clusters.items():
        standalone = sum(1 for m in members if any(p[0] == m for p in relationships['primary']))
        dependent = sum(1 for m in members if any(d[0] == m for d in relationships['dependent']))
        complementary = sum(1 for m in members 
                          if any(c[0] == m or c[1] == m for c in relationships['complementary']))
        
        freq_matrix.loc[cluster] = [
            standalone / len(members) if len(members) > 0 else 0,
            standalone,
            complementary,
            dependent
        ]
    
    sns.heatmap(freq_matrix, annot=True, cmap='YlOrRd', fmt='.2f')
    plt.title("Integration Patterns by Cluster")
    plt.tight_layout()
    plt.savefig(os.path.join(VISUALIZATIONS_DIR, 'integration_patterns.png'))
    plt.close()

def generate_analysis_report(df: pd.DataFrame) -> str:
    """Generate a comprehensive analysis report."""
    # Get various analyses
    freq = analyze_integration_frequency(df)
    pairs = analyze_integration_pairs(df)
    categories = analyze_integration_categories(df)
    complexity = analyze_app_complexity(df)
    rare_integrations = analyze_rare_integrations(df)
    
    # Get advanced analyses
    clusters = advanced_integration_clustering(
        [i for ints in df['processed_integrations'].dropna() 
         for i in ints.split(',') if ints]
    )
    relationships = analyze_integration_relationships(df)
    
    # Create visualizations
    create_visualizations(df, clusters, relationships)
    
    # Generate report
    report = [
        "# Shopify App Integration Pattern Analysis\n",
        f"Analysis based on {len(df)} apps with integrations.\n",
        
        "## Integration Frequency\n",
        "Top 20 most common integrations:\n",
        freq.head(20).to_string(),
        "\n\n",
        
        "## Common Integration Pairs\n",
        "Top 20 most common integration combinations:\n",
        pairs.head(20).to_string(),
        "\n\n",
        
        "## Integration Categories\n"
    ]
    
    # Add category analysis
    for category, integrations in categories.items():
        report.append(f"\n### {category.replace('_', ' ').title()}\n")
        for integration in sorted(integrations):
            count = freq.get(integration, 0)
            report.append(f"- {integration} ({count} apps)")
    
    # Add complexity analysis
    report.extend([
        "\n\n## App Complexity Analysis\n",
        "Distribution of apps by integration complexity:\n",
        "- Simple (1-2 integrations): " + str(complexity['simple']),
        "- Moderate (3-5 integrations): " + str(complexity['moderate']),
        "- Complex (6-8 integrations): " + str(complexity['complex']),
        "- Very Complex (9+ integrations): " + str(complexity['very_complex']),
        "\n\n"
    ])
    
    # Add relationship analysis
    report.extend([
        "## Integration Relationships\n",
        "\n### Complementary Integrations (Often Used Together)\n"
    ])
    for int1, int2, lift in sorted(relationships['complementary'], key=lambda x: x[2], reverse=True)[:10]:
        report.append(f"- {int1} + {int2} (Lift: {lift:.2f})")
    
    report.extend([
        "\n### Mutually Exclusive Integrations\n"
    ])
    for int1, int2, lift in sorted(relationships['exclusive'], key=lambda x: x[2])[:10]:
        report.append(f"- {int1} vs {int2} (Lift: {lift:.2f})")
    
    # Add rare integrations analysis
    report.extend([
        "\n\n## Analysis of Less Common Integrations\n",
        "Integrations that appear in 2 or fewer apps:\n\n"
    ])
    
    for _, row in rare_integrations.iterrows():
        report.extend([
            f"### {row['integration']}\n",
            f"Frequency: {row['frequency']} apps\n",
            f"Found in: {', '.join(row['apps'])}\n",
            f"Typically appears with: {', '.join(row['common_co_integrations'].keys())}\n",
            f"App types: {row['typical_app_types']}\n",
            f"Possible purpose: {guess_integration_purpose(row['integration'], row['typical_app_types'], list(row['common_co_integrations'].keys()))}\n\n"
        ])
    
    # Add cluster analysis
    report.extend([
        "\n\n## Integration Clusters\n",
        "Groups of similar integrations identified through advanced clustering:\n\n"
    ])
    
    for cluster_name, members in clusters.items():
        report.extend([
            f"### {cluster_name.replace('_', ' ').title()}\n",
            "Members:",
            '\n'.join(f"- {member}" for member in sorted(members)),
            "\n\n"
        ])
    
    # Add visualization references
    report.extend([
        "\n## Visualizations\n",
        "The following visualizations have been generated in the 'visualizations' directory:\n",
        "1. Integration Network Graph (integration_network.png)\n",
        "2. Cluster Sizes (cluster_sizes.png)\n",
        "3. Integration Patterns Heatmap (integration_patterns.png)\n"
    ])
    
    return "\n".join(report)

def main():
    """Main entry point."""
    try:
        # Read processed data
        logger.info(f"Reading processed data from {PROCESSED_FILE}")
        df = pd.read_csv(PROCESSED_FILE)
        
        # Generate analysis report
        logger.info("Generating analysis report")
        report = generate_analysis_report(df)
        
        # Save report
        logger.info(f"Saving analysis report to {ANALYSIS_OUTPUT}")
        with open(ANALYSIS_OUTPUT, 'w') as f:
            f.write(report)
        
        logger.info("Analysis complete")
        
    except Exception as e:
        logger.error(f"Error in analysis: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main() 