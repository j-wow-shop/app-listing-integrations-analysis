#!/usr/bin/env python3
"""
Script to analyze integration patterns and common combinations in Shopify apps.
"""
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import defaultdict
from itertools import combinations
import networkx as nx

# Configuration
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
VISUALIZATIONS_DIR = os.path.join(DATA_DIR, 'visualizations')
os.makedirs(VISUALIZATIONS_DIR, exist_ok=True)

def load_data():
    """Load and preprocess the data."""
    df = pd.read_csv(os.path.join(DATA_DIR, 'top_apps_raw.csv'))
    df['integrations'] = df['integrations'].apply(lambda x: [] if pd.isna(x) else x.split(','))
    return df

def analyze_common_pairs(df, min_occurrences=5):
    """Analyze commonly co-occurring integration pairs."""
    pair_counts = defaultdict(int)
    
    for integrations in df['integrations']:
        # Look at each pair of integrations
        for int1, int2 in combinations(sorted(integrations), 2):
            pair_counts[(int1, int2)] += 1
    
    # Convert to DataFrame for easier analysis
    pairs_df = pd.DataFrame([
        {'integration1': pair[0], 'integration2': pair[1], 'count': count}
        for pair, count in pair_counts.items()
        if count >= min_occurrences
    ])
    
    if not pairs_df.empty:
        pairs_df = pairs_df.sort_values('count', ascending=False)
    
    return pairs_df

def create_network_graph(pairs_df, min_count=10):
    """Create a network graph of integration relationships."""
    plt.figure(figsize=(15, 15))
    
    # Create graph
    G = nx.Graph()
    
    # Add edges with weights
    for _, row in pairs_df[pairs_df['count'] >= min_count].iterrows():
        G.add_edge(
            row['integration1'], 
            row['integration2'], 
            weight=row['count']
        )
    
    # Calculate node sizes based on degree centrality
    centrality = nx.degree_centrality(G)
    node_sizes = [v * 3000 for v in centrality.values()]
    
    # Set up the layout
    pos = nx.spring_layout(G, k=1, iterations=50)
    
    # Draw the network
    nx.draw(
        G, pos,
        node_color='lightblue',
        node_size=node_sizes,
        font_size=8,
        width=[G[u][v]['weight']/5 for u,v in G.edges()],
        with_labels=True
    )
    
    plt.title('Integration Relationship Network\n(Line thickness represents frequency of co-occurrence)')
    plt.tight_layout()
    plt.savefig(os.path.join(VISUALIZATIONS_DIR, 'integration_network.png'), dpi=300, bbox_inches='tight')
    plt.close()

def analyze_integration_stacks(df, min_stack_size=3, min_occurrences=3):
    """Analyze common integration stacks (combinations of 3 or more integrations)."""
    stack_counts = defaultdict(int)
    
    for integrations in df['integrations']:
        if len(integrations) >= min_stack_size:
            # Look at combinations of min_stack_size or more integrations
            for size in range(min_stack_size, len(integrations) + 1):
                for stack in combinations(sorted(integrations), size):
                    stack_counts[stack] += 1
    
    # Filter and sort stacks
    common_stacks = {
        stack: count for stack, count in stack_counts.items()
        if count >= min_occurrences
    }
    
    return dict(sorted(common_stacks.items(), key=lambda x: x[1], reverse=True))

def generate_pattern_report(df, pairs_df, stacks):
    """Generate a detailed report of integration patterns."""
    report = ["# Integration Pattern Analysis\n\n"]
    
    # Basic statistics
    report.append("## Overview Statistics\n")
    total_apps = len(df)
    apps_with_integrations = len(df[df['integrations'].apply(len) > 0])
    report.append(f"- Total apps analyzed: {total_apps}")
    report.append(f"- Apps with integrations: {apps_with_integrations} ({apps_with_integrations/total_apps*100:.1f}%)")
    
    # Integration pair analysis
    report.append("\n## Common Integration Pairs\n")
    report.append("Most frequently co-occurring integration pairs:\n")
    for _, row in pairs_df.head(10).iterrows():
        report.append(f"- {row['integration1']} + {row['integration2']}: {row['count']} apps")
    
    # Integration stacks analysis
    report.append("\n## Common Integration Stacks\n")
    report.append("Most common combinations of 3 or more integrations:\n")
    for stack, count in list(stacks.items())[:10]:
        report.append(f"- Stack of {len(stack)} integrations ({count} apps):")
        for integration in stack:
            report.append(f"  - {integration}")
        report.append("")
    
    # Integration density analysis
    integration_counts = df['integrations'].apply(len)
    report.append("\n## Integration Density\n")
    report.append("Distribution of number of integrations per app:\n")
    for count in range(1, integration_counts.max() + 1):
        apps_with_count = len(integration_counts[integration_counts == count])
        if apps_with_count > 0:
            report.append(f"- {count} integration(s): {apps_with_count} apps ({apps_with_count/total_apps*100:.1f}%)")
    
    # Save report
    with open(os.path.join(VISUALIZATIONS_DIR, 'pattern_analysis.md'), 'w') as f:
        f.write('\n'.join(report))

def create_density_plot(df):
    """Create a plot showing the distribution of integration counts per app."""
    plt.figure(figsize=(12, 6))
    
    integration_counts = df['integrations'].apply(len)
    sns.histplot(data=integration_counts, bins=range(0, integration_counts.max() + 2, 1))
    
    plt.title('Distribution of Integrations per App')
    plt.xlabel('Number of Integrations')
    plt.ylabel('Number of Apps')
    plt.tight_layout()
    
    plt.savefig(os.path.join(VISUALIZATIONS_DIR, 'integration_density.png'))
    plt.close()

def main():
    """Main execution function."""
    # Load data
    df = load_data()
    
    # Analyze integration pairs
    pairs_df = analyze_common_pairs(df)
    
    # Create network visualization
    create_network_graph(pairs_df)
    
    # Analyze integration stacks
    stacks = analyze_integration_stacks(df)
    
    # Create density plot
    create_density_plot(df)
    
    # Generate report
    generate_pattern_report(df, pairs_df, stacks)

if __name__ == "__main__":
    main() 