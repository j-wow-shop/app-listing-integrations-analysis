import pandas as pd
import re

def standardize_integration_name(name: str) -> str:
    """Standardize a single integration name"""
    if not name:
        return name
        
    # Convert to lowercase and strip whitespace
    name = name.lower().strip()
    
    # Standard name mappings
    name_mappings = {
        # Social Media
        'fb': 'Facebook',
        'facebook': 'Facebook',
        'ig': 'Instagram',
        'insta': 'Instagram',
        'instagram': 'Instagram',
        'whatsapp': 'WhatsApp',
        'wa': 'WhatsApp',
        'tiktok': 'TikTok',
        'youtube': 'YouTube',
        'pinterest': 'Pinterest',
        
        # Payment Services
        'paypal': 'PayPal',
        'stripe': 'Stripe',
        'klarna': 'Klarna',
        'shopify checkout': 'Shopify Checkout',
        'checkout': 'Shopify Checkout',
        
        # Marketing & Analytics
        'google analytics': 'Google Analytics',
        'ga': 'Google Analytics',
        'google ads': 'Google Ads',
        'klaviyo': 'Klaviyo',
        'mailchimp': 'Mailchimp',
        'activecampaign': 'ActiveCampaign',
        
        # Shipping & Logistics
        'ups': 'UPS',
        'usps': 'USPS',
        'dhl': 'DHL',
        'fedex': 'FedEx',
        
        # Marketplaces
        'amazon': 'Amazon',
        'ebay': 'eBay',
        'etsy': 'Etsy',
        'walmart': 'Walmart',
        
        # Shopify Services
        'pos': 'Shopify POS',
        'shopify pos': 'Shopify POS',
        'flow': 'Shopify Flow',
        'shopify flow': 'Shopify Flow',
        
        # Communication
        'zendesk': 'Zendesk',
        'gorgias': 'Gorgias',
        'intercom': 'Intercom',
        
        # Business Tools
        'zapier': 'Zapier',
        'quickbooks': 'QuickBooks',
        'xero': 'Xero',
        'hubspot': 'HubSpot',
        'salesforce': 'Salesforce',
        
        # AI/ML
        'chatgpt': 'ChatGPT',
        'openai': 'OpenAI',
        'gpt': 'ChatGPT',
        
        # File Storage
        'dropbox': 'Dropbox',
        'google drive': 'Google Drive',
        
        # Development
        'github': 'GitHub',
        'gitlab': 'GitLab',
        'bitbucket': 'Bitbucket',
        
        # Customer Management
        'customer accounts': 'Customer Accounts',
        'customer account': 'Customer Accounts'
    }
    
    # Check direct matches first
    if name in name_mappings:
        return name_mappings[name]
    
    # Check for partial matches (e.g., "facebook pixel" should still use "Facebook" capitalization)
    for pattern, replacement in name_mappings.items():
        if pattern in name:
            name = name.replace(pattern, replacement)
            
    # Capitalize remaining words properly
    words = name.split()
    capitalized_words = []
    for word in words:
        # Don't capitalize certain words in the middle of the phrase
        if word in ['and', 'or', 'in', 'on', 'at', 'to', 'for', 'with', 'by']:
            capitalized_words.append(word)
        else:
            capitalized_words.append(word.capitalize())
    
    return ' '.join(capitalized_words)

def standardize_integrations(csv_path: str, output_path: str):
    """
    Read CSV file, standardize integration names, and save to new file
    """
    # Read the CSV file
    df = pd.read_csv(csv_path)
    
    # Process integrations column
    def process_integration_list(integrations):
        if pd.isna(integrations) or not integrations:
            return []
            
        # Split on common separators
        if isinstance(integrations, str):
            # Handle various separators
            integrations = re.split(r',|\|', integrations)
            
            # Clean and standardize each integration
            cleaned = []
            for integration in integrations:
                standardized = standardize_integration_name(integration)
                if standardized:  # Only add non-empty values
                    cleaned.append(standardized)
                    
            return sorted(list(set(cleaned)))  # Remove duplicates and sort
        return []
    
    # Apply standardization
    df['integrations'] = df['integrations'].apply(process_integration_list)
    
    # Save to new file
    df.to_csv(output_path, index=False)
    print(f"Standardized integrations saved to {output_path}")
    
    # Print some statistics
    total_apps = len(df)
    apps_with_integrations = len(df[df['integrations'].apply(len) > 0])
    total_integrations = sum(df['integrations'].apply(len))
    unique_integrations = len(set([i for sublist in df['integrations'] for i in sublist]))
    
    print(f"\nStatistics:")
    print(f"Total apps processed: {total_apps}")
    print(f"Apps with integrations: {apps_with_integrations}")
    print(f"Total integration mentions: {total_integrations}")
    print(f"Unique integrations: {unique_integrations}")

if __name__ == "__main__":
    input_file = "data/apps_for_analysis.csv"
    output_file = "data/standardized_apps.csv"
    standardize_integrations(input_file, output_file) 