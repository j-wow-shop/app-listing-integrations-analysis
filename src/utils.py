"""
Utility functions for the app integration analysis project.
"""
import logging
from pathlib import Path
from typing import Union, Dict, Any

# Set up logging
def setup_logging(log_file: Union[str, Path] = None, level: int = logging.INFO) -> None:
    """
    Set up logging configuration.
    
    Args:
        log_file: Optional path to log file
        level: Logging level (default: INFO)
    """
    format_str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Basic configuration
    logging.basicConfig(
        level=level,
        format=format_str
    )
    
    # Add file handler if log_file specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter(format_str))
        logging.getLogger().addHandler(file_handler)

def safe_request(url: str, headers: Dict[str, str] = None, timeout: int = 30) -> Dict[str, Any]:
    """
    Make a safe HTTP request with proper error handling.
    
    Args:
        url: URL to request
        headers: Optional request headers
        timeout: Request timeout in seconds
        
    Returns:
        Dictionary containing response data or error information
    """
    import requests
    from requests.exceptions import RequestException
    
    try:
        response = requests.get(url, headers=headers, timeout=timeout)
        response.raise_for_status()
        return {
            'success': True,
            'status_code': response.status_code,
            'content': response.text
        }
    except RequestException as e:
        return {
            'success': False,
            'error': str(e),
            'status_code': getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None
        }

def ensure_dir(path: Union[str, Path]) -> Path:
    """
    Ensure a directory exists, creating it if necessary.
    
    Args:
        path: Directory path
        
    Returns:
        Path object for the directory
    """
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path 