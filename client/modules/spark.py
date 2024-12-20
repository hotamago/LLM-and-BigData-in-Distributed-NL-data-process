import requests
import sys
def fetch_content(record):
    # Log Python version on worker
    print(f"Worker Python Version: {sys.version}")
    
    url = record.get('url')
    title = record.get('title')
    snippet = record.get('snippet')
    try:
        response = requests.get(url, timeout=10)
        status_code = response.status_code
        content = response.text
    except Exception as e:
        # Log the exception for debugging
        print(f"Error fetching {url}: {e}")
        status_code = None
        content = None
    return (url, title, snippet, status_code, content)