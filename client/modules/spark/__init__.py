import requests
import sys
from .langflow import run_flow

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

def run_spark_flow(record):
    # Log Python version on worker
    print(f"Worker Python Version: {sys.version}")

    title = record.get('title')
    snippet = record.get('snippet')
    status_code = record.get('status_code')
    content = record.get('content')

    # Run the language processing flow
    try:
        data_processed = run_flow() # Todo: pass the API key
    except Exception as e:
        # Log the exception for debugging
        print(f"Error processing content: {e}")
        data_processed = None

    return (title, snippet, status_code, content, data_processed)