import requests
import sys
from .langflow import run_flow
import html2text
import json
from modules.json_tool import convert_json_string_to_object

def fetch_content(record):
    # Log Python version on worker
    print(f"Worker Python Version: {sys.version}")
    
    url = record.get('url')
    title = record.get('title')
    snippet = record.get('snippet')
    try:
        response = requests.get(url, timeout=10)
        status_code = response.status_code
        content = html2text.html2text(html=response.text)
    except Exception as e:
        # Log the exception for debugging
        print(f"Error fetching {url}: {e}")
        status_code = None
        content = None
    return (url, title, snippet, status_code, content)

def run_spark_flow(record, columns_info: list, cfg: dict):
    # Log Python version on worker
    print(f"Worker Python Version: {sys.version}")

    url = record.get('url')
    title = record.get('title')
    snippet = record.get('snippet')
    content = record.get('content')
    list_res = ["" for _ in range(len(columns_info))]

    # Run the language processing flow
    try:
        data_processed = run_flow(
            api_url=cfg['langflow']['api_url'],
            message=json.dumps({
                "columns_information": "\n".join([f"{col["name"]}: {col["description"]}" for col in columns_info]),
                "given_data": f"{url}\n\n{title}\n\n{snippet}\n\n{content}",
            }),
            flow_id=cfg['langflow']['flow_id']['process_data'],
        )
    except Exception as e:
        # Log the exception for debugging
        print(f"Error processing content: {e}")
        data_processed = None
    
    # Prepare the result list
    if data_processed is not None:
        # Try to parse the JSON string
        try:
            data_processed = convert_json_string_to_object(data_processed)
            # data_processed is dict now
            for i in range(len(list_res)):
                list_res[i] = data_processed.get(columns_info[i]['name'], "")
        except Exception as e:
            # Log the exception for debugging
            print(f"Error parsing data_processed: {e}")

    return [url, title, snippet, content, *list_res]