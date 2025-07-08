import requests
import sys
from modules.managerKey import ManagerKey
from modules.langflow import run_flow, run_flow_fj
import html2text
import json
from modules.json_tool import convert_json_string_to_object
import time


def fetch_content(record):
    # Log Python version on worker
    print(f"Worker Python Version: {sys.version}")

    url = record.get("url")
    title = record.get("title")
    snippet = record.get("snippet")
    log_str = ""
    status_code = 0
    content = ""

    try:
        response = requests.get(url, timeout=30)
        status_code = response.status_code
        content = html2text.html2text(html=response.text)
    except Exception as e:
        # Log the exception for debugging
        log_str = f"Error fetching {url}: {e}"

    return [url, title, snippet, status_code, content, log_str]


def run_spark_flow(record, dict_flowjson: dict, columns_info: list, cfg: dict):
    # Log Python version on worker
    log_str = f"Worker Python Version: {sys.version}"

    url = record.get("url")
    title = record.get("title")
    snippet = record.get("snippet")
    content = record.get("content")
    apiKey = record.get("apiKey")
    list_res = ["" for _ in range(len(columns_info))]

    # Run the language processing flow
    try:
        if cfg["run_api_langflow"]:
            data_processed = run_flow(
                api_url=cfg["langflow"]["api_url"],
                message=json.dumps(
                    {
                        "columns_information": "\n".join(
                            [
                                f"{col.get('name')}: {col.get('description')}"
                                for col in columns_info
                            ]
                        ),
                        "given_data": f"{url}\n\n{title}\n\n{snippet}\n\n{content}",
                    }
                ),
                flow_id=cfg["langflow"]["flow_id"]["process_data"],
            )
        else:
            data_processed = run_flow_fj(
                cfg=cfg["langflow"]["flow_json"]["process_data"],
                dict_flowjson=dict_flowjson,
                api_key=apiKey,
                message=json.dumps(
                    {
                        "columns_information": "\n".join(
                            [
                                f"{col.get('name')}: {col.get('description')}"
                                for col in columns_info
                            ]
                        ),
                        "given_data": f"{url}\n\n{title}\n\n{snippet}\n\n{content}",
                    }
                ),
            )
    except Exception as e:
        # Log the exception for debugging
        log_str += f"\nError running language processing flow: {e}"
        data_processed = None

    # Prepare the result list
    if data_processed is not None:
        # Try to parse the JSON string
        try:
            data_processed = convert_json_string_to_object(data_processed)
            # data_processed is dict now
            for i in range(len(list_res)):
                list_res[i] = data_processed.get(columns_info[i]["name"], "")
        except Exception as e:
            # Log the exception for debugging
            log_str += f"\nError parsing JSON string: {e}"

    return [url, title, snippet, content, log_str, *list_res]


def run_spark_flow_safe(
    record, dict_flowjson: dict, columns_info: list, worker_cfg: dict
):
    """
    Safer version of run_spark_flow with better error handling and retry logic
    """
    log_str = f"Worker Python Version: {sys.version}"

    url = record.get("url", "")
    title = record.get("title", "")
    snippet = record.get("snippet", "")
    content = record.get("content", "")
    apiKey = record.get("apiKey", "")
    list_res = ["" for _ in range(len(columns_info))]

    # Retry logic for network operations
    max_retries = 3
    retry_delay = 1

    for attempt in range(max_retries):
        try:
            # Run the language processing flow
            if worker_cfg["run_api_langflow"]:
                data_processed = run_flow(
                    api_url=worker_cfg["langflow"]["api_url"],
                    message=json.dumps(
                        {
                            "columns_information": "\n".join(
                                [
                                    f"{col.get('name')}: {col.get('description')}"
                                    for col in columns_info
                                ]
                            ),
                            "given_data": f"{url}\n\n{title}\n\n{snippet}\n\n{content}",
                        }
                    ),
                    flow_id=worker_cfg["langflow"]["flow_id"],
                )
            else:
                data_processed = run_flow_fj(
                    cfg=worker_cfg["langflow"]["flow_json"],
                    dict_flowjson=dict_flowjson,
                    api_key=apiKey,
                    message=json.dumps(
                        {
                            "columns_information": "\n".join(
                                [
                                    f"{col.get('name')}: {col.get('description')}"
                                    for col in columns_info
                                ]
                            ),
                            "given_data": f"{url}\n\n{title}\n\n{snippet}\n\n{content}",
                        }
                    ),
                )

            # If we get here, the API call succeeded
            break

        except Exception as e:
            log_str += f"\nAttempt {attempt + 1} failed: {str(e)}"

            # If this is the last attempt, set data_processed to None
            if attempt == max_retries - 1:
                data_processed = None
                log_str += f"\nAll {max_retries} attempts failed"
            else:
                # Wait before retrying
                time.sleep(retry_delay * (attempt + 1))

    # Prepare the result list
    if data_processed is not None:
        # Try to parse the JSON string
        try:
            data_processed = convert_json_string_to_object(data_processed)
            # data_processed is dict now
            for i in range(len(list_res)):
                column_name = columns_info[i]["name"]
                list_res[i] = (
                    str(data_processed.get(column_name, "")).strip()
                    if data_processed.get(column_name)
                    else ""
                )

        except Exception as e:
            # Log the exception for debugging
            log_str += f"\nError parsing JSON string: {e}"
            # Keep empty strings in list_res

    return [url, title, snippet, content, log_str, *list_res]
