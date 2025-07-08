import json
import os
import requests
from typing import Optional
from langflow.load import run_flow_from_json

import uuid

# Generate a unique user ID
user_id = str(uuid.uuid4())


def convert_hotaf_to_tweaks(hotaf: dict) -> dict:
    """
    Convert the HOTAF tweaks to the format expected by the LangFlow API.

    :param hotaf: The HOTAF tweaks
    :return: The tweaks in the LangFlow API format
    """
    tweaks = {}
    for value in hotaf.values():
        if value["id"] not in tweaks:
            tweaks[value["id"]] = {}
        tweaks[value["id"]][value["key"]] = value["value"]
    return tweaks


def run_flow(
    api_url: str,
    message: str,
    flow_id: str,
    output_type: str = "text",
    input_type: str = "text",
    tweaks: Optional[dict] = None,
    api_key: Optional[str] = None,
    cfg: Optional[dict] = None,
) -> str:
    """
    Run a flow with a given message and optional tweaks.

    :param api_url: The URL of the LangFlow API
    :param message: The message to send to the flow
    :param flow_id: The ID of the flow
    :param tweaks: Optional tweaks to customize the flow
    :return: The JSON response from the flow
    """
    api_url = f"{api_url}/api/v1/run/{flow_id}"

    payload = {
        "input_value": message,
        "output_type": output_type,
        "input_type": input_type,
    }
    headers = None
    if tweaks:
        payload["tweaks"] = tweaks
    if api_key:
        headers = {"x-api-key": api_key}
    if cfg:
        cfg["tweaks"]["api_key"]["value"] = api_key
        tweaks = convert_hotaf_to_tweaks(cfg["tweaks"])
        payload["tweaks"] = tweaks

    response = requests.post(api_url, json=payload, headers=headers)

    # Check if the request was successful
    if response.status_code != 200:
        raise Exception(
            f"Request failed with status code {response.status_code}\n message: {response.text}"
        )

    # Return text response
    return response.json()["outputs"][0]["outputs"][0]["outputs"]["text"]["message"]


def run_flow_fj(cfg: dict, dict_flowjson: dict, api_key: str, message: str = ""):
    cfg["tweaks"]["api_key"]["value"] = api_key
    tweaks = convert_hotaf_to_tweaks(cfg["tweaks"])
    try:
        res = run_flow_from_json(
            flow=dict_flowjson,
            input_value=message,
            tweaks=tweaks,
            output_type="text",
            input_type="text",
            fallback_to_env_vars=True,
            session_id=user_id,  # Set the user ID here
        )
    except Exception as e:
        raise Exception(f"Error running flow api: {api_key} \n error: {e}")
    res = res[0].model_dump()
    return res["outputs"][0]["outputs"]["text"]["message"]
