import requests
from typing import Optional

def run_flow(
        api_url: str,
        message: str,
        flow_id: str,
        output_type: str = "text",
        input_type: str = "text",
        tweaks: Optional[dict] = None,
        api_key: Optional[str] = None
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
    response = requests.post(api_url, json=payload, headers=headers)

    # Check if the request was successful
    if response.status_code != 200:
        raise Exception(f"Request failed with status code {response.status_code}\n message: {response.text}")
    
    # Return text response
    return response.json()['outputs'][0]["outputs"][0]["outputs"]["text"]["message"]
