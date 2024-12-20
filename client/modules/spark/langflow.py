from langflow.load import run_flow_from_json

def run_flow(api_key, message=""):
    TWEAKS = {
        "GoogleGenerativeAIModel-WF3Qa": {
            "google_api_key": api_key
        },
    }
    res = run_flow_from_json(flow="SparkLLM.json",
                            input_value=message,
                            session_id="", # provide a session id if you want to use session state
                            fallback_to_env_vars=True, # False by default
                            tweaks=TWEAKS)
    return res['outputs'][0]["outputs"][0]["outputs"]["text"]["message"]

def run_flow_key(api_key, message)