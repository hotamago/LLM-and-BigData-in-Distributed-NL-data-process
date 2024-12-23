import json
from modules.get_block_code import convert_block_to_text

# Def function to convert json string from LLM output to JSON object
def convert_json_string_to_object(json_string: str):
   return json.loads(convert_block_to_text(json_string, "json"))