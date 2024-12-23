import streamlit as st
from streamlit import session_state as ss
import pandas as pd

# Hota
from modules.get_block_code import convert_block_to_text
from modules.cache import hcache
from modules.helper import *

# Spark imports
import models
from config import cfg

# Spark plugin imports
from modules.spark import *

def render():
    st.write("Stage 6: Final data processing")
    user_input_stage_6 = st.text_area(
        "Enter final processing instructions",
        value=hcache.get("user_input_stage_6", default="")
    )
    # Cache user input
    hcache.set("user_input_stage_6", user_input_stage_6)

    # Show columns info
    if hcache.exists("columns_info"):
        st.write("Columns info:")
        st.table(hcache.get("columns_info", default=[]))

        if st.button("Generate final script"):
            columns_info = hcache.get("columns_info", [])
            script_result = gen_python_script_process(
                json.dumps(
                    {
                        "columns_info": "\n".join([f"{col["name"]}: {col["description"]}" for col in columns_info]),
                        "user_input": user_input_stage_6,
                        "read_parquet_path": 'cfg["hdfs"]["data_processed"]',
                        "write_parquet_path": 'cfg["hdfs"]["final_data"]',
                    }))
            script_result = convert_block_to_text(script_result, "python")
            hcache.set("final_script", script_result)

    if hcache.exists("final_script"):
        st.write("Final script (cached):")
        st.code(hcache.get("final_script", default=""), language="python")
        if st.button("Run final script"):
            try:
                st.success("Final script executed successfully.")
                exec(hcache.get("final_script", default=""))
            except Exception as e:
                st.error(f"Error running final script: {e}")