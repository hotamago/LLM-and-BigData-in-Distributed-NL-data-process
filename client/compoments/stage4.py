import streamlit as st
from streamlit import session_state as ss
import pandas as pd

# Hota
from modules.cache import hcache
from modules.helper import *

# Spark imports
import models
from config import cfg

# Spark plugin imports
from modules.spark import *

def render():
    st.write("Stage 4: Provide data processing requirements")
    user_input_stage_4 = st.text_area(
        "Enter your data processing requirements",
        value=hcache.get("user_input_stage_4", default="")
    )
    # Cache user input
    hcache.set("user_input_stage_4", user_input_stage_4)

    if st.button("Generate columns info", key="gen_columns_info"):
        columns_info = gen_columns_info(json.dumps(
            {
                "requirements": user_input_stage_4,
                "exited_columns": "url, title, snippet, content"
            }
        ))
        hcache.set("columns_info", columns_info)

    if hcache.exists("columns_info"):
        st.write("Generated columns info:")
        st.json(hcache.get("columns_info", default=[]))
        if st.button("Confirm columns info", key="confirm_columns_info"):
            ss.stage_process = 4
            st.rerun()