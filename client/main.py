import streamlit as st
from streamlit import session_state as ss

# Hota
from modules.cache import hcache
from modules.helper import *

# Page setup
st.set_page_config(page_title='Project 3', page_icon=':mag:', layout='wide')
st.title('Project 3 - Big Data Collection and Processing')

# Init session_state
if "stage_process" not in ss:
    ss.stage_process = 0
if "list_query_engine_search" not in ss:
    ss.list_query_engine_search = []
if "list_query_engine_search_gen" not in ss:
    ss.list_query_engine_search_gen = []

# Improve UI by using tabs
tab1, tab2 = st.tabs(["Home", "HDFS"])

with tab1:
    # Global variables
    if hcache.exists('list_query_engine_search'):
        ss.list_query_engine_search = hcache.get('list_query_engine_search', default=[])

    # Stage process
    with st.container():
        # Choice stage
        list_stage_process = [
            'Stage 1: Generate queries',
            'Stage 2: Get url',
            'Stage 3: Get content',
            'Stage 4: Gen columns info',
            'Stage 5: Process data',
            'Stage 6: Final process',
        ]
        stage_process = st.selectbox('Select stage', list_stage_process, index=ss.stage_process)

        # Stage 1
        if stage_process == list_stage_process[0]:
            from compoments.stage1 import render
            render()

        elif stage_process == list_stage_process[1]: # Stage 2: Using search engine api to search url web
            from compoments.stage2 import render
            render()
            
        elif stage_process == list_stage_process[2]: # Stage 3: From url send request to get content by spark then each spark send to hadoop to save\
            from compoments.stage3 import render
            render()
            
        elif stage_process == list_stage_process[3]:  # Stage 4
            from compoments.stage4 import render
            render()

        elif stage_process == list_stage_process[4]:  # Stage 5
            from compoments.stage5 import render
            render()

        elif stage_process == list_stage_process[5]:  # Stage 6
            from compoments.stage6 import render
            render()

        else:
            st.write('Stage not exited')

with tab2:
    from compoments.hdfs_explorser import render
    render()