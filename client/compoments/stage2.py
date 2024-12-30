import streamlit as st
from streamlit import session_state as ss
import pandas as pd

# Hota
from modules.cache import hcache
from modules.helper import *

# Spark imports
from config import cfg

# import search engine
import modules.searchEngine.serper as serper
import modules.managerKey as mk

def render():

    # import manager key
    manager_key = mk.ManagerKey(cfg['serper']['api_key'])

    # UI config
    st.write('Stage 2')
    # Config number of url get each query
    number_url_each_query = st.number_input('Number of url get each query', min_value=1, max_value=100000, value=hcache.get('number_url_each_query', default=10))
    # Cache number_url_each_query
    hcache.set('number_url_each_query', number_url_each_query)

    # Button to confirm to get url
    if st.button('Get url', key='get_url'):
        # Create config
        total = number_url_each_query
        per_page = 100
        pages = (total + per_page - 1) // per_page
        all_results = []
        for page in range(1, pages + 1):
            config = serper.Config(
                batch=True,
                typeSearch='search',
                autocomplete=False,
                query=ss.list_query_engine_search,
                country='us',
                location='',
                locale='en',
                page=page,
                num=min(per_page, total - (page - 1) * per_page),
            )
            # Create engine
            engine = serper.Engine(manager_key)
            res = engine._query(config)
            all_results.extend(res)

        # Save to cache
        hcache.set('url_search', all_results)

    # If url_search is not empty, show results
    if hcache.exists('url_search'):
        # Show results as table using pandas
        df = pd.DataFrame(hcache.get('url_search', default=[]))
        st.write(df)

        # Button to confirm the queries
        if st.button('Confirm queries', key='confirm_queries'):
            # update stage
            ss.stage_process = 2
            st.rerun()