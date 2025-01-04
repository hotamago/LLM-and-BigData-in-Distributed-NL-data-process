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

const_per_page = 100

def render():

    # import manager key
    manager_key = mk.ManagerKey(cfg['serper']['api_key'])

    # UI config
    st.write('Stage 2')
    # Show ss.list_query_engine_search
    st.write(ss.list_query_engine_search)
    # Split queries into batches of 100
    batch_size = 100
    query_batches = [ss.list_query_engine_search[i:i + batch_size] for i in range(0, len(ss.list_query_engine_search), batch_size)]
    # Config number of url get each query
    number_url_each_query = st.number_input('Number of url get each query', min_value=1, max_value=100000, value=hcache.get('number_url_each_query', default=10))
    # Cache number_url_each_query
    hcache.set('number_url_each_query', number_url_each_query)
    # Add progress bar
    progress = st.progress(0)
    # Button to confirm to get url
    if st.button('Get url', key='get_url'):
        # Create config
        total = number_url_each_query
        pages = (total + const_per_page - 1) // const_per_page
        all_results = []
        total_batches = len(query_batches)
        # Add outer loop for batches
        for batch_idx, batch in enumerate(query_batches, 1):
            for page in range(1, pages + 1):
                config = serper.Config(
                    batch=True,
                    typeSearch='search',
                    autocomplete=True,
                    query=batch,
                    country='us',
                    location='',
                    locale='en',
                    page=page,
                    num=min(number_url_each_query, 100),
                )
                # Create engine
                engine = serper.Engine(manager_key)
                res = engine._query(config)

                # Append to all_results
                all_results.extend(res)
                # Update progress
                progress.progress((batch_idx - 1) / total_batches + page / (total_batches * pages))

        # Save to cache
        hcache.set('url_search', all_results)

    # If url_search is not empty, show results
    if hcache.exists('url_search'):
        # Show top 10 results as table using pandas
        df = pd.DataFrame(hcache.get('url_search', default=[]))
        st.write("Top 10 URL Search Results")
        st.write(df.head(10))

        # Add analytics feature
        st.write("Analytics of URL Search Data")
        st.write(f"Total Results: {len(df)}")
        st.write(f"Unique URLs: {df.groupby('url').ngroups}")
        st.write("Top Domains")
        top_domains = df['url'].apply(lambda x: x.split('/')[2]).value_counts().head(10).reset_index()
        top_domains.columns = ['Domain', 'Count']
        st.write(top_domains)

        # Add download button for full url_search data as CSV
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download full URL search data as CSV",
            data=csv,
            file_name='url_search_data.csv',
            mime='text/csv',
        )

        # Button to confirm the queries
        if st.button('Confirm queries', key='confirm_queries'):
            # update stage
            ss.stage_process = 2
            st.rerun()