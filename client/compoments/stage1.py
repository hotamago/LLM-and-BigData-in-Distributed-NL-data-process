import streamlit as st
import json
from streamlit import session_state as ss
# Hota
from modules.cache import hcache
from modules.helper import *
from streamlit_ace import st_ace

def render():
    # Get inputs type area
    user_input = st_ace(
        value=hcache.get('user_input', default=''),
        placeholder='Each line is a query document',
        height=200,
        language='plain_text',
        theme='chrome',
        key='user_input'
    )
    # Get input type number
    number_queries_gen = st.number_input('Number queries', min_value=1, max_value=100000, value=hcache.get('number_queries_gen', default=10))

    if st.button('Generate queries', key='gen_query'):
        ss.list_query_engine_search_gen = []
        # Update cache
        hcache.set('user_input', user_input)
        hcache.set('number_queries_gen', number_queries_gen)

        list_user_input = user_input.strip().split('\n')

        # Generate queries
        number_queries_gen = hcache.get('number_queries_gen', default=10)
        max_batch = 50
        batches = (number_queries_gen + max_batch - 1) // max_batch

        list_user_query = []
        for query in list_user_input:
            for _ in range(batches):
                list_user_query.append({
                    "user_input": query,
                    "num_queries": min(max_batch, number_queries_gen - (_ * max_batch)),
                })
            
        with st.spinner('Generating queries...'):
            process_display = st.empty()
            for i in range(len(list_user_query)):
                query = list_user_query[i]
                process_display.write(f'Generate th: {i+1}/{len(list_user_query)}')
                with st.spinner(f'Generating queries for: {query["user_input"]}'):
                    ss.list_query_engine_search_gen.extend(gen_query(json.dumps(query)))

    # Show results
    if ss.list_query_engine_search:
        st.write('Old queries generated:')
        st.write(ss.list_query_engine_search)
        # Manual edit feature
        edited_old_queries = st_ace(
            value=json.dumps(ss.list_query_engine_search, indent=2),
            placeholder='Edit old queries',
            height=200,
            language='json',
            theme='chrome',
            key='edited_old_queries'
        )
        if st.button('Save old queries', key='save_old_queries'):
            ss.list_query_engine_search = json.loads(edited_old_queries)
            hcache.set('list_query_engine_search', ss.list_query_engine_search)

    if ss.list_query_engine_search_gen:
        st.write('Queries generated:')
        st.write(ss.list_query_engine_search_gen)
        # Manual edit feature
        edited_new_queries = st_ace(
            value=json.dumps(ss.list_query_engine_search_gen, indent=2),
            placeholder='Edit new queries',
            height=200,
            language='json',
            theme='chrome',
            key='edited_new_queries'
        )
        if st.button('Save new queries', key='save_new_queries'):
            ss.list_query_engine_search_gen = json.loads(edited_new_queries)

        # Button to confirm the queries
        if st.button('Confirm queries', key='confirm_queries'):
            # Update cache
            hcache.set('list_query_engine_search', ss.list_query_engine_search_gen)
            # update stage
            ss.stage_process = 1
            st.rerun()