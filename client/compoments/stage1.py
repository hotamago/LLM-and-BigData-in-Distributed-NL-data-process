import time
import streamlit as st
import json
from streamlit import session_state as ss
# Hota
from modules.cache import hcache
from modules.helper import *
from streamlit_ace import st_ace

def render():
    st.header("Step 1: Generate Search Queries")
    
    with st.expander("How to use this tool", expanded=True):
        st.info("""
        1. Enter your main topics (one per line) in the text area
        2. Choose how many search queries to generate for each topic
        3. Click 'Generate Queries' to create search terms
        4. Review the generated queries and confirm when ready
        """)
    
    # Get inputs type area - replacing st_ace with standard text_area
    user_input = st.text_area(
        'Enter your search topics (one per line):',
        value=hcache.get('user_input', default=''),
        placeholder='For example:\nartificial intelligence\nclimate change',
        height=150,
        key='user_input'
    )
    
    # Get input type number
    number_queries_gen = st.number_input('Number of queries to generate per topic:', 
                                         min_value=1, 
                                         value=hcache.get('number_queries_gen', default=10),
                                         help="This determines how many different search queries will be created for each topic you enter above")

    if st.button('Generate Queries', key='gen_query'):
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
            progress_bar = st.progress(0)
            process_display = st.empty()
            for i in range(len(list_user_query)):
                query = list_user_query[i]
                process_display.write(f'Progress: {i+1}/{len(list_user_query)} topics')
                progress_bar.progress((i+1)/len(list_user_query))
                # st.write(query)
                with st.spinner(f'Generating queries for: {query["user_input"]}'):
                    while True:
                        try:
                            ss.list_query_engine_search_gen.extend(gen_query(json.dumps(query)))
                            break
                        except Exception as e:
                            st.error(f"Error generating queries: {e}")
                            time.sleep(1)

    # Show results - Replace raw JSON display with a more user-friendly format
    if 'list_query_engine_search' in ss and ss.list_query_engine_search:
        st.subheader('Previously Generated Queries')
        
        # # Show as a simple list in a scrollable container
        # with st.container():
        #     for i, query in enumerate(ss.list_query_engine_search):
        #         st.write(f"{i+1}. {query}")
                
        if st.button('Edit Previous Queries', key='edit_old_queries'):
            # Convert to simple text for editing
            query_text = '\n'.join(ss.list_query_engine_search)
            edited_text = st.text_area(
                'Edit queries (one per line):',
                value=query_text,
                height=200,
                key='edited_old_queries_text'
            )
            
            if st.button('Save Changes', key='save_old_queries'):
                ss.list_query_engine_search = edited_text.strip().split('\n')
                hcache.set('list_query_engine_search', ss.list_query_engine_search)
                st.success("Queries updated successfully!")

    if 'list_query_engine_search_gen' in ss and ss.list_query_engine_search_gen:
        st.subheader('Newly Generated Queries')
        
        # # Show as a simple list in a scrollable container
        # with st.container():
        #     for i, query in enumerate(ss.list_query_engine_search_gen):
        #         st.write(f"{i+1}. {query}")
        
        if st.button('Edit New Queries', key='edit_new_queries'):
            # Convert to simple text for editing
            query_text = '\n'.join(ss.list_query_engine_search_gen)
            edited_text = st.text_area(
                'Edit queries (one per line):',
                value=query_text,
                height=200,
                key='edited_new_queries_text'
            )
            
            if st.button('Save Changes', key='save_new_queries'):
                ss.list_query_engine_search_gen = edited_text.strip().split('\n')
                st.success("Queries updated successfully!")

        # Button to confirm the queries
        st.markdown("---")
        st.write("When you're ready to proceed to the next step, click the button below:")
        if st.button('Use These Queries & Continue', key='confirm_queries'):
            # Update cache
            hcache.set('list_query_engine_search', ss.list_query_engine_search_gen)
            # update stage
            ss.stage_process = 1
            st.rerun()