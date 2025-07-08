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
    manager_key = mk.ManagerKey(cfg["serper"]["api_key"])

    # UI config
    st.header("Step 2: Find Web Resources")

    with st.expander("How this works", expanded=True):
        st.info("""
        1. This step searches the web for resources related to your queries
        2. You can set how many results to collect for each query
        3. Click 'Start Search' to begin gathering information
        4. When complete, you can review the results before continuing
        """)

    # Show query summary instead of raw data
    if "list_query_engine_search" in ss and ss.list_query_engine_search:
        st.subheader("Your Search Queries")
        st.write(f"Number of queries: {len(ss.list_query_engine_search)}")

        with st.expander("View all queries"):
            for i, query in enumerate(ss.list_query_engine_search):
                st.write(f"{i + 1}. {query}")

    # Config number of url get each query
    number_url_each_query = st.number_input(
        "Results to collect per query:",
        min_value=1,
        value=ss.hcache.get("number_url_each_query", default=10),
        help="Higher numbers will find more resources but take longer to process",
        on_change=lambda: ss.hcache.set("number_url_each_query", number_url_each_query),
    )

    # Cache number_url_each_query
    # ss.hcache.set('number_url_each_query', number_url_each_query)

    # Add progress bar
    progress_container = st.empty()

    # Button to confirm to get url
    if st.button("Start Search", key="get_url"):
        # Split queries into batches of 100
        batch_size = 100
        query_batches = [
            ss.list_query_engine_search[i : i + batch_size]
            for i in range(0, len(ss.list_query_engine_search), batch_size)
        ]

        # Create config
        total = number_url_each_query
        pages = (total + const_per_page - 1) // const_per_page
        all_results = []
        total_batches = len(query_batches)

        # Progress tracking
        progress = progress_container.progress(0)
        status_text = st.empty()

        # Add outer loop for batches
        for batch_idx, batch in enumerate(query_batches, 1):
            status_text.write(f"Processing batch {batch_idx} of {total_batches}...")
            for page in range(1, pages + 1):
                config = serper.Config(
                    batch=True,
                    typeSearch="search",
                    autocomplete=True,
                    query=batch,
                    country="us",
                    location="",
                    locale="en",
                    page=page,
                    num=min(number_url_each_query, 100),
                )
                # Create engine
                engine = serper.Engine(manager_key)
                res = engine._query(config)

                # Append to all_results
                all_results.extend(res)
                # Update progress
                progress.progress(
                    (batch_idx - 1) / total_batches + page / (total_batches * pages)
                )

        # Save to cache
        ss.hcache.set("url_search", all_results)
        status_text.success("Search completed successfully!")

    # If url_search is not empty, show results
    if ss.hcache.exists("url_search"):
        # Convert data to DataFrame for cleaner display
        df = pd.DataFrame(ss.hcache.get("url_search", default=[]))

        st.subheader("Search Results")

        # Create tabs for different views
        tab1, tab2, tab3 = st.tabs(["Results Summary", "Resource List", "Analytics"])

        with tab1:
            st.write(f"Total resources found: {len(df)}")
            st.write(
                f"Unique websites: {df['url'].apply(lambda x: x.split('/')[2]).nunique()}"
            )
            st.metric(
                "Average results per query",
                round(len(df) / len(ss.list_query_engine_search), 1),
            )

        with tab2:
            # Display cleaner results table with just title and URL
            clean_df = df[["title", "url"]].copy()
            clean_df.columns = ["Title", "Website URL"]
            st.dataframe(clean_df, use_container_width=True)

        with tab3:
            st.write("Top Websites Found")
            top_domains = (
                df["url"]
                .apply(lambda x: x.split("/")[2])
                .value_counts()
                .head(10)
                .reset_index()
            )
            top_domains.columns = ["Website", "Number of Resources"]
            st.bar_chart(top_domains.set_index("Website"))

        # Add download button for full url_search data as CSV
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Download results as spreadsheet (CSV)",
            data=csv,
            file_name="search_results.csv",
            mime="text/csv",
            help="Save these results to your computer",
        )

        # Button to continue
        st.markdown("---")
        st.write("When you're satisfied with these results, continue to the next step:")
        if st.button("Continue to Next Step", key="confirm_queries"):
            # update stage
            ss.stage_process = 2
            st.rerun()
