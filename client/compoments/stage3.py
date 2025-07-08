import streamlit as st
from streamlit import session_state as ss
import pandas as pd

# Hota
# from modules.cache import hcache
from modules.helper import *

# Spark imports
import models
from config import cfg

# Spark plugin imports
from modules.spark import fetch_content


def render():
    # Get url search
    url_search = ss.hcache.get("url_search", default=[])

    # UI config
    st.header("Step 3: Collect Content from Resources")

    with st.expander("How this works", expanded=True):
        st.info("""
        1. This step collects the actual content from the web resources found in the previous step
        2. Click 'Collect Content' to begin the process
        3. When complete, you can review the collected information
        4. Continue to the next step when satisfied
        """)

    # Show resource summary
    if url_search:
        st.subheader("Resource Summary")
        st.write(f"Ready to collect content from {len(url_search)} web resources")

    # Hidden technical configuration for backend
    # These are hidden from the display since they're technical details
    spark_master = cfg["spark"]["master"]
    spark_app_name = cfg["spark"]["app_name"]
    hdfs_output_path = cfg["hdfs"]["url_contents"]

    # Initialize SparkSession
    spark = get_spark_session()

    # Define the schema
    schema = models.url_content_collect()

    # Button to collect content
    content_button = st.button(
        "Collect Content",
        key="get_content",
        help="Start gathering content from the web resources",
    )

    # Progress components
    progress_container = st.empty()
    status_text = st.empty()

    if content_button:
        # Display progress info
        progress = progress_container.progress(0)
        status_text.write("Initializing content collection...")

        try:
            # The url_search list contains dictionaries with "url", "title", "snippet"
            sc = spark.sparkContext

            # Get total number of URLs for progress tracking
            total_urls = len(url_search)

            status_text.write(f"Collecting content from {total_urls} resources...")

            # Parallelize the list
            rdd = sc.parallelize(url_search)

            # Map each record to include fetched content
            rdd_content = rdd.map(fetch_content)

            # Progress update
            progress.progress(0.25)
            status_text.write("Processing collected content...")

            # Convert RDD to DataFrame with explicit schema
            df_content = rdd_content.toDF(schema)

            # Progress update
            progress.progress(0.5)

            # Filter out records with status_code != 200
            df_content = df_content.filter(df_content["status_code"] == 200)

            # Progress update
            progress.progress(0.75)
            status_text.write("Saving data...")

            # Write the DataFrame to HDFS in Parquet format
            df_content.write.mode("overwrite").parquet(hdfs_output_path)

            # Complete the progress bar
            progress.progress(1.0)
            status_text.success("Content collection completed successfully!")

        except Exception as e:
            status_text.error(f"An error occurred: {str(e)}")
            # Display a more user-friendly error message
            st.error(
                "We encountered a problem while collecting content. Please try again or contact support."
            )

    # If data exists in HDFS, show summary and preview
    try:
        # Check if data exists
        df_hdfs = spark.read.parquet(hdfs_output_path)
        rows_count = df_hdfs.count()

        if rows_count > 0:
            st.subheader("Collected Content Summary")

            # Create metrics for the content
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Resources", rows_count)
            with col2:
                # Calculate average content length
                avg_length = (
                    df_hdfs.select("content")
                    .rdd.map(lambda x: len(x[0]) if x[0] else 0)
                    .mean()
                )
                st.metric("Avg Content Length", f"{int(avg_length)} chars")
            with col3:
                # Count successful vs failed
                success_count = df_hdfs.filter(df_hdfs["status_code"] == 200).count()
                st.metric("Success Rate", f"{int(success_count / rows_count * 100)}%")

            # Display tabs for different views
            tab1, tab2 = st.tabs(["Content Preview", "Download Options"])

            with tab1:
                # Convert to pandas for display - limiting columns for clarity
                preview_df = (
                    df_hdfs.select("title", "url", "status_code").limit(10).toPandas()
                )
                preview_df.columns = ["Title", "Source URL", "Status"]
                st.dataframe(preview_df, use_container_width=True)

                # Show an example of full content
                with st.expander("Example Content"):
                    sample = df_hdfs.limit(1).collect()[0]
                    st.write("**Title:**", sample["title"])
                    st.write("**Source:**", sample["url"])
                    st.write("**Content:**")
                    st.write(
                        sample["content"][:500] + "..."
                        if len(sample["content"]) > 500
                        else sample["content"]
                    )

            with tab2:
                # Add download button for full data as CSV
                st.write("Download options for the collected content:")
                csv = (
                    df_hdfs.select("title", "url", "content", "status_code")
                    .toPandas()
                    .to_csv(index=False)
                    .encode("utf-8")
                )
                st.download_button(
                    label="Download content as spreadsheet (CSV)",
                    data=csv,
                    file_name="collected_content.csv",
                    mime="text/csv",
                    help="Save the collected content to your computer",
                )

            # Continue button
            st.markdown("---")
            st.write(
                "When you're satisfied with the collected content, continue to the next step:"
            )
            if st.button("Continue to Next Step", key="confirm_content"):
                ss.stage_process = 3
                st.rerun()
    except Exception as e:
        # Silently handle the case where data doesn't exist yet
        pass
