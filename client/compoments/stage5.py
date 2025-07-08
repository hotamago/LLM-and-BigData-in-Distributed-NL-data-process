import streamlit as st
from streamlit import session_state as ss
import pandas as pd
import io

from pyspark.sql.functions import udf, lit
from pyspark.sql.types import StringType

# Hota
# from modules.cache import ss.hcache
from modules.helper import *

# Spark imports
import models
from config import cfg, dict_flowjson

# Spark plugin imports
from modules.spark import *
from modules.managerKey import ManagerKey


def render():
    st.header("Step 5: Process and Extract Information")

    with st.expander("How this works", expanded=True):
        st.info("""
        1. This step automatically extracts the information you defined in the previous step
        2. Click 'Start Processing' to begin analyzing the collected content
        3. The system will extract and organize the information based on your requirements
        4. When complete, you can review the results and download the processed data
        """)

    # Get Spark session
    spark = get_spark_session()

    if ss.hcache.exists("columns_info"):
        # Create extraction fields summary
        columns_info = list(ss.hcache.get("columns_info", default=[]))

        # Display extraction plan summary
        st.subheader("Information Extraction Plan")

        # Show the fields in a nice table
        if columns_info:
            fields_df = pd.DataFrame(columns_info)
            fields_df.columns = ["Field Name", "Description"]
            st.table(fields_df)

        # Create schema (hidden technical details)
        schema = models.llm_process_content()
        from pyspark.sql.types import StructField, StringType

        # Collect names already in the schema to avoid duplicates
        existing_cols = {field.name for field in schema.fields}
        for col in columns_info:
            if col["name"] not in existing_cols:
                schema.add(StructField(col["name"], StringType(), nullable=True))
                existing_cols.add(col["name"])

        # Load content data (show simple status rather than raw data)
        try:
            df_url_contents = spark.read.parquet(cfg["hdfs"]["url_contents"])
            record_count = df_url_contents.count()

            # Display content summary
            st.subheader("Content Ready for Processing")

            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Documents", record_count)
            with col2:
                st.metric("Fields to Extract", len(columns_info))

            # Sample preview in expandable section
            with st.expander("Preview Sample Content", expanded=False):
                sample_df = df_url_contents.select("title", "url").limit(5).toPandas()
                sample_df.columns = ["Title", "URL"]
                st.dataframe(sample_df, use_container_width=True)

            # Batch size configuration
            st.subheader("Processing Configuration")

            # Calculate recommended batch size
            recommended_batch_size = min(50, max(10, record_count // 10))

            col1, col2 = st.columns([2, 1])
            with col1:
                batch_size = st.number_input(
                    "Batch Size",
                    min_value=1,
                    max_value=200,
                    value=recommended_batch_size,
                    step=5,
                    help="Number of documents to process in each batch. Smaller batches use less memory but take longer. Larger batches are faster but use more memory.",
                )
            with col2:
                st.info(f"Recommended: {recommended_batch_size}")

            # Show batch calculation
            num_batches = (record_count + batch_size - 1) // batch_size
            st.caption(
                f"With {record_count} documents, this will create {num_batches} batches of ~{batch_size} documents each."
            )

            # Process button with better wording
            if st.button("Start Processing", key="process_data"):
                # Processing steps with progress indicators
                progress_bar = st.progress(0)
                status_text = st.empty()

                try:
                    status_text.write("Initializing processing engine...")
                    progress_bar.progress(0.1)

                    status_text.write("Setting up data processing...")
                    progress_bar.progress(0.2)

                    # Get processing configuration
                    temp_cfg = cfg["langflow"]["flow_json"]["process_data"]
                    total_records = df_url_contents.count()

                    status_text.write(
                        f"Processing {total_records} documents using optimized streaming approach..."
                    )
                    progress_bar.progress(0.3)

                    # Create a more efficient processing approach using DataFrame operations
                    # Add row numbers for batch processing
                    from pyspark.sql.window import Window
                    from pyspark.sql.functions import row_number, col, lit

                    window = Window.orderBy(lit(1))  # Simple ordering
                    df_with_row_numbers = df_url_contents.withColumn(
                        "row_num", row_number().over(window)
                    )

                    # Use user-configured batch size
                    num_batches = (total_records + batch_size - 1) // batch_size

                    status_text.write(
                        f"Processing in {num_batches} batches of ~{batch_size} records each..."
                    )
                    progress_bar.progress(0.4)

                    # Process batches and save each batch immediately to avoid memory accumulation
                    successful_batches = 0

                    for batch_num in range(num_batches):
                        batch_progress = 0.4 + (batch_num / num_batches) * 0.5
                        progress_bar.progress(batch_progress)

                        start_row = batch_num * batch_size + 1
                        end_row = min((batch_num + 1) * batch_size, total_records)

                        status_text.write(
                            f"Processing and saving batch {batch_num + 1}/{num_batches} (rows {start_row}-{end_row})..."
                        )

                        # Get batch using DataFrame filtering (more efficient than RDD operations)
                        batch_df = df_with_row_numbers.filter(
                            (col("row_num") >= start_row) & (col("row_num") <= end_row)
                        ).drop("row_num")

                        # Add API key efficiently
                        def get_api_key():
                            apiKeyMan = ManagerKey(cfg["openrouter"]["api_key"])
                            return apiKeyMan.getKey()

                        api_key = get_api_key()
                        batch_df = batch_df.withColumn("apiKey", lit(api_key))

                        # Process batch using mapPartitions for better memory efficiency
                        def process_partition(iterator):
                            results = []
                            for row in iterator:
                                try:
                                    result = run_spark_flow(
                                        row.asDict(),
                                        dict_flowjson[temp_cfg["name"]],
                                        columns_info,
                                        cfg,
                                    )
                                    results.append(result)
                                except Exception as e:
                                    # Log error but continue processing
                                    print(f"Error processing row: {e}")
                                    continue
                            return iter(results)

                        # Convert to RDD, process, and convert back to DataFrame
                        batch_rdd = batch_df.rdd.mapPartitions(process_partition)

                        # Create DataFrame from processed results
                        try:
                            batch_processed_df = batch_rdd.toDF(schema)

                            # Optimize batch DataFrame before writing
                            batch_processed_df = batch_processed_df.coalesce(1)

                            # Write batch immediately to HDFS
                            write_mode = "overwrite" if batch_num == 0 else "append"
                            batch_processed_df.write.mode(write_mode).option(
                                "maxRecordsPerFile", "5000"
                            ).option("compression", "snappy").parquet(
                                cfg["hdfs"]["data_processed"]
                            )

                            successful_batches += 1

                        except Exception as batch_error:
                            st.warning(f"Batch {batch_num + 1} failed: {batch_error}")
                            continue

                        # Force cleanup between batches to free memory
                        if batch_num % 2 == 0:  # Every 2 batches
                            spark.catalog.clearCache()
                            import gc

                            gc.collect()

                    progress_bar.progress(0.95)
                    status_text.write("Finalizing data storage...")

                    if successful_batches > 0:
                        progress_bar.progress(1.0)
                        status_text.success(
                            f"Processing completed successfully! Processed and saved {successful_batches} batches."
                        )

                        # Clear any remaining cache
                        spark.catalog.clearCache()

                        # Prompt to view results
                        st.info(
                            "Scroll down to view and download the processed results"
                        )

                    else:
                        progress_bar.progress(1.0)
                        status_text.error("No batches were processed successfully")
                        st.error(
                            "Processing failed for all batches. Please check your data and configuration."
                        )

                except Exception as e:
                    progress_bar.progress(1.0)
                    status_text.error("Processing encountered an error")
                    st.error(f"There was a problem during processing: {str(e)}")

                    # Enhanced troubleshooting information
                    with st.expander("Detailed Error Information & Solutions"):
                        st.error(f"**Error Details:** {str(e)}")
                        st.write("""
                        **Memory-Related Solutions Applied:**
                        ✅ Increased Spark worker memory to 3GB each
                        ✅ Optimized Spark configurations for memory efficiency
                        ✅ Implemented streaming batch processing
                        ✅ Removed memory-intensive cache() operations
                        ✅ Added automatic garbage collection
                        
                        **If the error persists, try:**
                        - Restart the Docker containers to apply memory changes
                        - Reduce the dataset size for testing
                        - Check Docker Desktop memory allocation
                        """)

            # Show processed data results if available
            try:
                df_hdfs = spark.read.parquet(cfg["hdfs"]["data_processed"])
                processed_count = df_hdfs.count()

                if processed_count > 0:
                    st.subheader("Processed Results")

                    # Summary metrics
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Documents Processed", processed_count)
                    with col2:
                        st.metric(
                            "Fields Extracted", len(df_hdfs.columns) - 4
                        )  # Subtract base fields
                    with col3:
                        success_rate = (
                            f"{round(processed_count / record_count * 100, 1)}%"
                            if record_count > 0
                            else "N/A"
                        )
                        st.metric("Success Rate", success_rate)

                    # Create tabs for different views
                    tab1, tab2 = st.tabs(["Results Preview", "Download Options"])

                    with tab1:
                        # Get columns to display (prioritize the extracted fields)
                        display_columns = ["title", "url"] + [
                            col["name"] for col in columns_info
                        ]
                        available_columns = [
                            col for col in display_columns if col in df_hdfs.columns
                        ]

                        # Convert to pandas for better display
                        preview_df = (
                            df_hdfs.select(available_columns).limit(10).toPandas()
                        )

                        # Format column names for display
                        preview_df.columns = [
                            col.replace("_", " ").title() for col in preview_df.columns
                        ]

                        # Display the data
                        st.dataframe(preview_df, use_container_width=True)

                    with tab2:
                        st.write("Download your processed data:")

                        # Options for download
                        download_format = st.radio(
                            "Download Format",
                            options=["CSV (Spreadsheet)", "Excel"],
                            horizontal=True,
                        )

                        # Download limitations
                        max_rows = st.slider(
                            "Maximum rows to download",
                            min_value=100,
                            max_value=100000,
                            value=1000,
                            step=100,
                            help="Larger datasets may take longer to prepare for download",
                        )

                        # Prepare download data based on selection
                        download_df = df_hdfs.limit(max_rows).toPandas()

                        if download_format == "CSV (Spreadsheet)":
                            csv = download_df.to_csv(index=False).encode("utf-8")
                            st.download_button(
                                label="Download Data as CSV",
                                data=csv,
                                file_name="processed_data.csv",
                                mime="text/csv",
                            )
                        else:
                            # Excel format
                            buffer = io.BytesIO()
                            with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
                                download_df.to_excel(
                                    writer, sheet_name="Processed Data", index=False
                                )

                            excel_data = buffer.getvalue()
                            st.download_button(
                                label="Download Data as Excel",
                                data=excel_data,
                                file_name="processed_data.xlsx",
                                mime="application/vnd.ms-excel",
                            )

                    # Continue button
                    st.markdown("---")
                    st.write(
                        "When you're satisfied with the processed data, continue to the next step:"
                    )
                    if st.button("Continue to Next Step", key="confirm_processed_data"):
                        ss.stage_process = 5
                        st.rerun()

            except Exception as e:
                # Only show this if processing has been attempted
                if st.session_state.get("_process_attempted"):
                    st.warning(
                        "Processed data not found. Please run the processing step first."
                    )
                pass

        except Exception as e:
            st.error(
                "Unable to access content data. Please complete the previous steps first."
            )
            st.button(
                "Go Back to Previous Step",
                on_click=lambda: setattr(ss, "stage_process", 3),
            )
    else:
        st.warning("Information extraction fields have not been defined.")
        st.info(
            "Please go back to Step 4 to define what information you want to extract."
        )
        st.button("Go Back to Step 4", on_click=lambda: setattr(ss, "stage_process", 3))
