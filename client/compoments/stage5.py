import streamlit as st
from streamlit import session_state as ss
import pandas as pd
import io

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Hota
from modules.cache import hcache
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

    if hcache.exists("columns_info"):
        # Create extraction fields summary
        columns_info = list(hcache.get("columns_info", default=[]))
        
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

            # Process button with better wording
            if st.button("Start Processing", key="process_data"):
                # Processing steps with progress indicators
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                try:
                    status_text.write("Initializing processing engine...")
                    progress_bar.progress(0.1)

                    # Initialize ManagerKey inside the UDF to prevent serialization of the lock
                    def get_key():
                        apiKeyMan = ManagerKey(cfg["gemini"]["api_key"])
                        return apiKeyMan.getKey()

                    get_key_udf = udf(get_key, StringType())
                    df_url_contents = df_url_contents.withColumn("apiKey", get_key_udf())
                    
                    status_text.write("Extracting information from content...")
                    progress_bar.progress(0.3)

                    # Process data
                    temp_cfg = cfg['langflow']["flow_json"]["process_data"]
                    
                    # Process in batches to show progress
                    total_records = df_url_contents.count()
                    progress_bar.progress(0.4)
                    status_text.write(f"Processing {total_records} documents...")
                    
                    rdd_processed = df_url_contents.rdd.map(lambda row: run_spark_flow(row.asDict(), dict_flowjson[temp_cfg["name"]], columns_info, cfg)).cache()
                    progress_bar.progress(0.8)
                    status_text.write("Finalizing processed data...")
                    
                    df_processed = rdd_processed.toDF(schema)
                    progress_bar.progress(0.9)

                    # Save to HDFS
                    df_processed.write.mode("overwrite").parquet(cfg["hdfs"]["data_processed"])
                    progress_bar.progress(1.0)
                    status_text.success("Processing completed successfully!")
                    
                    # Prompt to view results
                    st.info("Scroll down to view and download the processed results")
                    
                except Exception as e:
                    progress_bar.progress(1.0)
                    status_text.error("Processing encountered an error")
                    st.error(f"There was a problem during processing: {str(e)}")
                    st.info("Please try again or contact support if the problem persists.")

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
                        st.metric("Fields Extracted", len(df_hdfs.columns) - 4)  # Subtract base fields
                    with col3:
                        success_rate = f"{round(processed_count / record_count * 100, 1)}%" if record_count > 0 else "N/A"
                        st.metric("Success Rate", success_rate)
                    
                    # Create tabs for different views
                    tab1, tab2 = st.tabs(["Results Preview", "Download Options"])
                    
                    with tab1:
                        # Get columns to display (prioritize the extracted fields)
                        display_columns = ["title", "url"] + [col["name"] for col in columns_info]
                        available_columns = [col for col in display_columns if col in df_hdfs.columns]
                        
                        # Convert to pandas for better display
                        preview_df = df_hdfs.select(available_columns).limit(10).toPandas()
                        
                        # Format column names for display
                        preview_df.columns = [col.replace("_", " ").title() for col in preview_df.columns]
                        
                        # Display the data
                        st.dataframe(preview_df, use_container_width=True)
                    
                    with tab2:
                        st.write("Download your processed data:")
                        
                        # Options for download
                        download_format = st.radio(
                            "Download Format", 
                            options=["CSV (Spreadsheet)", "Excel"],
                            horizontal=True
                        )
                        
                        # Download limitations
                        max_rows = st.slider(
                            "Maximum rows to download", 
                            min_value=100, 
                            max_value=10000, 
                            value=1000, 
                            step=100,
                            help="Larger datasets may take longer to prepare for download"
                        )
                        
                        # Prepare download data based on selection
                        download_df = df_hdfs.limit(max_rows).toPandas()
                        
                        if download_format == "CSV (Spreadsheet)":
                            csv = download_df.to_csv(index=False).encode('utf-8')
                            st.download_button(
                                label="Download Data as CSV",
                                data=csv,
                                file_name='processed_data.csv',
                                mime='text/csv',
                            )
                        else:
                            # Excel format
                            buffer = io.BytesIO()
                            with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                                download_df.to_excel(writer, sheet_name='Processed Data', index=False)
                            
                            excel_data = buffer.getvalue()
                            st.download_button(
                                label="Download Data as Excel",
                                data=excel_data,
                                file_name="processed_data.xlsx",
                                mime="application/vnd.ms-excel"
                            )
                    
                    # Continue button
                    st.markdown("---")
                    st.write("When you're satisfied with the processed data, continue to the next step:")
                    if st.button("Continue to Next Step", key="confirm_processed_data"):
                        ss.stage_process = 5
                        st.rerun()
            
            except Exception as e:
                # Only show this if processing has been attempted
                if st.session_state.get('_process_attempted'):
                    st.warning("Processed data not found. Please run the processing step first.")
                pass
                
        except Exception as e:
            st.error("Unable to access content data. Please complete the previous steps first.")
            st.button("Go Back to Previous Step", on_click=lambda: setattr(ss, 'stage_process', 3))
    else:
        st.warning("Information extraction fields have not been defined.")
        st.info("Please go back to Step 4 to define what information you want to extract.")
        st.button("Go Back to Step 4", on_click=lambda: setattr(ss, 'stage_process', 3))