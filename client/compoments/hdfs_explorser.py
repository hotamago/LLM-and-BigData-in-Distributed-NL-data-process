import streamlit as st
from streamlit import session_state as ss
import pandas as pd
import os

# Hota
from modules.cache import hcache
from modules.helper import *

# Spark imports
import models
from config import cfg

# Spark plugin imports
from modules.spark import *

def render():
    st.header("Data Storage Explorer")
    
    with st.expander("About this tool", expanded=True):
        st.info("""
        This tool allows you to explore data that has been collected and stored.
        Select a data collection from the dropdown menu to view its contents.
        You can preview the data, see statistics, and download the data as needed.
        """)
    
    # Get Spark session (hidden from user)
    spark = get_spark_session()

    # Get list of available data collections
    list_collections = list(cfg['hdfs'].keys())
    
    if not list_collections:
        st.warning("No data collections are currently available.")
        return
    
    # Collection selection with more user-friendly label
    collection = st.selectbox('Select Data Collection', list_collections, index=0)
    
    try:
        # Load the data
        df = spark.read.parquet(cfg['hdfs'][collection])
        
        # Create tabs for different views
        tab1, tab2, tab3 = st.tabs(["Preview", "Statistics", "Download"])
        
        with tab1:
            st.subheader("Data Preview")
            
            # Sample size selection
            sample_size = st.slider("Number of rows to preview", 5, 50, 10)
            
            # Convert to pandas for display
            pdf = df.limit(sample_size).toPandas()
            
            # Clean column names for display
            pdf.columns = [col.replace("_", " ").title() for col in pdf.columns]
            
            # Display the data
            st.dataframe(pdf, use_container_width=True)
            
            # Refresh button
            if st.button('Refresh Data'):
                df = spark.read.parquet(cfg['hdfs'][collection])
                st.success('Data refreshed successfully')
                st.rerun()
        
        with tab2:
            st.subheader("Data Statistics")
            
            # Record count
            st.metric("Total Records", df.count())
            
            # Try to get numeric columns for statistics
            try:
                numeric_stats = df.select([col for col in df.columns 
                                          if df.schema[col].dataType.simpleString() 
                                          in ('int', 'double', 'float', 'long')])
                                          
                if numeric_stats.columns:
                    st.write("Numeric Column Statistics")
                    stats_df = numeric_stats.describe().toPandas()
                    st.dataframe(stats_df, use_container_width=True)
                else:
                    st.write("No numeric columns found for statistics")
                
            except Exception as e:
                st.write("Could not calculate numeric statistics")
            
            # Column information
            st.write("Data Structure")
            columns = []
            for field in df.schema.fields:
                columns.append({
                    "Column Name": field.name,
                    "Data Type": field.dataType.simpleString()
                })
            st.dataframe(pd.DataFrame(columns), use_container_width=True)
        
        with tab3:
            st.subheader("Download Options")
            
            # Limit rows for download to prevent huge files
            max_download = st.number_input(
                "Maximum rows to download", 
                min_value=10, 
                value=1000,
                help="Larger values may take longer to process"
            )
            
            # Format selection
            download_format = st.radio(
                "Download Format",
                options=["CSV (Spreadsheet)", "JSON"],
                horizontal=True
            )
            
            if download_format == "CSV (Spreadsheet)":
                # CSV download
                csv = df.limit(max_download).toPandas().to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="Download Data as CSV",
                    data=csv,
                    file_name=f"{collection}.csv",
                    mime='text/csv',
                )
            else:
                # JSON download
                json_data = df.limit(max_download).toPandas().to_json(orient="records").encode('utf-8')
                st.download_button(
                    label="Download Data as JSON",
                    data=json_data,
                    file_name=f"{collection}.json",
                    mime='application/json',
                )
    
    except Exception as e:
        st.error(f"Could not load the selected data collection. It may be empty or in an unsupported format.")
        st.info("Try selecting a different data collection or running the data collection process again.")