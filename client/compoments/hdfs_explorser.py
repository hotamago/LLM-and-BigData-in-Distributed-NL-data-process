import streamlit as st
from streamlit import session_state as ss
import pandas as pd
from pathlib import Path  # Change to import Hadoop Path below

# Hota
from modules.cache import hcache
from modules.helper import *

# Spark imports
import models
from config import cfg

# Spark plugin imports
from modules.spark import *

def render():
    st.header("HDFS Explorer Enhanced")
    spark = get_spark_session()
    
    # Directory navigation
    if 'current_path' not in ss:
        ss.current_path = cfg["hdfs"]["base_path"]  # Ensure correct config key
    
    current_path = ss.current_path
    
    def list_files(path):
        # Function to list files and directories
        try:
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark._jsc.hadoopConfiguration()
            )
            hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
            files = fs.listStatus(hadoop_path)
            folders = [f.getPath().toString() for f in files if f.isDirectory()]
            files = [f.getPath().toString() for f in files if f.isFile()]
            return folders, files
        except Exception as e:
            st.error(f"Error listing files: {e}")
            return [], []
    
    folders, files = list_files(current_path)
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        selected_folder = st.selectbox("Select Folder", [".."] + folders, index=0)
        if selected_folder:
            if selected_folder == "..":
                parent = Path(current_path).parent
                ss.current_path = str(parent)
            else:
                ss.current_path = selected_folder
    
    with col2:
        st.write(f"Current Path: {ss.current_path}")
    
    st.write("Files:")
    for file in files:
        st.write(f"- {Path(file).name}")
        if st.button(f"Download {Path(file).name}", key=f"download_{file}"):
            try:
                file_content = spark.read.parquet(file).toPandas()
                st.download_button(
                    label="Download CSV",
                    data=file_content.to_csv(index=False).encode('utf-8'),
                    file_name=f"{Path(file).stem}.csv",
                    mime='text/csv',
                )
            except Exception as e:
                st.error(f"Error downloading file: {e}")
    
    # File upload functionality
    st.write("Upload a new file to HDFS:")
    uploaded_file = st.file_uploader("Choose a Parquet file", type=["parquet"])
    if uploaded_file:
        save_path = f"{ss.current_path}/{uploaded_file.name}"
        try:
            df = pd.read_parquet(uploaded_file)
            df_spark = spark.createDataFrame(df)
            df_spark.write.mode("overwrite").parquet(save_path)
            st.success(f"File uploaded to {save_path}")
        except Exception as e:
            st.error(f"Error uploading file: {e}")