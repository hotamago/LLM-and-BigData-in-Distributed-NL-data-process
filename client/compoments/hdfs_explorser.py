import streamlit as st
from streamlit import session_state as ss
import pandas as pd
import os  # Add this import

# Hota
from modules.cache import hcache
from modules.helper import *

# Spark imports
import models
from config import cfg

# Spark plugin imports
from modules.spark import *

# Directory navigation
def render():
    spark = get_spark_session()

    # UI config
    st.title('HDFS Explorer Enhanced')

    # Get list of hdfs
    list_hdfs = list(cfg['hdfs'].keys())
    hdfs = st.selectbox('Select HDFS Path', list_hdfs, index=0)

    st.subheader('Data Preview')
    df = spark.read.parquet(cfg['hdfs'][hdfs])
    st.dataframe(df.head(10))

    st.subheader('Statistics')
    st.write(df.describe().toPandas())

    st.subheader('Schema')
    st.write(df.printSchema())

    st.subheader('Operations')
    if st.button('Refresh Data'):
        df = spark.read.parquet(cfg['hdfs'][hdfs])
        st.success('Data refreshed successfully')

    st.write('Download Data')
    csv = df.toPandas().to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name=f"{hdfs}.csv",
        mime='text/csv',
    )