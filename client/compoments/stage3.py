import streamlit as st
from streamlit import session_state as ss
import pandas as pd

# Hota
from modules.cache import hcache
from modules.helper import *

# Spark imports
import models
from config import cfg

# Spark plugin imports
from modules.spark import fetch_content

def render():
    # Get url search
    url_search = hcache.get('url_search', default=[])

    # UI config
    st.write('Stage 3')
    # Show results as table using pandas
    df = pd.DataFrame(url_search)
    st.write(df)

    # Read Spark and HDFS configs from cfg
    spark_master = cfg["spark"]["master"]
    spark_app_name = cfg["spark"]["app_name"]
    hdfs_output_path = cfg["hdfs"]["url_contents"]

    # Initialize SparkSession with the specified Python executable
    spark = get_spark_session()

    # Define the schema
    schema = models.url_content_collect

    # Button to confirm to get content
    if st.button('Get content', key='get_content'):
        # The url_search list contains dictionaries with "url", "title", "snippet"
        # We'll parallelize the entire list to keep all fields together
        sc = spark.sparkContext
        rdd = sc.parallelize(url_search)

        # Map each record to include fetched content
        rdd_content = rdd.map(fetch_content)

        try:
            # Convert RDD to DataFrame with explicit schema
            df_content = rdd_content.toDF(schema)

            # Filter out records with status_code != 200
            df_content = df_content.filter(df_content["status_code"] == 200)

            # Show some sample content (may be large, so limit the snippet)
            st.write("Sample data from Spark DataFrame:")
            sample_data = df_content.limit(5).collect()
            temp_table = []
            for row in sample_data:
                temp_table.append({
                    "url": row["url"],
                    "title": row["title"],
                    "snippet": row["snippet"],
                    "status_code": row["status_code"],
                    "content_snippet": (row["content"][:200] if row["content"] else None)
                })
            temp_table = pd.DataFrame(temp_table)
            st.write(temp_table)

            # Write the DataFrame to HDFS in Parquet format
            df_content.write.mode("overwrite").parquet(hdfs_output_path)

            st.success(f"Data successfully written to HDFS at {hdfs_output_path}")

            # Optionally, move to next stage
            # ss.stage_process += 1
            # st.rerun()
        except Exception as e:
            st.error(f"An error occurred while processing the data: {e}")
            # Optionally, log the error
            print(f"Error during Spark processing: {e}")
    
    # Check if exists data in hdfs_output_path using hadoop then show button to confirm
    try:
        spark.read.parquet(hdfs_output_path)
        st.success("Data found in the specified HDFS path.")
        # Show example of data
        df_hdfs = spark.read.parquet(hdfs_output_path)
        st.write("Showing top 10 rows of data:")
        st.write(df_hdfs.limit(10).toPandas())
        # Button to confirm the queries
        if st.button("Confirm content data"):
            ss.stage_process = 3
            st.rerun()
    except Exception as e:
        # st.warning(f"Data not found or read error: {e}")
        pass