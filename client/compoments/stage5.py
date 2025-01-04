import streamlit as st
from streamlit import session_state as ss
import pandas as pd

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
    spark = get_spark_session()

    if hcache.exists("columns_info"):
        # Copy from models.llm_process_content
        schema = models.llm_process_content
        # Add fields to schema from columns_info
        columns_info = list(hcache.get("columns_info", default=[]))
        from pyspark.sql.types import StructField, StringType
        
        # Collect names already in the schema to avoid duplicates
        existing_cols = {field.name for field in schema.fields}
        for col in columns_info:
            if col["name"] not in existing_cols:
                schema.add(StructField(col["name"], StringType(), nullable=True))
                existing_cols.add(col["name"])
        st.write(schema)

        df_url_contents = spark.read.parquet(cfg["hdfs"]["url_contents"])
        st.write("Loaded URL contents from HDFS. 5 Sample:")
        st.write(df_url_contents.limit(5).toPandas())
        
        # Show number of records in df_url_contents
        st.write(f"Number of records in URL contents: {df_url_contents.count()}")

        if st.button("Process", key="process_data"):
            try:
                st.write("Running Spark flow to process data...")

                # Initialize ManagerKey inside the UDF to prevent serialization of the lock
                def get_key():
                    apiKeyMan = ManagerKey(cfg["gemini"]["api_key"])
                    return apiKeyMan.getKey()

                get_key_udf = udf(get_key, StringType())
                df_url_contents = df_url_contents.withColumn("apiKey", get_key_udf())

                # Process data
                temp_cfg = cfg['langflow']["flow_json"]["process_data"]
                rdd_processed = df_url_contents.rdd.map(lambda row: run_spark_flow(row.asDict(), dict_flowjson[temp_cfg["name"]], columns_info, cfg)).cache()
                df_processed = rdd_processed.toDF(schema)

                # Show output sample
                st.write("Processed data 5 sample:")
                st.write(df_processed.limit(5).toPandas())

                # Save to HDFS
                df_processed.write.mode("overwrite").parquet(cfg["hdfs"]["data_processed"])
                st.success(f"Data successfully written to HDFS at {cfg['hdfs']['data_processed']}")
            except Exception as e:
                st.error(f"Error in stage 5 processing: {e}")

        # Check if exists data in hdfs_output_path using hadoop then show button to confirm
        try:
            spark.read.parquet(cfg["hdfs"]["data_processed"])
            st.success("Data found in the specified HDFS path.")
            # Show example of data
            df_hdfs = spark.read.parquet(cfg["hdfs"]["data_processed"])
            st.write("Showing top 10 rows of data:")
            st.write(df_hdfs.limit(10).toPandas())
            
            # Show number of records in df_hdfs
            st.write(f"Number of records in processed data: {df_hdfs.count()}")
            
            # Add download button for full data as CSV
            csv = df_hdfs.toPandas().to_csv(index=False)
            st.download_button(
                label="Download full data as CSV",
                data=csv,
                file_name='processed_data.csv',
                mime='text/csv',
            )
            
            # Button to confirm the queries
            if st.button("Confirm processed data", key="confirm_processed_data"):
                ss.stage_process = 5
                st.rerun()
        except Exception as e:
            # st.warning(f"Data not found or read error: {e}")
            pass
    else:
        st.warning("Columns info not found. Please go back to Stage 4.")