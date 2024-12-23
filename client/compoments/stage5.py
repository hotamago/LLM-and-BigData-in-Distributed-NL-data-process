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
from modules.spark import *

def render():
    st.write("Stage 5: Process data with Spark")
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
        st.write("Loaded URL contents from HDFS. Sample:")
        st.write(df_url_contents.limit(5).toPandas())

        if st.button("Process", key="process_data"):
            try:
                st.write("Running Spark flow to process data...")

                # Process data
                rdd_processed = df_url_contents.rdd.map(lambda row: run_spark_flow(row.asDict(), columns_info, cfg)).cache()
                df_processed = rdd_processed.toDF(schema)

                # Show output sample
                st.write("Processed data sample:")
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
            # Button to confirm the queries
            if st.button("Confirm processed data", key="confirm_processed_data"):
                ss.stage_process = 5
                st.rerun()
        except Exception as e:
            # st.warning(f"Data not found or read error: {e}")
            pass
    else:
        st.warning("Columns info not found. Please go back to Stage 4.")