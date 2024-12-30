import streamlit as st
from modules.helper import get_spark_session, get_databases, get_tables, run_query

def render():
    st.header("Spark Explorer")
    
    spark = get_spark_session()
    st.write(f"Spark Master: {spark.sparkContext.master}")
    st.write(f"Spark App Name: {spark.sparkContext.appName}")
    
    if st.button("Reset Spark Session"):
        spark.stop()
        new_spark = get_spark_session()
        st.success("Spark session has been reset.")
        st.write(f"New Spark Master: {new_spark.sparkContext.master}")
        st.write(f"New Spark App Name: {new_spark.sparkContext.appName}")
    
    st.subheader("Databases")
    databases = get_databases(spark)
    selected_db = st.selectbox("Select Database", databases)
    
    if selected_db:
        st.subheader("Tables")
        tables = get_tables(spark, selected_db)
        selected_table = st.selectbox("Select Table", tables)
        
        if selected_table:
            query = st.text_area("Enter Spark SQL Query", value=f"SELECT * FROM {selected_db}.{selected_table} LIMIT 10")
            if st.button("Run Query"):
                result = run_query(spark, query)
                st.write(result)
