import streamlit as st
from modules.helper import get_spark_session, get_databases, get_tables, run_query

def render():
    st.header("Data Query Tool")
    
    with st.expander("About this tool", expanded=True):
        st.info("""
        This tool allows you to explore and query your data collections.
        1. Select a data source from the dropdown
        2. Select a data table
        3. Run pre-built queries or modify them as needed
        """)
    
    # Get Spark session (behind the scenes)
    spark = get_spark_session()
    
    # System health status (simplified)
    system_status = "Online" if not spark.sparkContext._jsc.sc().isStopped() else "Offline"
    st.sidebar.metric("System Status", system_status)
    
    # Reset option in sidebar
    with st.sidebar.expander("Advanced Options"):
        if st.button("Refresh System Connection"):
            spark.stop()
            new_spark = get_spark_session()
            st.success("System connection has been refreshed.")
    
    # Main interface
    st.subheader("Data Sources")
    
    # Get databases
    databases = get_databases(spark)
    
    if not databases:
        st.warning("No data sources are currently available.")
        return
    
    # Select database with user-friendly label
    selected_db = st.selectbox("Select a Data Source", databases)
    
    if selected_db:
        # Get tables
        tables = get_tables(spark, selected_db)
        
        if not tables:
            st.info(f"No data tables found in the '{selected_db}' data source.")
            return
        
        # Select table
        selected_table = st.selectbox("Select a Data Table", tables)
        
        if selected_table:
            st.subheader("Query Builder")
            
            # Pre-built queries
            query_type = st.radio(
                "Select Query Type",
                options=["View All Data", "Count Records", "Summary Statistics", "Custom Query"],
                horizontal=True
            )
            
            # Default query based on selection
            default_query = ""
            if query_type == "View All Data":
                default_query = f"SELECT * FROM {selected_db}.{selected_table} LIMIT 100"
            elif query_type == "Count Records":
                default_query = f"SELECT COUNT(*) AS Total_Records FROM {selected_db}.{selected_table}"
            elif query_type == "Summary Statistics":
                default_query = f"SELECT * FROM {selected_db}.{selected_table} LIMIT 1"
                # Get a sample row to identify columns
                sample = run_query(spark, default_query)
                if not sample.empty:
                    # Get numeric columns for statistics
                    numeric_cols = sample.select_dtypes(include=['number']).columns.tolist()
                    if numeric_cols:
                        stats_cols = ", ".join([f"AVG({col}) AS Avg_{col}, MAX({col}) AS Max_{col}, MIN({col}) AS Min_{col}" for col in numeric_cols])
                        default_query = f"SELECT {stats_cols} FROM {selected_db}.{selected_table}"
                    else:
                        default_query = f"SELECT COUNT(*) AS Total_Records FROM {selected_db}.{selected_table}"
            
            # Query editor (with friendly label)
            query = st.text_area(
                "Query Editor", 
                value=default_query,
                height=100,
                help="You can modify this query or write your own"
            )
            
            # Run query button
            if st.button("Run Query"):
                with st.spinner("Running query..."):
                    try:
                        result = run_query(spark, query)
                        
                        if result is not None and not result.empty:
                            st.success("Query completed successfully!")
                            
                            # Display results
                            st.subheader("Query Results")
                            st.dataframe(result, use_container_width=True)
                            
                            # Download option
                            csv = result.to_csv(index=False).encode('utf-8')
                            st.download_button(
                                label="Download Results as CSV",
                                data=csv,
                                file_name="query_results.csv",
                                mime='text/csv',
                            )
                        else:
                            st.info("The query returned no results.")
                    except Exception as e:
                        st.error(f"Error running query: {str(e)}")
                        st.info("Try a different query or check your query syntax.")
