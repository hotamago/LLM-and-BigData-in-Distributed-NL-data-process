import streamlit as st
from streamlit import session_state as ss
import pandas as pd
from streamlit_ace import st_ace
import json
import traceback

# Hota
from modules.get_block_code import convert_block_to_text
from modules.cache import hcache
from modules.helper import *

# Spark imports
import models
from config import cfg

# Spark plugin imports
from modules.spark import *

def render():
    st.header("Step 6: Final Data Processing")
    
    with st.expander("How this works", expanded=True):
        st.info("""
        1. This is the final step where you can apply additional processing to your data
        2. Describe what you want to do with the extracted information
        3. The system will generate and run a processing script based on your instructions
        4. You can review and download the final results
        """)
    
    # Show what data fields are available
    if hcache.exists("columns_info"):
        st.subheader("Available Data Fields")
        
        # Display columns in a user-friendly table
        columns_info = list(hcache.get("columns_info", default=[]))
        if columns_info:
            col_df = pd.DataFrame(columns_info)
            col_df.columns = ["Field Name", "Description"]
            st.table(col_df)
    
    # Examples of processing instructions
    with st.expander("Examples of processing instructions"):
        st.markdown("""
        **Example 1:** Filter the data to only include entries where the sentiment is positive and create a new field that combines the title and main topic.
        
        **Example 2:** Calculate the average sentiment score for each author and sort the results by this average.
        
        **Example 3:** Group the data by publication date and count how many articles were published each day.
        """)
    
    # Input for processing instructions
    st.subheader("Your Processing Instructions")
    user_input_stage_6 = st.text_area(
        "What would you like to do with the extracted data?",
        value=hcache.get("user_input_stage_6", default=""),
        height=120,
        placeholder="Describe what processing you want to apply to the data...",
        help="For example: filter by specific values, create new calculated fields, summarize by groups, etc."
    )
    
    # Cache user input
    hcache.set("user_input_stage_6", user_input_stage_6)

    # Generate script button
    if st.button("Generate Processing Script"):
        with st.spinner("Creating your processing script..."):
            # Show progress
            progress = st.progress(0)
            status = st.empty()
            
            status.write("Analyzing your instructions...")
            progress.progress(0.3)
            
            columns_info = hcache.get("columns_info", [])
            
            progress.progress(0.6)
            status.write("Generating script...")
            
            script_result = gen_python_script_process(
                json.dumps(
                    {
                        "columns_info": "\n".join([f"{col['name']}: {col['description']}" for col in columns_info]),
                        "user_input": user_input_stage_6,
                        "read_parquet_path": 'cfg["hdfs"]["data_processed"]',
                        "write_parquet_path": 'cfg["hdfs"]["final_data"]',
                        "list_variables_available": "\n".join([f"{k}: {v}" for k, v in {
                            "cfg": "config object",
                            "st": "streamlit object",
                            "hcache": "cache object",
                            "ss": "session state object",
                            "pd": "pandas object",
                            "json": "json object",
                        }.items()]),
                    }
                )
            )
            
            script_result = convert_block_to_text(script_result, "python")
            hcache.set("final_script", script_result)
            
            progress.progress(1.0)
            status.success("Processing script ready!")

    # Display script information and provide advanced access
    if hcache.exists("final_script"):
        st.subheader("Processing Script Generated")
        
        # Basic explanation of what the script does
        final_script = hcache.get("final_script", default="")
        script_parts = final_script.split("\n") 
        comment_lines = [line.strip("# ") for line in script_parts if line.strip().startswith("#") and len(line) > 2]
        
        if comment_lines:
            st.success("✅ Processing script has been generated successfully!")
            st.write("**What this script will do:**")
            for line in comment_lines[:5]:  # Show just the first few comment lines
                if not line.startswith("import") and len(line) > 10:
                    st.write(f"• {line}")
        else:
            st.success("✅ Processing script has been generated successfully!")
        
        # Advanced options in an expander (hidden by default)
        with st.expander("🔧 Advanced: View & Edit Script", expanded=False):
            st.warning("⚠️ Advanced users only: Edit the script only if you understand Python programming.")
            
            tab1, tab2 = st.tabs(["View Script", "Edit Script"])
            
            with tab1:
                # Show script in read-only mode
                st.code(final_script, language="python")
                
                # Show download option for the script
                st.download_button(
                    label="📥 Download Script",
                    data=final_script.encode('utf-8'),
                    file_name="processing_script.py",
                    mime="text/plain"
                )
            
            with tab2:
                # Advanced editing for technical users
                edited_script = st_ace(
                    value=final_script,
                    language='python',
                    theme='monokai',
                    keybinding='vscode',
                    height=300,
                    auto_update=True
                )
                
                if st.button("💾 Save Script Changes"):
                    hcache.set("final_script", edited_script)
                    st.success("Script changes saved!")
        
        # Run script button
        st.subheader("Run Processing")
        
        col1, col2 = st.columns([3, 1])
        with col1:
            st.write("Click the button to run the processing script on your data.")
        with col2:
            run_button = st.button("Run Processing Script")
        
        if run_button:
            with st.spinner("Processing your data..."):
                try:
                    # Progress tracking
                    progress = st.progress(0)
                    status = st.empty()
                    
                    status.write("Initializing Spark...")
                    progress.progress(0.1)
                    
                    try:
                        # Get Spark session
                        spark_no_access = get_spark_session()
                        progress.progress(0.2)
                        # Read initial data count
                        initial_df = spark_no_access.read.parquet(cfg["hdfs"]["data_processed"])
                        initial_count = initial_df.count()
                        progress.progress(0.3)
                        status.write(f"Processing {initial_count} records...")
                    except:
                        initial_count = 0
                        progress.progress(0.3)
                        status.write("Processing data...")
                    
                    # Execute the script
                    progress.progress(0.4)
                    script_to_run = hcache.get("final_script", default="")
                    
                    # Run the script
                    exec(script_to_run)
                    progress.progress(0.7)
                    
                    # Check results
                    try:
                        # Get Spark session
                        spark_no_access = get_spark_session()
                        progress.progress(0.8)
                        # Read final data count
                        final_df = spark_no_access.read.parquet(cfg["hdfs"]["final_data"])
                        final_count = final_df.count()
                        progress.progress(1.0)
                        status.success(f"Processing completed successfully! {final_count} records produced.")
                        
                        # Display results
                        st.subheader("Processing Results")
                        
                        # Show metrics
                        col1, col2 = st.columns(2)
                        with col1:
                            st.metric("Input Records", initial_count)
                        with col2:
                            st.metric("Output Records", final_count)
                        
                        # Show sample of results
                        st.write("Sample of processed data:")
                        result_preview = final_df.limit(10).toPandas()
                        st.dataframe(result_preview, use_container_width=True)
                        
                        # Download option
                        csv = final_df.limit(1000).toPandas().to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="Download Results (CSV)",
                            data=csv,
                            file_name="final_processed_data.csv",
                            mime="text/csv"
                        )
                        
                    except Exception as e:
                        progress.progress(1.0)
                        status.error("Could not verify results")
                        st.error(f"Processing completed, but could not read results: {str(e)}")
                        
                except Exception as e:
                    progress.progress(1.0)
                    status.error("Processing failed")
                    st.error(f"Error running processing script: {str(e)}")
                    
                    # Show more detailed error explanation
                    with st.expander("Technical Error Details"):
                        st.code(traceback.format_exc())
                    
                    st.info("Try simplifying your processing instructions or contact support.")
    else:
        st.info("Enter your processing instructions and click 'Generate Processing Script' to begin.")
        
    # Add a completion message at the end
    if hcache.exists("final_script") and 'final_df' in locals():
        st.success("🎉 Congratulations! You've completed all steps of the data processing workflow.")
        st.write("You can now download your results or explore them using the Data Query Tool.")