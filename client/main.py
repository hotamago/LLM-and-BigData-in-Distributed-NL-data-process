import streamlit as st
import modules.langflow as lf
import yaml
import json
from modules.cache import hcache
from streamlit import session_state as ss
import pandas as pd
import os

# Spark imports
from pyspark.sql import SparkSession
import pyspark.pandas as ps
import models

# Page setup
st.set_page_config(page_title='Project 3', page_icon=':mag:', layout='wide')
st.title('Project 3')

# Load config
@st.cache_data
def load_config(config_path="config.yml"):
    with open(config_path) as f:
        return yaml.load(f, Loader=yaml.FullLoader)

cfg = load_config()

# Helper functions
def gen_query(message: str) -> list:
    res = lf.run_flow(
        api_url=cfg['langflow']['api_url'],
        message=message,
        flow_id=cfg['langflow']['flow_id']['gen_query'],
    )
    return res.strip().split('\n')

def gen_columns_info(message: str) -> list:
    res = lf.run_flow(
        api_url=cfg['langflow']['api_url'],
        message=message,
        flow_id=cfg['langflow']['flow_id']['gen_columns_info'],
    )
    columns_obj = res.strip().split('\n')
    columns_obj = [list(map(lambda x: x.strip(), col.strip().split("\n"))) for col in columns_obj]
    return columns_obj

# Cache
# Define the cached SparkSession function
@st.cache_resource
def get_spark_session():
    """
    Initializes and returns a SparkSession.
    This function is cached to ensure only one SparkSession is active.
    """
    spark_master = cfg.get('spark', {}).get('master', 'spark://localhost:7077')
    spark_app_name = cfg.get('spark', {}).get('app_name', 'spark-collect-data')

    # Initialize SparkSession
    spark = SparkSession.builder \
        .master(spark_master) \
        .appName(spark_app_name) \
        .config("spark.submit.pyFiles", "modules.zip") \
        .getOrCreate()

    return spark

# Init session_state
if "stage_process" not in ss:
    ss.stage_process = 0
if "list_query_engine_search" not in ss:
    ss.list_query_engine_search = []

# Build sidebar
st.sidebar.title('Navigation')
selection = st.sidebar.radio('Go to', ['Home'])

if selection == 'Home':
    # Global variables
    if hcache.exists('list_query_engine_search'):
        ss.list_query_engine_search = hcache.get('list_query_engine_search', default=[])

    # Stage process
    with st.container():
        # Choice stage
        list_stage_process = ['Stage 1', 'Stage 2', 'Stage 3', 'Stage 4', 'Stage 5']
        stage_process = st.selectbox('Select stage', list_stage_process, index=ss.stage_process)

        # Stage 1
        if stage_process == list_stage_process[0]:
            # Get inputs type area
            user_input = st.text_area('Each line is a query document', height=200, value=hcache.get('user_input', default=''))
            # Get input type number
            number_queries_gen = st.number_input('Number queries', min_value=1, max_value=100, value=hcache.get('number_queries_gen', default=10))

            if st.button('Generate queries', key='gen_query'):
                ss.list_query_engine_search = []
                # Update cache
                hcache.set('user_input', user_input)
                hcache.set('number_queries_gen', number_queries_gen)

                list_user_input = user_input.strip().split('\n')

                # Generate queries
                list_user_query = []
                for query in list_user_input:
                    list_user_query.append({
                            "user_input": query,
                            "num_queries": number_queries_gen,
                        })
                    
                with st.spinner('Generating queries...'):
                    process_display = st.empty()
                    for i in range(len(list_user_query)):
                        query = list_user_query[i]
                        process_display.write(f'Generate th: {i+1}/{len(list_user_query)}')
                        with st.spinner(f'Generating queries for: {query["user_input"]}'):
                            ss.list_query_engine_search.extend(gen_query(json.dumps(query)))

            # Show results
            if ss.list_query_engine_search:
                st.write('Queries generated:')
                st.write(ss.list_query_engine_search)

                # Button to confirm the queries
                if st.button('Confirm queries', key='confirm_queries'):
                    # Update cache
                    hcache.set('list_query_engine_search', ss.list_query_engine_search)
                    # update stage
                    ss.stage_process = ss.stage_process + 1
                    st.rerun()

        elif stage_process == list_stage_process[1]: # Stage 2: Using search engine api to search url web
            # import search engine
            import modules.searchEngine.serper as serper
            import modules.managerKey as mk

            # import manager key
            manager_key = mk.ManagerKey(cfg['serper']['api_key'])

            # UI config
            st.write('Stage 2')
            # Config number of url get each query
            number_url_each_query = st.number_input('Number of url get each query', min_value=1, max_value=100, value=10)
            # Button to confirm to get url
            if st.button('Get url', key='get_url'):
                # Create config
                config = serper.Config(
                    batch=True,
                    typeSearch='search',
                    autocomplete=False,
                    query=ss.list_query_engine_search,
                    country='us',
                    location='',
                    locale='en',
                    page=1,
                    num=number_url_each_query,
                )

                # Create engine
                engine = serper.Engine(manager_key)
                res = engine._query(config)

                # Save to cache
                hcache.set('url_search', res)

            # If url_search is not empty, show results
            if hcache.exists('url_search'):
                # Show results as table using pandas
                df = pd.DataFrame(hcache.get('url_search', default=[]))
                st.write(df)

                # Button to confirm the queries
                if st.button('Confirm queries', key='confirm_queries'):
                    # update stage
                    ss.stage_process = ss.stage_process + 1
                    st.rerun()
            
        elif stage_process == list_stage_process[2]: # Stage 3: From url send request to get content by spark then each spark send to hadoop to save
            # Get url search
            url_search = hcache.get('url_search', default=[])

            # UI config
            st.write('Stage 3')
            # Show results as table using pandas
            df = pd.DataFrame(url_search)
            st.write(df)

            # Read Spark and HDFS configs from cfg
            spark_master = cfg.get('spark', {}).get('master', 'spark://localhost:7077')
            spark_app_name = cfg.get('spark', {}).get('app_name', 'spark-collect-data')
            hdfs_output_path = cfg.get('hdfs', {}).get('output_path', 'hdfs://localhost:9000/path/to/save/url_contents.parquet')

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

                from client.modules.spark import fetch_content

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
        elif stage_process == list_stage_process[3]: # User input prompt requirements for data procession, spark worker get data from hadoop and process by LLM using that prompt (for example process then return struct)
            pass
        elif stage_process == list_stage_process[4]: # User input prompt final process, 1 LLM will genarator MapReduce code to process data in hadoop then send to hadoop to process. After that, LLM will get result and return to user and save to .cache
            pass
        else:
            st.write('Stage not exited')