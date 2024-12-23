from pyspark.sql import SparkSession

# Hota
import modules.langflow as lf

# Spark imports
from config import cfg

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

def gen_query(message: str) -> list:
    res = lf.run_flow(
        api_url=cfg['langflow']['api_url'],
        message=message,
        flow_id=cfg['langflow']['flow_id']['gen_query'],
    )
    return res.strip().split('\n')

def convert_col_da(x: str) -> dict:
    x = x.strip().split(":")
    return {
        "name": x[0],
        "description": x[1],
    }
def gen_columns_info(message: str) -> list:
    res = lf.run_flow(
        api_url=cfg['langflow']['api_url'],
        message=message,
        flow_id=cfg['langflow']['flow_id']['gen_columns_name'],
    )
    columns_obj = res.strip().split('\n')
    columns_obj = map(convert_col_da, columns_obj)
    return columns_obj

def gen_python_script_process(message: str) -> list:
    res = lf.run_flow(
        api_url=cfg['langflow']['api_url'],
        message=message,
        flow_id=cfg['langflow']['flow_id']['final_process'],
    )
    return res