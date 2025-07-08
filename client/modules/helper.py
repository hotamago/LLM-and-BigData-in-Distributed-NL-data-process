from pyspark.sql import SparkSession

# Hota
from modules.langflow import run_flow_fj, run_flow

# Spark imports
from config import cfg, dict_flowjson

# Import manager key
from modules.managerKey import ManagerKey

geminiKeyMan = ManagerKey(cfg["gemini"]["api_key"])


# Helper functions
def get_spark_session():
    """
    Initializes and returns a SparkSession.
    This function is cached to ensure only one SparkSession is active.
    """
    spark_master = cfg.get("spark", {}).get("master", "spark://localhost:7077")
    spark_app_name = cfg.get("spark", {}).get("app_name", "spark-collect-data")

    # Initialize SparkSession with conservative memory configuration for 1GB workers
    spark = (
        SparkSession.builder.master(spark_master)
        .appName(spark_app_name)
        .config("spark.submit.pyFiles", "modules.zip")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "32MB")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.dynamicAllocation.enabled", "false")
        .config("spark.task.maxFailures", "1")
        .config("spark.stage.maxConsecutiveAttempts", "2")
        .config("spark.blacklist.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.files.maxPartitionBytes", "32MB")
        .config("spark.sql.files.openCostInBytes", "4MB")
        .config("spark.executor.memory", "700m")
        .config("spark.executor.memoryFraction", "0.6")
        .config("spark.executor.memoryStorageFraction", "0.3")
        .config("spark.python.worker.memory", "256m")
        .config("spark.worker.cleanup.enabled", "true")
        .config("spark.worker.cleanup.interval", "300")
        .config("spark.worker.cleanup.appDataTtl", "300")
        .config("spark.rpc.message.maxSize", "64")
        .getOrCreate()
    )

    return spark


def get_databases(spark: SparkSession) -> list:
    """
    Retrieves a list of databases from the Spark session.
    """
    return [db.name for db in spark.catalog.listDatabases()]


def get_tables(spark: SparkSession, database: str) -> list:
    """
    Retrieves a list of tables from the specified database.
    """
    spark.catalog.setCurrentDatabase(database)
    return [table.name for table in spark.catalog.listTables()]


def run_query(spark: SparkSession, query: str):
    """
    Executes a Spark SQL query and returns the result as a DataFrame.
    """
    try:
        df = spark.sql(query)
        return df.show(truncate=False)
    except Exception as e:
        return f"Error executing query: {e}"


def gen_query(message: str) -> list:
    temp_cfg = cfg["langflow"]["flow_json"]["gen_query"]
    if cfg["run_api_langflow"]:
        res = run_flow(
            api_url=cfg["langflow"]["api_url"],
            message=message,
            flow_id=cfg["langflow"]["flow_id"]["gen_query"],
            cfg=temp_cfg,
            api_key=geminiKeyMan.getKey(),
        )
    else:
        res = run_flow_fj(
            cfg=temp_cfg,
            dict_flowjson=dict_flowjson[temp_cfg["name"]],
            api_key=geminiKeyMan.getKey(),
            message=message,
        )
    return res.strip().split("\n")


def convert_col_da(x: str) -> dict:
    x = x.strip().split(":")
    return {
        "name": x[0],
        "description": x[1],
    }


def gen_columns_info(message: str) -> list:
    if cfg["run_api_langflow"]:
        res = run_flow(
            api_url=cfg["langflow"]["api_url"],
            message=message,
            flow_id=cfg["langflow"]["flow_id"]["gen_columns_name"],
        )
    else:
        temp_cfg = cfg["langflow"]["flow_json"]["gen_columns_name"]
        res = run_flow_fj(
            cfg=temp_cfg,
            dict_flowjson=dict_flowjson[temp_cfg["name"]],
            api_key=geminiKeyMan.getKey(),
            message=message,
        )
    columns_obj = res.strip().split("\n")
    columns_obj = map(convert_col_da, columns_obj)
    return columns_obj


def gen_python_script_process(message: str) -> list:
    if cfg["run_api_langflow"]:
        res = run_flow(
            api_url=cfg["langflow"]["api_url"],
            message=message,
            flow_id=cfg["langflow"]["flow_id"]["final_process"],
        )
    else:
        temp_cfg = cfg["langflow"]["flow_json"]["final_process"]
        res = run_flow_fj(
            cfg=temp_cfg,
            dict_flowjson=dict_flowjson[temp_cfg["name"]],
            api_key=geminiKeyMan.getKey(),
            message=message,
        )
    return res
