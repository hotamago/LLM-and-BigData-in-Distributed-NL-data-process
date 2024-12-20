from pyspark.sql.types import StructType, StructField, StringType, IntegerType

url_content_collect = StructType([
                StructField("url", StringType(), nullable=False),
                StructField("title", StringType(), nullable=True),
                StructField("snippet", StringType(), nullable=True),
                StructField("status_code", IntegerType(), nullable=False),
                StructField("content", StringType(), nullable=True),
            ])

llm_process_content = StructType([
                StructField("url", StringType(), nullable=False),
                StructField("title", StringType(), nullable=True),
                StructField("snippet", StringType(), nullable=True),
                StructField("status_code", IntegerType(), nullable=False),
                StructField("content", StringType(), nullable=True),
                StructField("data_processed", StringType(), nullable=True),
            ])