# client/models/__init__.py
# Import from engine.py
from .engine import Base, get_db, engine, SessionLocal # Removed init_db (not defined)
# Import models
from .account import Account, RoleEnum

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def url_content_collect() -> StructType:
    return StructType([
                StructField("url", StringType(), nullable=False),
                StructField("title", StringType(), nullable=True),
                StructField("snippet", StringType(), nullable=True),
                StructField("status_code", IntegerType(), nullable=False),
                StructField("content", StringType(), nullable=True),
                StructField("log", StringType(), nullable=True),
            ])

def llm_process_content() -> StructType:
    return StructType([
                StructField("url", StringType(), nullable=False),
                StructField("title", StringType(), nullable=True),
                StructField("snippet", StringType(), nullable=True),
                StructField("content", StringType(), nullable=True),
                StructField("log", StringType(), nullable=True),
            ])