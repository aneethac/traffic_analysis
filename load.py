from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, IntegerType 
from pyspark.sql import functions as F
from pyspark.sql.window import Window 

def create_spark_session(app_name: str) -> SparkSession:
    """Initializes and returns a SparkSession."""
    return SparkSession.builder.appName(app_name).getOrCreate()
 
# 1. ETL: Read Text, Transform, Write Parquet (Scalable Approach) 
def read_and_write_to_parquet(
    spark: SparkSession,
    input_path: str,
    output_path: str,
    key_column: str,
    value_column: str,
    partition_column: str,
    raw_input_schema: StructType,
    num_partitions: int = 200

) -> DataFrame:
    """
    Reads the raw text file, transforms the data into a structured format, 
    and writes it to a partitioned Parquet file for efficient analysis.
    
    Assumes: Input is clean (per problem statement).
    """
  
    # 1. Read the File
    df = (
        spark.read
        .option("delimiter", " ")
        .option("header", "false")
        .schema(raw_input_schema) 
        .csv(input_path)
    )
     
    # Transform and Cast
    structured_df = df.withColumn(
        # Convert string timestamp to proper TimestampType
        key_column,
        F.to_timestamp(F.col(key_column), "yyyy-MM-dd'T'HH:mm:ss")
    ).withColumn(
        # Cast car count string to Integer (for aggregation)
        value_column,
        F.col(value_column).cast(IntegerType())
    ).withColumn(
        # Extract the date part for partitioning and daily grouping
        partition_column,
        F.to_date(F.col(key_column))
    ).filter(
         F.col(key_column).isNotNull() & F.col(value_column).isNotNull()
    )  

    print(f"Writing structured data to Parquet at {output_path} partitioned by {partition_column}") 
    (
    structured_df 
    .repartitionByRange(num_partitions, key_column)
    .write
    .mode("overwrite")
    .partitionBy(partition_column)
    .parquet(output_path)
    )
    print("ETL Write complete.")

    return structured_df

def read_optimized_data(spark: SparkSession, parquet_path: str) -> DataFrame:
    """Reads the optimized Parquet data for analysis."""
    # Reading Parquet is much faster for scalable analysis
    return spark.read.parquet(parquet_path)
