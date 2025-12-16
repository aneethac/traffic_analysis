import os
import sys  
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession, DataFrame
# --- Configuration ---
APP_NAME = "SeekTrafficAnalysis"
input_file_path = "/Volumes/uc_0004_dev_prd_catalog/caife_utils/dropzone/traffic_data.txt"
parquet_output_path = "/Volumes/uc_0004_dev_prd_catalog/caife_utils/dropzone/traffic_table"
key_column="record_time"
value_column="car_count"
partition_column="record_date"
# Define the schema for reading the space-delimited text file
raw_input_schema = StructType([
    StructField(key_column, StringType(), False),
    StructField(value_column, StringType(), False)
])
num_partitions = 200
def run_analysis():
    """Main function to run the ETL and Analysis workflow."""
    
    # 1. ETL: Read Text, Transform, and Write to Optimized Parquet
    print("--- Starting ETL Phase: Read, Transform, Write Parquet ---")
    read_and_write_to_parquet(
        spark,
        input_path=input_file_path, 
        output_path=parquet_output_path,
        key_column=key_column, 
        value_column=value_column, 
        partition_column=partition_column,
        raw_input_schema=raw_input_schema,
        num_partitions=num_partitions
    )

    # 2. Analysis: Read from the Optimized Parquet Data
    df = read_optimized_data(spark, parquet_output_path)
    
    # 3. Execute all required calculations
    
    total = get_total(df, value_column="car_count")    
    print(f"Total number of cars crossed {total}")

    # Daily totals
    print(f"Total number of cars crossed by day ")
    daily_df = get_daily_totals(df, value_column="car_count", key_column="record_time", partition_column="record_date")
    daily_df.show(truncate=False)

    # Top 3 periods
    print(f"Top 3 1/2 periods")
    top_df = get_top_n(df, value_column="car_count", n=3)
    top_df.show(truncate=False)

    # Least consecutive period (3 rows)
    print(f"Least consecutive period")
    least_df = get_least_consecutive(df, value_column="car_count", key_column="record_time", window_size=3)
    least_df.show(truncate=False)

    
    # return optimized_df

if __name__ == "__main__":
    optimized_df = run_analysis()
